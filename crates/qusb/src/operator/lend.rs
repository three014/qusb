use std::{io, pin::pin, time::Duration};

use super::{Seq, compress_frame_len};
use crate::utils::{CloseStream, Interval, blocker, mpsc};
use bytes::{Bytes, BytesMut};
use compio_io::{AsyncRead, AsyncWrite};
use futures_concurrency::{future::Race, stream::Merge};
use futures_lite::{Stream, StreamExt, stream};
use futures_util::SinkExt;
use futures_util::stream::FuturesUnordered;
use proto::{
    data::{Data, Ring},
    msg::{Command, Header, UrbFrame, UsbDeviceId},
};
use rusb_async::UsbMemMut;
use tokio_util::sync::CancellationToken;
use tracing::{trace, Instrument, Span};
use vhci::{
    ioctl::{self, Endpoint, IocSetupPacket, UrbType},
    usbfs::Request,
};
use zerocopy::transmute;

pub(super) mod blocking;
pub mod device;

const TICK: Duration = Duration::from_micros(469);

pub enum CtrlReq {
    SetInterface(SetInterface),
    SetConfig(SetConfig),
    ClearStall(ClearStall),
}

#[derive(Debug)]
pub enum Error {
    /// For unrecoverable transfers that
    /// need to convey the result to the
    /// borrower. Will attempt to send
    /// an error header to the borrower.
    Usb(UsbError),
    /// For communication problems with
    /// the borrower. Will not send an
    /// error header to the borrower.
    Io(io::Error),
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum FrameKind {
    #[error("device failed to reset")]
    Reset,
    #[error("{kind:?} transfer failed on {endpoint:?}: {status:?}")]
    Transfer {
        kind: UrbType,
        endpoint: Endpoint,
        status: vhci::Status,
    },
}

impl FrameKind {
    pub const fn as_command(&self) -> Command {
        match self {
            FrameKind::Reset => Command::RetPort,
            FrameKind::Transfer { .. } => Command::RetSubmit,
        }
    }

    pub const fn status(&self) -> Option<vhci::Status> {
        match self {
            FrameKind::Reset => None,
            FrameKind::Transfer { status, .. } => Some(*status),
        }
    }

    pub const fn kind(&self) -> Option<UrbType> {
        match self {
            FrameKind::Reset => None,
            FrameKind::Transfer { kind, .. } => Some(*kind),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UsbError {
    pub id: UsbDeviceId,
    pub seqnum: u32,
    pub status: proto::msg::Status,
    pub kind: FrameKind,
}

impl std::fmt::Display for UsbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {}): {}", self.id, self.seqnum, self.kind)
    }
}

impl std::error::Error for UsbError {}

impl UsbError {
    pub const fn as_header(&self) -> Header {
        Header {
            total_frame_len: compress_frame_len(size_of::<Header>()),
            command: self.kind.as_command(),
            status: self.status,
            seqnum: self.seqnum,
        }
    }
}

impl From<UsbError> for io::Error {
    fn from(err: UsbError) -> Self {
        if let Some(status) = err.kind.status() {
            let errno = status.to_errno_raw(UrbType::Iso == err.kind.kind().unwrap());
            let error_kind = io::Error::from_raw_os_error(-errno).kind();
            io::Error::new(error_kind, err)
        } else {
            io::Error::other(err)
        }
    }
}

pub trait SendHandler {
    fn is_buf_empty(&self) -> bool;
    fn flush_buf(&mut self) -> Bytes;
    fn iso_completed(&mut self, iso: Seq<Iso>) -> Result<(), Error>;
    fn int_completed(&mut self, int: Seq<Int>) -> Result<(), Error>;
    fn ctrl_completed(&mut self, ctrl: Seq<Ctrl>) -> Result<(), Error>;
    fn bulk_completed(&mut self, bulk: Seq<Bulk>) -> Result<(), Error>;
    fn device_resetted(&mut self, reset: Seq<proto::msg::Status>) -> Result<(), Error>;
}

trait SendHandlerExt {
    fn handle_event(&mut self, event: LendEvent) -> Result<(), Error>;
}

impl<T: SendHandler> SendHandlerExt for T {
    fn handle_event(&mut self, event: LendEvent) -> Result<(), Error> {
        match event {
            LendEvent::Reset(seq) => self.device_resetted(seq),
            LendEvent::Ctrl(seq) => self.ctrl_completed(seq),
            LendEvent::Int(seq) => self.int_completed(seq),
            LendEvent::Iso(seq) => self.iso_completed(seq),
            LendEvent::Bulk(seq) => self.bulk_completed(seq),
        }
    }
}

pub trait RecvHandler {
    fn cancel_urb(&mut self, seqnum: u32);
    fn device_reset(
        &mut self,
        seqnum: u32,
    ) -> impl Future<Output = Seq<proto::msg::Status>> + 'static;
    fn set_config(&mut self, data: Seq<SetConfig>) -> impl Future<Output = Seq<Ctrl>> + 'static;
    fn set_interface(
        &mut self,
        data: Seq<SetInterface>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static;
    fn clear_stall(&mut self, data: Seq<ClearStall>) -> impl Future<Output = Seq<Ctrl>> + 'static;
    fn new_ctrl(&mut self, frame: Seq<Data<UrbFrame>>)
    -> impl Future<Output = Seq<Ctrl>> + 'static;
    fn new_int(&mut self, frame: Seq<Data<UrbFrame>>) -> impl Future<Output = Seq<Int>> + 'static;
    fn new_iso(&mut self, frame: Seq<Data<UrbFrame>>) -> impl Future<Output = Seq<Iso>> + 'static;
    fn new_bulk(&mut self, frame: Seq<Data<UrbFrame>>)
    -> impl Future<Output = Seq<Bulk>> + 'static;
}

#[derive(Debug, Clone, Copy)]
pub struct SetConfig {
    pub desired: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct SetInterface {
    pub setting: u8,
    pub interface: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct ClearStall {
    pub endpoint: u8,
}

pub struct SendLoop<W> {
    tx: W,
    resets: mpsc::AsyncReceiver<Seq<proto::msg::Status>>,
    ctrls: mpsc::AsyncReceiver<Seq<Ctrl>>,
    ints: mpsc::AsyncReceiver<Seq<Int>>,
    isos: mpsc::AsyncReceiver<Seq<Iso>>,
    bulks: mpsc::AsyncReceiver<Seq<Bulk>>,
}

enum LendEvent {
    Reset(Seq<proto::msg::Status>),
    Ctrl(Seq<Ctrl>),
    Int(Seq<Int>),
    Iso(Seq<Iso>),
    Bulk(Seq<Bulk>),
}

impl<W> SendLoop<W> {
    #[inline]
    async fn do_loop<H>(
        tx: &mut W,
        event_rx: impl Stream<Item = LendEvent> + Unpin,
        mut handler: H,
    ) -> Result<(), Option<Error>>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
        H: SendHandler,
    {
        use compio_io::AsyncWriteExt;
        enum State {
            Solicit,
            Timer,
            Flush,
        }

        enum Event<W> {
            Lend(LendEvent),
            FlushBuf,
            FlushComplete(io::Result<W>),
        }

        // State Transitions
        // Solicit -> [Timer(LendEvent)]
        // Timer -> [Timer(LendEvent), Flush(FlushBuf)]
        // Flush -> [Solicit(FlushComplete), Timer(FlushComplete), Flush(LendEvent)]

        #[inline]
        async fn arm_timer<W>(interval: &Interval) -> Option<Event<W>> {
            interval.tick().await;
            Some(Event::FlushBuf)
        }

        let _enter = Span::current();
        let interval = Interval::new(TICK);
        let mut tx_holder = Some(tx);
        let mut sleeper = pin!(blocker(None));
        let mut flush_op = pin!(blocker(None));
        let mut event_rx = event_rx.map(Event::Lend);

        let mut state = State::Solicit;
        loop {
            let event = {
                let race = (event_rx.next(), sleeper.as_mut(), flush_op.as_mut()).race();
                race.await.ok_or(None)?
            };
            state = match event {
                Event::Lend(event) => {
                    handler.handle_event(event)?;
                    match state {
                        State::Solicit => {
                            sleeper.set(blocker(Some(arm_timer(&interval))));
                            State::Timer
                        }
                        current => current,
                    }
                }
                Event::FlushBuf => {
                    sleeper.set(blocker(None));
                    let tx = tx_holder.take().unwrap();
                    let bytes = handler.flush_buf();
                    flush_op.set(blocker(Some(async move {
                        match tx.write_all(bytes).await.0 {
                            Ok(_) => Some(Event::FlushComplete(Ok(tx))),
                            Err(err) => Some(Event::FlushComplete(Err(err))),
                        }
                    })));
                    State::Flush
                }
                Event::FlushComplete(Ok(tx)) => {
                    flush_op.set(blocker(None));
                    tx_holder = Some(tx);
                    if handler.is_buf_empty() {
                        State::Solicit
                    } else {
                        sleeper.set(blocker(Some(arm_timer(&interval))));
                        State::Timer
                    }
                }
                Event::FlushComplete(Err(err)) => return Err(Some(Error::Io(err))),
            }
        }
    }

    pub async fn run(self, handler: impl SendHandler, cancel: CancellationToken) -> io::Result<()>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
    {
        let Self {
            mut tx,
            resets,
            ctrls,
            ints,
            isos,
            bulks,
        } = self;
        let reset = resets.map(LendEvent::Reset);
        let ctrls = ctrls.map(LendEvent::Ctrl);
        let ints = ints.map(LendEvent::Int);
        let isos = isos.map(LendEvent::Iso);
        let bulks = bulks.map(LendEvent::Bulk);
        let event_rx = (isos, ints, ctrls, bulks, reset).merge();
        let event_rx = pin!(stream::stop_after_future(event_rx, cancel.cancelled()));

        let result = match Self::do_loop(&mut tx, event_rx, handler)
            .in_current_span()
            .await
        {
            Ok(_) | Err(None) => Ok(()),
            Err(Some(Error::Usb(err))) => {
                use compio_io::AsyncWriteExt;
                let header = err.as_header();
                _ = tx.write_u64_le(transmute!(header)).await;
                cancel.cancel();
                Err(err.into())
            }
            Err(Some(Error::Io(err))) => Err(err),
        };

        _ = tx.close();
        _ = tx.stopped().await;
        result
    }
}

pub struct RecvLoop<R> {
    rx: R,
    buf: Ring,
    resets: mpsc::AsyncSender<Seq<proto::msg::Status>>,
    ctrls: mpsc::AsyncSender<Seq<Ctrl>>,
    ints: mpsc::AsyncSender<Seq<Int>>,
    isos: mpsc::AsyncSender<Seq<Iso>>,
    bulks: mpsc::AsyncSender<Seq<Bulk>>,
}

impl<R> RecvLoop<R> {
    async fn do_loop<H>(
        frame_rx: impl Stream<Item = io::Result<super::Recv>> + Unpin,
        mut resets: mpsc::AsyncSender<Seq<proto::msg::Status>>,
        mut ctrls: mpsc::AsyncSender<Seq<Ctrl>>,
        mut ints: mpsc::AsyncSender<Seq<Int>>,
        mut isos: mpsc::AsyncSender<Seq<Iso>>,
        mut bulks: mpsc::AsyncSender<Seq<Bulk>>,
        mut handler: H,
    ) -> Result<(), Option<io::Error>>
    where
        H: RecvHandler,
    {
        enum Event {
            Frame(super::Recv),
            CompletedBlocking(Seq<proto::msg::Status>),
            CompletedCtrl(Seq<Ctrl>),
            CompletedInt(Seq<Int>),
            CompletedIso(Seq<Iso>),
            CompletedBulk(Seq<Bulk>),
        }

        let _enter = Span::current();
        let mut reset_inprogress = FuturesUnordered::new();
        let mut ctrl_inprogress = FuturesUnordered::new();
        let mut int_inprogress = FuturesUnordered::new();
        let mut iso_inprogress = FuturesUnordered::new();
        let mut bulk_inprogress = FuturesUnordered::new();
        let mut frame_rx = frame_rx.map(|result| result.map(Event::Frame));

        // Block them from returning ready immediately
        reset_inprogress.push(blocker(None));
        ctrl_inprogress.push(blocker(None));
        int_inprogress.push(blocker(None));
        iso_inprogress.push(blocker(None));
        bulk_inprogress.push(blocker(None));

        loop {
            let frame_next = async { frame_rx.next().await };
            let blocking_next = async {
                let next: Seq<proto::msg::Status> = reset_inprogress.next().await?;
                Some(Ok::<_, io::Error>(Event::CompletedBlocking(next)))
            };
            let ctrl_next = async {
                let next: Seq<Ctrl> = ctrl_inprogress.next().await?;
                Some(Ok::<_, io::Error>(Event::CompletedCtrl(next)))
            };
            let int_next = async {
                let next: Seq<Int> = int_inprogress.next().await?;
                Some(Ok::<_, io::Error>(Event::CompletedInt(next)))
            };
            let iso_next = async {
                let next: Seq<Iso> = iso_inprogress.next().await?;
                Some(Ok::<_, io::Error>(Event::CompletedIso(next)))
            };
            let bulk_next = async {
                let next: Seq<Bulk> = bulk_inprogress.next().await?;
                Some(Ok::<_, io::Error>(Event::CompletedBulk(next)))
            };

            let events = (
                frame_next,
                iso_next,
                int_next,
                ctrl_next,
                blocking_next,
                bulk_next,
            );
            let event = events.race().await.ok_or(None)??;
            match event {
                Event::Frame(super::Recv::Urb((Header { seqnum, .. }, Some(urb_frame)))) => {
                    let kind = urb_frame.get().header.kind;
                    match kind {
                        UrbType::Iso => {
                            let fut = handler.new_iso(Seq {
                                seqnum,
                                data: urb_frame,
                            });
                            iso_inprogress.push(blocker(Some(fut)));
                        }
                        UrbType::Int => {
                            let fut = handler.new_int(Seq {
                                seqnum,
                                data: urb_frame,
                            });
                            int_inprogress.push(blocker(Some(fut)));
                        }
                        // EXPLANATION: We have four different operations that
                        // resolve to the same type of result. Therefore, we have
                        // four different future types that cannot exist together
                        // in an UnorderedFuture list. Because they all resolve
                        // to the same result, we can pack them into a Race future
                        // with dummy futures that will never return Ready.
                        // Boom, combining 4 futures into one.
                        //
                        // Is it gonna be a little bit slower? Probably? Idk, but
                        // I'm willing to accept having slightly slower control requests
                        // in exchange for other places having more understandable code.
                        UrbType::Ctrl => {
                            let ctrl_pkt = urb_frame.get().header.ctrl_packet;
                            trace! { %ctrl_pkt };
                            match CtrlKind::parse(ctrl_pkt) {
                                CtrlKind::Blocking(CtrlReq::SetInterface(req)) => {
                                    let fut = handler
                                        .set_interface(Seq { seqnum, data: req })
                                        .in_current_span();
                                    let hehe = (
                                        blocker(None),
                                        blocker(Some(fut)),
                                        blocker(None),
                                        blocker(None),
                                    )
                                        .race();
                                    ctrl_inprogress.push(blocker(Some(hehe)));
                                }
                                CtrlKind::Blocking(CtrlReq::SetConfig(req)) => {
                                    let fut = handler
                                        .set_config(Seq { seqnum, data: req })
                                        .in_current_span();
                                    let hehe = (
                                        blocker(None),
                                        blocker(None),
                                        blocker(Some(fut)),
                                        blocker(None),
                                    )
                                        .race();
                                    ctrl_inprogress.push(blocker(Some(hehe)));
                                }
                                CtrlKind::Blocking(CtrlReq::ClearStall(req)) => {
                                    let fut = handler
                                        .clear_stall(Seq { seqnum, data: req })
                                        .in_current_span();
                                    let hehe = (
                                        blocker(None),
                                        blocker(None),
                                        blocker(None),
                                        blocker(Some(fut)),
                                    )
                                        .race();
                                    ctrl_inprogress.push(blocker(Some(hehe)));
                                }
                                CtrlKind::Async => {
                                    let fut = handler.new_ctrl(Seq {
                                        seqnum,
                                        data: urb_frame,
                                    });
                                    let hehe = (
                                        blocker(Some(fut)),
                                        blocker(None),
                                        blocker(None),
                                        blocker(None),
                                    )
                                        .race();
                                    ctrl_inprogress.push(blocker(Some(hehe)));
                                }
                            }
                        }
                        UrbType::Bulk => {
                            let fut = handler.new_bulk(Seq {
                                seqnum,
                                data: urb_frame,
                            });
                            bulk_inprogress.push(blocker(Some(fut)));
                        }
                    }
                }
                Event::Frame(super::Recv::Urb(_)) => unreachable!(),
                Event::Frame(super::Recv::PortReset(Header { seqnum, .. })) => {
                    let fut = handler.device_reset(seqnum);
                    reset_inprogress.push(blocker(Some(fut)));
                }
                Event::Frame(super::Recv::Unlink(Header { seqnum, .. })) => {
                    handler.cancel_urb(seqnum);
                }
                Event::CompletedBlocking(seq) => {
                    _ = resets.send(seq).await;
                }
                Event::CompletedCtrl(seq) => {
                    _ = ctrls.send(seq).await;
                }
                Event::CompletedInt(seq) => {
                    _ = ints.send(seq).await;
                }
                Event::CompletedIso(seq) => {
                    _ = isos.send(seq).await;
                }
                Event::CompletedBulk(seq) => {
                    _ = bulks.send(seq).await;
                }
            }
        }
    }

    pub async fn run(self, handler: impl RecvHandler, cancel: CancellationToken) -> io::Result<()>
    where
        R: AsyncRead + Unpin + 'static,
    {
        let Self {
            mut rx,
            buf,
            resets: reset,
            ctrls,
            ints,
            isos,
            bulks,
        } = self;
        let result = {
            let frame_rx = stream::unfold((&mut rx, buf), |(mut rx, mut buf)| async {
                let result = super::recv_frame(&mut rx, &mut buf).await.transpose()?;
                Some((result, (rx, buf)))
            });
            let frame_rx = pin!(stream::stop_after_future(
                frame_rx,
                cancel.cancelled_owned()
            ));
            match Self::do_loop(frame_rx, reset, ctrls, ints, isos, bulks, handler)
                .in_current_span()
                .await
            {
                Ok(_) | Err(None) => Ok(()),
                Err(Some(err)) => Err(err),
            }
        };

        // Done! Drain the streams
        let mut buf = Ring::with_capacity(32);
        while buf
            .fill_with_reader(&mut rx)
            .await
            .is_ok_and(|bytes_read| 0 != bytes_read)
        {}
        result
    }
}

pub fn loops<W, R>(tx: W, rx: R, buf: Ring) -> (SendLoop<W>, RecvLoop<R>) {
    let (reset_tx, reset_rx) = mpsc::channel(0);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(0);
    let (int_tx, int_rx) = mpsc::channel(0);
    let (bulk_tx, bulk_rx) = mpsc::channel(8);
    let (iso_tx, iso_rx) = mpsc::channel(8);
    let send_loop = SendLoop {
        tx,
        resets: reset_rx.into_stream(),
        ctrls: ctrl_rx.into_stream(),
        ints: int_rx.into_stream(),
        isos: iso_rx.into_stream(),
        bulks: bulk_rx.into_stream(),
    };
    let recv_loop = RecvLoop {
        rx,
        buf,
        resets: reset_tx.into_sink(),
        ctrls: ctrl_tx.into_sink(),
        ints: int_tx.into_sink(),
        isos: iso_tx.into_sink(),
        bulks: bulk_tx.into_sink(),
    };
    (send_loop, recv_loop)
}

pub enum CtrlKind {
    Blocking(CtrlReq),
    Async,
}

impl CtrlKind {
    #[inline]
    pub const fn parse(setup_pkt: IocSetupPacket) -> Self {
        match setup_pkt.req() {
            Request::STANDARD_INTERFACE_SET_INTERFACE => {
                CtrlKind::Blocking(CtrlReq::SetInterface(SetInterface {
                    setting: setup_pkt.value() as u8,
                    interface: setup_pkt.index() as u8,
                }))
            }
            Request::STANDARD_DEVICE_SET_CONFIGURATION => {
                CtrlKind::Blocking(CtrlReq::SetConfig(SetConfig {
                    desired: setup_pkt.value() as u8,
                }))
            }
            Request::STANDARD_ENDPOINT_CLEAR_FEATURE => {
                CtrlKind::Blocking(CtrlReq::ClearStall(ClearStall {
                    endpoint: setup_pkt.index() as u8,
                }))
            }
            _ => CtrlKind::Async,
        }
    }
}

#[derive(Debug)]
pub enum ResultData {
    In(UsbMemMut),
    Out { bytes_transferred: usize },
}

impl ResultData {
    pub fn new(buf: BytesMut, dir: vhci::usbfs::Dir) -> Self {
        match dir {
            vhci::usbfs::Dir::Out => Self::Out {
                bytes_transferred: buf.len(),
            },
            vhci::usbfs::Dir::In => Self::In(buf),
        }
    }

    pub fn actual_transfer_len(&self) -> usize {
        match self {
            ResultData::In(buf) => buf.len(),
            ResultData::Out { bytes_transferred } => *bytes_transferred,
        }
    }

    pub fn get(&self) -> &[u8] {
        match self {
            ResultData::In(buf) => buf,
            ResultData::Out {
                bytes_transferred: _,
            } => &[],
        }
    }

    pub fn as_header_data(&self) -> HeaderData<'_> {
        let actual_transfer_len = self.actual_transfer_len() as u16;
        let transfer = self.get();
        let padding = super::padding(transfer.len() as u16);
        HeaderData {
            actual_transfer_len,
            transfer,
            padding,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeaderData<'a> {
    /// The length that goes into the UrbHeader
    pub actual_transfer_len: u16,
    /// The buffer we write into our buffer, which might
    /// be empty if the transfer was an outgoing one
    pub transfer: &'a [u8],
    /// The extra padding for the buffer above, so that
    /// we're aligned to 8 bytes (Can be zero length as well)
    pub padding: &'static [u8],
}

#[derive(Debug)]
pub struct Iso {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub interval: u16,
    pub raw_iso_buf: BytesMut,
    pub num_errors: u16,
    pub num_iso_packets: u16,
    pub status: vhci::Status,
}

#[derive(Debug)]
pub struct Int {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub interval: u16,
    pub status: vhci::Status,
}

#[derive(Debug)]
pub struct Ctrl {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub status: vhci::Status,
}

#[derive(Debug)]
pub struct Bulk {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub status: vhci::Status,
}
