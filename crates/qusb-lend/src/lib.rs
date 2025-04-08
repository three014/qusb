use std::{io, ops::DerefMut, pin::pin, time::Duration};

use bytes::{Bytes, BytesMut};
use compio_io::{AsyncRead, AsyncWrite};
use error::{Error, UsbError};
use futures_concurrency::future::Race;
use futures_concurrency::stream::Merge;
use futures_lite::stream::StreamExt;
use futures_lite::{Stream, stream};
use futures_util::stream::FuturesUnordered;
use proto::data::Ring;
use proto::unpacked::Frame;
use proto::{AbortableError, RecvError};
use proto::{
    TransferError,
    data::Data,
    msg::{Dir, Endpoint, IocSetupPacket, UrbFrame, padding, usbfs::Request},
    unpacked::Seq,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, trace};
use utils::{CloseStream, mpsc, time::Interval};
use utils::{MaybeFut, ResettableFuture};

const TICK: Duration = Duration::from_micros(469);

mod ctrl;
pub mod error;

enum CompletedTransfer {
    Reset(Seq<Result<(), AbortableError>>),
    Ctrl(Seq<Ctrl>),
    Int(Seq<Int>),
    Iso(Seq<Iso>),
    Bulk(Seq<Bulk>),
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
pub enum ResultData {
    In(BytesMut),
    Out { bytes_transferred: usize },
}

impl ResultData {
    pub fn new(buf: BytesMut, dir: Dir) -> Self {
        match dir {
            Dir::Out => Self::Out {
                bytes_transferred: buf.len(),
            },
            Dir::In => Self::In(buf),
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
        let padding = padding(transfer.len() as u16);
        HeaderData {
            actual_transfer_len,
            transfer,
            padding,
        }
    }
}

#[derive(Debug)]
pub struct Iso {
    pub res: ResultData,
    pub endpoint: Endpoint,
    pub interval: u16,
    pub raw_iso_buf: BytesMut,
    pub num_errors: u16,
    pub num_iso_packets: u16,
    pub status: Result<(), TransferError>,
}

#[derive(Debug)]
pub struct Int {
    pub res: ResultData,
    pub endpoint: Endpoint,
    pub interval: u16,
    pub status: Result<(), TransferError>,
}

#[derive(Debug)]
pub struct Ctrl {
    pub res: ResultData,
    pub endpoint: Endpoint,
    pub status: Result<(), TransferError>,
}

#[derive(Debug)]
pub struct Bulk {
    pub res: ResultData,
    pub endpoint: Endpoint,
    pub status: Result<(), TransferError>,
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

pub enum CtrlReq {
    SetInterface(SetInterface),
    SetConfig(SetConfig),
    ClearStall(ClearStall),
}

pub enum CtrlKind {
    Blocking(CtrlReq),
    Async,
}

impl CtrlKind {
    #[inline]
    pub const fn parse(setup_pkt: &IocSetupPacket) -> Self {
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

pub trait SendHandler {
    fn is_buf_empty(&self) -> bool;
    fn flush_buf(&mut self) -> Bytes;
    fn iso_completed(&mut self, iso: Seq<Iso>) -> Result<(), UsbError>;
    fn int_completed(&mut self, int: Seq<Int>) -> Result<(), UsbError>;
    fn ctrl_completed(&mut self, ctrl: Seq<Ctrl>) -> Result<(), UsbError>;
    fn bulk_completed(&mut self, bulk: Seq<Bulk>) -> Result<(), UsbError>;
    fn device_resetted(&mut self, reset: Seq<Result<(), AbortableError>>) -> Result<(), UsbError>;
}

impl<T, U> SendHandler for T
where
    T: DerefMut<Target = U>,
    U: SendHandler + 'static,
{
    fn is_buf_empty(&self) -> bool {
        self.deref().is_buf_empty()
    }

    fn flush_buf(&mut self) -> Bytes {
        self.deref_mut().flush_buf()
    }

    fn iso_completed(&mut self, iso: Seq<Iso>) -> Result<(), UsbError> {
        self.deref_mut().iso_completed(iso)
    }

    fn int_completed(&mut self, int: Seq<Int>) -> Result<(), UsbError> {
        self.deref_mut().int_completed(int)
    }

    fn ctrl_completed(&mut self, ctrl: Seq<Ctrl>) -> Result<(), UsbError> {
        self.deref_mut().ctrl_completed(ctrl)
    }

    fn bulk_completed(&mut self, bulk: Seq<Bulk>) -> Result<(), UsbError> {
        self.deref_mut().bulk_completed(bulk)
    }

    fn device_resetted(&mut self, reset: Seq<Result<(), AbortableError>>) -> Result<(), UsbError> {
        self.deref_mut().device_resetted(reset)
    }
}

trait SendHandlerExt {
    fn handle_event(&mut self, event: CompletedTransfer) -> Result<(), UsbError>;
}

impl<T: SendHandler> SendHandlerExt for T {
    fn handle_event(&mut self, event: CompletedTransfer) -> Result<(), UsbError> {
        match event {
            CompletedTransfer::Reset(seq) => self.device_resetted(seq),
            CompletedTransfer::Ctrl(seq) => self.ctrl_completed(seq),
            CompletedTransfer::Int(seq) => self.int_completed(seq),
            CompletedTransfer::Iso(seq) => self.iso_completed(seq),
            CompletedTransfer::Bulk(seq) => self.bulk_completed(seq),
        }
    }
}

pub trait RecvHandler {
    fn cancel_urb(&mut self, seqnum: u32);
    fn device_reset(
        &mut self,
        seqnum: u32,
    ) -> impl Future<Output = Seq<Result<(), AbortableError>>> + 'static;
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

impl<T, U> RecvHandler for T
where
    T: DerefMut<Target = U>,
    U: RecvHandler + 'static,
{
    fn cancel_urb(&mut self, seqnum: u32) {
        self.deref_mut().cancel_urb(seqnum);
    }

    fn device_reset(
        &mut self,
        seqnum: u32,
    ) -> impl Future<Output = Seq<Result<(), AbortableError>>> + 'static {
        self.deref_mut().device_reset(seqnum)
    }

    fn set_config(&mut self, data: Seq<SetConfig>) -> impl Future<Output = Seq<Ctrl>> + 'static {
        self.deref_mut().set_config(data)
    }

    fn set_interface(
        &mut self,
        data: Seq<SetInterface>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        self.deref_mut().set_interface(data)
    }

    fn clear_stall(&mut self, data: Seq<ClearStall>) -> impl Future<Output = Seq<Ctrl>> + 'static {
        self.deref_mut().clear_stall(data)
    }

    fn new_ctrl(
        &mut self,
        frame: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Ctrl>> + 'static {
        self.deref_mut().new_ctrl(frame)
    }

    fn new_int(&mut self, frame: Seq<Data<UrbFrame>>) -> impl Future<Output = Seq<Int>> + 'static {
        self.deref_mut().new_int(frame)
    }

    fn new_iso(&mut self, frame: Seq<Data<UrbFrame>>) -> impl Future<Output = Seq<Iso>> + 'static {
        self.deref_mut().new_iso(frame)
    }

    fn new_bulk(
        &mut self,
        frame: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = Seq<Bulk>> + 'static {
        self.deref_mut().new_bulk(frame)
    }
}

pub struct SendLoop<W> {
    tx: W,
    reset_rx: mpsc::AsyncReceiver<Seq<Result<(), AbortableError>>>,
    ctrl_rx: mpsc::AsyncReceiver<Seq<Ctrl>>,
    int_rx: mpsc::AsyncReceiver<Seq<Int>>,
    iso_rx: mpsc::AsyncReceiver<Seq<Iso>>,
    bulk_rx: mpsc::AsyncReceiver<Seq<Bulk>>,
}

impl<W> SendLoop<W> {
    #[inline]
    async fn do_loop(
        tx: &mut W,
        event_rx: impl Stream<Item = CompletedTransfer> + Unpin,
        mut handler: impl SendHandler,
    ) -> Result<(), Option<Error>>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
    {
        use compio_io::AsyncWriteExt;
        enum State {
            Solicit,
            Timer,
            Flush,
        }

        enum Event<W> {
            Lend(CompletedTransfer),
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

        let _enter = Span::current().entered();
        let interval = Interval::new(TICK);
        let mut tx_holder = Some(tx);
        let mut sleeper = pin!(MaybeFut::None);
        let mut flusher = pin!(MaybeFut::None);
        let mut event_rx = event_rx.map(Event::Lend);

        let mut state = State::Solicit;
        loop {
            let event = match state {
                State::Solicit => event_rx.next().await,
                State::Timer => (event_rx.next(), sleeper.as_mut()).race().await,
                State::Flush => (event_rx.next(), flusher.as_mut()).race().await,
            };
            state = match event.ok_or(None)? {
                Event::Lend(event) => {
                    handler.handle_event(event).map_err(Error::from)?;
                    match state {
                        State::Solicit => {
                            sleeper.reset(arm_timer(&interval));
                            State::Timer
                        }
                        current => current,
                    }
                }
                Event::FlushBuf => {
                    sleeper.clear();
                    let tx = tx_holder.take().unwrap();
                    let bytes = handler.flush_buf();
                    flusher.reset(async move {
                        match tx.write_all(bytes).await.0 {
                            Ok(_) => Some(Event::FlushComplete(Ok(tx))),
                            Err(err) => Some(Event::FlushComplete(Err(err))),
                        }
                    });
                    State::Flush
                }
                Event::FlushComplete(Ok(tx)) => {
                    flusher.clear();
                    tx_holder = Some(tx);
                    if handler.is_buf_empty() {
                        State::Solicit
                    } else {
                        sleeper.reset(arm_timer(&interval));
                        State::Timer
                    }
                }
                Event::FlushComplete(Err(err)) => return Err(Some(Error::Send(err))),
            }
        }
    }

    pub async fn run<H>(self, handler: H, cancel: CancellationToken) -> Result<(), Error>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
        H: SendHandler,
    {
        let reset = self.reset_rx.map(CompletedTransfer::Reset);
        let ctrls = self.ctrl_rx.map(CompletedTransfer::Ctrl);
        let ints = self.int_rx.map(CompletedTransfer::Int);
        let isos = self.iso_rx.map(CompletedTransfer::Iso);
        let bulks = self.bulk_rx.map(CompletedTransfer::Bulk);
        let event_rx = (isos, ints, ctrls, bulks, reset).merge();
        let event_rx = Box::pin(stream::stop_after_future(
            event_rx,
            cancel.cancelled_owned(),
        ));
        let mut tx = self.tx;

        let looper = Self::do_loop(&mut tx, event_rx, handler);
        let result = match Box::pin(looper.in_current_span()).await {
            Ok(()) | Err(None) => Ok(()),
            Err(Some(Error::Usb(err))) => {
                use compio_io::AsyncWriteExt;
                let header = err.as_header();
                _ = tx.write_u64_le(header.as_u64_le()).await;
                Err(Error::Usb(err))
            }
            Err(Some(err)) => Err(err),
        };

        _ = tx.close();
        _ = tx.stopped().await;
        result
    }
}

pub struct RecvLoop<R> {
    rx: R,
    buf: Ring,
    reset_tx: mpsc::AsyncSender<Seq<Result<(), AbortableError>>>,
    ctrl_tx: mpsc::AsyncSender<Seq<Ctrl>>,
    int_tx: mpsc::AsyncSender<Seq<Int>>,
    iso_tx: mpsc::AsyncSender<Seq<Iso>>,
    bulk_tx: mpsc::AsyncSender<Seq<Bulk>>,
}

impl<R> RecvLoop<R> {
    async fn do_loop(
        frame_rx: impl Stream<Item = Result<Frame, RecvError>> + Unpin,
        resets: mpsc::AsyncSender<Seq<Result<(), AbortableError>>>,
        ctrls: mpsc::AsyncSender<Seq<Ctrl>>,
        ints: mpsc::AsyncSender<Seq<Int>>,
        isos: mpsc::AsyncSender<Seq<Iso>>,
        bulks: mpsc::AsyncSender<Seq<Bulk>>,
        mut handler: impl RecvHandler,
    ) -> Result<(), Option<RecvError>> {
        use proto::msg::TransferKind;

        enum Event {
            Frame(Result<Frame, RecvError>),
            CompletedBlocking(Seq<Result<(), AbortableError>>),
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
        let mut frame_rx = frame_rx.map(Event::Frame);

        loop {
            let next_frame = async { frame_rx.next().await };
            let next_reset = async {
                if reset_inprogress.is_empty() {
                    std::future::pending().await
                } else {
                    reset_inprogress.next().await.map(Event::CompletedBlocking)
                }
            };
            let next_ctrl = async {
                if ctrl_inprogress.is_empty() {
                    std::future::pending().await
                } else {
                    ctrl_inprogress.next().await.map(Event::CompletedCtrl)
                }
            };
            let next_int = async {
                if int_inprogress.is_empty() {
                    std::future::pending().await
                } else {
                    int_inprogress.next().await.map(Event::CompletedInt)
                }
            };
            let next_iso = async {
                if iso_inprogress.is_empty() {
                    std::future::pending().await
                } else {
                    iso_inprogress.next().await.map(Event::CompletedIso)
                }
            };
            let next_bulk = async {
                if bulk_inprogress.is_empty() {
                    std::future::pending().await
                } else {
                    bulk_inprogress.next().await.map(Event::CompletedBulk)
                }
            };

            let events = (
                next_frame, next_iso, next_int, next_ctrl, next_bulk, next_reset,
            );
            match events.race().await.ok_or(None)? {
                Event::Frame(Ok(Frame::Urb(seq))) => {
                    let kind = seq.data.get().header.kind;
                    match kind {
                        TransferKind::Isochronous => {
                            let fut = handler.new_iso(seq);
                            iso_inprogress.push(fut);
                        }
                        TransferKind::Interrupt => {
                            let fut = handler.new_int(seq);
                            int_inprogress.push(fut);
                        }
                        TransferKind::Control => {
                            let ctrl_pkt = seq.data.get().header.ctrl_packet;
                            trace! { %ctrl_pkt };
                            match CtrlKind::parse(&ctrl_pkt) {
                                CtrlKind::Blocking(CtrlReq::SetInterface(req)) => {
                                    let fut = handler
                                        .set_interface(Seq {
                                            seqnum: seq.seqnum,
                                            data: req,
                                        })
                                        .in_current_span();
                                    ctrl_inprogress.push(ctrl::Kind::SetInterface { fut });
                                }
                                CtrlKind::Blocking(CtrlReq::SetConfig(req)) => {
                                    let fut = handler
                                        .set_config(Seq {
                                            seqnum: seq.seqnum,
                                            data: req,
                                        })
                                        .in_current_span();
                                    ctrl_inprogress.push(ctrl::Kind::SetConfig { fut });
                                }
                                CtrlKind::Blocking(CtrlReq::ClearStall(req)) => {
                                    let fut = handler
                                        .clear_stall(Seq {
                                            seqnum: seq.seqnum,
                                            data: req,
                                        })
                                        .in_current_span();
                                    ctrl_inprogress.push(ctrl::Kind::ClearStall { fut });
                                }
                                CtrlKind::Async => {
                                    let fut = handler.new_ctrl(seq);
                                    ctrl_inprogress.push(ctrl::Kind::Async { fut });
                                }
                            }
                        }
                        TransferKind::Bulk => {
                            let fut = handler.new_bulk(seq);
                            bulk_inprogress.push(fut);
                        }
                    }
                }
                Event::Frame(Ok(Frame::PortReset(seqnum))) => {
                    let fut = handler.device_reset(seqnum);
                    reset_inprogress.push(fut);
                }
                Event::Frame(Ok(Frame::Unlink(seqnum))) => {
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
                Event::Frame(Err(err)) => return Err(Some(err)),
            }
        }
    }

    pub async fn run<H>(self, handler: H, cancel: CancellationToken) -> Result<(), Error>
    where
        R: AsyncRead + Unpin + 'static,
        H: RecvHandler,
    {
        let Self {
            mut rx,
            buf,
            reset_tx: reset,
            ctrl_tx: ctrls,
            int_tx: ints,
            iso_tx: isos,
            bulk_tx: bulks,
        } = self;
        let result = {
            let frame_rx = stream::unfold((&mut rx, buf), |(mut rx, mut buf)| async {
                let result = match proto::recv_frame(&mut rx, &mut buf).await {
                    Ok(frame) => proto::parse_frame(frame).map_err(|_err| RecvError::CorruptedData),
                    Err(err) => Err(err?),
                };
                Some((result, (rx, buf)))
            });
            let frame_rx = Box::pin(stream::stop_after_future(
                frame_rx,
                cancel.cancelled_owned(),
            ));
            let looper = Self::do_loop(frame_rx, reset, ctrls, ints, isos, bulks, handler);
            match Box::pin(looper.in_current_span()).await {
                Ok(()) | Err(None) => Ok(()),
                Err(Some(err)) => Err(Error::Recv(err)),
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
    let (reset_tx, reset_rx) = mpsc::channel(1);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
    let (int_tx, int_rx) = mpsc::channel(1);
    let (bulk_tx, bulk_rx) = mpsc::channel(8);
    let (iso_tx, iso_rx) = mpsc::channel(8);
    let send_loop = SendLoop {
        tx,
        reset_rx,
        ctrl_rx,
        int_rx,
        iso_rx,
        bulk_rx,
    };
    let recv_loop = RecvLoop {
        rx,
        buf,
        reset_tx,
        ctrl_tx,
        int_tx,
        iso_tx,
        bulk_tx,
    };
    (send_loop, recv_loop)
}
