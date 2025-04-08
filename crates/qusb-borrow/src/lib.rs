use std::{io, ops::DerefMut, pin::pin, time::Duration};

use bytes::Bytes;
use compio_io::{AsyncRead, AsyncWrite};
use error::{Error, VhciError};
use futures_concurrency::future::Race;
use futures_lite::{Stream, StreamExt, stream};
use futures_util::stream::FuturesUnordered;
use proto::{
    data::{Data, Ring},
    msg::UrbFrame,
    unpacked::{Frame, Seq, Seqnum},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};
use utils::{CloseStream, MaybeFut, ResettableFuture, mpsc, time::Interval};
use vhci::{
    ioctl::{IocPortStat, IocUrb, UrbHandle, UrbType, Work},
    usbfs::Request,
};

pub mod error {
    use std::io;

    use proto::{AbortableError, RecvError};
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum VhciError {
        #[error("driver call failed: {0}")]
        Driver(#[from] io::Error),
        #[error("error from physical device: {0}")]
        Phys(#[from] AbortableError),
    }

    #[derive(Debug, Error)]
    pub enum Error {
        #[error(transparent)]
        Vhci(#[from] VhciError),
        #[error("error while receiving data from peer: {0}")]
        Recv(#[from] RecvError),
        #[error("error while sending data to peer: {0}")]
        Send(#[from] io::Error),
    }
}

const TICK: Duration = Duration::from_micros(87);

pub trait SendHandler {
    fn port_stat(&mut self, stat: IocPortStat);
    fn set_address(
        &mut self,
        urb: IocUrb,
        handle: UrbHandle,
    ) -> impl Future<Output = io::Result<()>> + 'static;
    fn process_urb(&mut self, urb: IocUrb, handle: UrbHandle) -> io::Result<()>;
    fn cancel_urb(&mut self, handle: UrbHandle);
    fn is_buf_empty(&self) -> bool;
    fn flush_buf(&mut self) -> Bytes;
}

impl<T, U> SendHandler for T
where
    T: DerefMut<Target = U>,
    U: SendHandler + 'static,
{
    fn port_stat(&mut self, stat: IocPortStat) {
        self.deref_mut().port_stat(stat);
    }

    fn set_address(
        &mut self,
        urb: IocUrb,
        handle: UrbHandle,
    ) -> impl Future<Output = io::Result<()>> + 'static {
        self.deref_mut().set_address(urb, handle)
    }

    fn process_urb(&mut self, urb: IocUrb, handle: UrbHandle) -> io::Result<()> {
        self.deref_mut().process_urb(urb, handle)
    }

    fn cancel_urb(&mut self, handle: UrbHandle) {
        self.deref_mut().cancel_urb(handle);
    }

    fn is_buf_empty(&self) -> bool {
        self.deref().is_buf_empty()
    }

    fn flush_buf(&mut self) -> Bytes {
        self.deref_mut().flush_buf()
    }
}

enum WorkResult<F> {
    SetAddress(F),
    ProcessUrb(io::Result<()>),
    /// Nothing to report on, but still check the buffer.
    MustveBeenTheWind,
}

trait SendHandlerExt {
    fn handle_work(
        &mut self,
        work: Work,
    ) -> WorkResult<impl Future<Output = io::Result<()>> + 'static>;
}

impl<T: SendHandler> SendHandlerExt for T {
    fn handle_work(
        &mut self,
        work: Work,
    ) -> WorkResult<impl Future<Output = io::Result<()>> + 'static> {
        match work {
            Work::PortStat(next) => {
                self.port_stat(next);
                WorkResult::MustveBeenTheWind
            }
            Work::ProcessUrb((urb, handle))
                if UrbType::Ctrl == urb.typ
                    && urb.address.is_for_unassigned()
                    && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
            {
                let fut = self.set_address(urb, handle);
                WorkResult::SetAddress(fut)
            }
            Work::ProcessUrb((urb, handle)) => {
                WorkResult::ProcessUrb(self.process_urb(urb, handle))
            }
            Work::CancelUrb(handle) => {
                self.cancel_urb(handle);
                WorkResult::MustveBeenTheWind
            }
        }
    }
}

pub trait RecvHandler {
    fn device_reset(&mut self, seqnum: Seqnum) -> io::Result<()>;
    fn urb_reply(
        &mut self,
        seq: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = io::Result<()>> + 'static;
}

impl<T, U> RecvHandler for T
where
    T: DerefMut<Target = U>,
    U: RecvHandler + 'static,
{
    fn device_reset(&mut self, seqnum: Seqnum) -> io::Result<()> {
        self.deref_mut().device_reset(seqnum)
    }

    fn urb_reply(
        &mut self,
        seq: Seq<Data<UrbFrame>>,
    ) -> impl Future<Output = io::Result<()>> + 'static {
        self.deref_mut().urb_reply(seq)
    }
}

pub struct SendLoop<W> {
    tx: W,
    work_rx: mpsc::AsyncReceiver<Work>,
}

impl<W> SendLoop<W> {
    #[inline]
    pub const fn new(tx: W, work_rx: mpsc::AsyncReceiver<Work>) -> Self {
        Self { tx, work_rx }
    }

    #[inline]
    pub async fn do_loop(
        tx: &mut W,
        work_rx: impl Stream<Item = Work> + Unpin,
        mut handler: impl SendHandler,
    ) -> Result<(), Option<Error>>
    where
        W: AsyncWrite + Unpin + CloseStream + 'static,
    {
        use compio_io::AsyncWriteExt;

        enum Event<W> {
            SetAddress(io::Result<()>),
            Work(Work),
            FlushBuf,
            FlushComplete(io::Result<W>),
        }

        #[derive(Debug, PartialEq, Eq)]
        enum State {
            /// In this state, the writer is not available as it
            /// is in the process of writing data asynchronously.
            ///
            /// When the write is complete, the state moves to
            /// `Solicit` or `Timer` depending on whether the
            /// buffer is empty or not.
            Flush,
            /// The buffer is empty and we are waiting on work.
            /// From here we can only transition to `Timer` once
            /// there is data in the buffer.
            Solicit,
            /// The buffer has data in it and we are waiting for our
            /// timer to complete before we flush the buffer, which
            /// gives time for more data to be written to the buffer.
            ///
            /// From here we can only transition to the `Flush` state.
            Timer,
        }

        // State Transitions:
        // Solicit -> [Solicit(SetAddress, Work), Timer(Work)]
        // Timer -> [Timer(SetAddress, Work), Flush(FlushBuf)]
        // Flush -> [Solicit(FlushComplete), Timer(FlushComplete), Flush(SetAddress, Work)]

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
        let mut addr_setter = pin!(MaybeFut::None);
        let mut work_rx = work_rx.map(Event::Work);

        let mut state = State::Solicit;
        loop {
            let event = match state {
                State::Solicit if addr_setter.is_empty() => work_rx.next().await,
                State::Timer if addr_setter.is_empty() => {
                    (work_rx.next(), sleeper.as_mut()).race().await
                }
                State::Flush if addr_setter.is_empty() => {
                    (work_rx.next(), flusher.as_mut()).race().await
                }
                State::Solicit => (work_rx.next(), addr_setter.as_mut()).race().await,
                State::Timer => {
                    (work_rx.next(), sleeper.as_mut(), addr_setter.as_mut())
                        .race()
                        .await
                }
                State::Flush => {
                    (work_rx.next(), flusher.as_mut(), addr_setter.as_mut())
                        .race()
                        .await
                }
            };
            state = match event.ok_or(None)? {
                Event::Work(work) => match handler.handle_work(work) {
                    WorkResult::SetAddress(fut) => {
                        addr_setter.reset(async { Some(Event::SetAddress(fut.await)) });
                        state
                    }
                    WorkResult::ProcessUrb(Ok(())) | WorkResult::MustveBeenTheWind
                        if State::Solicit == state && !handler.is_buf_empty() =>
                    {
                        sleeper.reset(arm_timer(&interval));
                        State::Timer
                    }
                    WorkResult::ProcessUrb(Err(err)) => {
                        return Err(Some(Error::Vhci(VhciError::Driver(err))));
                    }
                    _ => state,
                },
                Event::FlushBuf => {
                    sleeper.clear();
                    let tx = tx_holder.take().unwrap();
                    let bytes = handler.flush_buf();
                    flusher.reset(async move {
                        match tx.write_all(bytes).await.0 {
                            Ok(()) => Some(Event::FlushComplete(Ok(tx))),
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
                Event::SetAddress(Ok(())) => {
                    addr_setter.clear();
                    state
                }
                Event::SetAddress(Err(err)) => {
                    return Err(Some(Error::Vhci(VhciError::Driver(err))));
                }
                Event::FlushComplete(Err(err)) => return Err(Some(Error::Send(err))),
            }
        }
    }

    pub async fn run<H: SendHandler>(
        mut self,
        handler: H,
        cancel: CancellationToken,
    ) -> Result<(), Error>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
    {
        let work_rx = Box::pin(stream::stop_after_future(
            self.work_rx,
            cancel.cancelled_owned(),
        ));
        let looper = Self::do_loop(&mut self.tx, work_rx, handler);
        let result = match Box::pin(looper.in_current_span()).await {
            Ok(()) | Err(None) => Ok(()),
            Err(Some(err)) => Err(err),
        };
        _ = self.tx.close();
        _ = self.tx.stopped().await;
        result
    }
}

pub struct RecvLoop<R> {
    rx: R,
    buf: Ring,
}
impl<R> RecvLoop<R> {
    #[inline]
    pub const fn new(rx: R, buf: Ring) -> Self {
        Self { rx, buf }
    }

    #[inline]
    async fn do_loop<H>(
        frame_rx: impl Stream<Item = Result<Frame, Error>> + Unpin,
        mut handler: H,
    ) -> Result<(), Option<Error>>
    where
        H: RecvHandler,
    {
        enum Event {
            Frame(Result<Frame, Error>),
            GivebackComplete(io::Result<()>),
        }

        let _guard = Span::current().entered();
        let mut replies = FuturesUnordered::new();
        let mut frame_rx = frame_rx.map(Event::Frame);

        loop {
            let event = if replies.is_empty() {
                frame_rx.next().await
            } else {
                (frame_rx.next(), replies.next()).race().await
            };
            match event.ok_or(None)? {
                Event::GivebackComplete(Ok(())) => {}
                Event::Frame(Ok(Frame::Urb(seq))) => {
                    let fut = handler.urb_reply(seq).in_current_span();
                    let fut = async move { Event::GivebackComplete(fut.await) };
                    replies.push(fut);
                }
                Event::Frame(Ok(Frame::PortReset(seqnum))) => {
                    handler
                        .device_reset(seqnum)
                        .map_err(VhciError::Driver)
                        .map_err(Error::Vhci)?;
                }
                Event::Frame(Err(err)) => return Err(Some(err)),
                Event::GivebackComplete(Err(err)) => {
                    return Err(Some(Error::Vhci(VhciError::Driver(err))));
                }
                Event::Frame(Ok(Frame::Unlink(_))) => {
                    unreachable!("parse_frame should've filtered this out")
                }
            };
        }
    }

    pub async fn run<H>(mut self, handler: H, cancel: CancellationToken) -> Result<(), Error>
    where
        R: AsyncRead + Unpin + 'static,
        H: RecvHandler,
    {
        let result = {
            let frame_rx = stream::unfold((&mut self.rx, self.buf), |(mut rx, mut buf)| async {
                let result = match proto::recv_frame(&mut rx, &mut buf)
                    .await
                    .map(proto::parse_frame)
                {
                    Ok(Ok(frame)) => Ok(frame),
                    Ok(Err(err)) => Err(Error::Vhci(VhciError::Phys(err))),
                    Err(err) => Err(Error::Recv(err?)),
                };
                Some((result, (rx, buf)))
            });
            let frame_rx = Box::pin(stream::stop_after_future(
                frame_rx,
                cancel.cancelled_owned(),
            ));
            let looper = Self::do_loop(frame_rx, handler);
            match Box::pin(looper.in_current_span()).await {
                Ok(()) | Err(None) => Ok(()),
                Err(Some(err)) => Err(err),
            }
        };

        // Done! Drain the streams
        let mut buf = Ring::with_capacity(32);
        while buf
            .fill_with_reader(&mut self.rx)
            .await
            .is_ok_and(|bytes_read| 0 != bytes_read)
        {}
        result
    }
}
