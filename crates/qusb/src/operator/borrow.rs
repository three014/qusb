use std::{future::Future, io, pin::pin, time::Duration};

use bytes::Bytes;
use compio_io::{AsyncRead, AsyncWrite};
use futures_concurrency::future::Race;
use futures_lite::{Stream, StreamExt, stream};
use futures_util::stream::FuturesUnordered;
use proto::{
    data::{Data, Ring},
    msg::{Header, UrbFrame},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};
use vhci::{
    ioctl::{self, UrbType, Work},
    usbfs::Request,
};

use crate::{
    stub::WorkReceiver,
    utils::{CloseStream, Interval, blocker},
};

const TICK: Duration = Duration::from_micros(43);

pub trait SendHandler {
    fn port_stat(&mut self, stat: ioctl::IocPortStat);
    fn set_address(
        &mut self,
        urb: ioctl::IocUrb,
        handle: ioctl::UrbHandle,
    ) -> impl Future<Output = io::Result<()>> + 'static;
    fn process_urb(&mut self, urb: ioctl::IocUrb, handle: ioctl::UrbHandle) -> io::Result<()>;
    fn cancel_urb(&mut self, handle: ioctl::UrbHandle);
    fn is_buf_empty(&self) -> bool;
    fn flush_buf(&mut self) -> Bytes;
}

trait SendHandlerExt {
    fn handle_work(
        &mut self,
        work: Work,
    ) -> io::Result<Option<impl Future<Output = io::Result<()>> + 'static>>;
}

impl<T: SendHandler> SendHandlerExt for T {
    fn handle_work(
        &mut self,
        work: Work,
    ) -> io::Result<Option<impl Future<Output = io::Result<()>> + 'static>> {
        match work {
            Work::PortStat(next) => {
                self.port_stat(next);
                Ok(None)
            }
            Work::ProcessUrb((urb, handle))
                if UrbType::Ctrl == urb.typ
                    && urb.address.is_for_unassigned()
                    && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
            {
                let fut = self.set_address(urb, handle);
                Ok(Some(fut))
            }
            Work::ProcessUrb((urb, handle)) => self.process_urb(urb, handle).map(|_| None),
            Work::CancelUrb(handle) => {
                self.cancel_urb(handle);
                Ok(None)
            }
        }
    }
}

pub struct SendLoop<W> {
    tx: W,
    work_rx: WorkReceiver,
}

impl<W> SendLoop<W> {
    #[inline]
    pub const fn new(tx: W, work_rx: WorkReceiver) -> Self {
        Self { tx, work_rx }
    }

    #[inline]
    pub async fn do_loop<H>(
        tx: &mut W,
        work_rx: impl Stream<Item = Work> + Unpin,
        mut handler: H,
    ) -> Result<(), Option<io::Error>>
    where
        W: AsyncWrite + Unpin + CloseStream + 'static,
        H: SendHandler,
    {
        use compio_io::AsyncWriteExt;

        enum Event<W> {
            SetAddress(io::Result<()>),
            Work(Work),
            FlushBuf,
            FlushComplete(io::Result<W>),
        }

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
        let mut sleeper = pin!(blocker(None));
        let mut flush_op = pin!(blocker(None));
        let mut set_addr = pin!(blocker(None));
        let mut work_rx = work_rx.map(Event::Work);

        let mut state = State::Solicit;
        loop {
            let event = {
                let race = (
                    work_rx.next(),
                    sleeper.as_mut(),
                    flush_op.as_mut(),
                    set_addr.as_mut(),
                )
                    .race();
                race.await.ok_or(None)?
            };
            state = match event {
                Event::Work(work) => {
                    if let Some(fut) = handler.handle_work(work)? {
                        set_addr.set(blocker(Some(
                            async move { Some(Event::SetAddress(fut.await)) },
                        )));
                        state
                    } else {
                        match state {
                            State::Solicit if !handler.is_buf_empty() => {
                                sleeper.set(blocker(Some(arm_timer(&interval))));
                                State::Timer
                            }
                            current => current,
                        }
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
                Event::SetAddress(Ok(_)) => {
                    set_addr.set(blocker(None));
                    state
                }
                Event::SetAddress(Err(err)) | Event::FlushComplete(Err(err)) => {
                    return Err(Some(err));
                }
            }
        }
    }

    pub async fn run(self, handler: impl SendHandler, cancel: CancellationToken) -> io::Result<()>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
    {
        let Self { mut tx, work_rx } = self;
        let work_rx = pin!(futures_lite::stream::stop_after_future(
            work_rx,
            cancel.cancelled_owned()
        ));
        let result = match Self::do_loop(&mut tx, work_rx, handler)
            .in_current_span()
            .await
        {
            Ok(_) | Err(None) => Ok(()),
            Err(Some(err)) => Err(err),
        };

        _ = tx.close();
        _ = tx.stopped().await;
        result
    }
}

pub trait RecvHandler {
    fn device_reset(&mut self, seqnum: u32) -> io::Result<()>;
    fn urb_reply(
        &mut self,
        seqnum: u32,
        data: Data<UrbFrame>,
    ) -> impl Future<Output = io::Result<()>> + 'static;
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
        frame_rx: impl Stream<Item = io::Result<super::Recv>> + Unpin,
        mut handler: H,
    ) -> Result<(), Option<io::Error>>
    where
        H: RecvHandler,
    {
        enum Event {
            Frame(super::Recv),
            GivebackComplete,
        }

        let _guard = Span::current().entered();
        let mut replies = FuturesUnordered::new();
        let mut frame_rx = frame_rx.map(|result| result.map(Event::Frame));

        replies.push(blocker(None));

        loop {
            let event = {
                let race = (frame_rx.next(), replies.next()).race();
                race.await.ok_or(None)??
            };
            match event {
                Event::GivebackComplete => {}
                Event::Frame(super::Recv::Urb((
                    Header {
                        seqnum,
                        status: proto::msg::Status::Success,
                        ..
                    },
                    data,
                ))) => {
                    let reply = handler.urb_reply(seqnum, data.unwrap()).in_current_span();
                    let fut = async move { reply.await.map(|_| Event::GivebackComplete) };
                    replies.push(blocker(Some(fut)));
                }
                Event::Frame(super::Recv::PortReset(Header {
                    seqnum,
                    status: proto::msg::Status::Success,
                    ..
                })) => {
                    handler.device_reset(seqnum)?;
                }
                Event::Frame(super::Recv::Unlink(_)) => {
                    Err(io::Error::new(io::ErrorKind::InvalidData, "unlink"))?;
                }
                Event::Frame(super::Recv::Urb((Header { status, .. }, _)))
                | Event::Frame(super::Recv::PortReset(Header { status, .. })) => {
                    #[inline(never)]
                    #[cold]
                    fn make_error(status: proto::msg::Status) -> Result<(), Option<io::Error>> {
                        match status {
                            proto::msg::Status::Success => unreachable!(),
                            proto::msg::Status::Failed => todo!(),
                            proto::msg::Status::DevBusy => todo!(),
                            proto::msg::Status::DevErr => {
                                Err(Some(io::Error::other("lender device in error state")))
                            }
                            proto::msg::Status::NoDev => Err(Some(io::Error::new(
                                io::ErrorKind::NotFound,
                                "device disconnected on lender side",
                            ))),
                            proto::msg::Status::Unexpected => todo!(),
                            proto::msg::Status::VersionMismatch => todo!(),
                            proto::msg::Status::Timeout => todo!(),
                            proto::msg::Status::Proto => todo!(),
                        }
                    }

                    return make_error(status);
                }
            };
        }
    }

    pub async fn run(self, handler: impl RecvHandler, cancel: CancellationToken) -> io::Result<()>
    where
        R: AsyncRead + Unpin + 'static,
    {
        let Self { mut rx, buf } = self;
        let result = {
            let frame_rx = stream::unfold((&mut rx, buf), |(mut rx, mut buf)| async {
                let result = super::recv_frame(&mut rx, &mut buf).await.transpose()?;
                Some((result, (rx, buf)))
            });
            let frame_rx = pin!(stream::stop_after_future(
                frame_rx,
                cancel.cancelled_owned()
            ));
            match Self::do_loop(frame_rx, handler).in_current_span().await {
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
