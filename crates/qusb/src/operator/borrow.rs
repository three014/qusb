use std::{
    future::Future,
    io,
    pin::pin,
    time::Duration,
};

use bytes::Bytes;
use compio::{
    io::{AsyncRead, AsyncWrite},
    runtime::time::sleep,
};
use futures_concurrency::{future::Race, stream::Merge};
use futures_lite::{stream, StreamExt};
use proto::{
    data::{Data, Ring},
    msg::{Header, UrbFrame},
};
use tokio_util::sync::CancellationToken;
use vhci::{
    ioctl::{self, UrbType, Work},
    usbfs::Request,
};

use crate::{stub::WorkReceiver, utils::CloseStream};

pub trait SendHandler {
    fn port_stat(&mut self, stat: ioctl::IocPortStat);
    fn set_address(
        &mut self,
        urb: ioctl::IocUrb,
        handle: ioctl::UrbHandle,
    ) -> impl Future<Output = io::Result<()>>;
    fn process_urb(&mut self, urb: ioctl::IocUrb, handle: ioctl::UrbHandle) -> io::Result<()>;
    fn cancel_urb(&mut self, handle: ioctl::UrbHandle);
    fn is_buf_empty(&self) -> bool;
    fn flush_buf(&mut self) -> Bytes;
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

    pub async fn run<H>(mut self, mut handler: H, cancel: CancellationToken) -> io::Result<()>
    where
        W: AsyncWrite + CloseStream + Unpin + 'static,
        H: SendHandler,
    {
        use compio::io::AsyncWriteExt;

        enum Event {
            Cancelled,
            Work(Work),
            FlushBuf,
        }

        let _guard = tracing::Span::current().entered();
        const TICK: Duration = Duration::from_micros(100);
        let work_rx = self.work_rx.map(Event::Work);
        let cancelled = stream::once_future(async move {
            cancel.cancelled_owned().await;
            Event::Cancelled
        });
        let mut timer = pin!(sleep(TICK));

        let mut main_events = pin!((cancelled, work_rx).merge());
        let mut main = main_events.next();

        loop {
            let event = {
                if handler.is_buf_empty() {
                    pin!(&mut main).await
                } else {
                    let timer = async {
                        timer.as_mut().await;
                        Some(Event::FlushBuf)
                    };
                    (pin!(&mut main), timer).race().await
                }
            };

            match event {
                Some(Event::Work(Work::PortStat(next))) => {
                    handler.port_stat(next);
                    main = main_events.next();
                }
                Some(Event::Work(Work::ProcessUrb((urb, handle))))
                    if UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
                {
                    handler.set_address(urb, handle).await?;
                    main = main_events.next();
                }
                Some(Event::Work(Work::ProcessUrb((urb, handle)))) => {
                    handler.process_urb(urb, handle)?;
                    main = main_events.next();
                }
                Some(Event::Work(Work::CancelUrb(handle))) => {
                    handler.cancel_urb(handle);
                    main = main_events.next();
                }
                Some(Event::FlushBuf) => {
                    let bytes = handler.flush_buf();
                    assert_eq!(bytes.len() % 8, 0);
                    self.tx.write_all(bytes).await.0?;
                    timer.set(sleep(TICK));
                }
                Some(Event::Cancelled) | None => break,
            }
        }

        _ = self.tx.close();
        Ok(())
    }
}

pub trait RecvHandler {
    fn device_reset(&mut self, seqnum: u32) -> io::Result<()>;
    fn urb_reply(
        &mut self,
        seqnum: u32,
        data: Data<UrbFrame>,
    ) -> impl Future<Output = io::Result<()>>;
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

    pub async fn run<H>(self, mut handler: H, cancel: CancellationToken) -> io::Result<()>
    where
        R: AsyncRead + Unpin + 'static,
        H: RecvHandler,
    {
        enum Event {
            Cancelled,
            Frame(io::Result<super::Recv>),
        }

        let _guard = tracing::Span::current().entered();
        let cancel = stream::once_future(async move {
            cancel.cancelled_owned().await;
            Event::Cancelled
        });
        let frame = stream::unfold((self.rx, self.buf), |(mut rx, mut buf)| async {
            let result = super::recv_frame(&mut rx, &mut buf).await.transpose()?;
            Some((Event::Frame(result), (rx, buf)))
        });

        let mut events = pin!((cancel, frame).merge());
        while let Some(event) = events.next().await {
            match event {
                Event::Cancelled => break,
                Event::Frame(Ok(recv)) => match recv {
                    super::Recv::Urb((
                        Header {
                            seqnum,
                            status: proto::msg::Status::Success,
                            ..
                        },
                        data,
                    )) => {
                        handler.urb_reply(seqnum, data.unwrap()).await?;
                    }
                    super::Recv::PortReset(Header {
                        seqnum,
                        status: proto::msg::Status::Success,
                        ..
                    }) => {
                        handler.device_reset(seqnum)?;
                    },
                    super::Recv::Unlink(_) => {
                        Err(io::Error::new(io::ErrorKind::InvalidData, "unlink"))?;
                    }
                    super::Recv::Urb((Header { status, .. }, _))
                    | super::Recv::PortReset(Header { status, .. }) => match status {
                        proto::msg::Status::Success => unreachable!(),
                        proto::msg::Status::Failed => todo!(),
                        proto::msg::Status::DevBusy => todo!(),
                        proto::msg::Status::DevErr => {
                            Err(io::Error::other("lender device in error state"))?;
                        }
                        proto::msg::Status::NoDev => {
                            Err(io::Error::new(
                                io::ErrorKind::NotFound,
                                "device disconnected on lender side",
                            ))?;
                        }
                        proto::msg::Status::Unexpected => todo!(),
                        proto::msg::Status::VersionMismatch => todo!(),
                        proto::msg::Status::Timeout => todo!(),
                        proto::msg::Status::Proto => todo!(),
                    },
                },
                Event::Frame(Err(err)) => Err(err)?,
            }
        }
        Ok(())
    }
}
