use std::{
    future::{poll_fn, Future},
    io,
    pin::pin,
    time::Duration,
};

use bytes::Bytes;
use futures_core::Stream;
use proto::{
    data::{Data, Ring},
    msg::{Header, UrbFrame},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use vhci::{
    ioctl::{self, UrbType, Work},
    usbfs::Request,
};

use crate::stub::WorkReceiver;

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
        W: AsyncWrite + Unpin + 'static,
        H: SendHandler,
    {
        use tokio::io::AsyncWriteExt;

        enum Event {
            Cancelled,
            Work(Option<Work>),
            FlushBuf,
        }

        let _guard = tracing::Span::current().entered();
        let mut timer = pin!(tokio_timerfd::Interval::new_interval(
            Duration::from_micros(25)
        )?);
        let mut wait_to_send = poll_fn(|cx| timer.as_mut().poll_next(cx));
        let mut cancelled = pin!(cancel.cancelled());

        loop {
            let event = tokio::select! {
                biased;
                _ = &mut cancelled => {
                    Event::Cancelled
                }
                result = &mut wait_to_send, if !handler.is_buf_empty() => {
                    result.unwrap().unwrap();
                    Event::FlushBuf
                }
                maybe_work = self.work_rx.recv() => {
                    Event::Work(maybe_work)
                }
            };

            match event {
                Event::Work(Some(Work::PortStat(next))) => {
                    handler.port_stat(next);
                }
                Event::Work(Some(Work::ProcessUrb((urb, handle))))
                    if UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
                {
                    handler.set_address(urb, handle).in_current_span().await?;
                }
                Event::Work(Some(Work::ProcessUrb((urb, handle)))) => {
                    handler.process_urb(urb, handle)?;
                }
                Event::Work(Some(Work::CancelUrb(handle))) => {
                    handler.cancel_urb(handle);
                }
                Event::FlushBuf => {
                    let mut bytes = handler.flush_buf();
                    assert_eq!(bytes.len() % 8, 0);
                    self.tx.write_all_buf(&mut bytes).in_current_span().await?;
                }
                Event::Cancelled | Event::Work(None) => break Ok(()),
            }
        }
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

    pub async fn run<H>(mut self, mut handler: H, cancel: CancellationToken) -> io::Result<()>
    where
        R: AsyncRead + Unpin + 'static,
        H: RecvHandler,
    {
        let _guard = tracing::Span::current().entered();
        loop {
            let recv = tokio::select! {
                biased;
                _ = cancel.cancelled() => break Ok(()),
                maybe_frame = super::recv_frame(&mut self.rx, &mut self.buf) => {
                    match maybe_frame {
                        Ok(Some(frame)) => frame,
                        Ok(None) => break Ok(()),
                        Err(err) => break Err(err),
                    }
                }
            };

            match recv {
                super::Recv::Urb((
                    Header {
                        seqnum,
                        status: proto::msg::Status::Success,
                        ..
                    },
                    data,
                )) => {
                    handler.urb_reply(seqnum, data).in_current_span().await?;
                }
                super::Recv::PortReset(Header {
                    seqnum,
                    status: proto::msg::Status::Success,
                    ..
                }) => handler.device_reset(seqnum)?,
                super::Recv::Unlink(_) => {
                    break Err(io::Error::new(io::ErrorKind::InvalidData, "unlink"))
                }
                super::Recv::Urb((Header { status, .. }, _))
                | super::Recv::PortReset(Header { status, .. }) => match status {
                    proto::msg::Status::Success => unreachable!(),
                    proto::msg::Status::Failed => todo!(),
                    proto::msg::Status::DevBusy => todo!(),
                    proto::msg::Status::DevErr => {
                        break Err(io::Error::other("lender device in error state"))
                    }
                    proto::msg::Status::NoDev => {
                        break Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "device disconnected on lender side",
                        ))
                    }
                    proto::msg::Status::Unexpected => todo!(),
                    proto::msg::Status::VersionMismatch => todo!(),
                    proto::msg::Status::Timeout => todo!(),
                    proto::msg::Status::Proto => todo!(),
                },
            }
        }
    }
}
