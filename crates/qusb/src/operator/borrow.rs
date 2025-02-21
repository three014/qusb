use std::{future::Future, io, marker::PhantomData};

use bytes::Bytes;
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
use zerocopy::transmute;

use crate::stub::WorkReceiver;

pub trait SendHandler {
    fn port_stat(&mut self, stat: ioctl::IocPortStat) -> Option<Header>;
    fn set_address(
        &mut self,
        urb: ioctl::IocUrb,
        handle: ioctl::UrbHandle,
    ) -> impl Future<Output = io::Result<()>>;
    fn process_urb(
        &mut self,
        urb: ioctl::IocUrb,
        handle: ioctl::UrbHandle,
    ) -> io::Result<Bytes>;
    fn cancel_urb(&mut self, handle: ioctl::UrbHandle) -> Option<Header>;
}

pub struct SendLoop<W> {
    tx: W,
    work_rx: WorkReceiver,
    _p: PhantomData<*const ()>,
}

impl<W> SendLoop<W> {
    #[inline]
    pub const fn new(tx: W, work_rx: WorkReceiver) -> Self {
        Self {
            tx,
            work_rx,
            _p: PhantomData,
        }
    }

    pub async fn run<H>(mut self, mut handler: H, cancel: CancellationToken) -> io::Result<()>
    where
        W: AsyncWrite + Unpin + 'static,
        H: SendHandler,
    {
        use tokio::io::AsyncWriteExt;

        let _guard = tracing::Span::current().entered();
        loop {
            let work = tokio::select! {
                biased;
                maybe_work = self.work_rx.recv() => {
                    match maybe_work {
                        Some(work) => work,
                        None => break Ok(()),
                    }
                }
                _ = cancel.cancelled() => {
                    break Ok(())
                }
            };

            match work {
                Work::PortStat(next) => {
                    if let Some(header) = handler.port_stat(next) {
                        self.tx
                            .write_u64_le(transmute!(header))
                            .in_current_span()
                            .await?;
                    }
                }
                Work::ProcessUrb((urb, handle))
                    if UrbType::Ctrl == urb.typ
                        && urb.address.is_for_unassigned()
                        && Request::STANDARD_DEVICE_SET_ADDRESS == urb.setup_packet.req() =>
                {
                    handler.set_address(urb, handle).in_current_span().await?;
                }
                Work::ProcessUrb((urb, handle)) => {
                    let mut bytes = handler.process_urb(urb, handle)?;
                    self.tx.write_all_buf(&mut bytes).in_current_span().await?;
                }
                Work::CancelUrb(handle) => {
                    if let Some(header) = handler.cancel_urb(handle) {
                        self.tx
                            .write_u64_le(transmute!(header))
                            .in_current_span()
                            .await?;
                    }
                }
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
    _p: PhantomData<*const ()>,
}

impl<R> RecvLoop<R> {
    #[inline]
    pub const fn new(rx: R, buf: Ring) -> Self {
        Self {
            rx,
            buf,
            _p: PhantomData,
        }
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
                maybe_frame = super::recv_frame(&mut self.rx, &mut self.buf) => {
                    match maybe_frame {
                        Ok(Some(frame)) => frame,
                        Ok(None) => break Ok(()),
                        Err(err) => break Err(err.into()),
                    }
                }
                _ = cancel.cancelled() => {
                    break Ok(())
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
