use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Poll, ready},
};

use proto::TransferError;
use tracing::{Span, error, warn};

use crate::operator::Seq;

pub trait BlockingOps {
    fn set_alt_setting_async(&self, seqnum: u32, interface: u8, setting: u8) -> SetInterface;
    fn set_config_async(&self, seqnum: u32, config: u8) -> SetConfig;
    fn clear_stall_async(&self, seqnum: u32, endpoint: u8) -> ClearStall;
}

enum State {
    Init {
        handle: Option<Arc<super::device::Handle>>,
    },
    Waiting {
        join: compio_runtime::JoinHandle<rusb::Result<()>>,
    },
    Complete(Result<(), TransferError>),
}

impl BlockingOps for Arc<super::device::Handle> {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn set_alt_setting_async(&self, seqnum: u32, interface: u8, setting: u8) -> SetInterface {
        SetInterface {
            seqnum,
            interface,
            setting,
            state: State::Init {
                handle: Some(Arc::clone(self)),
            },
        }
    }

    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn set_config_async(&self, seqnum: u32, config: u8) -> SetConfig {
        SetConfig {
            seqnum,
            config,
            state: State::Init {
                handle: Some(Arc::clone(self)),
            },
        }
    }

    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn clear_stall_async(&self, seqnum: u32, endpoint: u8) -> ClearStall {
        ClearStall {
            seqnum,
            endpoint,
            state: State::Init {
                handle: Some(Arc::clone(self)),
            },
        }
    }
}

pub struct SetInterface {
    seqnum: u32,
    interface: u8,
    setting: u8,
    state: State,
}

impl Future for SetInterface {
    type Output = Seq<Result<(), TransferError>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            self.state = match self.state {
                State::Init { ref mut handle } => {
                    let device = handle.take().unwrap();
                    let interface = self.interface;
                    let setting = self.setting;
                    let span = Span::current();
                    let join = compio_runtime::spawn_blocking(move || {
                        let _enter = span.entered();
                        device
                            .set_alt_setting(interface, setting)
                            .inspect_err(|_| error!("{}", std::io::Error::last_os_error()))
                    });
                    State::Waiting { join }
                }
                State::Waiting { ref mut join } => match ready!(pin!(join).poll(cx)).unwrap() {
                    Ok(_) => {
                        // debug!(
                        //     "({}) set alternate setting {} for interface {}",
                        //     self.seqnum, self.setting, self.interface
                        // );
                        State::Complete(Ok(()))
                    }
                    Err(rusb::Error::NotFound) => todo!(),
                    Err(rusb::Error::NoDevice) => todo!(),
                    Err(err) => {
                        warn! {
                            %err,
                            "({}) couldn't set alternate setting {} for interface {}",
                            self.seqnum,
                            self.setting,
                            self.interface,
                        };
                        State::Complete(Err(TransferError::Reportable(proto::ReportableError::Stall)))
                    }
                },
                State::Complete(status) => {
                    break Poll::Ready(Seq {
                        seqnum: self.seqnum,
                        data: status,
                    });
                }
            };
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SetConfig {
    seqnum: u32,
    config: u8,
    state: State,
}

impl Future for SetConfig {
    type Output = Seq<Result<(), TransferError>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            self.state = match self.state {
                State::Init { ref mut handle } => {
                    let handle = handle.take().unwrap();
                    let config = self.config;
                    let join = compio_runtime::spawn_blocking(move || handle.set_config(config));
                    State::Waiting { join }
                }
                State::Waiting { ref mut join } => match ready!(pin!(join).poll(cx)).unwrap() {
                    Ok(_) => {
                        // debug!("({}) set config {}", self.seqnum, self.config);
                        State::Complete(Ok(()))
                    }
                    Err(err) => {
                        warn!(%err, "({}) couldn't set configuration", self.seqnum);
                        State::Complete(Err(TransferError::Reportable(proto::ReportableError::Stall)))
                    }
                },
                State::Complete(status) => {
                    break Poll::Ready(Seq {
                        seqnum: self.seqnum,
                        data: status,
                    });
                }
            }
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ClearStall {
    seqnum: u32,
    endpoint: u8,
    state: State,
}

impl Future for ClearStall {
    type Output = Seq<Result<(), TransferError>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            self.state = match self.state {
                State::Init { ref mut handle } => {
                    let handle = handle.take().unwrap();
                    let endpoint = self.endpoint;
                    let join = compio_runtime::spawn_blocking(move || {
                        handle.as_device().clear_halt(endpoint)
                    });
                    State::Waiting { join }
                }
                State::Waiting { ref mut join } => match ready!(pin!(join).poll(cx)).unwrap() {
                    Ok(_) => State::Complete(Ok(())),
                    Err(err) => {
                        warn! { %err, "({}) couldn't clear stall for endpoint {}", self.seqnum, self.endpoint };
                        State::Complete(Err(TransferError::Reportable(proto::ReportableError::Stall)))
                    }
                },
                State::Complete(status) => {
                    break Poll::Ready(Seq {
                        seqnum: self.seqnum,
                        data: status,
                    });
                }
            }
        }
    }
}
