use std::{
    future::Future,
    pin::pin,
    sync::{Arc, Mutex},
    task::{ready, Poll},
};

use nohash_hasher::IntSet;
use tracing::{debug, trace, warn};

use crate::operator::{is_config_active, Seq};

pub trait BlockingOps {
    fn set_alt_setting_async(&self, seqnum: u32, interface: u8, setting: u8) -> SetInterface;
    fn set_config_async(
        &self,
        seqnum: u32,
        config: u8,
        interfaces: Arc<Mutex<IntSet<u8>>>,
    ) -> SetConfig;
    fn clear_stall_async(&self, seqnum: u32, endpoint: u8) -> ClearStall;
}

enum State {
    Init {
        handle: Option<Arc<rusb::DeviceHandle<rusb::Context>>>,
    },
    Waiting {
        join: compio::runtime::JoinHandle<rusb::Result<()>>,
    },
    Complete(vhci::Status),
}

impl BlockingOps for Arc<rusb::DeviceHandle<rusb::Context>> {
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
    fn set_config_async(
        &self,
        seqnum: u32,
        config: u8,
        interfaces: Arc<Mutex<IntSet<u8>>>,
    ) -> SetConfig {
        if is_config_active(self, config) {
            trace!("({seqnum}) config {config} is already set");
            SetConfig {
                seqnum,
                config,
                interfaces: Some(interfaces),
                state: State::Complete(vhci::Status::Success),
            }
        } else {
            SetConfig {
                seqnum,
                config,
                interfaces: Some(interfaces),
                state: State::Init {
                    handle: Some(Arc::clone(self)),
                },
            }
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
    type Output = Seq<vhci::Status>;

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
                    let join = compio::runtime::spawn_blocking(move || {
                        device.set_alternate_setting(interface, setting)
                    });
                    State::Waiting { join }
                }
                State::Waiting { ref mut join } => match ready!(pin!(join).poll(cx)).unwrap() {
                    Ok(_) => State::Complete(vhci::Status::Success),
                    Err(rusb::Error::NotFound) => todo!(),
                    Err(rusb::Error::NoDevice) => todo!(),
                    Err(err) => {
                        warn! {
                            %err,
                            "({}) couldn't set alternate setting {} for interface {}",
                            self.seqnum,
                            self.setting,
                            self.interface,
                        }
                        State::Complete(vhci::Status::Stall)
                    }
                },
                State::Complete(status) => {
                    break Poll::Ready(Seq {
                        seqnum: self.seqnum,
                        data: status,
                    })
                }
            };
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SetConfig {
    seqnum: u32,
    config: u8,
    interfaces: Option<Arc<Mutex<IntSet<u8>>>>,
    state: State,
}

impl Future for SetConfig {
    type Output = Seq<vhci::Status>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            self.state = match self.state {
                State::Init { ref mut handle } => {
                    let handle = handle.take().unwrap();
                    let config = self.config;
                    let interfaces = self.interfaces.take().unwrap();
                    let join = compio::runtime::spawn_blocking(move || {
                        handle.set_active_configuration(config)?;
                        let mut claimed_interfaces = interfaces.lock().unwrap();
                        for interface in 0..16 {
                            if claimed_interfaces.insert(interface)
                                && handle.claim_interface(interface).is_ok()
                            {
                                // Debug if needed
                            }
                        }
                        if !is_config_active(&handle, config) {
                            handle.set_active_configuration(config)?;
                        }
                        Ok(())
                    });
                    State::Waiting { join }
                }
                State::Waiting { ref mut join } => match ready!(pin!(join).poll(cx)).unwrap() {
                    Ok(_) => {
                        debug!("({}) set config {}", self.seqnum, self.config);
                        State::Complete(vhci::Status::Success)
                    }
                    Err(err) => {
                        warn!(%err, "({}) couldn't set configuration", self.seqnum);
                        State::Complete(vhci::Status::Stall)
                    }
                },
                State::Complete(status) => {
                    break Poll::Ready(Seq {
                        seqnum: self.seqnum,
                        data: status,
                    })
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
    type Output = Seq<vhci::Status>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            self.state = match self.state {
                State::Init { ref mut handle } => {
                    let handle = handle.take().unwrap();
                    let endpoint = self.endpoint;
                    let join = compio::runtime::spawn_blocking(move || handle.clear_halt(endpoint));
                    State::Waiting { join }
                }
                State::Waiting { ref mut join } => match ready!(pin!(join).poll(cx)).unwrap() {
                    Ok(_) => State::Complete(vhci::Status::Success),
                    Err(err) => {
                        warn! { %err, "({}) couldn't clear stall for endpoint {}", self.seqnum, self.endpoint };
                        State::Complete(vhci::Status::Stall)
                    }
                },
                State::Complete(status) => {
                    break Poll::Ready(Seq {
                        seqnum: self.seqnum,
                        data: status,
                    })
                }
            }
        }
    }
}
