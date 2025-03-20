use futures_util::sink::SinkExt;
use futures_concurrency::stream::Merge;
use futures_lite::StreamExt;
use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
};

use tracing::{error, info, trace, warn};
use vhci::{
    DataRate, Port, PortChange, PortStatus,
    ioctl::{Address, UrbHandle},
    usbfs::Request,
    utils::{BoundedI16, BoundedU8, TimeoutMillis},
};

use crate::utils::{Ctrl, LinkResult, NoHash, ThreeKeyMap, mpsc, oneshot};

pub type RegisterPayload = (Port, WorkReceiver);

/// Specifies which port to plug virtual USB device into.
pub enum RegisterPort {
    Any,
    Port(Port),
}

/// Request struct to register a new
/// virtual USB device with the VHCI.
pub struct Register {
    port: RegisterPort,
    data_rate: DataRate,
}

pub type WorkReceiver = mpsc::AsyncReceiver<vhci::ioctl::Work>;
type Mailer = ThreeKeyMap<Port, NoHash<Address>, UrbHandle, mpsc::AsyncSender<vhci::ioctl::Work>>;

type RegisterResult = (
    io::Result<Port>,
    oneshot::Sender<io::Result<RegisterPayload>>,
);

type DisconnectResult = (io::Result<()>, oneshot::Sender<io::Result<()>>);

enum ControllerResult {
    Register(RegisterResult),
    Disconnect(DisconnectResult),
}

enum Event {
    Register(Ctrl<Register, RegisterPayload>),
    GivebackUrb(UrbHandle),
    Disconnect(Ctrl<Port>),
    Work(vhci::ioctl::IocWork),
    Controller(ControllerResult),
}

struct Demuxer {
    register_rx: mpsc::AsyncReceiver<Ctrl<Register, RegisterPayload>>,
    giveback_rx: mpsc::AsyncReceiver<UrbHandle>,
    disconnect_rx: mpsc::AsyncReceiver<Ctrl<Port>>,
    vhci: vhci::Controller,
}

impl Demuxer {
    fn new(
        register_rx: mpsc::AsyncReceiver<Ctrl<Register, RegisterPayload>>,
        giveback_rx: mpsc::AsyncReceiver<UrbHandle>,
        disconnect_rx: mpsc::AsyncReceiver<Ctrl<Port>>,
        vhci: vhci::Controller,
    ) -> Self {
        Self {
            register_rx,
            giveback_rx,
            disconnect_rx,
            vhci,
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn demux(self) {
        let Self {
            register_rx,
            giveback_rx,
            disconnect_rx,
            mut vhci,
        } = self;

        let mut register_queue: VecDeque<Port> = VecDeque::new();
        let mut mailer = Mailer::with_capacities(8, 8, 10, 1024);
        let (work_tx, work_rx) = mpsc::channel(32);
        let work_rx: mpsc::AsyncReceiver<vhci::ioctl::IocWork> = work_rx.into_stream();
        let vhci_work_rx = vhci.work_receiver().unwrap();
        let vhci = Arc::new(Mutex::new(vhci));
        let (controller_tx, controller_rx) = mpsc::channel::<ControllerResult>(0);

        let reg = register_rx.map(Event::Register);
        let giveback = giveback_rx.map(Event::GivebackUrb);
        let disconnect = disconnect_rx.map(Event::Disconnect);
        let work = work_rx.map(Event::Work);
        let controller = controller_rx.into_stream().map(Event::Controller);

        let mut events = (reg, giveback, disconnect, work, controller).merge();
        let handle = compio_runtime::spawn_blocking(move || recv_work(vhci_work_rx, work_tx));
        while let Some(event) = events.next().await {
            match event {
                Event::Register(Ctrl {
                    data:
                        Register {
                            port: RegisterPort::Any,
                            data_rate,
                        },
                    tx,
                }) => {
                    let vhci = Arc::clone(&vhci);
                    let controller_tx = controller_tx.clone();
                    compio_runtime::spawn_blocking(move || {
                        let result = vhci.lock().unwrap().port_connect_any(data_rate);
                        let msg = ControllerResult::Register((result, tx));
                        _ = controller_tx.send(msg);
                    })
                    .detach();
                }
                Event::Register(Ctrl {
                    data:
                        Register {
                            port: RegisterPort::Port(port),
                            data_rate,
                        },
                    tx,
                }) => {
                    let vhci = Arc::clone(&vhci);
                    let controller_tx = controller_tx.clone();
                    compio_runtime::spawn_blocking(move || {
                        let result = vhci.lock().unwrap().port_connect(port, data_rate);
                        let result = result.map(|_| port);
                        let msg = ControllerResult::Register((result, tx));
                        _ = controller_tx.send(msg);
                    })
                    .detach();
                }
                Event::GivebackUrb(handle) => {
                    _ = mailer.remove_by_key3(&handle);
                }
                Event::Disconnect(Ctrl { data: port, tx }) => {
                    let vhci = Arc::clone(&vhci);
                    let controller_tx = controller_tx.clone();
                    compio_runtime::spawn_blocking(move || {
                        let result = vhci.lock().unwrap().port_disconnect(port);
                        let msg = ControllerResult::Disconnect((result, tx));
                        _ = controller_tx.send(msg);
                    })
                    .detach();
                }
                Event::Work(ioc_work) => {
                    // SAFETY: Per the function's safety contract,
                    //         the work item is unaltered from the
                    //         ioctl call.
                    let work = unsafe { ioc_work.into_inner() };
                    let tx = {
                        let queue = &mut register_queue;
                        match work {
                            vhci::ioctl::Work::PortStat(ref stat) => {
                                if stat.change().contains(PortChange::RESET)
                                    && (!stat.status()).contains(PortStatus::RESET)
                                    && stat.status().contains(PortStatus::ENABLE)
                                    && !queue.contains(&stat.index())
                                {
                                    queue.push_back(stat.index());
                                    mailer.unlink_all_but_key1(&stat.index());
                                }
                                mailer.get_by_key1(&stat.index()).cloned()
                            }
                            vhci::ioctl::Work::ProcessUrb((ref urb, ref handle)) => match urb.typ {
                                vhci::ioctl::UrbType::Ctrl
                                    if urb.address.is_for_unassigned()
                                        && urb.endpoint.is_anycast()
                                        && !queue.is_empty()
                                        && Request::STANDARD_DEVICE_SET_ADDRESS
                                            == urb.setup_packet.req() =>
                                {
                                    // TODO: The logic around here is all messed up lmao
                                    let address = Address::new(urb.setup_packet.value() as u8)
                                        .expect("host should've assigned value address");
                                    let port = queue.pop_front().unwrap();

                                    mailer.remove_by_key2(&NoHash(Address::new(0).unwrap()));
                                    mailer.link_key2_to_key1(NoHash(address), &port);
                                    assert_eq!(
                                        LinkResult::Success,
                                        mailer.link_key3_to_key1(*handle, &port),
                                    );
                                    mailer.get_by_key1(&port).cloned()
                                }
                                vhci::ioctl::UrbType::Ctrl
                                    if urb.address.is_for_unassigned() && !queue.is_empty() =>
                                {
                                    let port = queue.front().copied().unwrap();
                                    assert_eq!(
                                        LinkResult::Success,
                                        mailer.link_key3_to_key1(*handle, &port),
                                    );
                                    mailer.get_by_key1(&port).cloned()
                                }
                                _ => {
                                    let address = NoHash(urb.address);
                                    match mailer.link_key3_to_key2(*handle, &address) {
                                        LinkResult::Success | LinkResult::NewKeyAlreadyExists => {
                                            mailer.get_by_key2(&address).cloned()
                                        }
                                        LinkResult::ExistingKeyDoesNotExist => panic!(
                                            "key {address:?} does not exist, was trying to link {handle:?}"
                                        ),
                                    }
                                }
                            },
                            vhci::ioctl::Work::CancelUrb(ref handle) => {
                                let tx = mailer.get_by_key3(handle).cloned();
                                mailer.remove_by_key3(handle);
                                tx
                            }
                        }
                    };

                    if let Some(mut tx) = tx {
                        if tx.send(work).await.is_err() {
                            // TODO: Remove by which key? By the value itself?
                        }
                    } else {
                        trace!("received work item that belonged to no port: {work:?}");
                    }
                }
                Event::Controller(ControllerResult::Register((Ok(port), tx))) => {
                    let (work_tx, work_rx) = mpsc::channel(32);
                    assert!(
                        mailer.insert_by_key1(port, work_tx.into_sink()).is_none(),
                        "{port:?}"
                    );
                    if tx.send(Ok((port, work_rx.into_stream()))).is_err() {
                        mailer.remove_by_key1(&port);
                    }
                }
                Event::Controller(ControllerResult::Register((Err(err), tx))) => {
                    _ = tx.send(Err(err));
                }
                Event::Controller(ControllerResult::Disconnect((result, tx))) => {
                    _ = tx.send(result);
                }
            }
        }

        info!("disconnecting leftover devices and closing work_rx");
        mailer.key1_iter().for_each(|port| {
            let mut vhci = vhci.lock().unwrap();
            if let Err(err) = vhci.port_disconnect(*port) {
                warn!("error while disconnecting vhci device: {err}");
            }
        });

        info!("waiting on recv_work thread");
        drop(events);
        match handle.await {
            Ok(Err(err)) => warn!(%err, "vhci_recv_work thread failed"),
            Err(err) => error!("vhci_recv_work thread panicked?? {err:?}"),
            _ => (),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    handle: Option<Arc<compio_runtime::JoinHandle<()>>>,
    register_tx: mpsc::AsyncSender<Ctrl<Register, (Port, WorkReceiver)>>,
    giveback_tx: mpsc::AsyncSender<UrbHandle>,
    disconnect_tx: mpsc::AsyncSender<Ctrl<Port>>,
    remote: vhci::Remote,
}

impl Controller {
    pub fn start(num_ports: BoundedU8<1, 32>) -> io::Result<Self> {
        let (register_tx, register_rx) = mpsc::channel(0);
        let (giveback_tx, giveback_rx) = mpsc::channel(512);
        let (disconnect_tx, disconnect_rx) = mpsc::channel(0);
        let vhci = vhci::Controller::open(num_ports)?;
        let remote = vhci.remote();
        let handle = Some(Arc::new(compio_runtime::spawn(
            Demuxer::new(
                register_rx.into_stream(),
                giveback_rx.into_stream(),
                disconnect_rx.into_stream(),
                vhci,
            )
            .demux(),
        )));

        Ok(Self {
            handle,
            register_tx: register_tx.into_sink(),
            giveback_tx: giveback_tx.into_sink(),
            disconnect_tx: disconnect_tx.into_sink(),
            remote,
        })
    }

    pub async fn register(
        &mut self,
        port: RegisterPort,
        data_rate: DataRate,
    ) -> io::Result<(Port, WorkReceiver)> {
        let (rx, register) = Ctrl::new(Register { port, data_rate });

        // Let the work runner know that a new port needs enumeration.
        self.register_tx.send(register).await.unwrap();
        rx.await.unwrap()
    }

    #[inline]
    pub fn fetch_data<T: vhci::Urb + vhci::IsoPacketDataMut + vhci::TransferMut>(
        &self,
        urb: T,
    ) -> io::Result<()> {
        self.remote.fetch_data(urb)
    }

    #[inline]
    pub async fn giveback_urb<T: vhci::Urb + vhci::IsoPacketGivebackMut + vhci::TransferMut>(
        &mut self,
        urb: T,
    ) -> io::Result<()> {
        _ = self.giveback_tx.send(urb.handle()).await;
        self.remote.giveback(urb)
    }

    #[inline]
    pub async fn disconnect(&mut self, port: Port) -> io::Result<()> {
        let (rx, disconnect) = Ctrl::new(port);

        self.disconnect_tx.send(disconnect).await.unwrap();
        rx.await.unwrap()
    }

    #[inline(always)]
    pub fn reset_done(&self, port: Port, enable: bool) -> io::Result<()> {
        self.remote.port_reset_done(port, enable)
    }

    #[inline]
    pub fn remote(&self) -> VhciRemote {
        VhciRemote {
            remote: self.remote.clone(),
            giveback_tx: self.giveback_tx.clone(),
        }
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take().and_then(Arc::into_inner) {
            info!("about to wait for vhci controller handle");
            handle.detach();
        }
    }
}

#[derive(Debug, Clone)]
pub struct VhciRemote {
    remote: vhci::Remote,
    giveback_tx: mpsc::AsyncSender<UrbHandle>,
}

impl VhciRemote {
    #[inline]
    pub fn fetch_data<T>(&self, urb: T) -> io::Result<()>
    where
        T: vhci::Urb + vhci::IsoPacketDataMut + vhci::TransferMut,
    {
        self.remote.fetch_data(urb)
    }

    #[inline]
    pub async fn giveback_urb<T>(&mut self, urb: T) -> io::Result<()>
    where
        T: vhci::Urb + vhci::IsoPacketGivebackMut + vhci::TransferMut + Send + Sync + 'static,
    {
        _ = self.giveback_tx.send(urb.handle()).await;
        let remote = self.remote.clone();
        let op = compio_runtime::spawn_blocking(move || remote.giveback(urb));
        op.await.unwrap()
    }

    #[inline(always)]
    pub fn reset_done(&self, port: Port, enable: bool) -> io::Result<()> {
        self.remote.port_reset_done(port, enable)
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn recv_work(
    work_rx: vhci::WorkReceiver,
    work_tx: mpsc::Sender<vhci::ioctl::IocWork>,
) -> io::Result<()> {
    if work_tx.is_disconnected() {
        info!("done with receiving work for vhci");
        return Ok(());
    }

    const TIMEOUT: TimeoutMillis = TimeoutMillis::Time(BoundedI16::new(999).unwrap());
    while (match work_rx.fetch_work_timeout(TIMEOUT) {
        Ok(work) => work_tx.send(work).ok(),
        Err(err)
            if err.kind() == io::ErrorKind::TimedOut
                || err.kind() == io::ErrorKind::Interrupted
                || err
                    .raw_os_error()
                    .is_some_and(|err| err == vhci::libc::ENODATA) =>
        {
            (!work_tx.is_disconnected()).then_some(())
        }
        Err(err) => return Err(err),
    })
    .is_some()
    {}

    info!("done with receiving work for vhci");
    Ok(())
}

#[cfg(test)]
mod tests {
    // use tracing::{debug, warn};
    // use vhci::{libc, PortChange, PortFlag, PortStatus};

    // use super::*;
    // use std::{thread, time::Duration};
    // use std::time::Instant;

    // #[tokio::test]
    // #[tracing_test::traced_test]
    // async fn can_listen_for_work() {
    //     let mut controller = Controller::start(BoundedU8::new(8).unwrap()).unwrap();

    //     let mut prev_stat = vhci::ioctl::IocPortStat::default();
    //     let mut addr = 0xff;
    //     let (port, mut work_rx) = controller
    //         .register(RegisterPort::Any, DataRate::Full)
    //         .await
    //         .unwrap();

    //     trace!("connected {port:?}, starting process for registering");
    //     let start = Instant::now();
    //     while Duration::from_secs(10) > start.elapsed() {
    //         debug!("==============================================");
    //         match work_rx.recv().await {
    //             Some(vhci::ioctl::Work::PortStat(next_stat)) => {
    //                 debug!("got port stat for {:?}", next_stat.index());
    //                 debug!("status: {:?}", next_stat.status());
    //                 debug!("change: {:?}", next_stat.change());
    //                 debug!("index: {:?}", next_stat.index());
    //                 debug!("flags: {:?}", next_stat.flags());
    //                 if next_stat.change().contains(PortChange::CONNECTION) {
    //                     trace!("CONNECTION state changed -> invalidating address");
    //                     addr = 0xff;
    //                 }
    //                 if next_stat.change().contains(PortChange::RESET)
    //                     && (!next_stat.status()).contains(PortStatus::RESET)
    //                     && next_stat.status().contains(PortStatus::ENABLE)
    //                 {
    //                     trace!("RESET successful -> use default address");
    //                     addr = 0;
    //                 }
    //                 if prev_stat.status().contains(PortStatus::POWER)
    //                     && (!next_stat.status()).contains(PortStatus::POWER)
    //                 {
    //                     trace!("port is powered off");
    //                 }
    //                 if (!prev_stat.status()).contains(PortStatus::RESET)
    //                     && next_stat
    //                         .status()
    //                         .contains(PortStatus::RESET | PortStatus::CONNECTION)
    //                 {
    //                     trace!("port is resetting -> completing reset");
    //                     controller.reset_done(next_stat.index(), true).unwrap();
    //                 }
    //                 if (!prev_stat.flags()).contains(PortFlag::RESUMING)
    //                     && next_stat.flags().contains(PortFlag::RESUMING)
    //                     && next_stat.status().contains(PortStatus::CONNECTION)
    //                 {
    //                     trace!("port is resuming -> completing resume");
    //                     todo!("do the actual resume thing");
    //                 }
    //                 prev_stat = next_stat;
    //             }
    //             Some(vhci::ioctl::Work::ProcessUrb((urb, handle))) => {
    //                 debug!(
    //                     "got process urb for {:?} at {:?}",
    //                     urb.address, urb.endpoint
    //                 );
    //                 if addr != urb.address.get() {
    //                     warn!("not for usb device at {port:?}. skipping.");
    //                     continue;
    //                 }

    //                 let mut urb = UrbWithData::from_ioctl(urb, handle);
    //                 if urb.needs_data_fetch() {
    //                     match controller.fetch_data(&mut urb) {
    //                         Ok(_) => {}
    //                         Err(err)
    //                             if err
    //                                 .raw_os_error()
    //                                 .is_some_and(|errno| libc::ECANCELED == errno) => {}
    //                         Err(err) => Err(err).unwrap(),
    //                     }
    //                 }

    //                 if vhci::ioctl::UrbType::Ctrl == urb.kind()
    //                     && urb.endpoint().is_anycast()
    //                     && Request::STANDARD_DEVICE_SET_ADDRESS == urb.control_packet().req()
    //                 {
    //                     if let Some(new_addr) =
    //                         Address::new(urb.control_packet().value().try_into().unwrap())
    //                     {
    //                         urb.set_status(vhci::Status::Success);
    //                         addr = new_addr.get();
    //                         trace!("SET_ADDRESS (addr={:#x})", addr);
    //                     }
    //                 }

    //                 urb.set_status(vhci::Status::Stall);
    //                 controller.giveback_urb(urb).await.unwrap();
    //                 break;
    //             }
    //             Some(vhci::ioctl::Work::CancelUrb(_handle)) => unreachable!(),
    //             None => break,
    //         }
    //     }

    //     trace!("disconnecting {port:?}");
    //     controller.disconnect(port).await.unwrap();
    //     trace!("shutting down VHCI controller");
    //     controller.shutdown().await;
    // }
}
