use std::{
    collections::VecDeque,
    io,
    pin::pin,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;
use tracing::{info, trace, warn};
use vhci::{
    ioctl::{Address, UrbHandle},
    usbfs::Request,
    utils::{BoundedI16, BoundedU8, TimeoutMillis},
    DataRate, Port, PortChange, PortStatus,
};

use crate::utils::{Ctrl, LinkResult, NoHash, ThreeKeyMap};

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

pub type WorkReceiver = Receiver<vhci::ioctl::Work>;
type Mailer = ThreeKeyMap<Port, NoHash<Address>, UrbHandle, Sender<vhci::ioctl::Work>>;
type Receiver<T> = kanal::AsyncReceiver<T>;
type Sender<T> = kanal::AsyncSender<T>;

struct Demuxer {
    register_rx: Receiver<Ctrl<Register, RegisterPayload>>,
    giveback_rx: Receiver<UrbHandle>,
    disconnect_rx: Receiver<Ctrl<Port>>,
    vhci: vhci::Controller,
}

impl Demuxer {
    fn new(
        register_rx: Receiver<Ctrl<Register, RegisterPayload>>,
        giveback_rx: Receiver<UrbHandle>,
        disconnect_rx: Receiver<Ctrl<Port>>,
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
    async fn demux_vhci(self) -> io::Result<()> {
        let Self {
            register_rx,
            giveback_rx,
            disconnect_rx,
            mut vhci,
        } = self;

        let register_queue = Mutex::new(VecDeque::new());
        let mailer = Mutex::new(Mailer::with_capacities(8, 8, 10, 1024));
        let (work_tx, work_rx) = kanal::bounded(0);
        let work_rx = work_rx.to_async();
        let work_receiver = vhci.work_receiver().unwrap();
        let handle = std::thread::spawn(move || recv_work(work_receiver, work_tx));

        struct Context {
            vhci: Mutex<vhci::Controller>,
            mailer: Mutex<Mailer>,
            register_queue: Mutex<VecDeque<Port>>,
        }

        type RegisterResult = (
            io::Result<Port>,
            oneshot::Sender<io::Result<RegisterPayload>>,
        );

        type DisconnectResult = (io::Result<()>, oneshot::Sender<io::Result<()>>);

        enum Continuation {
            Register(RegisterResult),
            Disconnect(DisconnectResult),
        }

        enum Event {
            Register(Option<Ctrl<Register, RegisterPayload>>),
            GivebackUrb(Option<UrbHandle>),
            Disconnect(Option<Ctrl<Port>>),
            Work(vhci::ioctl::IocWork),
            Task(Option<Result<Continuation, tokio::task::JoinError>>),
        }

        let ctx = Arc::new(Context {
            vhci: Mutex::new(vhci),
            mailer,
            register_queue,
        });

        let mut set = tokio::task::JoinSet::new();

        let mut register = pin!(register_rx.recv());
        let mut giveback = pin!(giveback_rx.recv());
        let mut disconnect = pin!(disconnect_rx.recv());
        let mut work = pin!(work_rx.recv());

        loop {
            let events_in_progress = !set.is_empty();
            let event = tokio::select! {
                req = &mut register => {
                    register.set(register_rx.recv());
                    Event::Register(req.ok())
                }
                handle = &mut giveback => {
                    giveback.set(giveback_rx.recv());
                    Event::GivebackUrb(handle.ok())
                }
                req = &mut disconnect => {
                    disconnect.set(disconnect_rx.recv());
                    Event::Disconnect(req.ok())
                }
                new_work = &mut work => {
                    work.set(work_rx.recv());
                    Event::Work(new_work.ok().expect("we can only get here if we shutdown in the wrong order"))
                }
                task = set.join_next(), if events_in_progress => {
                    Event::Task(task)
                }
            };
            match event {
                Event::Register(Some(Ctrl {
                    data:
                        Register {
                            port: RegisterPort::Any,
                            data_rate,
                        },
                    tx,
                })) => {
                    let ctx = Arc::clone(&ctx);
                    set.spawn_blocking(move || {
                        Continuation::Register((
                            ctx.vhci.lock().unwrap().port_connect_any(data_rate),
                            tx,
                        ))
                    });
                }
                Event::Register(Some(Ctrl {
                    data:
                        Register {
                            port: RegisterPort::Port(port),
                            data_rate,
                        },
                    tx,
                })) => {
                    let ctx = Arc::clone(&ctx);
                    set.spawn_blocking(move || {
                        Continuation::Register((
                            ctx.vhci
                                .lock()
                                .unwrap()
                                .port_connect(port, data_rate)
                                .map(|_| port),
                            tx,
                        ))
                    });
                }
                Event::GivebackUrb(Some(handle)) => {
                    _ = ctx.mailer.lock().unwrap().remove_by_key3(&handle);
                }
                Event::Disconnect(Some(Ctrl { data: port, tx })) => {
                    let ctx = Arc::clone(&ctx);
                    set.spawn_blocking(move || {
                        {
                            let mut mailer = ctx.mailer.lock().unwrap();
                            mailer.unlink_all_but_key1(&port);
                            mailer.remove_by_key1(&port);
                        }
                        let mut queue = ctx.register_queue.lock().unwrap();
                        if let Some(index) = queue.iter().position(|queue_port| port == *queue_port)
                        {
                            queue.remove(index);
                        }
                        let result = (ctx.vhci.lock().unwrap().port_disconnect(port), tx);
                        Continuation::Disconnect(result)
                    });
                }
                Event::Work(ioc_work) => {
                    // SAFETY: Per the function's safety contract,
                    //         the work item is unaltered from the
                    //         ioctl call.
                    let work = unsafe { ioc_work.into_inner() };
                    let tx = {
                        let mut queue = ctx.register_queue.lock().unwrap();
                        let mut mailer = ctx.mailer.lock().unwrap();
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

                    if let Some(tx) = tx {
                        if tx.send(work).await.is_err() {
                            // TODO: Remove by which key? By the value itself?
                        }
                    } else {
                        trace!("received work item that belonged to no port: {work:?}");
                    }
                }
                Event::Task(Some(Ok(Continuation::Register((Ok(port), tx))))) => {
                    let (work_tx, work_rx) = kanal::bounded_async(32);
                    let mut mailer = ctx.mailer.lock().unwrap();
                    assert!(mailer.insert_by_key1(port, work_tx).is_none(), "{port:?}");
                    if tx.send(Ok((port, work_rx))).is_err() {
                        mailer.remove_by_key1(&port);
                    }
                }
                Event::Task(Some(Ok(Continuation::Register((Err(err), tx))))) => {
                    _ = tx.send(Err(err));
                }
                Event::Task(Some(Ok(Continuation::Disconnect((result, tx))))) => {
                    _ = tx.send(result);
                }
                Event::Task(Some(Err(_err))) => todo!(),
                Event::Register(None) | Event::Disconnect(None) | Event::GivebackUrb(None) => break,
                Event::Task(None) => (),
            };
        }

        info!("disconnecting leftover devices and closing work_rx");
        ctx.mailer.lock().unwrap().key1_iter().for_each(|port| {
            let mut vhci = ctx.vhci.lock().unwrap();
            if let Err(err) = vhci.port_disconnect(*port) {
                warn!("error while disconnecting vhci device: {err}");
            }
        });
        work_rx.close();
        while (work_rx.recv().await).is_ok() {}
        // drop(work_rx);

        info!("waiting on recv_work thread");
        let waiter = || handle.join().expect("recv thread should not panic");
        _ = tokio::task::spawn_blocking(waiter).await;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    handle: Option<Arc<tokio::task::JoinHandle<io::Result<()>>>>,
    register_tx: Sender<Ctrl<Register, (Port, WorkReceiver)>>,
    giveback_tx: Sender<UrbHandle>,
    disconnect_tx: Sender<Ctrl<Port>>,
    remote: vhci::Remote,
}

impl Controller {
    pub fn start(num_ports: BoundedU8<1, 32>) -> io::Result<Self> {
        let (register_tx, register_rx) = kanal::bounded_async(0);
        let (giveback_tx, giveback_rx) = kanal::bounded_async(0);
        let (disconnect_tx, disconnect_rx) = kanal::bounded_async(0);
        let vhci = vhci::Controller::open(num_ports)?;
        let remote = vhci.remote();
        let handle = Some(Arc::new(tokio::task::spawn(
            Demuxer::new(register_rx, giveback_rx, disconnect_rx, vhci).demux_vhci(),
        )));

        Ok(Self {
            handle,
            register_tx,
            giveback_tx,
            disconnect_tx,
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
        &self,
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
            let rt = tokio::runtime::Handle::current();
            rt.spawn(async move {
                match handle.await {
                    Ok(Ok(_)) => trace!("controller thread finished with no issues"),
                    Ok(Err(_err)) => unimplemented!("i/o error? {_err}"),
                    Err(_err) => unimplemented!("thread panicked? {_err}"),
                }
            });
        }
    }
}

#[derive(Debug, Clone)]
pub struct VhciRemote {
    remote: vhci::Remote,
    giveback_tx: Sender<UrbHandle>,
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
    pub async fn giveback_urb<T>(&self, urb: T) -> io::Result<()>
    where
        T: vhci::Urb + vhci::IsoPacketGivebackMut + vhci::TransferMut + Send + 'static,
    {
        _ = self.giveback_tx.send(urb.handle()).await;
        let remote = self.remote.clone();
        tokio::task::spawn_blocking(move || remote.giveback(urb));
        Ok(())
    }

    #[inline(always)]
    pub fn reset_done(&self, port: Port, enable: bool) -> io::Result<()> {
        self.remote.port_reset_done(port, enable)
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn recv_work(
    work_rx: vhci::WorkReceiver,
    work_tx: kanal::Sender<vhci::ioctl::IocWork>,
) -> io::Result<()> {
    if work_tx.is_closed() {
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
            (!work_tx.is_closed()).then_some(())
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

    use super::*;
    use std::{thread, time::Duration};
    // use std::time::Instant;

    #[tokio::test]
    async fn controller_idles() {
        let _controller = Controller::start(BoundedU8::new(3).unwrap()).unwrap();

        thread::sleep(Duration::from_millis(500));
    }

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
