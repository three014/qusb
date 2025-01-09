use std::{
    borrow::Cow,
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use fxhash::FxHashSet;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};
use vhci::{
    ioctl::{Address, UrbHandle},
    usbfs::STANDARD_DEVICE_SET_ADDRESS,
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

pub type WorkReceiver = mpsc::Receiver<vhci::ioctl::Work>;
type Mailer = ThreeKeyMap<Port, NoHash<Address>, UrbHandle, mpsc::Sender<vhci::ioctl::Work>>;

struct Demuxer {
    register_rx: mpsc::Receiver<Ctrl<Register, RegisterPayload>>,
    giveback_rx: mpsc::Receiver<UrbHandle>,
    disconnect_rx: mpsc::Receiver<Ctrl<Port>>,
    vhci: vhci::Controller,
}

impl Demuxer {
    fn new(
        register_rx: mpsc::Receiver<Ctrl<Register, RegisterPayload>>,
        giveback_rx: mpsc::Receiver<UrbHandle>,
        disconnect_rx: mpsc::Receiver<Ctrl<Port>>,
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
    async fn run(self) -> io::Result<()> {
        let Self {
            mut register_rx,
            mut giveback_rx,
            mut disconnect_rx,
            mut vhci,
        } = self;

        let register_queue = VecDeque::new();
        let mailer = Mailer::with_capacities(8, 8, 10, 67);
        let (work_tx, mut work_rx) = mpsc::channel(64);
        let work_receiver = vhci.work_receiver().unwrap();
        let handle = std::thread::spawn(move || recv_work(work_receiver, work_tx));

        let mut dbg_register_complete_bucket = FxHashSet::with_hasher(Default::default());
        let mut dbg_register_inprogress_bucket = FxHashSet::with_hasher(Default::default());
        let mut dbg_urb_bucket = FxHashSet::with_hasher(Default::default());

        struct Context {
            vhci: vhci::Controller,
            mailer: Mailer,
            register_queue: VecDeque<Port>,
        }

        enum Event {
            Register(Option<Ctrl<Register, RegisterPayload>>),
            GivebackUrb(Option<UrbHandle>),
            Disconnect(Option<Ctrl<Port>>),
            Work(vhci::ioctl::IocWork),
        }

        struct Ctx {
            ctx: Box<Context>,
            cont: Continuation,
        }

        type RegisterResult = (
            io::Result<Port>,
            oneshot::Sender<io::Result<RegisterPayload>>,
        );

        type DisconnectResult = (io::Result<()>, oneshot::Sender<io::Result<()>>);

        enum Continuation {
            Register(RegisterResult),
            Other,
            Disconnect(DisconnectResult),
        }

        let mut ctx = Box::new(Context {
            vhci,
            mailer,
            register_queue,
        });

        loop {
            let event = tokio::select! {
                req = register_rx.recv() => {
                    Event::Register(req)
                }
                handle = giveback_rx.recv() => {
                    Event::GivebackUrb(handle)
                }
                req = disconnect_rx.recv() => {
                    Event::Disconnect(req)
                }
                work = work_rx.recv() => {
                    Event::Work(work.expect("we can only get here if we shutdown in the wrong order"))
                }
            };

            let cont = match event {
                Event::Register(Some(Ctrl {
                    data:
                        Register {
                            port: RegisterPort::Any,
                            data_rate,
                        },
                    tx,
                })) => tokio::task::spawn_blocking(move || Ctx {
                    cont: Continuation::Register((ctx.vhci.port_connect_any(data_rate), tx)),
                    ctx,
                })
                .await
                .unwrap(),
                Event::Register(Some(Ctrl {
                    data:
                        Register {
                            port: RegisterPort::Port(port),
                            data_rate,
                        },
                    tx,
                })) => tokio::task::spawn_blocking(move || Ctx {
                    cont: Continuation::Register((
                        ctx.vhci.port_connect(port, data_rate).map(|_| port),
                        tx,
                    )),
                    ctx,
                })
                .await
                .unwrap(),
                Event::GivebackUrb(Some(handle)) => {
                    dbg_urb_bucket.remove(&handle);
                    dbg_register_complete_bucket.remove(&handle);
                    ctx.mailer.remove_by_key3(&handle);
                    Ctx {
                        cont: Continuation::Other,
                        ctx,
                    }
                }
                Event::Disconnect(Some(Ctrl { data: port, tx })) => {
                    tokio::task::spawn_blocking(move || {
                        assert!(ctx.mailer.remove_by_key1(&port).is_some(), "{port:?}");
                        if let Some(index) = ctx
                            .register_queue
                            .iter()
                            .position(|queue_port| port == *queue_port)
                        {
                            ctx.register_queue.remove(index);
                        }
                        Ctx {
                            cont: Continuation::Disconnect((ctx.vhci.port_disconnect(port), tx)),
                            ctx,
                        }
                    })
                    .await
                    .unwrap()
                }
                Event::Work(work) => {
                    trace!("got work!");
                    // let now = Instant::now();
                    let tx = match work.get() {
                        vhci::ioctl::WorkRef::PortStat(stat) => {
                            if stat.change().contains(PortChange::RESET)
                                && (!stat.status()).contains(PortStatus::RESET)
                                && stat.status().contains(PortStatus::ENABLE)
                            {
                                if !ctx.register_queue.contains(&stat.index()) {
                                    ctx.register_queue.push_back(stat.index());
                                }
                            }
                            ctx.mailer.get_by_key1(&stat.index()).map(Cow::Borrowed)
                        }
                        vhci::ioctl::WorkRef::ProcessUrb((urb, handle)) => match urb.typ {
                            vhci::ioctl::UrbType::Ctrl
                                if urb.endpoint.is_anycast()
                                    && !ctx.register_queue.is_empty()
                                    && STANDARD_DEVICE_SET_ADDRESS
                                        == (
                                            urb.setup_packet.request_type(),
                                            urb.setup_packet.req(),
                                        ) =>
                            {
                                // TODO: The logic around here is all messed up lmao
                                let address = Address::new(urb.setup_packet.value() as u8)
                                    .expect("host should've assigned value address");
                                let port = Port::new(address.get() - 1)
                                    .expect("host should've assigned valid address");
                                assert_eq!(port, ctx.register_queue.pop_front().unwrap(), "wouldn't the new address correspond to the port currently registering its device?");

                                ctx.mailer.remove_by_key2(&NoHash(Address::new(0).unwrap()));
                                ctx.mailer.link_key2_to_key1(NoHash(address), &port);
                                // assert_eq!(
                                //     LinkResult::Success,
                                //     ctx.mailer.link_key2_to_key1(NoHash(address), &port),
                                //     "{address:?}/{port:?}\nin-progress bucket: {:?}\ncompleted bucket: {:?}\nurb bucket: {:?}",
                                //     dbg_register_inprogress_bucket,
                                //     dbg_register_complete_bucket,
                                //     dbg_urb_bucket
                                // );
                                assert_eq!(
                                    LinkResult::Success,
                                    ctx.mailer.link_key3_to_key1(handle, &port),
                                    "{handle:?}/{port:?}\nin-progress bucket: {:?}\ncompleted bucket: {:?}\nurb bucket: {:?}",
                                    dbg_register_inprogress_bucket,
                                    dbg_register_complete_bucket,
                                    dbg_urb_bucket
                                );
                                dbg_register_complete_bucket.insert(handle);
                                dbg_register_inprogress_bucket.remove(&handle);
                                ctx.mailer.get_by_key1(&port).map(Cow::Borrowed)
                            }
                            vhci::ioctl::UrbType::Ctrl
                                if urb.endpoint.is_anycast() && !ctx.register_queue.is_empty() =>
                            {
                                let port = ctx.register_queue.front().copied().unwrap();
                                assert_eq!(
                                    LinkResult::Success,
                                    ctx.mailer.link_key3_to_key1(handle, &port),
                                    "{handle:?}/{port:?}\nin-progress bucket: {:?}\ncompleted bucket: {:?}\nurb bucket: {:?}",
                                    dbg_register_inprogress_bucket,
                                    dbg_register_complete_bucket,
                                    dbg_urb_bucket
                                );
                                dbg_register_inprogress_bucket.insert(handle);
                                ctx.mailer.get_by_key1(&port).map(Cow::Borrowed)
                            }
                            _ => {
                                let address = NoHash(urb.address);
                                match ctx.mailer.link_key3_to_key2(handle, &address) {
                                    LinkResult::Success => {
                                        dbg_urb_bucket.insert(handle);
                                        ctx.mailer.get_by_key2(&address).map(Cow::Borrowed)
                                    },
                                    LinkResult::NewKeyAlreadyExists => {
                                        None
                                    },
                                    LinkResult::ExistingKeyDoesNotExist => panic!(
                                        "LinkResult::ExistingKeyDoesNotExist\nurb: {urb:?}\n{handle:?}/{address:?}\nin-progress bucket: {:?}\ncompleted bucket: {:?}\nurb bucket: {:?}",
                                        dbg_register_inprogress_bucket,
                                        dbg_register_complete_bucket,
                                        dbg_urb_bucket
                                    ),
                                }
                            }
                        },
                        vhci::ioctl::WorkRef::CancelUrb(handle) => {
                            let tx = ctx.mailer.get_by_key3(&handle).cloned();
                            ctx.mailer.remove_by_key3(&handle);
                            tx.map(Cow::Owned)
                        }
                    };
                    if let Some(tx) = tx {
                        // SAFETY: Work item is still unaltered from the ioctl
                        if tx.send(unsafe { work.into_inner() }).await.is_err() {
                            // TODO: Remove by which key? By the value itself?
                        }
                    }
                    // let dur = now.elapsed();
                    // if Duration::from_micros(15) < dur {
                    //     trace!("took {dur:?} to route URB");
                    // }
                    Ctx {
                        ctx,
                        cont: Continuation::Other,
                    }
                }
                Event::Register(None) | Event::Disconnect(None) | Event::GivebackUrb(None) => break,
            };

            let Ctx { ctx: c, cont } = cont;
            ctx = c;
            match cont {
                Continuation::Register((Ok(port), tx)) => {
                    ctx.register_queue.push_back(port);
                    let (work_tx, work_rx) = mpsc::channel(32);
                    assert!(
                        ctx.mailer.insert_by_key1(port, work_tx).is_none(),
                        "{port:?}"
                    );
                    if tx.send(Ok((port, work_rx))).is_err() {
                        ctx.register_queue.pop_back();
                        ctx.mailer.remove_by_key1(&port);
                    }
                }
                Continuation::Register((Err(err), tx)) => {
                    let _ = tx.send(Err(err));
                }
                Continuation::Disconnect((result, tx)) => {
                    let _ = tx.send(result);
                }
                Continuation::Other => (),
            }
        }

        let Context {
            mut vhci, mailer, ..
        } = *ctx;

        trace!("disconnecting leftover devices and closing work_rx");
        mailer.key1_iter().for_each(|port| {
            if let Err(err) = vhci.port_disconnect(*port) {
                debug!("error while disconnecting vhci device: {err}");
            }
        });
        work_rx.close();
        while (work_rx.recv().await).is_some() {}
        drop(work_rx);

        trace!("about to wait on recv_work thread");
        tokio::task::spawn_blocking(|| handle.join().expect("recv thread should not panic"))
            .await
            .unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    handle: Arc<Mutex<Option<tokio::task::JoinHandle<io::Result<()>>>>>,
    register_tx: mpsc::Sender<Ctrl<Register, (Port, WorkReceiver)>>,
    giveback_tx: mpsc::Sender<UrbHandle>,
    disconnect_tx: mpsc::Sender<Ctrl<Port>>,
    remote: vhci::Remote,
}

impl Controller {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn start(num_ports: BoundedU8<1, 32>) -> io::Result<Self> {
        let (register_tx, register_rx) = mpsc::channel(4);
        let (giveback_tx, giveback_rx) = mpsc::channel(32);
        let (disconnect_tx, disconnect_rx) = mpsc::channel(2);
        let vhci = vhci::Controller::open(num_ports)?;
        let remote = vhci.remote();
        let handle = Arc::new(Mutex::new(Some(tokio::spawn(
            Demuxer::new(register_rx, giveback_rx, disconnect_rx, vhci).run(),
        ))));

        Ok(Self {
            handle,
            register_tx,
            giveback_tx,
            disconnect_tx,
            remote,
        })
    }

    #[tracing::instrument(level = "trace", skip_all)]
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

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn fetch_data<T: vhci::Urb + vhci::IsoPacketDataMut + vhci::TransferMut>(
        &self,
        urb: T,
    ) -> io::Result<()> {
        if let Ok(tokio::runtime::RuntimeFlavor::MultiThread) =
            tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor())
        {
            tokio::task::block_in_place(|| self.remote.fetch_data(urb))
        } else {
            self.remote.fetch_data(urb)
        }
    }

    pub async fn giveback_urb<T: vhci::Urb + vhci::IsoPacketGivebackMut + vhci::TransferMut>(
        &self,
        urb: T,
    ) -> io::Result<()> {
        let _ = self.giveback_tx.send(urb.handle()).await;
        if let Ok(tokio::runtime::RuntimeFlavor::MultiThread) =
            tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor())
        {
            tokio::task::block_in_place(|| self.remote.giveback(urb))
        } else {
            self.remote.giveback(urb)
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn disconnect(&mut self, port: Port) -> io::Result<()> {
        let (rx, disconnect) = Ctrl::new(port);

        self.disconnect_tx.send(disconnect).await.unwrap();
        rx.await.unwrap()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn reset_done(&self, port: Port, enable: bool) -> io::Result<()> {
        if let Ok(tokio::runtime::RuntimeFlavor::MultiThread) =
            tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor())
        {
            tokio::task::block_in_place(|| self.remote.port_reset_done(port, enable))
        } else {
            self.remote.port_reset_done(port, enable)
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn shutdown(self) {
        drop(self.register_tx);
        drop(self.disconnect_tx);
        let handle = self.handle.lock().unwrap().take();
        drop(self.handle);
        if let Some(handle) = handle {
            trace!("about to wait for async handle");
            match handle.await {
                Ok(Ok(_)) => trace!("controller thread finished with no issues"),
                Ok(Err(_err)) => todo!("Figure out what kind of I/O errors we can get"),
                Err(_err) => todo!("Figure out what might make the thread panic"),
            }
        }
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn recv_work(
    work_rx: vhci::WorkReceiver,
    work_tx: mpsc::Sender<vhci::ioctl::IocWork>,
) -> io::Result<()> {
    if work_tx.is_closed() {
        trace!("done with receiving work for VHCI");
        return Ok(());
    }

    const TIMEOUT: TimeoutMillis = TimeoutMillis::Time(BoundedI16::new(999).unwrap());
    while (match work_rx.fetch_work_timeout(TIMEOUT) {
        Ok(work) => work_tx.blocking_send(work).ok(),
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

    trace!("done with receiving work for VHCI");
    Ok(())
}

#[cfg(test)]
mod tests {
    use tracing::warn;
    use vhci::{libc, PortChange, PortFlag, PortStatus, UrbWithData};

    use super::*;
    use std::{
        thread,
        time::{Duration, Instant},
    };

    #[tokio::test]
    async fn controller_idles() {
        let controller = Controller::start(BoundedU8::new(3).unwrap()).unwrap();

        thread::sleep(Duration::from_millis(500));
        controller.shutdown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn can_listen_for_work() {
        let mut controller = Controller::start(BoundedU8::new(8).unwrap()).unwrap();

        let mut prev_stat = vhci::ioctl::IocPortStat::default();
        let mut addr = 0xff;
        let (port, mut work_rx) = controller
            .register(RegisterPort::Any, DataRate::Full)
            .await
            .unwrap();

        trace!("connected {port:?}, starting process for registering");
        let start = Instant::now();
        while Duration::from_secs(10) > start.elapsed() {
            debug!("==============================================");
            match work_rx.recv().await {
                Some(vhci::ioctl::Work::PortStat(next_stat)) => {
                    debug!("got port stat for {:?}", next_stat.index());
                    debug!("status: {:?}", next_stat.status());
                    debug!("change: {:?}", next_stat.change());
                    debug!("index: {:?}", next_stat.index());
                    debug!("flags: {:?}", next_stat.flags());
                    if next_stat.change().contains(PortChange::CONNECTION) {
                        trace!("CONNECTION state changed -> invalidating address");
                        addr = 0xff;
                    }
                    if next_stat.change().contains(PortChange::RESET)
                        && (!next_stat.status()).contains(PortStatus::RESET)
                        && next_stat.status().contains(PortStatus::ENABLE)
                    {
                        trace!("RESET successful -> use default address");
                        addr = 0;
                    }
                    if prev_stat.status().contains(PortStatus::POWER)
                        && (!next_stat.status()).contains(PortStatus::POWER)
                    {
                        trace!("port is powered off");
                    }
                    if (!prev_stat.status()).contains(PortStatus::RESET)
                        && next_stat
                            .status()
                            .contains(PortStatus::RESET | PortStatus::CONNECTION)
                    {
                        trace!("port is resetting -> completing reset");
                        controller.reset_done(next_stat.index(), true).unwrap();
                    }
                    if (!prev_stat.flags()).contains(PortFlag::RESUMING)
                        && next_stat.flags().contains(PortFlag::RESUMING)
                        && next_stat.status().contains(PortStatus::CONNECTION)
                    {
                        trace!("port is resuming -> completing resume");
                        todo!("do the actual resume thing");
                    }
                    prev_stat = next_stat;
                }
                Some(vhci::ioctl::Work::ProcessUrb((urb, handle))) => {
                    debug!(
                        "got process urb for {:?} at {:?}",
                        urb.address, urb.endpoint
                    );
                    if addr != urb.address.get() {
                        warn!("not for usb device at {port:?}. skipping.");
                        continue;
                    }

                    let mut urb = UrbWithData::from_ioctl(urb, handle);
                    if urb.needs_data_fetch() {
                        match controller.fetch_data(&mut urb) {
                            Ok(_) => {}
                            Err(err)
                                if err
                                    .raw_os_error()
                                    .is_some_and(|errno| libc::ECANCELED == errno) => {}
                            Err(err) => Err(err).unwrap(),
                        }
                    }

                    let urb_ctrl_req = (
                        urb.control_packet().request_type(),
                        urb.control_packet().req(),
                    );
                    if vhci::ioctl::UrbType::Ctrl == urb.kind()
                        && urb.endpoint().is_anycast()
                        && STANDARD_DEVICE_SET_ADDRESS == urb_ctrl_req
                    {
                        if let Some(new_addr) =
                            Address::new(urb.control_packet().value().try_into().unwrap())
                        {
                            urb.set_status(vhci::Status::Success);
                            addr = new_addr.get();
                            trace!("SET_ADDRESS (addr={:#x})", addr);
                        }
                    }

                    urb.set_status(vhci::Status::Stall);
                    controller.giveback_urb(urb).await.unwrap();
                    break;
                }
                Some(vhci::ioctl::Work::CancelUrb(_handle)) => unreachable!(),
                None => break,
            }
        }

        trace!("disconnecting {port:?}");
        controller.disconnect(port).await.unwrap();
        trace!("shutting down VHCI controller");
        controller.shutdown().await;
    }
}
