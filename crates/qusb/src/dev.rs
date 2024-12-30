use std::{
    future::Future,
    io,
    ops::ControlFlow,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use tracing::trace;
use vhci::{
    utils::{BoundedI16, BoundedU8, TimeoutMillis},
    DataRate, Port, PortChange, PortFlag, PortStat, PortStatus, Urb, UrbControl, UrbHandle, Work,
};

use crate::utils::{Ctrl, SimpleMap};

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

/// A multi-key map with no hashing for sending [`vhci::Work`] items to
/// the respective [`Receiver`].
///
/// Maps a [`vhci::Port`] and/or [`vhci::UrbHandle`] to a [`Sender`].
/// Allows for multiple URB handles to point to the same sender.
///
/// # Using nohash_hasher
///
/// We can utilize [`nohash_hasher`] for performance because we know that:
/// 1. The { `Port`:`Sender` } map will contain no duplicate keys
///    due to only one device being plugged into one port at any time,
/// 2. The { `UrbHandle`:`Sender` } map will also contain no duplicate
///    keys due to URBs needing a unique identifier.
///
/// # Time complexity
///
/// Inserting a new { `Port`:`Sender` } -> O(1)
/// Linking a handle to a port/sender -> O(1)
/// Accessing a `Sender` from either a port or handle -> O(1)
/// Removing a { `Port`:`Sender` } -> 2 * O(N) + 2 * O(M)
///
/// [`Sender`]: tokio::sync::mpsc::Sender
/// [`Receiver`]: tokio::sync::mpsc::Receiver
#[derive(Debug, Default)]
struct Mailer {
    port_to_work: SimpleMap<Port, usize>,
    handle_to_work: SimpleMap<UrbHandle, usize>,
    work_line: Vec<mpsc::Sender<Work>>,
}

impl Mailer {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            port_to_work: SimpleMap::with_capacity_and_hasher(cap, Default::default()),
            handle_to_work: SimpleMap::with_capacity_and_hasher(cap, Default::default()),
            work_line: Vec::with_capacity(cap),
        }
    }

    /// Inserts a `Sender` using a `Port` as the key.
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn insert_tx(&mut self, port: Port, tx: mpsc::Sender<Work>) {
        self.work_line.push(tx);
        let index = self.work_line.len() - 1;
        self.port_to_work.insert(port, index);
    }

    /// Links a [`UrbHandle`] to a [`Port`] so that they both point
    /// to the same [`Sender`].
    ///
    /// The function returns `false` if:
    /// - `handle` was already mapped to `port`
    /// - `port` doesn't exist in mapping
    ///
    /// Returns `true` if the linking was successful.
    ///
    /// # Examples
    ///
    /// This example does not compile IRL because [`Mailer`] is a crate data
    /// structure only and because [`UrbHandle`]'s inner field is not publicly
    /// accessible, but otherwise works:
    ///
    /// ```compile_fail
    /// use tokio::sync::mpsc;
    ///
    /// let mut mailer = Mailer::default();
    /// let port = vhci::Port::new(1).unwrap();
    /// let handle = vhci::UrbHandle(0x4000);
    /// let (tx, rx) = mpsc::channel(8);
    /// mailer.insert_tx(port, tx);
    /// assert_eq!(mailer.link_handle_to_work(handle, port), true);
    /// ```
    ///
    /// [`Sender`]: tokio::sync::mpsc::Sender
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn link_handle_to_port(&mut self, handle: UrbHandle, port: Port) -> bool {
        if self.handle_to_work.contains_key(&handle) {
            return false;
        };
        let index = self.port_to_work.get(&port);
        if let Some(&index) = index {
            self.handle_to_work.insert(handle, index);
            true
        } else {
            false
        }
    }

    /// Unlinks a [`UrbHandle`] from a { `Port`:`Sender` } mapping.
    ///
    /// Returns `true` if the handle was previously linked to the port,
    /// `false` otherwise.
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn unlink_handle_from_port(&mut self, handle: &UrbHandle) -> bool {
        self.handle_to_work.remove(handle).is_some()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn remove_by_port(&mut self, port: &Port) {
        if let Some(index) = self.port_to_work.get(port).copied() {
            self.remove_by_index(index);
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn remove_by_tx(&mut self, tx: &mpsc::Sender<Work>) {
        if let Some(index) = self.work_line.iter().position(|cur| tx.same_channel(cur)) {
            self.remove_by_index(index);
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn remove_by_index(&mut self, tx_index: usize) {
        self.port_to_work.retain(|_, &mut value| tx_index != value);
        self.handle_to_work
            .retain(|_, &mut value| tx_index != value);
        let last_index = self.work_line.len() - 1;
        self.work_line.swap_remove(tx_index);
        if !self.handle_to_work.is_empty() {
            self.handle_to_work
                .values_mut()
                .filter(|&&mut value| last_index == value)
                .for_each(|value| *value = tx_index);
        }
        if !self.port_to_work.is_empty() {
            self.port_to_work
                .values_mut()
                .filter(|&&mut value| last_index == value)
                .for_each(|value| *value = tx_index);
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn get_tx_from_handle(&self, handle: UrbHandle) -> Option<mpsc::Sender<Work>> {
        let index = self.handle_to_work.get(&handle)?;
        self.work_line.get(*index).cloned()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn get_tx_from_port(&self, port: Port) -> Option<mpsc::Sender<Work>> {
        let index = self.port_to_work.get(&port)?;
        self.work_line.get(*index).cloned()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn contains_port(&self, port: &Port) -> bool {
        self.port_to_work.contains_key(port)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn contains_handle(&self, handle: &UrbHandle) -> bool {
        self.handle_to_work.contains_key(handle)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn get_tx_from_work(&mut self, work: &Work) -> Option<mpsc::Sender<Work>> {
        match work {
            Work::CancelUrb(ref urb_handle) => self.get_tx_from_handle(*urb_handle),
            Work::ProcessUrb(ref urb) => match urb {
                Urb::Ctrl(urb_control) => {
                    let port = Port::new((urb_control.w_index & 0x00ff) as u8 + 1).unwrap();
                    self.link_handle_to_port(urb.handle(), port);
                    self.get_tx_from_port(port)
                }
                _ => self.get_tx_from_handle(urb.handle()),
            },
            Work::PortStat(ref stat) => self.get_tx_from_port(stat.index),
        }
    }
}

struct Demuxer {
    register_rx: mpsc::Receiver<
        Ctrl<
            Register,
            (
                Port,
                mpsc::Receiver<Work>,
                oneshot::Receiver<io::Result<UrbControl>>,
            ),
        >,
    >,
    disconnect_rx: mpsc::Receiver<Ctrl<Port>>,
    vhci: vhci::Controller,
}

impl Demuxer {
    fn new(
        register_rx: mpsc::Receiver<
            Ctrl<
                Register,
                (
                    Port,
                    mpsc::Receiver<Work>,
                    oneshot::Receiver<io::Result<UrbControl>>,
                ),
            >,
        >,
        disconnect_rx: mpsc::Receiver<Ctrl<Port>>,
        vhci: vhci::Controller,
    ) -> Self {
        Self {
            register_rx,
            disconnect_rx,
            vhci,
        }
    }

    async fn run(self) -> io::Result<()> {
        let Self {
            mut register_rx,
            mut disconnect_rx,
            mut vhci,
        } = self;

        let mailer = Mailer::default();
        let (work_tx, mut work_rx) = mpsc::channel(64);
        let work_receiver = vhci.work_receiver().unwrap();
        let (port_connect_tx, port_connect_rx) = mpsc::channel(vhci.free_ports() as usize);
        let handle = std::thread::spawn(move || recv_work(work_receiver, work_tx, port_connect_rx));

        struct Context {
            vhci: vhci::Controller,
            mailer: Mailer,
            port_connect_tx: mpsc::Sender<Ctrl<(), UrbControl>>,
        }

        enum Event {
            Register(
                Option<
                    Ctrl<
                        Register,
                        (
                            Port,
                            mpsc::Receiver<Work>,
                            oneshot::Receiver<io::Result<UrbControl>>,
                        ),
                    >,
                >,
            ),
            Disconnect(Option<Ctrl<Port>>),
            Work(Work),
        }

        struct Ctx {
            ctx: Box<Context>,
            cont: Continuation,
        }

        enum Continuation {
            Register(
                (
                    io::Result<Port>,
                    oneshot::Sender<
                        io::Result<(
                            Port,
                            mpsc::Receiver<Work>,
                            oneshot::Receiver<io::Result<UrbControl>>,
                        )>,
                    >,
                ),
            ),
            Other,
            Disconnect((io::Result<()>, oneshot::Sender<io::Result<()>>)),
        }

        let mut ctx = Box::new(Context {
            vhci,
            mailer,
            port_connect_tx,
        });

        loop {
            let event = tokio::select! {
                req = register_rx.recv() => {
                    Event::Register(req)
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
                Event::Disconnect(Some(Ctrl { data: port, tx })) => {
                    tokio::task::spawn_blocking(move || {
                        ctx.mailer.remove_by_port(&port);
                        Ctx {
                            cont: Continuation::Disconnect((ctx.vhci.port_disconnect(port), tx)),
                            ctx,
                        }
                    })
                    .await
                    .unwrap()
                }
                Event::Work(work) => {
                    let next = ctx.mailer.get_tx_from_work(&work).map(|tx| (work, tx));
                    if let Some((work, tx)) = next {
                        if tx.send(work).await.is_err() {
                            ctx.mailer.remove_by_tx(&tx);
                        }
                    }
                    Ctx {
                        ctx,
                        cont: Continuation::Other,
                    }
                }
                Event::Register(None) | Event::Disconnect(None) => break,
            };

            let Ctx { ctx: c, cont } = cont;
            ctx = c;
            match cont {
                Continuation::Register((Ok(port), tx)) => {
                    let (ctrl_rx, get_urb) = Ctrl::new(());

                    // Let the work runner know that a new device
                    // was plugged in and needs to go through the
                    // enumeration process.
                    ctx.port_connect_tx
                        .send(get_urb)
                        .await
                        .expect("work runner shouldn't close until demuxer completes");

                    let (work_tx, work_rx) = mpsc::channel(32);

                    // Send these items back to the requester:
                    // - port: in case the requester didn't specify a port
                    // - work_rx: gives the requester a pipe to receive port status updates and URBs
                    // - ctrl_rx: allows the requester to receive the control setup packet.
                    if tx.send(Ok((port, work_rx, ctrl_rx))).is_err() {
                        ctx.mailer.remove_by_port(&port);
                        if let Err(_err) = ctx.vhci.port_disconnect(port) {
                            todo!("find out why this might happen")
                        }
                    }

                    // TODO: continue ctrl urb stuff here
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
            mut vhci,
            mailer,
            port_connect_tx,
        } = *ctx;

        trace!("disconnecting leftover devices and closing work_rx");
        for port in mailer.port_to_work.into_keys() {
            let _ = vhci.port_disconnect(port);
        }
        work_rx.close();
        while let Some(_) = work_rx.recv().await {}
        drop(work_rx);
        drop(port_connect_tx);

        trace!("about to wait on recv_work thread");
        tokio::task::spawn_blocking(|| handle.join().expect("recv thread should not panic"))
            .await
            .unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    handle: Arc<Mutex<Option<tokio::task::JoinHandle<io::Result<()>>>>>,
    register_tx: mpsc::Sender<
        Ctrl<
            Register,
            (
                Port,
                mpsc::Receiver<Work>,
                oneshot::Receiver<io::Result<UrbControl>>,
            ),
        >,
    >,
    disconnect_tx: mpsc::Sender<Ctrl<Port>>,
    remote: vhci::Remote,
}

impl Controller {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn start(num_ports: BoundedU8<1, 32>) -> io::Result<Self> {
        let (register_tx, register_rx) = mpsc::channel(4);
        let (disconnect_tx, disconnect_rx) = mpsc::channel(2);
        let vhci = vhci::Controller::open(num_ports)?;
        let remote = vhci.remote();
        let handle = Arc::new(Mutex::new(Some(tokio::spawn(
            Demuxer::new(register_rx, disconnect_rx, vhci).run(),
        ))));

        Ok(Self {
            handle,
            register_tx,
            disconnect_tx,
            remote,
        })
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn register<F, Fut>(
        &mut self,
        port: RegisterPort,
        data_rate: DataRate,
        handle_ctrl: F,
    ) -> io::Result<(Port, mpsc::Receiver<Work>)>
    where
        F: FnOnce(UrbControl, &mut Self) -> Fut,
        Fut: Future<Output = io::Result<()>>,
    {
        let (rx, register) = Ctrl::new(Register { port, data_rate });

        // Let the work runner know that a new port needs enumeration.
        self.register_tx.send(register).await.unwrap();
        let (port, mut work_rx, mut ctrl_rx) = rx.await.unwrap()?;

        let mut prev = PortStat {
            status: PortStatus::empty(),
            change: PortChange::empty(),
            index: Port::new(1).unwrap(),
            flags: PortFlag::empty(),
        };

        trace!("registered {port:?}, now listening for work");
        loop {
            tokio::select! {
                work = work_rx.recv() => {
                    if let Work::PortStat(next) = work.unwrap() {
                        if (!prev.status).contains(PortStatus::RESET)
                            && next
                                .status
                                .contains(PortStatus::RESET | PortStatus::CONNECTION)
                        {
                            self.reset_done(next.index, true).await.unwrap();
                            prev = next;
                        }
                    }
                }
                ctrl = &mut ctrl_rx => {
                    let ctrl = ctrl.unwrap().unwrap();
                    break handle_ctrl(ctrl, self).await?;
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                    return Err(std::io::ErrorKind::TimedOut.into());
                }
            }
        }

        Ok((port, work_rx))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn fetch_data(&self, urb: &mut Urb) -> io::Result<()> {
        if let Ok(tokio::runtime::RuntimeFlavor::MultiThread) =
            tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor())
        {
            tokio::task::block_in_place(|| self.remote.fetch_data(urb))
        } else {
            self.remote.fetch_data(urb)
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn disconnect(&mut self, port: Port) -> io::Result<()> {
        let (rx, disconnect) = Ctrl::new(port);

        self.disconnect_tx.send(disconnect).await.unwrap();
        rx.await.unwrap()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn reset_done(&self, port: Port, enable: bool) -> io::Result<()> {
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
        if let Some(handle) = self.handle.lock().unwrap().take() {
            trace!("about to wait for async handle");
            match handle.await {
                Ok(Ok(_)) => trace!("controller thread finished with no issues"),
                Ok(Err(_err)) => todo!("Figure out what kind of I/O errors we can get"),
                Err(_err) => todo!("Figure out what might make the thread panic"),
            }
        }
    }
}

#[tracing::instrument(level = "trace")]
fn recv_work(
    work_rx: vhci::WorkReceiver,
    work_tx: mpsc::Sender<Work>,
    mut port_connect_rx: mpsc::Receiver<Ctrl<(), UrbControl>>,
) -> io::Result<()> {
    if work_tx.is_closed() {
        trace!("done with receiving work for VHCI");
        return Ok(());
    }

    const TIMEOUT: TimeoutMillis = TimeoutMillis::Time(BoundedI16::new(999).unwrap());
    while let Some(_) = match work_rx.fetch_work_timeout(TIMEOUT) {
        Ok(work) => match work {
            Work::ProcessUrb(Urb::Ctrl(ctrl))
                if !port_connect_rx.is_empty()
                    && matches!(ctrl.epadr.direction(), vhci::usbfs::Direction::In)
                    && ctrl.devadr.is_anycast() =>
            {
                // FIXME: This is incorrect. A setup packet sent to the default
                //        pipe must get a response from every USB device that doesn't
                //        already have an address assigned by the kernel.
                port_connect_rx
                    .blocking_recv()
                    .unwrap()
                    .tx
                    .send(Ok(ctrl))
                    .ok()
            }
            work => work_tx.blocking_send(work).ok(),
        },
        Err(err)
            if err.kind() == io::ErrorKind::TimedOut
                || err.kind() == io::ErrorKind::Interrupted
                || err
                    .raw_os_error()
                    .is_some_and(|err| err == vhci::libc::ENODATA) =>
        {
            trace!("no data, but might try again");
            (!work_tx.is_closed()).then_some(())
        }
        Err(err) => return Err(err),
    } {}

    trace!("done with receiving work for VHCI");
    Ok(())
}

// fn handle_vhci_req<'a, S, R, F>(
//     rx: &'a mut mpsc::Receiver<Ctrl<S, R>>,
//     mut vhci_fn: F,
// ) -> ControlFlow<(), bool>
// where
//     F: FnMut(S) -> std::io::Result<R>,
//     R: std::fmt::Debug,
// {
//     let req = match rx.try_recv() {
//         Ok(ctrl) => Some(ctrl),
//         Err(mpsc::error::TryRecvError::Empty) => None,
//         Err(mpsc::error::TryRecvError::Disconnected) => ControlFlow::Break(())?,
//     };

//     if let Some(Ctrl { data, tx }) = req {
//         let result = vhci_fn(data);
//         tx.send(result)
//             .expect("if recv is dropped then that thread has panicked");
//         ControlFlow::Continue(true)
//     } else {
//         ControlFlow::Continue(false)
//     }
// }

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use vhci::{PortChange, PortFlag, PortStat, PortStatus};

    use super::*;

    #[test]
    fn remove_last_tx_works() {
        let mut mailer = Mailer::default();

        let (tx, _rx) = tokio::sync::mpsc::channel(20);
        mailer.insert_tx(Port::new(1).unwrap(), tx);
        mailer.remove_by_port(&Port::new(1).unwrap());

        assert!(mailer.port_to_work.is_empty());
        assert!(mailer.work_line.is_empty());
    }

    #[test]
    fn remove_tx_works() {
        let mut mailer = Mailer::default();

        let (tx, _rx) = tokio::sync::mpsc::channel(20);
        mailer.insert_tx(Port::new(1).unwrap(), tx);

        let (tx, _rx) = tokio::sync::mpsc::channel(20);
        mailer.insert_tx(Port::new(5).unwrap(), tx);

        mailer.remove_by_port(&Port::new(1).unwrap());
        let index = mailer.port_to_work.get(&Port::new(5).unwrap()).unwrap();
        assert_eq!(*index, 0);
    }

    #[tokio::test]
    async fn controller_idles() {
        let controller = Controller::start(BoundedU8::new(3).unwrap()).unwrap();

        thread::sleep(Duration::from_millis(500));
        controller.shutdown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[should_panic(expected = "urb ctrl not complete")]
    async fn can_listen_for_work() {
        let mut controller = Controller::start(BoundedU8::new(8).unwrap()).unwrap();

        let (port, work_rx) = controller
            .register(RegisterPort::Any, DataRate::Full, |urb, ctrl| async move {
                // STEPS FOR ENUMERATION
                // 1.
                todo!("Figure out the fastest way to get the USB data from the other host");
            })
            .await
            .unwrap();

        trace!("disconnecting {port:?}");
        controller.disconnect(port).await.unwrap();
        trace!("shutting down VHCI controller");
        controller.shutdown().await;
    }
}
