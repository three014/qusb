use std::{
    collections::VecDeque,
    ops::ControlFlow,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use vhci::{
    utils::{ClosedBoundedI16, TimeoutMillis},
    DataRate, Port, Urb, UrbHandle, Vhci, Work,
};

use crate::utils::{OpenBoundedU8, SimpleMap};

mod task;

pub struct Ctrl<S, R> {
    data: S,
    tx: oneshot::Sender<std::io::Result<R>>,
}

impl<S, R> Ctrl<S, R> {
    pub fn new(data: S) -> (oneshot::Receiver<std::io::Result<R>>, Ctrl<S, R>) {
        let (tx, rx) = oneshot::channel();
        let ctrl = Self { data, tx };
        (rx, ctrl)
    }
}

pub enum RegisterPort {
    Any,
    Port(Port),
}

pub struct Register {
    port: RegisterPort,
    data_rate: DataRate,
}

#[derive(Debug, Default)]
struct Mailer {
    port_to_work: SimpleMap<Port, usize>,
    handle_to_work: SimpleMap<UrbHandle, usize>,
    work_line: Vec<mpsc::Sender<Work>>,
}

impl Mailer {
    pub fn insert_tx(&mut self, port: Port, tx: mpsc::Sender<Work>) {
        self.work_line.push(tx);
        let index = self.work_line.len() - 1;
        self.port_to_work.insert(port, index);
    }

    /// Returns `false` if:
    /// - `handle` was already mapped to `port`
    /// - `port` doesn't exist in mapping
    ///
    /// Returns `true` otherwise.
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

    pub fn unlink_handle_from_port(&mut self, handle: &UrbHandle) -> bool {
        self.handle_to_work.remove(handle).is_some()
    }

    pub fn remove_tx(&mut self, port: &Port) {
        if let Some(index) = self.port_to_work.remove(port) {
            self.handle_to_work.retain(|_, &mut value| index != value);
            let last_index = self.work_line.len() - 1;
            self.work_line.swap_remove(index);
            if !self.handle_to_work.is_empty() {
                self.handle_to_work
                    .values_mut()
                    .filter(|&&mut value| last_index == value)
                    .for_each(|value| *value = index);
            }
            if !self.port_to_work.is_empty() {
                self.port_to_work
                    .values_mut()
                    .filter(|&&mut value| last_index == value)
                    .for_each(|value| *value = index);
            }
        }
    }

    pub fn get_tx_from_handle(&self, handle: UrbHandle) -> Option<&mpsc::Sender<Work>> {
        let index = self.handle_to_work.get(&handle)?;
        self.work_line.get(*index)
    }

    pub fn get_tx_from_port(&self, port: Port) -> Option<&mpsc::Sender<Work>> {
        let index = self.port_to_work.get(&port)?;
        self.work_line.get(*index)
    }

    fn contains_port(&self, port: &Port) -> bool {
        self.port_to_work.contains_key(port)
    }

    fn contains_handle(&self, handle: &UrbHandle) -> bool {
        self.handle_to_work.contains_key(handle)
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    handle: Arc<Mutex<Option<std::thread::JoinHandle<std::io::Result<()>>>>>,
    register_tx: mpsc::Sender<Ctrl<Register, (Port, mpsc::Receiver<Work>)>>,
    fetch_data_tx: mpsc::Sender<Ctrl<Urb, Urb>>,
    disconnect_tx: mpsc::Sender<Ctrl<Port, ()>>,
}

impl Controller {
    pub fn start(num_ports: OpenBoundedU8<0, 32>) -> std::io::Result<Self> {
        let (register_tx, mut register_rx) = mpsc::channel(4);
        let (fetch_data_tx, mut fetch_data_rx) = mpsc::channel(8);
        let (disconnect_tx, mut disconnect_rx) = mpsc::channel(2);
        let mut vhci = Vhci::open(num_ports)?;

        let runner = move || -> std::io::Result<()> {
            use task::*;
            let mut mailer = Mailer::default();
            let mut work_queue = VecDeque::new();
            let mut sched = task::Scheduler::<5>::new();
            sched.push(Task::new(
                TaskName::Register,
                register,
                Nice::new(20).unwrap(),
                2,
            ));
            sched.push(Task::new(TaskName::FetchData, fetch_data, Nice::NORMAL, 2));
            sched.push(Task::new(
                TaskName::Disconnect,
                disconnect,
                Nice::new(24).unwrap(),
                0,
            ));
            sched.push(Task::new(
                TaskName::MailOutgoing,
                mail_outgoing_work,
                Nice::new(24).unwrap(),
                2,
            ));
            sched.push(Task::new(
                TaskName::RecvWork,
                recv_work,
                Nice::new(1).unwrap(),
                4,
            ));

            let start = std::time::Instant::now();
            let mut sleep_time = std::time::Duration::ZERO;
            while let std::ops::ControlFlow::Continue(sched_sleep_time) = sched.run_next(Context {
                register_rx: &mut register_rx,
                fetch_data_rx: &mut fetch_data_rx,
                disconnect_rx: &mut disconnect_rx,
                vhci: &mut vhci,
                mailer: &mut mailer,
                outgoing: &mut work_queue,
            }) {
                sleep_time += sched_sleep_time;
            }
            let elapsed = start.elapsed();
            println!("Total run time: {elapsed:?}");
            println!("Task run time: {:?}", sched.time_running());
            println!("Scheduler sleep time: {:?}", sleep_time);
            println!("{sched:#?}");

            // TODO: Disconnect all devices somehow

            Ok(())
        };

        let handle = Arc::new(Mutex::new(Some(std::thread::spawn(runner))));

        Ok(Self {
            handle,
            register_tx,
            fetch_data_tx,
            disconnect_tx,
        })
    }

    pub async fn register(
        &mut self,
        port: RegisterPort,
        data_rate: DataRate,
    ) -> std::io::Result<(Port, mpsc::Receiver<Work>)> {
        let (rx, register) = Ctrl::new(Register { port, data_rate });

        self.register_tx.send(register).await.unwrap();
        rx.await.unwrap()
    }

    pub async fn fetch_data(&mut self, urb: Urb) -> std::io::Result<Urb> {
        let (rx, fetch_data) = Ctrl::new(urb);

        self.fetch_data_tx.send(fetch_data).await.unwrap();
        rx.await.unwrap()
    }

    pub async fn disconnect(&mut self, port: Port) -> std::io::Result<()> {
        let (rx, disconnect) = Ctrl::new(port);

        self.disconnect_tx.send(disconnect).await.unwrap();
        rx.await.unwrap()
    }

    pub fn shutdown(self) {
        drop(self.register_tx);
        if let Some(handle) = self.handle.lock().unwrap().take() {
            match handle.join() {
                Ok(Ok(_)) => println!("Controller thread finished with no issues"),
                Ok(Err(_err)) => todo!("Figure out what kind of I/O errors we can get"),
                Err(_err) => todo!("Figure out what might make the thread panic"),
            }
        }
    }
}

fn register<'a>(ctx: task::Context<'a>) -> ControlFlow<(), (task::Context<'a>, bool)> {
    let register = match ctx.register_rx.try_recv() {
        Ok(register) => Some(register),
        Err(mpsc::error::TryRecvError::Empty) => None,
        Err(mpsc::error::TryRecvError::Disconnected) => ControlFlow::Break(())?,
    };

    let result = if let Some(Ctrl {
        data: Register {
            port: RegisterPort::Any,
            data_rate,
        },
        tx,
    }) = register
    {
        Some((ctx.vhci.port_connect_any(data_rate), tx))
    } else if let Some(Ctrl {
        data:
            Register {
                port: RegisterPort::Port(port),
                data_rate,
            },
        tx,
    }) = register
    {
        Some((ctx.vhci.port_connect(port, data_rate).map(|_| port), tx))
    } else {
        None
    };

    match result {
        Some((Ok(port), oneshot_tx)) => {
            let (mpsc_tx, mpsc_rx) = mpsc::channel::<Work>(32);
            ctx.mailer.insert_tx(port, mpsc_tx);
            oneshot_tx
                .send(Ok((port, mpsc_rx)))
                .expect("if recv is dropped then that thread must've panicked");
            ControlFlow::Continue((ctx, true))
        }
        Some((Err(err), oneshot_tx)) => {
            let _ = oneshot_tx.send(Err(err));
            ControlFlow::Continue((ctx, true))
        }
        None => ControlFlow::Continue((ctx, false)),
    }
}

fn fetch_data<'a>(ctx: task::Context<'a>) -> ControlFlow<(), (task::Context<'a>, bool)> {
    let fetch_data = match ctx.fetch_data_rx.try_recv() {
        Ok(fetch_data) => Some(fetch_data),
        Err(mpsc::error::TryRecvError::Empty) => None,
        Err(mpsc::error::TryRecvError::Disconnected) => ControlFlow::Break(())?,
    };

    if let Some(Ctrl { data: mut urb, tx }) = fetch_data {
        let result = ctx.vhci.fetch_data(&mut urb).map(|_| urb);
        tx.send(result)
            .expect("if recv is dropped then that thread must've panicked");
        ControlFlow::Continue((ctx, true))
    } else {
        ControlFlow::Continue((ctx, false))
    }
}

fn disconnect<'a>(ctx: task::Context<'a>) -> ControlFlow<(), (task::Context<'a>, bool)> {
    let disconnect = match ctx.disconnect_rx.try_recv() {
        Ok(disconnect) => Some(disconnect),
        Err(mpsc::error::TryRecvError::Empty) => None,
        Err(mpsc::error::TryRecvError::Disconnected) => ControlFlow::Break(())?,
    };

    if let Some(Ctrl { data: ref port, tx }) = disconnect {
        let result = ctx.vhci.port_disconnect(*port);
        tx.send(result)
            .expect("if recv is dropped then that thread has panicked");
        ctx.mailer.remove_tx(port);
        ControlFlow::Continue((ctx, true))
    } else {
        ControlFlow::Continue((ctx, false))
    }
}

fn mail_outgoing_work<'a>(ctx: task::Context<'a>) -> ControlFlow<(), (task::Context<'a>, bool)> {
    if ctx.outgoing.is_empty() {
        return ControlFlow::Continue((ctx, false));
    }

    let next = ctx.outgoing.pop_front().and_then(|work| match work {
        Work::CancelUrb(ref urb_handle) => ctx
            .mailer
            .get_tx_from_handle(*urb_handle)
            .map(|tx| (work, tx)),
        Work::ProcessUrb(ref urb) => match urb {
            vhci::Urb::Ctrl(urb_control) => {
                let port = Port::new((urb_control.w_index & 0x00ff) as u8 + 1).unwrap();
                ctx.mailer.link_handle_to_port(urb.handle(), port);
                ctx.mailer.get_tx_from_port(port).map(|tx| (work, tx))
            }
            _ => ctx
                .mailer
                .get_tx_from_handle(urb.handle())
                .map(|tx| (work, tx)),
        },
        Work::PortStat(ref stat) => ctx.mailer.get_tx_from_port(stat.index).map(|tx| (work, tx)),
    });

    if let Some((work, tx)) = next {
        if tx.blocking_send(work).is_err() {
            todo!("Remove tx from mailer since there's no one listening")
        }
        ControlFlow::Continue((ctx, true))
    } else {
        // Even though we didn't actually mail anything, we still removed
        // a work item from the queue, so that still counts as progress.
        ControlFlow::Continue((ctx, true))
    }
}

fn recv_work<'a>(ctx: task::Context<'a>) -> ControlFlow<(), (task::Context<'a>, bool)> {
    let timeout = TimeoutMillis::Time(ClosedBoundedI16::new(1).unwrap());
    match ctx.vhci.fetch_work_timeout(timeout) {
        Ok(work) => {
            ctx.outgoing.push_back(work);
            ControlFlow::Continue((ctx, true))
        }
        Err(err)
            if err.kind() == std::io::ErrorKind::TimedOut
                || err.kind() == std::io::ErrorKind::Interrupted
                || err.raw_os_error().unwrap() == vhci::libc::ENODATA =>
        {
            ControlFlow::Continue((ctx, false))
        }
        Err(_err) => {
            todo!("Figure out what kinda errors we can get here");
            // UPDATE: I'm pretty sure the only other errors we can get
            //         here are EINVAL errors, which shouldn't be possible
            //         anymore since we validate input already.
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn remove_last_tx_works() {
        let mut mailer = Mailer::default();

        let (tx, _rx) = tokio::sync::mpsc::channel(20);
        mailer.insert_tx(Port::new(1).unwrap(), tx);
        mailer.remove_tx(&Port::new(1).unwrap());

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

        mailer.remove_tx(&Port::new(1).unwrap());
        let index = mailer.port_to_work.get(&Port::new(5).unwrap()).unwrap();
        assert_eq!(*index, 0);
    }

    #[test]
    fn controller_idles() {
        let controller = Controller::start(OpenBoundedU8::new(3).unwrap()).unwrap();

        thread::sleep(Duration::from_millis(500));
        controller.shutdown();
    }

    #[tokio::test]
    async fn can_listen_for_work() {
        let mut controller = Controller::start(OpenBoundedU8::new(8).unwrap()).unwrap();

        let (port_a, mut a) = controller
            .register(RegisterPort::Port(Port::new(4).unwrap()), DataRate::Full)
            .await
            .unwrap();

        let (port_b, mut b) = controller
            .register(RegisterPort::Port(Port::new(2).unwrap()), DataRate::Full)
            .await
            .unwrap();

        for _ in 0..8 {
            tokio::select! {
                work = a.recv() => {
                    if let Some(work) = work {
                        println!("{work:?}");
                    }
                },
                work = b.recv() => {
                    if let Some(work) = work {
                        println!("{work:?}");
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    break;
                }
            }
        }

        controller.disconnect(port_a).await.unwrap();
        controller.disconnect(port_b).await.unwrap();

        controller.shutdown();
    }
}
