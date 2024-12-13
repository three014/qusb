use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use vhci::{DataRate, Port, Urb, UrbHandle, Vhci, Work};

use crate::utils::{OpenBoundedU8, SimpleMap};

mod task;

pub struct FetchData {
    urb: Urb,
    tx: oneshot::Sender<std::io::Result<Urb>>,
}

pub enum RegisterPort {
    Any,
    Port(Port),
}

pub struct Register {
    port: RegisterPort,
    data_rate: DataRate,
    tx: oneshot::Sender<std::io::Result<mpsc::Receiver<Work>>>,
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
    register_tx: mpsc::Sender<Register>,
    fetch_data_tx: mpsc::Sender<FetchData>,
}

impl Controller {
    pub fn start(num_ports: OpenBoundedU8<0, 32>) -> std::io::Result<Self> {
        let (register_tx, mut register_rx) = mpsc::channel::<Register>(4);
        let (fetch_data_tx, mut fetch_data_rx) = mpsc::channel::<FetchData>(8);
        let mut vhci = Vhci::open(num_ports)?;

        let runner = move || -> std::io::Result<()> {
            use task::*;
            let mut mailer = Mailer::default();
            let mut work_queue = VecDeque::new();
            let mut sched = task::Scheduler::<4>::new();
            sched.push(Task {
                name: TaskName::Register,
                f: register,
                pri: Priority::Background,
            });
            sched.push(Task {
                name: TaskName::FetchData,
                f: fetch_data,
                pri: Priority::Normal,
            });
            sched.push(Task {
                name: TaskName::MailOutgoing,
                f: mail_outgoing_work,
                pri: Priority::High,
            });
            sched.push(Task {
                name: TaskName::RecvWork,
                f: recv_work,
                pri: Priority::High,
            });

            while sched
                .run_next(Context {
                    register_rx: &mut register_rx,
                    fetch_data_rx: &mut fetch_data_rx,
                    vhci: &mut vhci,
                    mailer: &mut mailer,
                    outgoing: &mut work_queue,
                })
                .is_continue()
            {}

            // TODO: Disconnect all devices somehow

            Ok(())
        };

        let handle = Arc::new(Mutex::new(Some(std::thread::spawn(runner))));

        Ok(Self {
            handle,
            register_tx,
            fetch_data_tx,
        })
    }

    pub async fn register(
        &mut self,
        port: RegisterPort,
        data_rate: DataRate,
    ) -> std::io::Result<mpsc::Receiver<Work>> {
        let (tx, rx) = oneshot::channel();
        let register = Register {
            port,
            data_rate,
            tx,
        };

        self.register_tx.send(register).await.unwrap();
        rx.await.unwrap()
    }

    pub async fn fetch_data(&mut self, urb: Urb) -> std::io::Result<Urb> {
        let (tx, rx) = oneshot::channel();
        let fetch_data = FetchData { urb, tx };

        self.fetch_data_tx.send(fetch_data).await.unwrap();
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

        thread::sleep(Duration::from_secs(1));
        controller.shutdown();
    }

    #[tokio::test]
    async fn can_listen_for_work() {
        let mut controller = Controller::start(OpenBoundedU8::new(8).unwrap()).unwrap();

        let mut a = controller
            .register(RegisterPort::Any, DataRate::Full)
            .await
            .unwrap();

        let mut b = controller
            .register(RegisterPort::Any, DataRate::Full)
            .await
            .unwrap();

        for _ in 0..4 {
            tokio::select! {
                work = a.recv() => {
                    let work = work.unwrap();
                    println!("{work:?}");
                },
                work = b.recv() => {
                    let work = work.unwrap();
                    println!("{work:?}");
                }
            }
        }

        controller.shutdown();
    }
}
