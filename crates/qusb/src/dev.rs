use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use vhci::{
    utils::{ClosedBoundedI16, TimeoutMillis},
    DataRate, Port, UrbHandle, Work,
};

use crate::utils::{OpenBoundedU8, SimpleMap};

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
    pub fn insert_port_tx(&mut self, port: Port, tx: mpsc::Sender<Work>) {
        self.work_line.push(tx);
        let index = self.work_line.len() - 1;
        self.port_to_work.insert(port, index);
    }

    pub fn map_handle_to_tx(&mut self, handle: UrbHandle, port: Port) -> bool {
        let index = self.port_to_work.get(&port);
        if let Some(&index) = index {
            self.handle_to_work.insert(handle, index);
            true
        } else {
            false
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

    fn contains_port(&self, index: Port) -> bool {
        self.port_to_work.contains_key(&index)
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    handle: Arc<Mutex<Option<std::thread::JoinHandle<std::io::Result<()>>>>>,
    register_tx: mpsc::Sender<Register>,
}

impl Controller {
    pub fn new(num_ports: OpenBoundedU8<0, 32>) -> std::io::Result<Self> {
        let (register_tx, mut register_rx) = mpsc::channel::<Register>(8);
        let mut vhci = vhci::Vhci::open(num_ports)?;

        let runner = move || -> std::io::Result<()> {
            let timeout = TimeoutMillis::Time(ClosedBoundedI16::new(1000).unwrap());
            let mut mailer = Mailer::default();
            let mut work_queue = VecDeque::new();

            loop {
                let register = match register_rx.try_recv() {
                    Ok(register) => Some(register),
                    Err(mpsc::error::TryRecvError::Empty) => None,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                };

                let result = if let Some(Register {
                    port: RegisterPort::Any,
                    data_rate,
                    tx,
                }) = register
                {
                    Some((vhci.port_connect_any(data_rate), tx))
                } else if let Some(Register {
                    port: RegisterPort::Port(port),
                    data_rate,
                    tx,
                }) = register
                {
                    Some((vhci.port_connect(port, data_rate).map(|_| port), tx))
                } else {
                    None
                };

                for work in work_queue.drain(..).filter_map(|work: std::io::Result<Work>| work.ok()) {
                    let tx = match work {
                        Work::CancelUrb(ref urb_handle) => {
                            mailer.get_tx_from_handle(*urb_handle)
                        }
                        Work::ProcessUrb(ref urb) => match urb {
                            vhci::Urb::Ctrl(urb_control) => mailer.get_tx_from_port(
                                Port::new((urb_control.w_index & 0x00ff) as u8 + 1).unwrap(),
                            ),
                            _ => mailer.get_tx_from_handle(urb.handle()),
                        },
                        Work::PortStat(ref stat) => mailer.get_tx_from_port(stat.index),
                    };

                    if let Some(tx) = tx {
                        if tx.blocking_send(work).is_err() {
                            todo!("Remove tx from mailer since there's no one listening")
                        }
                    }
                }

                match result {
                    Some((Ok(port), oneshot_tx)) => {
                        let (mpsc_tx, mpsc_rx) = mpsc::channel::<Work>(32);
                        mailer.insert_port_tx(port, mpsc_tx);
                        oneshot_tx.send(Ok(mpsc_rx)).unwrap();
                    }
                    Some((Err(err), oneshot_tx)) => {
                        let _ = oneshot_tx.send(Err(err));
                    }
                    None => (),
                }

                work_queue.push_back(vhci.fetch_work_timeout(timeout));
            }

            Ok(())
        };

        let handle = Arc::new(Mutex::new(Some(std::thread::spawn(runner))));

        Ok(Self {
            handle,
            register_tx,
        })
    }

    pub fn shutdown(self) {}
}
