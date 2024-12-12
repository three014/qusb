use std::{collections::VecDeque, ops::ControlFlow, time::Duration};

use heapless::{binary_heap::Min, BinaryHeap};
use tokio::sync::mpsc;
use vhci::{
    utils::{ClosedBoundedI16, TimeoutMillis},
    Port, Work,
};

use super::{Mailer, Register, RegisterPort};

pub struct TaskData<'a, 'b, 'c, 'd> {
    pub register_rx: &'a mut mpsc::Receiver<Register>,
    pub vhci: &'b mut vhci::Vhci,
    pub mailer: &'c mut Mailer,
    pub outgoing: &'d mut VecDeque<Work>,
}

#[derive(Debug)]
struct Key {
    timer: Duration,
    task: fn(TaskData) -> ControlFlow<(), ()>,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timer.cmp(&other.timer)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.timer.eq(&other.timer)
    }
}

impl Eq for Key {}

pub struct Scheduler<const N: usize> {
    tasks: BinaryHeap<Key, Min, N>,
}

impl<const N: usize> Scheduler<N> {
    pub const fn new() -> Self {
        Self {
            tasks: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, task: fn(TaskData) -> ControlFlow<(), ()>) {
        self.tasks
            .push(Key {
                timer: Duration::ZERO,
                task,
            })
            .unwrap()
    }

    pub fn run_next(&mut self, data: TaskData) -> ControlFlow<(), ()> {
        let Key { mut timer, task } = self
            .tasks
            .pop()
            .expect("should be called after populating heap with tasks");
        let now = std::time::Instant::now();
        task(data)?;
        timer += now.elapsed(); //.mul_f64(1.0);
        self.tasks
            .push(Key { timer, task })
            .expect("should have had enough room since we just took out this key");
        ControlFlow::Continue(())
    }
}

pub fn recv_register(data: TaskData) -> ControlFlow<(), ()> {
    let TaskData {
        register_rx,
        vhci,
        mailer,
        outgoing: _,
    } = data;
    let register = match register_rx.try_recv() {
        Ok(register) => Some(register),
        Err(mpsc::error::TryRecvError::Empty) => None,
        Err(mpsc::error::TryRecvError::Disconnected) => ControlFlow::Break(())?,
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

    match result {
        Some((Ok(port), oneshot_tx)) => {
            let (mpsc_tx, mpsc_rx) = mpsc::channel::<Work>(32);
            mailer.insert_tx(port, mpsc_tx);
            oneshot_tx
                .send(Ok(mpsc_rx))
                .expect("If recv is dropped then that thread must've panic'd");
        }
        Some((Err(err), oneshot_tx)) => {
            let _ = oneshot_tx.send(Err(err));
        }
        None => (),
    }

    ControlFlow::Continue(())
}

pub fn mail_outgoing_work(data: TaskData) -> ControlFlow<(), ()> {
    let TaskData {
        register_rx: _,
        vhci: _,
        mailer,
        outgoing,
    } = data;
    for work in outgoing.drain(..) {
        let tx = match work {
            Work::CancelUrb(ref urb_handle) => mailer.get_tx_from_handle(*urb_handle),
            Work::ProcessUrb(ref urb) => match urb {
                vhci::Urb::Ctrl(urb_control) => {
                    let port = Port::new((urb_control.w_index & 0x00ff) as u8 + 1).unwrap();
                    mailer.link_handle_to_port(urb.handle(), port);
                    mailer.get_tx_from_port(port)
                }
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
    ControlFlow::Continue(())
}

pub fn recv_work(data: TaskData) -> ControlFlow<(), ()> {
    let TaskData {
        register_rx: _,
        vhci,
        mailer: _,
        outgoing,
    } = data;
    let timeout = TimeoutMillis::Time(ClosedBoundedI16::new(1000).unwrap());
    match vhci.fetch_work_timeout(timeout) {
        Ok(work) => outgoing.push_back(work),
        Err(err)
            if err.kind() == std::io::ErrorKind::TimedOut
                || err.kind() == std::io::ErrorKind::Interrupted
                || err.raw_os_error().unwrap() == vhci::libc::ENODATA =>
        {
            ()
        }
        Err(_err) => {
            todo!("Figure out what kinda errors we can get here");
        }
    }
    ControlFlow::Continue(())
}
