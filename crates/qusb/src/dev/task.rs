use std::{
    collections::VecDeque, fmt::Debug, future::Future, ops::{Add, ControlFlow}, time::{Duration, Instant}
};

use heapless::{binary_heap::Min, BinaryHeap, Vec};
use tokio::sync::mpsc;
use vhci::{
    utils::{ClosedBoundedI16, TimeoutMillis},
    Port, Work,
};

use super::{FetchData, Mailer, Register, RegisterPort};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskName {
    Register,
    FetchData,
    MailOutgoing,
    RecvWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    High = 1,
    Normal,
    Low,
    Background,
}

#[derive(Debug)]
pub struct Context<'a, 'b, 'c, 'd, 'e> {
    pub register_rx: &'a mut mpsc::Receiver<Register>,
    pub fetch_data_rx: &'b mut mpsc::Receiver<FetchData>,
    pub vhci: &'c mut vhci::Vhci,
    pub mailer: &'d mut Mailer,
    pub outgoing: &'e mut VecDeque<Work>,
}

pub struct Task {
    pub name: TaskName,
    pub f: for<'a, 'b, 'c, 'd, 'e> fn(
        Context<'a, 'b, 'c, 'd, 'e>,
    ) -> ControlFlow<(), (Context<'a, 'b, 'c, 'd, 'e>, bool)>,
    pub pri: Priority,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("name", &self.name)
            .field("priority", &self.pri)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Key {
    timer: Duration,
    task: Task,
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

#[derive(Debug)]
pub struct Scheduler<const N: usize> {
    ready: BinaryHeap<Key, Min, N>,
    blocked: Vec<Option<Key>, N>,
    next_run: Option<Instant>,
}

impl<const N: usize> Scheduler<N> {
    pub const fn new() -> Self {
        Self {
            ready: BinaryHeap::new(),
            blocked: Vec::new(),
            next_run: None,
        }
    }

    pub fn push(&mut self, task: Task) {
        self.ready
            .push(Key {
                timer: Duration::ZERO,
                task,
            })
            .unwrap();
        self.blocked.push(None).unwrap();
    }

    pub fn run_next(&mut self, ctx: Context) -> ControlFlow<(), ()> {
        println!("{self:#?}");
        let next = self
            .ready
            .pop()
            .expect("should be called after populating heap with tasks");
        let Key { mut timer, task } = next;

        if let Some(sleep_dir) = self
            .next_run
            .and_then(|next_run| next_run.checked_duration_since(Instant::now()))
            .filter(|_| self.ready.len() == 1)
        {
            std::thread::sleep(sleep_dir);
        }

        let now = Instant::now();
        let (data, made_progress) = (task.f)(ctx)?;
        let elapsed = now.elapsed().as_secs_f64() * (0.5 * task.pri as u64 as f64);
        timer += Duration::from_secs_f64(elapsed);

        // Check state, possibly move tasks to waiting queue
        match task.name {
            TaskName::Register if made_progress => {
                // We have a new port! Wake up RecvWork and/or FetchData!
                if let Some(recv_work) = self.blocked[TaskName::RecvWork as usize].take() {
                    self.ready.push(recv_work).unwrap();
                }
                if let Some(fetch_data) = self.blocked[TaskName::FetchData as usize].take() {
                    self.ready.push(fetch_data).unwrap();
                }
                self.ready.push(Key { timer, task }).unwrap();
            }
            TaskName::Register if !made_progress && !data.vhci.is_active() => {
                // Set a timer for the next time we can run
                let _ = self
                    .next_run
                    .insert(Instant::now() + Duration::from_millis(200));
                self.ready.push(Key { timer, task }).unwrap();
            }
            TaskName::FetchData if !made_progress && !data.vhci.is_active() => {
                // We don't have any active ports, so we can add this task to
                // the waiting line until qusb registers a port
                let _ = self.blocked[TaskName::FetchData as usize].insert(Key { timer, task });
            }
            TaskName::MailOutgoing if !made_progress => {
                // No outgoing data right now, so let's not do that
                // work until we get more data from the vhci.
                let _ = self.blocked[TaskName::MailOutgoing as usize].insert(Key { timer, task });
            }
            TaskName::RecvWork if made_progress => {
                // We have work for the mailer! Let's add them back to the queue.
                if let Some(mailer) = self.blocked[TaskName::MailOutgoing as usize].take() {
                    self.ready.push(mailer).unwrap();
                }
                self.ready.push(Key { timer, task }).unwrap();
            }
            TaskName::RecvWork if !made_progress && !data.vhci.is_active() => {
                // We don't have any active ports, so we can add this task to
                // the waiting line until qusb registers a port
                let _ = self.blocked[TaskName::RecvWork as usize].insert(Key { timer, task });
            }
            // We're probably okay, so let's just add them back to the queue.
            _ => self.ready.push(Key { timer, task }).unwrap(),
        }

        ControlFlow::Continue(())
    }
}

pub fn register<'a, 'b, 'c, 'd, 'e>(
    ctx: Context<'a, 'b, 'c, 'd, 'e>,
) -> ControlFlow<(), (Context<'a, 'b, 'c, 'd, 'e>, bool)> {
    let register = match ctx.register_rx.try_recv() {
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
        Some((ctx.vhci.port_connect_any(data_rate), tx))
    } else if let Some(Register {
        port: RegisterPort::Port(port),
        data_rate,
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
                .send(Ok(mpsc_rx))
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

pub fn fetch_data<'a, 'b, 'c, 'd, 'e>(
    ctx: Context<'a, 'b, 'c, 'd, 'e>,
) -> ControlFlow<(), (Context<'a, 'b, 'c, 'd, 'e>, bool)> {
    let fetch_data = match ctx.fetch_data_rx.try_recv() {
        Ok(fetch_data) => Some(fetch_data),
        Err(mpsc::error::TryRecvError::Empty) => None,
        Err(mpsc::error::TryRecvError::Disconnected) => ControlFlow::Break(())?,
    };

    if let Some(FetchData { mut urb, tx }) = fetch_data {
        let result = ctx.vhci.fetch_data(&mut urb).map(|_| urb);
        tx.send(result)
            .expect("if recv is dropped then that thread must've panicked");
        ControlFlow::Continue((ctx, true))
    } else {
        ControlFlow::Continue((ctx, false))
    }
}

pub fn mail_outgoing_work<'a, 'b, 'c, 'd, 'e>(
    ctx: Context<'a, 'b, 'c, 'd, 'e>,
) -> ControlFlow<(), (Context<'a, 'b, 'c, 'd, 'e>, bool)> {
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

pub fn recv_work<'a, 'b, 'c, 'd, 'e>(
    ctx: Context<'a, 'b, 'c, 'd, 'e>,
) -> ControlFlow<(), (Context<'a, 'b, 'c, 'd, 'e>, bool)> {
    let timeout = TimeoutMillis::Time(ClosedBoundedI16::new(400).unwrap());
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
        }
    }
}
