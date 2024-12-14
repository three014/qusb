use std::{collections::VecDeque, fmt::Debug, ops::ControlFlow, time::Duration};

use heapless::{binary_heap::Min, BinaryHeap, Vec};
use quanta::Instant;
use tokio::sync::mpsc;
use vhci::{
    utils::{ClosedBoundedI16, TimeoutMillis},
    Port, Urb, Vhci, Work,
};

use super::{Ctrl, Mailer, Register, RegisterPort};

/// Keeps track of a task's decision to sleep
/// for some amount of time.
#[derive(Debug, Clone, Copy)]
pub struct Sleeper {
    next_run: Instant,
    task_name: TaskName,
}

impl PartialEq for Sleeper {
    fn eq(&self, other: &Self) -> bool {
        self.next_run.eq(&other.next_run)
    }
}
impl Eq for Sleeper {}
impl Ord for Sleeper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.next_run.cmp(&other.next_run)
    }
}
impl PartialOrd for Sleeper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Used to identify a task in the blocked map.
///
/// The names listed here **MUST** correspond to the names
/// used in the [`Task`] block and **CANNOT** be used more than
/// once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskName {
    Register = 0,
    FetchData,
    Disconnect,
    MailOutgoing,
    RecvWork,
}

/// The priority of a task in the scheduler. Lower values
/// mean the task is more likely to be scheduled.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Nice(crate::utils::OpenBoundedU8<0, 25>);

impl Nice {
    /// Creates a new [`Nice`] value.
    ///
    /// Returns `None` if the value is not in
    /// the allowed range.
    pub const fn new(p: u8) -> Option<Self> {
        if let Some(num) = crate::utils::OpenBoundedU8::new(p) {
            Some(Self(num))
        } else {
            None
        }
    }

    /// Returns the inner value.
    pub const fn get(&self) -> u8 {
        self.0.get()
    }

    pub const NORMAL: Self = Self::new(16).unwrap();
}

impl std::fmt::Debug for Nice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Nice").field(&self.get()).finish()
    }
}

/// External data that all tasks are allowed to use.
#[derive(Debug)]
pub struct Context<'a> {
    pub register_rx: &'a mut mpsc::Receiver<Ctrl<Register, mpsc::Receiver<Work>>>,
    pub fetch_data_rx: &'a mut mpsc::Receiver<Ctrl<Urb, Urb>>,
    pub disconnect_rx: &'a mut mpsc::Receiver<Ctrl<Port, ()>>,
    pub vhci: &'a mut Vhci,
    pub mailer: &'a mut Mailer,
    pub outgoing: &'a mut VecDeque<Work>,
}

/// Represents a task for the scheduler to run.
pub struct Task {
    name: TaskName,
    f: for<'a> fn(Context<'a>) -> ControlFlow<(), (Context<'a>, bool)>,
    nice: Nice,
    tries: u8,
    timer: Duration,
    tries_left: u8,
}

impl Task {
    pub const fn new(
        name: TaskName,
        f: for<'a> fn(Context<'a>) -> ControlFlow<(), (Context<'a>, bool)>,
        nice: Nice,
        tries: u8,
    ) -> Self {
        Self {
            name,
            f,
            nice,
            tries,
            timer: Duration::ZERO,
            tries_left: tries,
        }
    }

    pub const fn with_timer(self, timer: Duration) -> Self {
        Self { timer, ..self }
    }

    pub const fn with_tries_left(self, tries_left: u8) -> Self {
        Self { tries_left, ..self }
    }
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("name", &self.name)
            .field("nice", &self.nice)
            .field("timer", &self.timer)
            .finish()
    }
}
impl Ord for Task {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timer.cmp(&other.timer)
    }
}
impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.timer.eq(&other.timer)
    }
}
impl Eq for Task {}

#[derive(Debug)]
pub struct Scheduler<const N: usize> {
    ready: BinaryHeap<Task, Min, N>,
    blocked: Vec<Option<Task>, N>,
    sleep: BinaryHeap<Sleeper, Min, N>,
    clock: quanta::Clock,
}

impl<const N: usize> Scheduler<N> {
    pub fn new() -> Self {
        let mut blocked = Vec::new();
        for _ in 0..N {
            blocked.push(None).unwrap();
        }
        Self {
            ready: BinaryHeap::new(),
            blocked,
            sleep: BinaryHeap::new(),
            clock: quanta::Clock::new(),
        }
    }

    pub fn push(&mut self, task: Task) {
        self.ready.push(task).unwrap();
    }

    pub fn time_running(&self) -> Duration {
        self.ready
            .iter()
            .chain(self.blocked.iter().filter_map(|block| block.as_ref()))
            .map(|block| {
                block
                    .timer
                    .mul_f64(Nice::NORMAL.get() as f64 / block.nice.get() as f64)
            })
            .reduce(|acc, timer| acc + timer)
            .unwrap()
    }

    pub fn run_next(&mut self, ctx: Context) -> ControlFlow<(), Duration> {
        // ---- Step 1: Check if we need to bring out a sleeping task ----
        while self.sleep.peek().is_some_and(|sleeper| {
            sleeper
                .next_run
                .checked_duration_since(Instant::now())
                .is_none()
        }) {
            let sleeper = self.sleep.pop().unwrap();
            let block = self.blocked[sleeper.task_name as usize].take().unwrap();
            self.ready.push(block).unwrap();
        }

        // ---- Step 2: Check if we have a task to run ----
        if let Some(next) = self.ready.pop() {
            // ---- Step 3: Run the task! ----
            let Task {
                mut timer,
                name,
                f,
                nice,
                tries,
                tries_left,
            } = next;
            let now = self.clock.now();
            let (data, made_progress) = f(ctx)?;
            // timer += now.elapsed();
            let elapsed = now.elapsed();
            timer += elapsed.mul_f64((1.0 / Nice::NORMAL.get() as f64) * nice.get() as f64);

            // ---- Step 4: Check the state of the task, possibly move
            //      task to the blocked queue and possibly set a sleeper ----
            match name {
                TaskName::Register if made_progress => {
                    // We have work! Wake everyone up!
                    for sleeper in self.sleep.iter() {
                        if let Some(block) = self.blocked[sleeper.task_name as usize].take() {
                            self.ready.push(block).unwrap();
                        }
                    }
                    self.sleep.clear();

                    // We also told ourselves to move a few tasks out of the
                    // blocked queue if we got a new port.
                    for task_name in [
                        TaskName::FetchData,
                        TaskName::Disconnect,
                        TaskName::RecvWork,
                    ] {
                        if let Some(block) = self.blocked[task_name as usize].take() {
                            self.ready.push(block).unwrap();
                        }
                    }

                    self.ready
                        .push(Task::new(name, f, nice, tries).with_timer(timer))
                        .unwrap();
                }
                TaskName::Register if !made_progress && tries_left == 0 => {
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now()
                                + elapsed.mul_f64(tries as f64 * nice.get() as f64),
                            task_name: TaskName::Register,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::Register as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::Register if !made_progress => {
                    self.ready
                        .push(
                            Task::new(name, f, nice, tries)
                                .with_timer(timer)
                                .with_tries_left(tries_left - 1),
                        )
                        .unwrap();
                }
                TaskName::FetchData if !made_progress && !data.vhci.is_active() => {
                    // We don't have any active ports, so we can add this task to
                    // the waiting line until qusb registers a port
                    let _ = self.blocked[TaskName::FetchData as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::FetchData if !made_progress && tries_left == 0 => {
                    // In this case we do have active ports, but no one has asked
                    // us to fetch any data, so we'll sleep for a bit
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64(tries as f64 * nice.get() as f64),
                            task_name: TaskName::FetchData,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::FetchData as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::FetchData if !made_progress => {
                    self.ready
                        .push(
                            Task::new(name, f, nice, tries)
                                .with_timer(timer)
                                .with_tries_left(tries_left - 1),
                        )
                        .unwrap()
                }
                TaskName::Disconnect if !made_progress && !data.vhci.is_active() => {
                    // We don't have any active ports, so we can add this task to
                    // the waiting line until qusb registers a port
                    let _ = self.blocked[TaskName::Disconnect as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::Disconnect if !made_progress && tries_left == 0 => {
                    // In this case no one has asked us to disconnect, but we are active,
                    // so we'll sleep for a bit
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64((tries + 1) as f64 * nice.get() as f64),
                            task_name: TaskName::Disconnect,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::Disconnect as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::Disconnect if !made_progress => {
                    self.ready
                        .push(
                            Task::new(name, f, nice, tries)
                                .with_timer(timer)
                                .with_tries_left(tries_left - 1),
                        )
                        .unwrap();
                }
                TaskName::MailOutgoing if !made_progress => {
                    // No outgoing data right now, so let's not do that
                    // work until we get more data from the vhci.
                    let _ = self.blocked[TaskName::MailOutgoing as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::RecvWork if made_progress => {
                    // We have work for the mailer! Let's add them back to the queue.
                    // println!("New work at {:?}", self.clock.now());
                    if let Some(mailer) = self.blocked[TaskName::MailOutgoing as usize].take() {
                        self.ready.push(mailer).unwrap();
                    }
                    self.ready
                        .push(Task::new(name, f, nice, tries).with_timer(timer))
                        .unwrap();
                }
                TaskName::RecvWork if !made_progress && !data.vhci.is_active() => {
                    // We don't have any active ports, so we can add this task to
                    // the waiting line until qusb registers a port
                    let _ = self.blocked[TaskName::RecvWork as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::RecvWork if !made_progress && tries_left == 0 => {
                    // We are active, but we didn't have any work, so we can sleep for a liiiitle bit
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64((tries + 1) as f64 * nice.get() as f64),
                            task_name: TaskName::RecvWork,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::RecvWork as usize]
                        .insert(Task::new(name, f, nice, tries).with_timer(timer));
                }
                TaskName::RecvWork if !made_progress => {
                    self.ready
                        .push(
                            Task::new(name, f, nice, tries)
                                .with_timer(timer)
                                .with_tries_left(tries_left - 1),
                        )
                        .unwrap();
                }
                // We're probably okay, so let's just add them back to the queue.
                _ => self
                    .ready
                    .push(Task::new(name, f, nice, tries).with_timer(timer))
                    .unwrap(),
            }
        } else {
            let sleeper = self
                .sleep
                .peek()
                .expect("if no tasks in ready queue, then we have at least one sleeper");
            if let Some(sleep_dur) = sleeper.next_run.checked_duration_since(self.clock.now()) {
                // let now = self.clock.now();
                crate::utils::precise_sleep(sleep_dur.as_secs_f64());
                // println!("Slept for {:?}", now.elapsed());
                return ControlFlow::Continue(sleep_dur);
            }
        }

        ControlFlow::Continue(Duration::ZERO)
    }
}

pub fn register<'a>(ctx: Context<'a>) -> ControlFlow<(), (Context<'a>, bool)> {
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

pub fn fetch_data<'a>(ctx: Context<'a>) -> ControlFlow<(), (Context<'a>, bool)> {
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

pub fn disconnect<'a>(ctx: Context<'a>) -> ControlFlow<(), (Context<'a>, bool)> {
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

pub fn mail_outgoing_work<'a>(ctx: Context<'a>) -> ControlFlow<(), (Context<'a>, bool)> {
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

pub fn recv_work<'a>(ctx: Context<'a>) -> ControlFlow<(), (Context<'a>, bool)> {
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
