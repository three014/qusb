use std::{collections::VecDeque, fmt::Debug, ops::ControlFlow, time::Duration};

use heapless::{binary_heap::Min, BinaryHeap, Vec};
use quanta::Instant;
use tokio::sync::mpsc;
use vhci::{Port, Urb, Vhci, Work};

use super::{Ctrl, Mailer, Register};

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

impl std::fmt::Display for Sleeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task {:?} - in {:?}",
            self.task_name,
            self.next_run.saturating_duration_since(Instant::now())
        )
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

    /// The default value of a [`Nice`].
    ///
    /// A task's virtual timer
    /// is guaranteed to equal the actual amount of
    /// time it ran when using this value.
    pub const NORMAL: Self = Self::new(16).unwrap();
}

impl Default for Nice {
    fn default() -> Self {
        Self::NORMAL
    }
}

impl std::fmt::Debug for Nice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Nice").field(&self.get()).finish()
    }
}

/// External data that all tasks are allowed to use.
#[derive(Debug)]
pub struct Context<'a> {
    pub register_rx: &'a mut mpsc::Receiver<Ctrl<Register, (Port, mpsc::Receiver<Work>)>>,
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

    const fn reset_tries(self) -> Task {
        Self {
            tries_left: self.tries,
            ..self
        }
    }

    const fn out_of_tries(&self) -> bool {
        self.tries_left == 0
    }

    const fn mark_try(self) -> Self {
        Self {
            tries_left: self.tries_left - 1,
            ..self
        }
    }

    const fn sleep_factor(&self) -> f64 {
        (self.tries + 1) as f64 * self.nice.get() as f64
    }

    const fn nice_factor(&self) -> f64 {
        (1.0 / Nice::NORMAL.get() as f64) * self.nice.get() as f64
    }

    const fn realtime_factor(&self) -> f64 {
        Nice::NORMAL.get() as f64 / self.nice.get() as f64
    }

    fn run_time(&self) -> Duration {
        self.timer.mul_f64(self.realtime_factor())
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

pub struct Scheduler<const N: usize> {
    ready: BinaryHeap<Task, Min, N>,
    blocked: Vec<Option<Task>, N>,
    sleep: BinaryHeap<Sleeper, Min, N>,
    clock: quanta::Clock,
}

impl<const N: usize> std::fmt::Debug for Scheduler<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("ready", &self.ready)
            .field("blocked", &self.blocked)
            .field("sleep", &self.sleep)
            .finish()
    }
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
            .chain(self.blocked.iter().filter_map(|task| task.as_ref()))
            .map(|task| task.run_time())
            .reduce(|acc, timer| acc + timer)
            .unwrap()
    }

    pub fn run_next(&mut self, ctx: Context) -> ControlFlow<(), Duration> {
        // ---- Step 1: Check if we need to bring out a sleeping task ----
        while self.sleep.peek().is_some_and(|sleeper| {
            sleeper
                .next_run
                .checked_duration_since(self.clock.now())
                .is_none()
        }) {
            let sleeper = self.sleep.pop().unwrap();
            let task = self.blocked[sleeper.task_name as usize].take().unwrap();
            self.ready.push(task).unwrap();
        }

        // ---- Step 2: Check if we have a task to run ----
        if let Some(mut task) = self.ready.pop() {
            // ---- Step 3: Run the task! ----
            let now = self.clock.now();
            let (ctx, made_progress) = (task.f)(ctx)?;
            let elapsed = now.elapsed();
            task.timer += elapsed.mul_f64(task.nice_factor());

            // ---- Step 4: Check the state of the task, possibly move
            //      task to the blocked queue and possibly set a sleeper ----
            match task.name {
                TaskName::Register if made_progress => {
                    // We have work! Wake everyone up!
                    for sleeper in self.sleep.iter() {
                        if let Some(task) = self.blocked[sleeper.task_name as usize].take() {
                            self.ready.push(task).unwrap();
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
                        if let Some(task) = self.blocked[task_name as usize].take() {
                            self.ready.push(task).unwrap();
                        }
                    }

                    self.ready.push(task).unwrap();
                }
                TaskName::Register if !made_progress && task.out_of_tries() => {
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64(task.sleep_factor()),
                            task_name: TaskName::Register,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::Register as usize].insert(task.reset_tries());
                }
                TaskName::Register if !made_progress => {
                    self.ready.push(task.mark_try()).unwrap();
                }
                TaskName::FetchData if !made_progress && !ctx.vhci.is_active() => {
                    // We don't have any active ports, so we can add this task to
                    // the waiting line until qusb registers a port
                    let _ = self.blocked[TaskName::FetchData as usize].insert(task.reset_tries());
                }
                TaskName::FetchData if !made_progress && task.out_of_tries() => {
                    // In this case we do have active ports, but no one has asked
                    // us to fetch any data, so we'll sleep for a bit
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64(task.sleep_factor()),
                            task_name: TaskName::FetchData,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::FetchData as usize].insert(task.reset_tries());
                }
                TaskName::FetchData if !made_progress => self.ready.push(task.mark_try()).unwrap(),
                TaskName::Disconnect if !made_progress && !ctx.vhci.is_active() => {
                    // We don't have any active ports, so we can add this task to
                    // the waiting line until qusb registers a port
                    let _ = self.blocked[TaskName::Disconnect as usize].insert(task.reset_tries());
                }
                TaskName::Disconnect if !made_progress && task.out_of_tries() => {
                    // In this case no one has asked us to disconnect, but we are active,
                    // so we'll sleep for a bit
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64(task.sleep_factor()),
                            task_name: TaskName::Disconnect,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::Disconnect as usize].insert(task.reset_tries());
                }
                TaskName::Disconnect if !made_progress => {
                    self.ready.push(task.mark_try()).unwrap();
                }
                TaskName::MailOutgoing if !made_progress => {
                    // No outgoing data right now, so let's not do that
                    // work until we get more data from the vhci.
                    let _ =
                        self.blocked[TaskName::MailOutgoing as usize].insert(task.reset_tries());
                }
                TaskName::RecvWork if made_progress => {
                    // We have work for the mailer! Let's add them back to the queue.
                    // println!("New work at {:?}", self.clock.now());
                    if let Some(mailer) = self.blocked[TaskName::MailOutgoing as usize].take() {
                        self.ready.push(mailer).unwrap();
                    }
                    self.ready.push(task.reset_tries()).unwrap();
                }
                TaskName::RecvWork if !made_progress && !ctx.vhci.is_active() => {
                    // We don't have any active ports, so we can add this task to
                    // the waiting line until qusb registers a port
                    let _ = self.blocked[TaskName::RecvWork as usize].insert(task.reset_tries());
                }
                TaskName::RecvWork if !made_progress && task.out_of_tries() => {
                    // We are active, but we didn't have any work, so we can sleep for a liiiitle bit
                    self.sleep
                        .push(Sleeper {
                            next_run: self.clock.now() + elapsed.mul_f64(task.sleep_factor()),
                            task_name: TaskName::RecvWork,
                        })
                        .unwrap();
                    let _ = self.blocked[TaskName::RecvWork as usize].insert(task.reset_tries());
                }
                TaskName::RecvWork if !made_progress => {
                    self.ready.push(task.mark_try()).unwrap();
                }
                // We're probably okay, so let's just add them back to the queue.
                _ => self.ready.push(task.reset_tries()).unwrap(),
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
