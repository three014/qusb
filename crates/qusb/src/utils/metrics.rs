use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, VecDeque},
    iter::Sum,
    ops::Div,
};

pub struct Max<const N: usize, T, V> {
    inner: BTreeMap<Reverse<T>, V>,
}

impl<const N: usize, T: Ord, V> Max<N, T, V> {
    pub const fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn push(&mut self, item: impl Into<T>, data: V) {
        self.inner.insert(Reverse(item.into()), data);
        if self.inner.len() > N {
            self.inner.pop_last();
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<const N: usize, T: Ord + std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug
    for Max<N, T, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.inner.iter().map(|(Reverse(item), data)| (item, data)))
            .finish()
    }
}

pub struct Min<const N: usize, T> {
    inner: BTreeSet<T>,
}

impl<const N: usize, T: Ord> Min<N, T> {
    pub const fn new() -> Self {
        Self {
            inner: BTreeSet::new(),
        }
    }

    pub fn push(&mut self, item: impl Into<T>) {
        self.inner.insert(item.into());
        if self.inner.len() > N {
            self.inner.pop_last();
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<const N: usize, T: Ord + std::fmt::Debug> std::fmt::Debug for Min<N, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.inner.iter()).finish()
    }
}

pub struct RollingAvg<const N: usize, T> {
    inner: VecDeque<T>,
}

impl<const N: usize, T> RollingAvg<N, T> {
    pub const fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    pub fn preallocated() -> Self {
        Self {
            inner: VecDeque::with_capacity(N),
        }
    }

    pub fn push(&mut self, item: impl Into<T>) {
        if self.inner.len() >= N {
            self.inner.pop_front();
        }
        self.inner.push_back(item.into());
    }

    pub fn mean<'a>(&'a self) -> Option<T>
    where
        T: Sum<&'a T> + Div<usize, Output = T>,
    {
        if self.inner.is_empty() {
            return None;
        }
        let len = self.inner.len();
        let sum: T = self.inner.iter().sum();
        Some(sum / len)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Duration(std::time::Duration);

impl<'a> Sum<&'a Duration> for Duration {
    fn sum<I: Iterator<Item = &'a Duration>>(iter: I) -> Self {
        Duration(iter.map(|dur| dur.0).sum())
    }
}

impl Div<usize> for Duration {
    type Output = Duration;

    fn div(self, rhs: usize) -> Self::Output {
        Duration(self.0.checked_div(rhs.try_into().unwrap()).unwrap())
    }
}

impl From<std::time::Duration> for Duration {
    fn from(value: std::time::Duration) -> Self {
        Duration(value)
    }
}

impl From<Duration> for std::time::Duration {
    fn from(value: Duration) -> Self {
        value.0
    }
}
