#![allow(dead_code)]

use core::fmt;
use fxhash::FxBuildHasher;
use nohash_hasher::IsEnabled;
use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    io,
    time::{Duration, Instant},
};

pub mod mpsc {
    pub type AsyncSender<T> = flume::r#async::SendSink<'static, T>;
    pub type AsyncReceiver<T> = flume::r#async::RecvStream<'static, T>;
    pub type Sender<T> = flume::Sender<T>;
    pub type Receiver<T> = flume::Receiver<T>;

    pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        flume::bounded(capacity)
    }
}
pub mod oneshot {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_lite::FutureExt;

    pub struct Sender<T> {
        inner: flume::Sender<T>,
    }

    impl<T> Sender<T> {
        #[inline]
        pub fn send(self, item: T) -> Result<(), flume::SendError<T>> {
            self.inner.send(item)
        }
    }

    pub struct AsyncReceiver<T: 'static> {
        inner: flume::r#async::RecvFut<'static, T>,
    }

    impl<T> Future for AsyncReceiver<T> {
        type Output = Result<T, flume::RecvError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.inner.poll(cx)
        }
    }

    pub fn channel_async<T>() -> (Sender<T>, AsyncReceiver<T>) {
        let (tx, rx) = flume::bounded(1);
        (
            Sender { inner: tx },
            AsyncReceiver {
                inner: rx.into_recv_async(),
            },
        )
    }
}
pub mod metrics {
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
}

pub struct Interval {
    start: Instant,
    period: Duration,
}

impl Interval {
    pub fn new(period: Duration) -> Self {
        Self {
            start: Instant::now(),
            period,
        }
    }

    #[inline]
    pub async fn tick(&self) {
        let time_til_next_tick = {
            let elapsed = self.start.elapsed().as_secs_f64();
            let period = self.period.as_secs_f64();
            period - (elapsed % period)
        };
        compio_runtime::time::sleep(Duration::from_secs_f64(time_til_next_tick)).await
    }
}

#[inline(always)]
pub async fn blocker<T, F>(f: Option<F>) -> T
where
    F: Future<Output = T>,
{
    if let Some(f) = f {
        f.await
    } else {
        #[inline(never)]
        #[cold]
        async fn pending<T>() -> T {
            std::future::pending().await
        }

        pending().await
    }
}

#[cold]
#[inline(never)]
pub fn cold<T>(t: T) -> T {
    t
}

pub struct Counter {
    inner: u32,
}

impl Counter {
    pub const fn new(start: u32) -> Self {
        Self { inner: start }
    }

    /// Increments the inner value by 1,
    /// then returns the previous value.
    pub const fn increment(&mut self) -> u32 {
        let prev = self.inner;
        self.inner += 1;
        prev
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkResult {
    Success,
    NewKeyAlreadyExists,
    ExistingKeyDoesNotExist,
}

#[derive(Debug)]
pub struct ThreeKeyMap<K1, K2, K3, V> {
    key1_map: SimpleMap<K1, usize>,
    key2_map: SimpleMap<K2, usize>,
    key3_map: SimpleMap<K3, usize>,
    values: Vec<(V, usize)>,
}

impl<K1, K2, K3, V> ThreeKeyMap<K1, K2, K3, V> {
    pub fn with_capacities(values_cap: usize, k1_cap: usize, k2_cap: usize, k3_cap: usize) -> Self {
        Self {
            key1_map: SimpleMap::with_capacity_and_hasher(k1_cap, Default::default()),
            key2_map: SimpleMap::with_capacity_and_hasher(k2_cap, Default::default()),
            key3_map: SimpleMap::with_capacity_and_hasher(k3_cap, Default::default()),
            values: Vec::with_capacity(values_cap),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.key1_map.is_empty()
            && self.key2_map.is_empty()
            && self.key3_map.is_empty()
            && self.values.is_empty()
    }

    pub fn key1_iter(&self) -> impl Iterator<Item = &K1> {
        self.key1_map.keys()
    }

    /// Internal function that removes a value from the map at `index`,
    /// via a swap remove, then updates the index of all other affected
    /// key/value pairings.
    ///
    /// This operation is O(K1 + K2 + K3), where each K is the number
    /// of key/value pairings for that key.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    fn swap_remove(&mut self, index: usize) -> Option<V> {
        let last_item_position = self.values.len() - 1;
        let (value, _) = self.values.swap_remove(index);

        self.key1_map
            .values_mut()
            .filter(|&&mut key1_index| last_item_position == key1_index)
            .for_each(|value| *value = index);
        self.key2_map
            .values_mut()
            .filter(|&&mut key2_index| last_item_position == key2_index)
            .for_each(|value| *value = index);
        self.key3_map
            .values_mut()
            .filter(|&&mut key3_index| last_item_position == key3_index)
            .for_each(|value| *value = index);

        Some(value)
    }

    pub fn unlink_all_but_key1<Q>(&mut self, k: &Q)
    where
        Q: ?Sized + Hash + Eq,
        K1: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        if let Some(&index) = self.key1_map.get(k) {
            self.key1_map.retain(|q, v| q.borrow() == k || *v != index);
            self.key2_map.retain(|_, v| *v != index);
            self.key3_map.retain(|_, v| *v != index);
        }
    }

    pub fn link_key1_to_key2<Q>(&mut self, new_k: K1, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K1: Hash + Eq + IsEnabled,
        K2: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        TwoKeyOperation {
            map1: &mut self.key1_map,
            map2: &mut self.key2_map,
            values: &mut self.values,
        }
        .link_key_to_key(new_k, existing_k)
    }

    pub fn link_key1_to_key3<Q>(&mut self, new_k: K1, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K1: Hash + Eq + IsEnabled,
        K3: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        TwoKeyOperation {
            map1: &mut self.key1_map,
            map2: &mut self.key3_map,
            values: &mut self.values,
        }
        .link_key_to_key(new_k, existing_k)
    }

    pub fn link_key2_to_key1<Q>(&mut self, new_k: K2, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K2: Hash + Eq + IsEnabled,
        K1: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        TwoKeyOperation {
            map1: &mut self.key2_map,
            map2: &mut self.key1_map,
            values: &mut self.values,
        }
        .link_key_to_key(new_k, existing_k)
    }

    pub fn link_key2_to_key3<Q>(&mut self, new_k: K2, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K2: Hash + Eq + IsEnabled,
        K3: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        TwoKeyOperation {
            map1: &mut self.key2_map,
            map2: &mut self.key3_map,
            values: &mut self.values,
        }
        .link_key_to_key(new_k, existing_k)
    }

    pub fn link_key3_to_key1<Q>(&mut self, new_k: K3, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K3: Hash + Eq + IsEnabled,
        K1: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        TwoKeyOperation {
            map1: &mut self.key3_map,
            map2: &mut self.key1_map,
            values: &mut self.values,
        }
        .link_key_to_key(new_k, existing_k)
    }

    pub fn link_key3_to_key2<Q>(&mut self, new_k: K3, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K3: Hash + Eq + IsEnabled,
        K2: Borrow<Q> + Hash + Eq + IsEnabled,
    {
        TwoKeyOperation {
            map1: &mut self.key3_map,
            map2: &mut self.key2_map,
            values: &mut self.values,
        }
        .link_key_to_key(new_k, existing_k)
    }
}

impl<K1, K2, K3, V> ThreeKeyMap<K1, K2, K3, V>
where
    K1: IsEnabled + Hash + Eq,
{
    pub fn insert_by_key1(&mut self, k: K1, v: V) -> Option<V> {
        OneKeyOperation {
            map: &mut self.key1_map,
            values: &mut self.values,
        }
        .insert_by_key(k, v)
    }

    pub fn link_key1_to_key1<Q>(&mut self, k: K1, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K1: Borrow<Q>,
    {
        OneKeyOperation {
            map: &mut self.key1_map,
            values: &mut self.values,
        }
        .link_key_to_key(k, existing_k)
    }

    pub fn remove_by_key1<Q>(&mut self, k: &Q) -> Option<V>
    where
        Q: ?Sized + Hash + Eq,
        K1: Borrow<Q>,
    {
        let index = self.key1_map.remove(k)?;
        let (_, count) = self.values.get_mut(index)?;
        *count -= 1;
        (0 == *count).then(|| self.swap_remove(index)).flatten()
    }

    pub fn get_by_key1<Q>(&self, k: &Q) -> Option<&V>
    where
        Q: ?Sized + Hash + Eq,
        K1: Borrow<Q>,
    {
        OneKeyOperationRef {
            map: &self.key1_map,
            values: &self.values,
        }
        .get_by_key(k)
    }
}

impl<K1, K2, K3, V> ThreeKeyMap<K1, K2, K3, V>
where
    K2: IsEnabled + Hash + Eq,
{
    pub fn insert_by_key2(&mut self, k: K2, v: V) -> Option<V> {
        OneKeyOperation {
            map: &mut self.key2_map,
            values: &mut self.values,
        }
        .insert_by_key(k, v)
    }

    pub fn link_key2_to_key2<Q>(&mut self, k: K2, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K2: Borrow<Q>,
    {
        OneKeyOperation {
            map: &mut self.key2_map,
            values: &mut self.values,
        }
        .link_key_to_key(k, existing_k)
    }

    pub fn remove_by_key2<Q>(&mut self, k: &Q) -> Option<V>
    where
        Q: ?Sized + Hash + Eq,
        K2: Borrow<Q>,
    {
        OneKeyOperation {
            map: &mut self.key2_map,
            values: &mut self.values,
        }
        .remove_by_key(k)
        .filter(|&(_, count)| 0 == count)
        .and_then(|(index, _)| self.swap_remove(index))
    }

    pub fn get_by_key2<Q>(&self, k: &Q) -> Option<&V>
    where
        Q: ?Sized + Hash + Eq,
        K2: Borrow<Q>,
    {
        OneKeyOperationRef {
            map: &self.key2_map,
            values: &self.values,
        }
        .get_by_key(k)
    }
}

impl<K1, K2, K3, V> ThreeKeyMap<K1, K2, K3, V>
where
    K3: IsEnabled + Hash + Eq,
{
    pub fn insert_by_key3(&mut self, k: K3, v: V) -> Option<V> {
        OneKeyOperation {
            map: &mut self.key3_map,
            values: &mut self.values,
        }
        .insert_by_key(k, v)
    }

    pub fn link_key3_to_key3<Q>(&mut self, k: K3, existing_k: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K3: Borrow<Q>,
    {
        OneKeyOperation {
            map: &mut self.key3_map,
            values: &mut self.values,
        }
        .link_key_to_key(k, existing_k)
    }

    pub fn remove_by_key3<Q>(&mut self, k: &Q) -> Option<V>
    where
        Q: ?Sized + Hash + Eq,
        K3: Borrow<Q>,
    {
        OneKeyOperation {
            map: &mut self.key3_map,
            values: &mut self.values,
        }
        .remove_by_key(k)
        .filter(|&(_, count)| 0 == count)
        .and_then(|(index, _)| self.swap_remove(index))
    }

    pub fn get_by_key3<Q>(&self, k: &Q) -> Option<&V>
    where
        Q: ?Sized + Hash + Eq,
        K3: Borrow<Q>,
    {
        OneKeyOperationRef {
            map: &self.key3_map,
            values: &self.values,
        }
        .get_by_key(k)
    }
}

struct TwoKeyOperation<'a, K1, K2, V> {
    map1: &'a mut SimpleMap<K1, usize>,
    map2: &'a mut SimpleMap<K2, usize>,
    values: &'a mut Vec<(V, usize)>,
}

impl<K1, K2, V> TwoKeyOperation<'_, K1, K2, V>
where
    K1: IsEnabled + Hash + Eq,
    K2: IsEnabled + Hash + Eq,
{
    fn link_key_to_key<Q>(&mut self, new_key: K1, existing_key: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K2: Borrow<Q>,
    {
        if self.map1.contains_key(new_key.borrow()) {
            return LinkResult::NewKeyAlreadyExists;
        }

        if let Some(&index) = self.map2.get(existing_key) {
            self.map1.insert(new_key, index);
            let (_, count) = &mut self.values[index];
            *count += 1;
            LinkResult::Success
        } else {
            LinkResult::ExistingKeyDoesNotExist
        }
    }
}

struct OneKeyOperationRef<'a, K, V> {
    map: &'a SimpleMap<K, usize>,
    values: &'a Vec<(V, usize)>,
}

impl<'a, K, V> OneKeyOperationRef<'a, K, V>
where
    K: IsEnabled + Hash + Eq,
{
    fn get_by_key<Q>(&self, k: &Q) -> Option<&'a V>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        let index = self.map.get(k)?;
        self.values.get(*index).map(|(v, _)| v)
    }
}

struct OneKeyOperation<'a, K, V> {
    map: &'a mut SimpleMap<K, usize>,
    values: &'a mut Vec<(V, usize)>,
}

impl<K, V> OneKeyOperation<'_, K, V>
where
    K: IsEnabled + Hash + Eq,
{
    fn link_key_to_key<Q>(&mut self, new_key: K, existing_key: &Q) -> LinkResult
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        if self.map.contains_key(new_key.borrow()) {
            return LinkResult::NewKeyAlreadyExists;
        }

        if let Some(&index) = self.map.get(existing_key) {
            self.map.insert(new_key, index);
            let (_, count) = &mut self.values[index];
            *count += 1;
            LinkResult::Success
        } else {
            LinkResult::ExistingKeyDoesNotExist
        }
    }

    fn insert_by_key(&mut self, k: K, v: V) -> Option<V> {
        self.values.push((v, 1));
        let index = self.values.len() - 1;
        self.map
            .insert(k, index)
            .and_then(|_| self.values.pop())
            .map(|v| v.0)
    }

    fn remove_by_key<Q>(&mut self, k: &Q) -> Option<(usize, usize)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        let index = self.map.remove(k)?;
        let (_, count) = self.values.get_mut(index)?;
        *count = count.checked_sub(1).unwrap();
        Some((index, *count))
    }

    fn get_mut_by_key<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        let index = self.map.get_mut(k)?;
        self.values.get_mut(*index).map(|(v, _)| v)
    }
}

pub type SimpleMap<K, V> = HashMap<K, V, FxBuildHasher>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NoHash<T>(pub T);
impl<T: Copy> Copy for NoHash<T> {}
// impl IsEnabled for NoHash<quinn::StreamId> {}
impl IsEnabled for NoHash<vhci::ioctl::Address> {}

pub struct Ctrl<S, R = (), E = std::io::Error> {
    pub data: S,
    pub(crate) tx: oneshot::Sender<Result<R, E>>,
}

impl<S, R, E> Ctrl<S, R, E> {
    pub(crate) fn new(data: S) -> (oneshot::AsyncReceiver<Result<R, E>>, Ctrl<S, R, E>) {
        let (tx, rx) = oneshot::channel_async();
        let ctrl = Self { data, tx };
        (rx, ctrl)
    }
}

#[inline]
pub const fn align(val: usize, alignment: usize) -> usize {
    (val + (alignment - 1)) & !(alignment - 1)
}

#[inline]
pub const fn align_to_usize(val: usize) -> usize {
    align(val, size_of::<usize>())
}

// /// A synchronous sleep function that uses
// /// spinlocking for small sleep durations.
// ///
// /// Credit goes to [Blat Blatnik](https://blog.bearcats.nl/accurate-sleep-function/)
// /// for the implementation.
// pub(crate) fn precise_sleep(clock: &quanta::Clock, mut seconds: f64) {
//     let mut estimate = 5e-3;
//     let mut mean = 5e-3;
//     let mut m2 = 0.0;
//     let mut count = 1;

//     while seconds > estimate {
//         let start = clock.now();
//         std::thread::sleep(std::time::Duration::from_millis(1));

//         let observed = start.elapsed().as_secs_f64();
//         seconds -= observed;

//         count += 1;
//         let delta = observed - mean;
//         mean += delta / count as f64;
//         m2 += delta * (observed - mean);
//         let stddev = (m2 / (count - 1) as f64).sqrt();
//         estimate = mean + stddev;
//     }

//     // spin lock
//     let start = clock.now();
//     while start.elapsed().as_secs_f64() < seconds {}
// }

// /// Serializes an object that implements [`serde::Serialize`] into an async writer.
// /// Uses a user-provided buffer to prevent unneeded allocations, but does reallocate
// /// the buffer if not big enough to hold the serialized data.
// #[tracing::instrument(level = "trace", skip(writer))]
// pub(crate) async fn serialize_into_writer<T, W>(
//     item: &T,
//     writer: &mut W,
//     buf: &mut Vec<u8>,
// ) -> Result<(), Error>
// where
//     T: Serialize + std::fmt::Debug,
//     W: AsyncWrite + Unpin,
// {
//     let msg = loop {
//         match postcard::to_slice_cobs(item, buf) {
//             Ok(msg) => break msg,
//             Err(postcard::Error::SerializeBufferFull) => {
//                 buf.resize(buf.len() + 512, 0);
//             }
//             Err(err) => Err(err)?,
//         }
//     };
//     writer.write_all(msg).await?;
//     buf.clear();
//     Ok(())
// }

// /// Deserializes an object that implements [`serde::Deserialize`] from an async bufreader.
// ///
// /// Returns `None` if the reader doesn't provide any data.
// #[tracing::instrument(level = "trace", skip(reader))]
// pub(crate) async fn deserialize_from_reader<T, R>(
//     reader: &mut R,
//     buf: &mut Vec<u8>,
// ) -> Result<Option<T>, Error>
// where
//     T: DeserializeOwned + std::fmt::Debug,
//     R: AsyncBufRead + Sized + Unpin,
// {
//     if 0 == reader.read_until(0, buf).await? {
//         return Ok(None);
//     }
//     let recv: T = postcard::from_bytes_cobs(buf)?;
//     buf.clear();
//     Ok(Some(recv))
// }

pub trait CloseStream {
    fn close(&mut self) -> io::Result<()>;
    fn stopped(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

impl CloseStream for compio_quic::SendStream {
    fn close(&mut self) -> io::Result<()> {
        self.finish().map_err(io::Error::from)
    }

    async fn stopped(&mut self) -> io::Result<()> {
        self.stopped().await.map_err(io::Error::from)?;
        Ok(())
    }
}

pub struct Timer(Instant);

impl Timer {
    #[inline]
    pub fn start() -> Timer {
        Self(Instant::now())
    }

    #[inline]
    pub fn stop_and_report(self, threshold: Option<Duration>, msg: fmt::Arguments) {
        let elapsed = self.0.elapsed();
        if threshold.unwrap_or(Duration::ZERO) < elapsed {
            tracing::trace!("{msg} took {elapsed:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use vhci::{
        Port,
        ioctl::{Address, UrbHandle},
    };

    use super::*;

    type Mailer =
        ThreeKeyMap<Port, NoHash<Addr>, UrbHandle, mpsc::AsyncSender<vhci::ioctl::IocWork>>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Addr(Address);
    impl std::hash::Hash for Addr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.0.get().hash(state);
        }
    }
    impl IsEnabled for NoHash<Addr> {}

    #[test]
    fn remove_last_tx_works() {
        let mut mailer = Mailer::with_capacities(8, 8, 10, 64);

        let (tx, _rx) = mpsc::channel(20);
        mailer.insert_by_key1(Port::new(1).unwrap(), tx.into_sink());
        mailer.remove_by_key1(&Port::new(1).unwrap());

        assert!(mailer.is_empty());
    }

    #[test]
    fn remove_tx_works() {
        let mut mailer = Mailer::with_capacities(4, 4, 6, 32);

        let (tx, _rx) = mpsc::channel(20);
        mailer.insert_by_key1(Port::new(1).unwrap(), tx.into_sink());

        let (tx, _rx) = mpsc::channel(20);
        mailer.insert_by_key1(Port::new(5).unwrap(), tx.into_sink());

        mailer.remove_by_key1(&Port::new(1).unwrap());
        let index = mailer.key1_map.get(&Port::new(5).unwrap()).unwrap();
        assert_eq!(*index, 0);
    }

    #[test]
    fn align_zero_to_zero() {
        assert_eq!(align_to_usize(0), 0);
    }

    #[test]
    fn align_seven_to_eight() {
        assert_eq!(align_to_usize(7), 8);
    }

    #[test]
    fn align_eight_to_eight() {
        assert_eq!(align_to_usize(8), 8);
    }

    #[test]
    fn align_to_cache() {
        assert_eq!(align(0, 64), 0);
        assert_eq!(align(3, 64), 64);
    }
}
