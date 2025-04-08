use std::{io, pin::Pin, task::Poll};

pub mod mpsc {
    pub type AsyncSender<T> = tachyonix::Sender<T>;
    pub type AsyncReceiver<T> = tachyonix::Receiver<T>;

    pub fn channel<T>(capacity: usize) -> (AsyncSender<T>, AsyncReceiver<T>) {
        tachyonix::channel(capacity)
    }
}
pub mod time {
    use std::time::{Duration, Instant};

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
}

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

pin_project_lite::pin_project! {
    #[project = MaybeProj]
    pub enum MaybeFut<F> {
        Some { #[pin] fut: F },
        None,
    }
}

impl<F> Future for MaybeFut<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            MaybeProj::Some { fut } => fut.poll(cx),
            MaybeProj::None => Poll::Pending,
        }
    }
}

impl<F> From<Option<F>> for MaybeFut<F> {
    fn from(value: Option<F>) -> Self {
        match value {
            Some(fut) => Self::Some { fut },
            None => Self::None,
        }
    }
}

pub trait ResettableFuture<F> {
    fn clear(&mut self);
    fn reset(&mut self, f: F);
    fn is_empty(&self) -> bool;
}

impl<F> ResettableFuture<F> for Pin<&mut MaybeFut<F>> {
    fn clear(&mut self) {
        self.set(None.into());
    }

    fn reset(&mut self, f: F) {
        self.set(Some(f).into());
    }

    fn is_empty(&self) -> bool {
        match self.as_ref().get_ref() {
            MaybeFut::Some { .. } => false,
            MaybeFut::None => true,
        }
    }
}

#[inline]
const fn align(val: usize, alignment: usize) -> usize {
    (val + (alignment - 1)) & !(alignment - 1)
}

#[inline]
pub const fn align_to_usize(val: usize) -> usize {
    align(val, size_of::<usize>())
}
