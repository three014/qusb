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
    let (tx, rx) = flume::bounded(0);
    (
        Sender { inner: tx },
        AsyncReceiver {
            inner: rx.into_recv_async(),
        },
    )
}
