use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::{
    utils::{deserialize_from_reader, serialize_into_writer},
    Error,
};

pub struct Sender<W: AsyncWrite> {
    tx: W,
    buf: Vec<u8>,
}

impl<W: AsyncWrite> Sender<W> {
    pub(crate) fn new(tx: W) -> Self {
        Self {
            tx,
            buf: Vec::new(),
        }
    }

    pub(crate) fn as_writer_mut(&mut self) -> &mut W {
        &mut self.tx
    }
}

impl<W: AsyncWrite + Unpin> Sender<W> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn send<T: Serialize + std::fmt::Debug>(&mut self, item: &T) -> Result<(), Error> {
        serialize_into_writer::<T, _>(item, &mut self.tx, &mut self.buf).await
    }
}

pub struct Receiver<R: AsyncBufRead> {
    rx: R,
    buf: Vec<u8>,
}

impl<R: AsyncBufRead> Receiver<R> {
    pub(crate) fn new(rx: R) -> Self {
        Self {
            rx,
            buf: Vec::new(),
        }
    }

    pub(crate) fn as_reader_mut(&mut self) -> &mut R {
        &mut self.rx
    }
}

impl<R: AsyncBufRead + Unpin> Receiver<R> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn recv<T: DeserializeOwned + std::fmt::Debug>(
        &mut self,
    ) -> Result<Option<T>, Error> {
        deserialize_from_reader(&mut self.rx, &mut self.buf).await
    }
}
