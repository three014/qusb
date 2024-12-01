use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncBufRead, AsyncWrite, BufReader};

use crate::{
    utils::{deserialize_from_reader, serialize_into_writer},
    Error,
};

pub struct Sender2<W: AsyncWrite> {
    tx: W,
    buf: Vec<u8>,
}

impl<W: AsyncWrite> Sender2<W> {
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

impl<W: AsyncWrite + Unpin> Sender2<W> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn send<T: Serialize + std::fmt::Debug>(&mut self, item: &T) -> Result<(), Error> {
        serialize_into_writer::<T, _>(item, &mut self.tx, &mut self.buf).await
    }
}

pub struct Receiver2<R: AsyncBufRead> {
    rx: R,
    buf: Vec<u8>,
}

impl<R: AsyncBufRead> Receiver2<R> {
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

impl<R: AsyncBufRead + Unpin> Receiver2<R> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn recv<T: DeserializeOwned + std::fmt::Debug>(
        &mut self,
    ) -> Result<Option<T>, Error> {
        deserialize_from_reader(&mut self.rx, &mut self.buf).await
    }
}

#[tracing::instrument(level = "trace")]
pub(crate) fn new<T, R>(
    tx: quinn::SendStream,
    rx: quinn::RecvStream,
) -> (
    Sender<T, quinn::SendStream>,
    Receiver<R, BufReader<quinn::RecvStream>>,
)
where
    T: Serialize + std::fmt::Debug,
    R: DeserializeOwned + std::fmt::Debug,
{
    (Sender::new(tx), Receiver::new(BufReader::new(rx)))
}

#[derive(Debug)]
pub struct Sender<T: Serialize + std::fmt::Debug, W: AsyncWrite + std::fmt::Debug> {
    phantom: PhantomData<T>,
    tx: W,
    buf: Vec<u8>,
}

impl<T: Serialize + std::fmt::Debug, W: AsyncWrite + std::fmt::Debug> Sender<T, W> {
    pub fn new(tx: W) -> Self {
        Self {
            phantom: PhantomData,
            tx,
            buf: Vec::new(),
        }
    }

    #[tracing::instrument(level = "trace")]
    pub fn convert<U: Serialize + std::fmt::Debug>(mut self) -> Sender<U, W> {
        self.buf.clear();
        Sender {
            phantom: PhantomData,
            tx: self.tx,
            buf: self.buf,
        }
    }

    pub fn into_writer(self) -> W {
        self.tx
    }
}

impl<T: Serialize + std::fmt::Debug, W: AsyncWrite + Unpin + std::fmt::Debug> Sender<T, W> {
    #[tracing::instrument(level = "trace")]
    pub async fn send(&mut self, obj: &T) -> Result<(), Error> {
        let min_reserve = 128_usize.saturating_sub(self.buf.len());
        self.buf.reserve(min_reserve);
        serialize_into_writer::<T, _>(obj, &mut self.tx, &mut self.buf).await
    }
}

#[derive(Debug)]
pub struct Receiver<T: DeserializeOwned + std::fmt::Debug, R: AsyncBufRead + std::fmt::Debug> {
    phantom: PhantomData<T>,
    rx: R,
    buf: Vec<u8>,
}

impl<T: DeserializeOwned + std::fmt::Debug, R: AsyncBufRead + std::fmt::Debug> Receiver<T, R> {
    pub fn new(rx: R) -> Self {
        Self {
            phantom: PhantomData,
            rx,
            buf: Vec::new(),
        }
    }

    #[tracing::instrument(level = "trace")]
    pub fn convert<U: DeserializeOwned + std::fmt::Debug>(mut self) -> Receiver<U, R> {
        self.buf.clear();
        Receiver {
            phantom: PhantomData,
            rx: self.rx,
            buf: self.buf,
        }
    }

    pub fn into_reader(self) -> R {
        self.rx
    }
}

impl<T, R> Receiver<T, R>
where
    T: DeserializeOwned + std::fmt::Debug,
    R: AsyncBufRead + Unpin + std::fmt::Debug,
{
    #[tracing::instrument(skip(self), level = "trace")]
    pub async fn recv(&mut self) -> Result<Option<T>, Error> {
        let min_reserve = 128_usize.saturating_sub(self.buf.len());
        self.buf.reserve(min_reserve);
        deserialize_from_reader(&mut self.rx, &mut self.buf).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn version_sends_properly() {
        let tx_buf = Vec::<u8>::with_capacity(256);

        let mut tx = Sender::<spec::Version, Vec<u8>>::new(tx_buf);
        tracing::debug!("About to send my message!");
        tx.send(&spec::VERSION).await.unwrap();

        let rx_buf = tx.tx;
        let mut rx = Receiver::<spec::Version, BufReader<&[u8]>>::new(BufReader::with_capacity(
            256, &*rx_buf,
        ));
        tracing::debug!("About to recv my message!");
        let version = rx.recv().await.unwrap();

        assert_eq!(version.unwrap(), spec::VERSION);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn can_send_multiple_messages_with_same_type() {
        let tx_buf = Vec::<u8>::with_capacity(256);

        let data = spec::UsbDeviceInfo {
            id: spec::UsbDeviceId {
                bus_number: 2,
                device_addr: 1,
            },
            bus_id: spec::BusId(std::borrow::Cow::Borrowed("1-2".try_into().unwrap())),
            vendor_id: 2,
            product_id: 6,
            class: 4,
            subclass: 45,
            protocol: 1,
            interfaces: vec![
                spec::InterfaceInfo {
                    interface_number: 1,
                    class: 2,
                    subclass: 1,
                    protocol: 3,
                },
                spec::InterfaceInfo {
                    interface_number: 2,
                    class: 9,
                    subclass: 1,
                    protocol: 3,
                },
            ],
        };

        let mut data2 = data.clone();
        data2.id = spec::UsbDeviceId {
            bus_number: 29,
            device_addr: 45,
        };
        data2.bus_id = spec::BusId(std::borrow::Cow::Borrowed("1-2.4.5".try_into().unwrap()));

        let mut tx = Sender::<spec::UsbDeviceInfo, Vec<u8>>::new(tx_buf);
        tracing::debug!("About to send my messages!");
        tx.send(&data2).await.unwrap();
        tx.send(&data).await.unwrap();

        let rx_buf = tx.tx;
        let mut rx = Receiver::<spec::UsbDeviceInfo, BufReader<&[u8]>>::new(
            BufReader::with_capacity(256, &*rx_buf),
        );

        tracing::debug!("About to recv my messages!");
        let data_copied2 = rx.recv().await.unwrap();
        let data_copied = rx.recv().await.unwrap();
        // println!("{data2:#?}");
        // println!("{data_copied2:#?}");

        // The copies of the data should be somewhat different than what was sent,
        // since we're getting back `String`s instead of `&str`s
    }
}
