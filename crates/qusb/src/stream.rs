use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncBufRead, AsyncWrite, BufReader};

use crate::{
    utils::{deserialize_from_reader, serialize_into_writer},
    Error,
};

pub fn new<T, R>(
    tx: quinn::SendStream,
    rx: quinn::RecvStream,
) -> (
    Sender<T, quinn::SendStream>,
    Receiver<R, BufReader<quinn::RecvStream>>,
)
where
    T: Serialize,
    R: DeserializeOwned,
{
    (Sender::new(tx), Receiver::new(BufReader::new(rx)))
}

pub struct Sender<T: Serialize, W: AsyncWrite> {
    phantom: PhantomData<T>,
    tx: W,
    buf: Vec<u8>,
}

impl<T: Serialize, W: AsyncWrite> Sender<T, W> {
    pub const fn new(tx: W) -> Self {
        Self {
            phantom: PhantomData,
            tx,
            buf: Vec::new(),
        }
    }

    pub fn convert<U: Serialize>(mut self) -> Sender<U, W> {
        self.buf.clear();
        Sender {
            phantom: PhantomData,
            tx: self.tx,
            buf: self.buf,
        }
    }
}

impl<T: Serialize, W: AsyncWrite + Unpin> Sender<T, W> {
    pub async fn send(&mut self, obj: &T) -> Result<(), Error> {
        serialize_into_writer::<T, _>(obj, &mut self.tx, &mut self.buf).await
    }
}

pub struct Receiver<T: DeserializeOwned, R: AsyncBufRead> {
    phantom: PhantomData<T>,
    rx: R,
    buf: Vec<u8>,
}

impl<T: DeserializeOwned, R: AsyncBufRead> Receiver<T, R> {
    pub const fn new(rx: R) -> Self {
        Self {
            phantom: PhantomData,
            rx,
            buf: Vec::new(),
        }
    }

    pub fn convert<U: DeserializeOwned>(mut self) -> Receiver<U, R> {
        self.buf.clear();
        Receiver {
            phantom: PhantomData,
            rx: self.rx,
            buf: self.buf,
        }
    }
}

impl<T: DeserializeOwned, R: AsyncBufRead + Unpin> Receiver<T, R> {
    pub async fn recv(&mut self) -> Result<T, Error> {
        deserialize_from_reader(&mut self.rx, &mut self.buf).await
    }
}

// pub struct ConnectingReq {
//     tx: quinn::SendStream,
//     rx: BufReader<quinn::RecvStream>,
// }

// impl ConnectingReq {
//     pub fn new(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
//         Self {
//             tx,
//             rx: BufReader::new(rx),
//         }
//     }

//     pub async fn handshake(mut self) -> Requester {
//         let mut buf = Vec::new();
//         utils::serialize_into_writer(
//             &spec::VERSION,
//             &mut self.tx,
//             &mut buf,
//         )
//         .await
//         .unwrap();
//         buf.clear();

//         let status: Result<(), spec::Error> =
//             utils::deserialize_from_reader(&mut self.rx, &mut buf)
//                 .await
//                 .unwrap();
//         buf.clear();

//         if let Err(err) = status {
//             match err {
//                 spec::Error::UnexpectedReq => {
//                     panic!("remote peer thought we had a weird request?")
//                 }
//                 _ => {
//                     unreachable!("huh??")
//                 }
//             }
//         }

//         Requester {
//             tx: self.tx,
//             rx: self.rx,
//             scratch: buf,
//         }
//     }
// }

// pub struct ConnectingResp {
//     tx: quinn::SendStream,
//     rx: BufReader<quinn::RecvStream>,
// }

// impl ConnectingResp {
//     pub fn new(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
//         Self {
//             tx,
//             rx: BufReader::new(rx)
//         }
//     }

//     pub async fn handshake(mut self) -> Responder {
//         let mut buf = Vec::new();

//         let header: spec::Header = utils::deserialize_from_reader(&mut self.rx, &mut buf).await.unwrap();
//         buf.clear();

//         if header.version != spec::VERSION || header.stream_type != spec::StreamType::Ctrl {
//             utils::serialize_into_writer(&Err::<(), _>(spec::Error::UnexpectedReq), &mut self.tx, &mut buf).await.unwrap();
//             buf.clear();
//             panic!("we got a weird request");
//         }

//         utils::serialize_into_writer(&Ok::<_, spec::Error>(()), &mut self.tx, &mut buf).await.unwrap();
//         buf.clear();

//         Responder { tx: self.tx, rx: self.rx, scratch: buf }
//     }
// }
