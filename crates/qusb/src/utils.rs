use std::{collections::VecDeque, marker::PhantomData, pin::Pin};

use bytes::Buf;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::Error;


pub struct BorrowedBuffer<'a> {
    pub data: &'a mut [u8],
    pub index: usize,
}

impl Buf for BorrowedBuffer<'_> {
    fn remaining(&self) -> usize {
        self.data.len() - self.index
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.index..]
    }

    fn advance(&mut self, cnt: usize) {
        if cnt > self.remaining() {
            panic!("cannot advance buffer that far")
        }
        self.index += cnt;
    }
}

pub async fn serialize_into_writer<T: Serialize, W: AsyncWrite + Unpin>(
    item: &T,
    writer: &mut W,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    let msg = loop {
        match postcard::to_slice_cobs(item, buf) {
            Ok(msg) => break msg,
            Err(postcard::Error::SerializeBufferFull) => buf.reserve(1024),
            Err(err) => Err(err)?
        }
    };
    writer.write_all(msg).await?;
    buf.clear();
    Ok(())
}

pub async fn deserialize_from_reader<
    T: DeserializeOwned,
    R: AsyncBufRead + Sized + Unpin,
>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<T, Error> {
    reader.read_until(0, buf).await?;
    let recv: T = postcard::from_bytes_cobs(buf)?;
    Ok(recv)
}

#[derive(Debug)]
pub struct RWStream {
    tx: quinn::SendStream,
    rx: quinn::RecvStream,
}

impl AsyncRead for RWStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.rx).poll_read(cx, buf)
    }
}

impl AsyncWrite for RWStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        <quinn::SendStream as AsyncWrite>::poll_write(Pin::new(&mut self.tx), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        <quinn::SendStream as AsyncWrite>::poll_flush(Pin::new(&mut self.tx), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        <quinn::SendStream as AsyncWrite>::poll_shutdown(Pin::new(&mut self.tx), cx)
    }
}
