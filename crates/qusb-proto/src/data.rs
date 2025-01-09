use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::{io, marker::PhantomData};

use bytes::Buf;

use bytes::BytesMut;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

use zerocopy::{Immutable, KnownLayout, TryFromBytes};

use crate::GetSliceLen;
use crate::GetSliceLenErr;

pub struct IterMutDst<'a, T: ?Sized + 'a> {
    buf: &'a mut [u8],
    _p: PhantomData<&'a mut T>,
}

impl<'a, T> Iterator for IterMutDst<'a, T>
where
    T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
{
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = std::mem::take(&mut self.buf);
        let len = T::get_slice_len(slice).ok()?;
        let (item, remaining) = T::try_mut_from_prefix_with_elems(slice, len).ok()?;
        self.buf = remaining;
        Some(item)
    }
}

pub struct IterDst<'a, T: ?Sized + 'a> {
    buf: &'a [u8],
    _p: PhantomData<&'a T>,
}

impl<'a, T> Iterator for IterDst<'a, T>
where
    T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let len = T::get_slice_len(self.buf).ok()?;
        let (item, remaining) = T::try_ref_from_prefix_with_elems(self.buf, len)
            // .inspect_err(|err| println!("{err}"))
            .ok()?;
        self.buf = remaining;
        Some(item)
    }
}

pub struct Data<T: ?Sized> {
    buf: BytesMut,
    _p: PhantomData<T>,
}

impl<T> Data<T>
where
    T: TryFromBytes + Immutable + KnownLayout + ?Sized,
{
    pub fn get(&self) -> &T {
        T::try_ref_from_bytes(&self.buf).unwrap()
    }

    pub fn get_mut(&mut self) -> &mut T {
        T::try_mut_from_bytes(&mut self.buf).unwrap()
    }

    fn new(buf: BytesMut) -> Self {
        Self {
            buf,
            _p: PhantomData,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("data doesn't match the type")]
    CorruptedData,
    #[error("buffer is too short to read the provided type (missing >{num_bytes_needed} bytes, buf.len() == {buf_len})")]
    BufferShort {
        num_bytes_needed: usize,
        buf_len: usize,
    },
}

impl From<GetSliceLenErr> for ReadError {
    fn from(value: GetSliceLenErr) -> Self {
        match value {
            GetSliceLenErr::NoConfidence => ReadError::CorruptedData,
            GetSliceLenErr::BufferShort {
                num_bytes_needed,
                buf_len,
            } => ReadError::BufferShort {
                num_bytes_needed,
                buf_len,
            },
        }
    }
}

#[derive(Debug)]
pub struct Ring {
    buf: BytesMut,
}

impl Ring {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
        }
    }

    pub fn reserve(&mut self, num_bytes: usize) -> Option<BytesMut> {
        if num_bytes > self.buf.spare_capacity_mut().len() {
            return None;
        }

        Some(self.buf.split_off(self.buf.len()))
    }

    pub fn peek<T>(&self) -> Result<&T, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of_t = std::mem::size_of::<T>();
        if self.buf.len() < size_of_t {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of_t - self.buf.len(),
                buf_len: self.buf.len(),
            });
        }
        T::try_ref_from_bytes(&self.buf[..size_of_t]).map_err(|_| ReadError::CorruptedData)
    }

    pub fn peek_dst<T>(&self) -> Result<&T, ReadError>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let len = T::get_slice_len(&self.buf).map_err(ReadError::from)?;
        T::try_ref_from_prefix_with_elems(&self.buf, len)
            .map_err(|_| ReadError::CorruptedData)
            .map(|(item, _)| item)
    }

    pub fn peek_mut<T>(&mut self) -> Result<&mut T, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of - self.buf.len(),
                buf_len: self.buf.len(),
            });
        }
        T::try_mut_from_bytes(&mut self.buf[..size_of]).map_err(|_| ReadError::CorruptedData)
    }

    pub fn peek_mut_dst<T>(&mut self) -> Result<&mut T, ReadError>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let len = T::get_slice_len(&self.buf).map_err(ReadError::from)?;
        T::try_mut_from_prefix_with_elems(&mut self.buf, len)
            .map_err(|_| ReadError::CorruptedData)
            .map(|(item, _)| item)
    }

    pub fn read<T>(&mut self) -> Result<T, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of - self.buf.len(),
                buf_len: self.buf.len(),
            });
        }
        let item =
            T::try_read_from_bytes(&self.buf[..size_of]).map_err(|_| ReadError::CorruptedData)?;
        self.buf.advance(size_of);
        Ok(item)
    }

    pub fn consume(&mut self, num_bytes: usize)
    {
        self.buf.advance(num_bytes);
    }

    pub fn claim<T>(&mut self) -> Result<Data<T>, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of - self.len(),
                buf_len: self.len(),
            });
        }
        let buf = self.buf.split_to(size_of);
        Ok(Data::new(buf))
    }

    pub fn claim_dst<T>(&mut self) -> Result<Data<T>, ReadError>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let item: &T = self.peek_dst()?;
        let size_of = std::mem::size_of_val(item);
        let buf = self.buf.split_to(size_of);
        Ok(Data::new(buf))
    }

    pub fn giveback_chunk<T: ?Sized>(&mut self, data: Data<T>) {
        let len = data.buf.len();
        self.buf.unsplit(data.buf);
        self.buf.advance(len);
    }

    pub fn iter<'a, T>(&'a self) -> impl Iterator<Item = &'a T>
    where
        T: TryFromBytes + KnownLayout + Immutable + 'a,
    {
        self.buf
            .chunks_exact(std::mem::size_of::<T>())
            .map_while(|chunk| T::try_ref_from_bytes(chunk).ok())
    }

    pub fn iter_dst<T>(&self) -> IterDst<'_, T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        IterDst {
            buf: &self.buf,
            _p: PhantomData,
        }
    }

    pub fn iter_mut<'a, T>(&'a mut self) -> impl Iterator<Item = &'a mut T>
    where
        T: TryFromBytes + KnownLayout + Immutable + 'a,
    {
        self.buf
            .chunks_exact_mut(std::mem::size_of::<T>())
            .map_while(|chunk| T::try_mut_from_bytes(chunk).ok())
    }

    pub fn iter_mut_dst<T>(&mut self) -> IterMutDst<'_, T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        IterMutDst {
            buf: &mut self.buf,
            _p: PhantomData,
        }
    }

    pub async fn fill_with_reader<R>(&mut self, mut rx: R) -> io::Result<usize>
    where
        R: AsyncRead + Unpin,
    {
        rx.read_buf(&mut self.buf).await
    }

    pub fn try_consolidate(&mut self) {
        let size = self.buf.capacity() - self.buf.len();
        let _ = self.buf.try_reclaim(size);
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub async fn fill_until<R>(&mut self, mut rx: R, num_bytes: usize) -> io::Result<Option<()>>
    where
        R: AsyncRead + Unpin,
    {
        while num_bytes > self.len() {
            if 0 == self.fill_with_reader(&mut rx).await? {
                return Ok(None);
            }
        }

        Ok(Some(()))
    }
}
