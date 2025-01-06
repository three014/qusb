use std::{io, marker::PhantomData};

use bytes::Buf;

use bytes::BufMut;
use bytes::BytesMut;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

use zerocopy::ConvertError;
use zerocopy::FromZeros;
use zerocopy::{Immutable, KnownLayout, TryFromBytes};

use crate::GetSliceLen;

fn invalid<A, S, V>(_x: ConvertError<A, S, V>) -> io::Error {
    io::Error::from(io::ErrorKind::InvalidData)
}

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
        let len = T::get_slice_len(slice)?;
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
        let len = T::get_slice_len(self.buf)?;
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

    pub fn peek<T>(&self) -> io::Result<&T>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(io::ErrorKind::InvalidData.into());
        }
        T::try_ref_from_bytes(&self.buf[..size_of]).map_err(invalid)
    }

    pub fn peek_dst<T>(&self) -> io::Result<&T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let len = T::get_slice_len(&self.buf)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
        T::try_ref_from_prefix_with_elems(&self.buf, len)
            .map_err(invalid)
            .map(|(item, _)| item)
    }

    pub fn peek_mut<T>(&mut self) -> io::Result<&mut T>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(io::ErrorKind::InvalidData.into());
        }
        T::try_mut_from_bytes(&mut self.buf[..size_of]).map_err(invalid)
    }

    pub fn peek_mut_dst<T>(&mut self) -> io::Result<&mut T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let len = T::get_slice_len(&self.buf)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
        T::try_mut_from_prefix_with_elems(&mut self.buf, len)
            .map_err(invalid)
            .map(|(item, _)| item)
    }

    pub fn read<T>(&mut self) -> io::Result<T>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let item = T::try_read_from_bytes(&self.buf[..size_of]).map_err(invalid)?;
        self.buf.advance(size_of);
        Ok(item)
    }

    pub fn consume<T>(&mut self, item: &T)
    where
        T: TryFromBytes + KnownLayout + Immutable + ?Sized,
    {
        self.buf.advance(std::mem::size_of_val(item));
    }

    pub fn claim<T>(&mut self) -> std::io::Result<Data<T>>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.len() < size_of {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let buf = self.buf.split_to(size_of);
        Ok(Data::new(buf))
    }

    pub fn claim_dst<T>(&mut self) -> io::Result<Data<T>>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let item: &T = self.peek_dst()?;
        let size_of = std::mem::size_of_val(item);
        let buf = self.buf.split_to(size_of);
        Ok(Data::new(buf))
    }

    pub fn iter<'a, T>(&'a self) -> impl Iterator<Item = &'a T>
    where
        T: TryFromBytes + KnownLayout + Immutable + 'a,
    {
        self.buf
            .chunks_exact(std::mem::size_of::<T>())
            .map_while(|chunk| T::try_ref_from_bytes(chunk).ok())
    }

    pub fn iter_dst<'a, T>(&'a self) -> IterDst<'a, T>
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

    pub fn iter_mut_dst<'a, T>(&'a mut self) -> IterMutDst<'a, T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        IterMutDst {
            buf: &mut self.buf,
            _p: PhantomData,
        }
    }

    pub async fn read_into_from_reader<R>(&mut self, mut rx: R) -> io::Result<usize>
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
}
