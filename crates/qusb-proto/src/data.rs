use std::fmt::Debug;
use std::ptr::NonNull;
use std::{io, marker::PhantomData};

use bytes::{Buf as _, BufMut};

use bytes::BytesMut;
use compio_buf::{BufResult, IoBuf, IoBufMut, SetBufInit};
use compio_io::AsyncRead;
use thiserror::Error;

use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::GetSliceLen;
use crate::GetSliceLenErr;

pub struct IterMutDst<'a, T: ?Sized + 'a> {
    buf: &'a mut [u8],
    _p: PhantomData<&'a mut T>,
}

impl<'a, T> Iterator for IterMutDst<'a, T>
where
    T: TryFromBytes + GetSliceLen + Immutable + ?Sized + IntoBytes,
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
    value: NonNull<T>,
    buf: BytesMut,
    _p: PhantomData<T>,
}

impl<T> Data<T>
where
    T: KnownLayout + TryFromBytes + Immutable + ?Sized + IntoBytes,
{
    pub fn get(&self) -> &T {
        // SAFETY: The value is accessible as long as we hold the buffer.
        unsafe { self.value.as_ref() }
    }

    pub fn get_mut(&mut self) -> &mut T {
        // SAFETY: The value is accessible as long as we hold the buffer.
        unsafe { self.value.as_mut() }
    }

    #[inline]
    pub fn read(&self) -> T
    where
        T: Sized,
    {
        // SAFETY: The value is accessible as long as we hold the buffer.
        // Plus, we already verified that type T can be trivially read
        // when we created Data<T>.
        unsafe { self.value.read() }
    }

    #[inline]
    fn new(mut buf: BytesMut) -> Result<Self, ReadError> {
        let ptr = T::try_mut_from_bytes(&mut buf).map_err(|_| ReadError::CorruptedData)?;
        // SAFETY: We own the buffer so we ensure that
        // its data never gets mutated nor moved.
        let value = unsafe { NonNull::new_unchecked(ptr) };
        Ok(Self {
            buf,
            value,
            _p: PhantomData,
        })
    }

    // #[inline]
    // pub fn as_bytes(&self) -> &[u8] {
    //     &self.buf
    // }

    // #[inline]
    // pub fn len(&self) -> usize {
    //     self.buf.len()
    // }

    // #[inline]
    // pub fn is_empty(&self) -> bool {
    //     self.buf.is_empty()
    // }

    /// Splits the internal slice at `size_of::<<T as GetSliceLen>::Header>()`,
    /// so that the first `Data` contains the header, and the second `Data`
    /// contains the remainder of the data interpreted as type `U`.
    #[inline]
    pub fn split<U>(mut self) -> (T::Header, Data<U>)
    where
        T: GetSliceLen,
        U: TryFromBytes + Immutable + KnownLayout + ?Sized + IntoBytes,
    {
        let header = self.get().header();
        self.buf.advance(size_of::<T::Header>());
        (header, Data::new(self.buf).unwrap())
    }
}

impl Data<[u8]> {
    /// Consumes the `Data` and returns the underlying
    /// slice as a `BytesMut`.
    #[inline(always)]
    pub fn into_bytes_mut(self) -> BytesMut {
        self.buf
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("data doesn't match the type")]
    CorruptedData,
    #[error("buffer is too short to read the provided type (missing >{num_bytes_needed} bytes)")]
    BufferShort { num_bytes_needed: usize },
}

impl From<GetSliceLenErr> for ReadError {
    fn from(value: GetSliceLenErr) -> Self {
        match value {
            GetSliceLenErr::NoConfidence => ReadError::CorruptedData,
            GetSliceLenErr::BufferShort { num_bytes_needed } => {
                ReadError::BufferShort { num_bytes_needed }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Buf {
    inner: Option<BytesMut>,
}

impl Buf {
    #[inline(always)]
    fn new(buf: BytesMut) -> Self {
        Self { inner: Some(buf) }
    }

    // #[inline]
    // fn with_owned<T>(&mut self, f: impl FnOnce(BytesMut) -> (BytesMut, T)) -> T {
    //     let buf = self
    //         .inner
    //         .take()
    //         .expect("should have been initialized with Some()");

    //     let (buf, value) = f(buf);
    //     self.inner = Some(buf);
    //     value
    // }

    #[inline]
    async fn with_owned_async<T>(&mut self, f: impl AsyncFnOnce(BytesMut) -> (BytesMut, T)) -> T {
        let buf = self
            .inner
            .take()
            .expect("should have been initialized with Some()");

        let (buf, value) = f(buf).await;
        self.inner = Some(buf);
        value
    }

    #[inline(always)]
    fn as_ref(&self) -> &BytesMut {
        self.inner.as_ref().unwrap()
    }

    #[inline(always)]
    fn as_mut(&mut self) -> &mut BytesMut {
        self.inner.as_mut().unwrap()
    }

    // #[inline(always)]
    // fn into_buf(mut self) -> BytesMut {
    //     self.inner
    //         .take()
    //         .expect("every function that takes out the buf should put it back")
    // }
}

#[derive(Debug)]
#[repr(transparent)]
struct BufWrapper(BytesMut);

unsafe impl BufMut for BufWrapper {
    #[inline]
    fn remaining_mut(&self) -> usize {
        let buf = &self.0;
        buf.capacity() - buf.len()
    }

    #[inline(always)]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        unsafe { self.0.advance_mut(cnt) };
    }

    #[inline(always)]
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        self.0.chunk_mut()
    }

    #[inline(always)]
    fn put<T: bytes::buf::Buf>(&mut self, src: T)
    where
        Self: Sized,
    {
        self.0.put(src);
    }

    #[inline(always)]
    fn put_slice(&mut self, src: &[u8]) {
        self.0.put_slice(src);
    }

    #[inline(always)]
    fn put_bytes(&mut self, val: u8, cnt: usize) {
        self.0.put_bytes(val, cnt);
    }
}

unsafe impl IoBuf for BufWrapper {
    fn as_buf_ptr(&self) -> *const u8 {
        self.0.as_buf_ptr()
    }

    fn buf_len(&self) -> usize {
        self.0.buf_len()
    }

    fn buf_capacity(&self) -> usize {
        self.0.buf_capacity()
    }
}

impl SetBufInit for BufWrapper {
    unsafe fn set_buf_init(&mut self, len: usize) {
        unsafe { self.0.set_buf_init(len) };
    }
}

unsafe impl IoBufMut for BufWrapper {
    fn as_buf_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_buf_mut_ptr()
    }
}

#[derive(Debug)]
pub struct Ring {
    buf: Buf,
}

impl Ring {
    /// Creates a new `Ring` with the specified
    /// capacity.
    ///
    /// A call to this function with a `cap` of 0
    /// does not allocate from the heap.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Buf::new(BytesMut::with_capacity(cap)),
        }
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.buf.as_mut().reserve(additional);
    }

    pub fn peek<T>(&self) -> Result<&T, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of_t = std::mem::size_of::<T>();
        if self.buf.as_ref().len() < size_of_t {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of_t - self.buf.as_ref().len(),
            });
        }
        T::try_ref_from_bytes(&self.buf.as_ref()[..size_of_t]).map_err(|_| ReadError::CorruptedData)
    }

    pub fn peek_dst<T>(&self) -> Result<&T, ReadError>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let len = T::get_slice_len(self.buf.as_ref()).map_err(ReadError::from)?;
        T::try_ref_from_prefix_with_elems(self.buf.as_ref(), len)
            .map_err(|_| ReadError::CorruptedData)
            .map(|(item, _)| item)
    }

    pub fn peek_mut<T>(&mut self) -> Result<&mut T, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable + IntoBytes,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.as_ref().len() < size_of {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of - self.buf.as_ref().len(),
            });
        }
        T::try_mut_from_bytes(&mut self.buf.as_mut()[..size_of])
            .map_err(|_| ReadError::CorruptedData)
    }

    pub fn peek_mut_dst<T>(&mut self) -> Result<&mut T, ReadError>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let len = T::get_slice_len(self.buf.as_ref()).map_err(ReadError::from)?;
        T::try_mut_from_prefix_with_elems(self.buf.as_mut(), len)
            .map_err(|_| ReadError::CorruptedData)
            .map(|(item, _)| item)
    }

    /// Copies out `size_of::<T>()` bytes from the buffer
    /// as a pointer read operation, then consumes
    /// `size_of::<T>()` bytes from `Ring`.
    ///
    /// # Error
    ///
    /// `Ring` uses `zerocopy` internally, so
    /// this function fails if [`TryFromBytes`] fails.
    pub fn read<T>(&mut self) -> Result<T, ReadError>
    where
        T: TryFromBytes + KnownLayout + Immutable,
    {
        let size_of = std::mem::size_of::<T>();
        if self.buf.as_ref().len() < size_of {
            return Err(ReadError::BufferShort {
                num_bytes_needed: size_of - self.buf.as_ref().len(),
            });
        }
        let item = T::try_read_from_bytes(&self.buf.as_ref()[..size_of])
            .map_err(|_| ReadError::CorruptedData)?;
        self.buf.as_mut().advance(size_of);
        Ok(item)
    }

    /// Advances the internal buffer by `num_bytes`.
    ///
    /// This does not affect the starting place to
    /// a future call to [`Ring::fill_with_reader`].
    ///
    /// However, future calls to any of the reading
    /// functions ([`Ring::peek`], [`Ring::claim_dst`],
    /// [`Ring::read`], etc.) will start `num_bytes`
    /// after the current start of the buffer.
    ///
    /// # Panics
    ///
    /// This function panics if `num_bytes > self.remaining()`.
    pub fn consume(&mut self, num_bytes: usize) {
        self.buf.as_mut().advance(num_bytes);
    }

    // pub fn claim<T>(&mut self) -> Result<Data<T>, ReadError>
    // where
    //     T: TryFromBytes + KnownLayout + Immutable,
    // {
    //     let size_of = std::mem::size_of::<T>();
    //     if self.buf.len() < size_of {
    //         return Err(ReadError::BufferShort {
    //             num_bytes_needed: size_of - self.len(),
    //             buf_len: self.len(),
    //         });
    //     }
    //     let buf = self.buf.split_to(size_of);
    //     Ok(Data::new(buf))
    // }

    /// Takes ownership of the next value `T`.
    ///
    /// Does not copy the data, but claims a mutable portion
    /// of the internal buffer. Therefore, if T has a custom
    /// `Drop` implementation, then the caller must know that
    /// dropping `Data<T>` does not call `T::drop`
    ///
    /// # Error
    ///
    /// `Ring` uses `zerocopy` internally, so
    /// this function fails if [`TryFromBytes`] fails.
    pub fn claim_dst<T>(&mut self) -> Result<Data<T>, ReadError>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        let slice_len = T::get_slice_len(self.buf.as_ref()).map_err(ReadError::from)?;
        let size_of_header = size_of::<T::Header>();
        let data_byte_len = slice_len * size_of::<T::Data>();
        let buf = self.buf.as_mut().split_to(size_of_header + data_byte_len);
        Data::new(buf)
    }

    /// Returns a lazy iterator into the ring's internal
    /// buffer that provides a `&T` for each successfully
    /// converted `&[u8]`.
    ///
    /// The iterator yields `None` for the first slice that
    /// [`TryFromBytes`] returns an error.
    pub fn iter<'a, T>(&'a self) -> impl Iterator<Item = &'a T>
    where
        T: TryFromBytes + KnownLayout + Immutable + 'a,
    {
        self.buf
            .as_ref()
            .chunks_exact(std::mem::size_of::<T>())
            .map_while(|chunk| T::try_ref_from_bytes(chunk).ok())
    }

    /// Returns a lazy iterator into the ring's internal
    /// buffer that provides a `&T` for each successfully
    /// converted `&[u8]`.
    ///
    /// The iterator yields `None` for the first slice that
    /// [`TryFromBytes`] returns an error.
    ///
    /// This version of the function allows for DST's, and
    /// does not require that each slice be the same size.
    pub fn iter_dst<T>(&self) -> IterDst<'_, T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        IterDst {
            buf: self.buf.as_ref(),
            _p: PhantomData,
        }
    }

    /// Returns a lazy iterator into the ring's internal
    /// buffer that provides a `&mut T` for each successfully
    /// converted `&mut [u8]`.
    ///
    /// The iterator yields `None` for the first slice that
    /// [`TryFromBytes`] returns an error.
    pub fn iter_mut<'a, T>(&'a mut self) -> impl Iterator<Item = &'a mut T>
    where
        T: TryFromBytes + KnownLayout + Immutable + IntoBytes + 'a,
    {
        self.buf
            .as_mut()
            .chunks_exact_mut(std::mem::size_of::<T>())
            .map_while(|chunk| T::try_mut_from_bytes(chunk).ok())
    }

    /// Returns a lazy iterator into the ring's internal
    /// buffer that provides a `&mut T` for each successfully
    /// converted `&mut [u8]`.
    ///
    /// The iterator yields `None` for the first slice that
    /// [`TryFromBytes`] returns an error.
    ///
    /// This version of the function allows for DST's, and
    /// does not require that each slice be the same size.
    pub fn iter_mut_dst<T>(&mut self) -> IterMutDst<'_, T>
    where
        T: TryFromBytes + GetSliceLen + Immutable + ?Sized,
    {
        IterMutDst {
            buf: self.buf.as_mut(),
            _p: PhantomData,
        }
    }

    /// Pulls some bytes from the specified reader into `self`,
    /// advancing the ring's internal buffer.
    ///
    /// On success, returns the number of bytes read into
    /// the internal buffer.
    ///
    /// # Cancel Safety
    ///
    /// This function is NOT cancellation safe. If the future created
    /// by this function gets dropped before completion it is all
    /// but guaranteed that the inner buffer will be dropped.
    #[inline]
    pub async fn fill_with_reader<R>(&mut self, mut rx: R) -> io::Result<usize>
    where
        R: AsyncRead + Unpin,
    {
        self.buf
            .with_owned_async(|mut buf: BytesMut| async move {
                let mut start = buf.split();
                let BufResult(result, end) = rx.read(BufWrapper(buf)).await;
                start.unsplit(end.0);
                (start, result)
            })
            .await
    }

    /// Attempts to cheaply reclaim the already allocated
    /// capacity by shifting the current data to the front
    /// of the `Ring`. Does not indicate to the user whether
    /// the transaction failed or succeeded.
    #[inline]
    pub fn try_consolidate(&mut self) {
        let size = self.buf.as_ref().capacity() - self.buf.as_ref().len();
        let _ = self.buf.as_mut().try_reclaim(size);
    }

    /// Returns the number of bytes contained in this `Ring`.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.as_ref().len()
    }

    /// Returns true if the `Ring` has a length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.as_ref().is_empty()
    }

    /// Returns the number of bytes between the current position
    /// and the end of the buffer.
    pub fn remaining(&self) -> usize {
        self.buf.as_ref().remaining()
    }

    pub async fn fill_until<R>(&mut self, mut rx: R, num_bytes: usize) -> io::Result<Option<()>>
    where
        R: AsyncRead + Unpin,
    {
        const EXTRA: usize = 16 << 10;
        while num_bytes > self.len() {
            let is_full = {
                let buf = self.buf.as_ref();
                let len = buf.len();
                buf.capacity() < len + num_bytes
            };
            if is_full {
                self.reserve(EXTRA);
            }
            if 0 == self.fill_with_reader(&mut rx).await? {
                return Ok(None);
            }
        }

        Ok(Some(()))
    }
}
