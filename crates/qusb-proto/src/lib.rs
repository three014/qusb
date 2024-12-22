use zerocopy::KnownLayout;

pub use lstr;
pub use zerocopy;

pub mod urb;
pub mod msg {
    //! # The structure of a message in Qusb
    //!
    //! A client peer initiates a stream with a version header,
    //! The client can then send a request message.
    //!
    //! The server peer must first validate that
    //! the version header received matches the host's
    //! version. If the versions match, the server can
    //! read the request message and send a response.
    //! Otherwise, the server must send an error response
    //! and is free to end the stream right there.
    //!
    //! All message data must be sent in network endian.
    //!
    //! # Message Format
    //!
    //! Request::ListDevices:
    //!
    //! | Offset | Length | Value      | Description                                     |
    //! |--------|--------|------------|-------------------------------------------------|
    //! | 0      | 1      |            | Major number                                    |
    //! | 1      | 1      |            | Minor number                                    |
    //! | 2      | 2      |            | Patch number                                    |
    //! | 4      | 4      | 0x00000000 | Request code: Retrieve the list of USB devices. |
    //!
    //! Request::BorrowDevice:
    //!
    //! | Offset | Length | Value      | Description                                  |
    //! |--------|--------|------------|----------------------------------------------|
    //! | 0      | 1      |            | Major number                                 |
    //! | 1      | 1      |            | Minor number                                 |
    //! | 2      | 2      |            | Patch number                                 |
    //! | 4      | 4      | 0x00000001 | Request code: Borrow a USB device from peer. |
    //! | 8      | 1      |            | USB bus number                               |
    //! | 9      | 1      |            | USB device number                            |
    //!
    //! Response::ListDevices:
    //!
    //! | Offset                    | Length   | Value      | Description                                                   |
    //! |---------------------------|----------|------------|---------------------------------------------------------------|
    //! |                           | 1        | 0x00       | Status: 0 for OK                                              |
    //! |                           | 3        | 0x000000   | zeroed bytes for padding                                      |
    //! |                           |          |            | From now on the devices are described, if any.                |
    //! |                           | 2        | P          | len(path): The length of the next field in bytes.             |
    //! |                           | 256      |            | path: Path of the device on the peer.                         |
    //! |                           | 1        | I          | len(busid): The length of the next field in bytes.            |
    //! |                           | 32       |            | busid: Bus ID of the USB device.                              |
    //! |                           | 1        |            | busnum                                                        |
    //! |                           | 1        |            | devnum                                                        |
    //! |                           | 4        |            | speed                                                         |
    //! |                           | 2        |            | idVendor                                                      |
    //! |                           | 2        |            | idProduct                                                     |
    //! |                           | 2        |            | bcdDevice                                                     |
    //! |                           | 1        |            | bDeviceClass                                                  |
    //! |                           | 1        |            | bDeviceSubClass                                               |
    //! |                           | 1        |            | bDeviceProtocol                                               |
    //! |                           | 1        | T          | bNumInterfaces                                                |
    //! |                           |          | m_0        | From now on each interface is described T times:              |
    //! |                           | 1        |            | bInterfaceNumber
    //! |                           | 1        |            | bInterfaceClass                                               |
    //! |                           | 1        |            | bInterfaceSubClass                                            |
    //! |                           | 1        |            | bInterfaceProtocol                                            |
    //! |                           |          |            | The second USB device starts at i=1 with the len(path) field. |
    //!
    //! Non-zero status response:
    //!
    //! | Offset                    | Length   | Value      | Description                                                   |
    //! |---------------------------|----------|------------|---------------------------------------------------------------|
    //! | 0                         | 1        | 0x00       | Status: Nonzero status                                        |
    //! | 1                         | 1        |            | Major number (only if status == VersionMismatch)              |
    //! | 2                         | 1        |            | Minor number (only if status == VersionMismatch)              |
    //! | 3                         | 3        |            | Patch number (only if status == VersionMismatch)              |
    //!
    //! Response::BorrowDevice:
    //!
    //! | Offset                    | Length   | Value      | Description                                                   |
    //! |---------------------------|----------|------------|---------------------------------------------------------------|
    //! | 0                         | 1        | 0x00       | Status: 0 for OK                                              |
    //!
    //!

    use thiserror::Error;
    use zerocopy::network_endian::{U16, U32};
    use zerocopy_derive::*;

    use crate::GetSliceLen;

    pub mod tx {
        use zerocopy::network_endian::{U16, U32};

        use super::UsbInterfaceInfo;

        pub trait UsbDeviceInfo {
            fn path(&self) -> &lstr::LimitedStr<256>;
            fn bus_id(&self) -> &lstr::LimitedStr<32>;
            fn busnum(&self) -> u8;
            fn devnum(&self) -> u8;
            fn speed(&self) -> U32;
            fn id_vendor(&self) -> U16;
            fn id_product(&self) -> U16;
            fn bcd_device(&self) -> U16;
            fn b_device_class(&self) -> u8;
            fn b_device_subclass(&self) -> u8;
            fn b_device_protocol(&self) -> u8;
            fn b_num_interfaces(&self) -> u8;
            fn interfaces(&self) -> &[UsbInterfaceInfo];
        }
    }

    #[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
    #[repr(C)]
    pub struct Version {
        pub major: u8,
        pub minor: u8,
        pub patch: U16,
    }

    impl std::fmt::Display for Version {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
        }
    }

    #[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
    #[repr(C)]
    pub enum Request {
        ListDevices,
        BorrowDevice,
    }

    #[derive(
        Debug, Error, Clone, Copy, IntoBytes, FromZeros, KnownLayout, Immutable, Unaligned,
    )]
    #[repr(u8)]
    pub enum Status {
        #[error("request succeeded")]
        Success = 0,
        #[error("request failed")]
        Failed = 1,
        #[error("device busy (exported)")]
        DevBusy,
        #[error("device in error state")]
        DevErr,
        #[error("device not found")]
        NoDev,
        #[error("unexpected data")]
        Unexpected,
        #[error("incompatible versions")]
        VersionMismatch,
    }

    #[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
    #[repr(C)]
    pub struct UsbDeviceId {
        pub bus_number: u8,
        pub device_addr: u8,
    }

    #[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
    #[repr(C)]
    pub struct UsbInterfaceInfo {
        pub b_interface_number: u8,
        pub b_interface_class: u8,
        pub b_interface_subclass: u8,
        pub b_interface_protocol: u8,
    }

    #[derive(Debug, FromBytes, KnownLayout, Immutable, Unaligned)]
    #[repr(C)]
    pub struct UsbDeviceInfo {
        pub path_len: U16,
        pub path: [u8; 256],
        pub bus_id_len: u8,
        pub bus_id: [u8; 32],
        pub busnum: u8,
        pub devnum: u8,
        pub speed: U32,
        pub id_vendor: U16,
        pub id_product: U16,
        pub bcd_device: U16,
        pub b_device_class: u8,
        pub b_device_subclass: u8,
        pub b_device_protocol: u8,
        // pub b_configuration_value: u8, // TODO: Can't access without opening device on Linux
        // pub b_num_configurations: u8, // TODO: Can't access...
        pub b_num_interfaces: u8, // TODO: nusb returns a plain iterator that doesn't know its length
        pub interfaces: [UsbInterfaceInfo],
    }

    impl GetSliceLen for UsbDeviceInfo {
        fn get_slice_len(buf: &[u8]) -> Option<usize> {
            let size_of_base = 307;
            if buf.len() < size_of_base {
                None
            } else {
                let start = size_of_base - std::mem::size_of::<u8>();
                let len = buf[start];
                Some(len as usize)
            }
        }
    }
}

pub mod data {
    use std::{io, marker::PhantomData};

    use bytes::Buf;

    use bytes::BytesMut;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncReadExt;

    use zerocopy::ConvertError;
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
}

pub const QUSB_VER: msg::Version = msg::Version {
    major: 0,
    minor: 1,
    patch: zerocopy::network_endian::U16::ZERO,
};

pub const BUS_ID_SIZE: usize = 32;

/// You received a DST from the internet in the
/// form of bytes, and you want to find out the
/// number of elements in the trailing slice.
/// What to do? Implement [`GetSliceLen`]!
///
/// `GetSliceLen` allows a DST to search a buffer of
/// bytes for the value that corresponds to
/// the number of elements in the DST.
///
/// The returned value will still be fed through
/// [`zerocopy::TryFromBytes`], so it's not like
/// this will cause UB if the value is incorrect.
/// More likely, if the value is incorrect, then
/// the whole buffer is messed-up anyway.
///
/// Just, please don't try to do your own `unsafe`
/// stuff based on this value.
pub trait GetSliceLen
where
    Self: KnownLayout<PointerMetadata = usize>,
{
    /// If `buf` was some DST `T`, then this
    /// function returns the number of elements
    /// in the slice at the end of the DST instance.
    ///
    /// The implementer should return `None` if the
    /// buffer was shorter than std::mem::size_of::<T>()
    /// or if they have reason to believe their value
    /// is incorrect.
    ///
    /// # Assumptions
    ///
    /// - The bytes of `buf` are in network endian
    /// - `buf` may not be exactly the length of
    ///   the DST and its trailing slice.
    fn get_slice_len(buf: &[u8]) -> Option<usize>;
}

/*
I want to send ISO packets using QUIC
datagrams, not with QUIC streams.
ISO packets are best effort and unreliable
so I might as well do the same thing.
*/

#[cfg(test)]
mod tests {}
