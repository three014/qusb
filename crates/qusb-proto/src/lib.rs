use serde::{Deserialize, Serialize};
use std::ops::Deref;
use thiserror::Error;

pub use lstr;
pub use zerocopy;

pub mod urb;
pub mod state {
    use std::{
        marker::{PhantomData, Unpin},
        ops::Deref,
    };

    use bytes::{Buf, Bytes, BytesMut};
    use thiserror::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use zerocopy::{
        Immutable, KnownLayout, TryFromBytes,
    };

    #[derive(Debug, Error)]
    pub enum Error {
        #[error("encounted invalid data when reading from buffer")]
        InvalidData,
        #[error("{0}")]
        IoError(#[from] std::io::Error),
    }

    pub struct Data<T> {
        buf: BytesMut,
        _p: PhantomData<T>,
    }

    impl<T> Data<T>
    where
        T: TryFromBytes + Immutable + KnownLayout,
    {
        fn get(&self) -> &T {
            T::try_ref_from_bytes(&self.buf).unwrap()
        }

        fn get_mut(&mut self) -> &mut T {
            T::try_mut_from_bytes(&mut self.buf).unwrap()
        }

        fn new(buf: BytesMut) -> Self {
            Self {
                buf,
                _p: PhantomData,
            }
        }
    }

    pub struct Ring {
        buf: BytesMut,
    }

    impl Ring {
        fn peek<T>(&self) -> Result<&T, Error>
        where
            T: TryFromBytes + KnownLayout + Immutable,
        {
            let size_of = std::mem::size_of::<T>();
            if self.buf.len() < size_of {
                return Err(Error::InvalidData);
            }
            T::try_ref_from_bytes(&self.buf[..size_of]).map_err(|_| Error::InvalidData)
        }

        fn peek_mut<T>(&mut self) -> Result<&mut T, Error>
        where
            T: TryFromBytes + KnownLayout + Immutable,
        {
            let size_of = std::mem::size_of::<T>();
            if self.buf.len() < size_of {
                return Err(Error::InvalidData);
            }
            T::try_mut_from_bytes(&mut self.buf[..size_of]).map_err(|_| Error::InvalidData)
        }

        fn read<T>(&mut self) -> Result<T, Error>
        where
            T: TryFromBytes + KnownLayout + Immutable,
        {
            let size_of = std::mem::size_of::<T>();
            if self.buf.len() < size_of {
                return Err(Error::InvalidData);
            }
            let item =
                T::try_read_from_bytes(&self.buf[..size_of]).map_err(|_| Error::InvalidData)?;
            self.buf.advance(size_of);
            Ok(item)
        }

        // fn peek_item_dst<T>(&self, dst_elems: usize) -> Result<&T, TryCastError<&[u8], T>>
        // where
        //     T: TryFromBytes + KnownLayout<PointerMetadata = usize> + Immutable,
        // {
        //     T::try_ref_from_prefix_with_elems(&self.buf, dst_elems).map(|(item, _)| item)
        // }

        fn consume<T>(&mut self, item: &T)
        where
            T: TryFromBytes + KnownLayout + Immutable,
        {
            self.buf.advance(std::mem::size_of_val(item));
        }

        fn get<T>(&mut self) -> Result<Data<T>, Error>
        where
            T: TryFromBytes + KnownLayout + Immutable,
        {
            let size_of = std::mem::size_of::<T>();
            if self.buf.len() < size_of {
                return Err(Error::InvalidData);
            }
            let buf = self.buf.split_to(size_of);
            Ok(Data::new(buf))
        }

        async fn read_into<R>(&mut self, mut rx: R) -> std::io::Result<usize>
        where
            R: AsyncRead + Unpin,
        {
            rx.read_buf(&mut self.buf).await
        }

        fn try_reclaim(&mut self, additional: usize) -> bool {
            self.buf.try_reclaim(additional)
        }
    }

    pub struct ClientIdle;

    pub struct State<S, W, R> {
        buf: Ring,
        rx: R,
        tx: W,
        inner: S,
    }

    impl<W, R> State<ClientIdle, W, R> where R: AsyncRead {
        
    }
}

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
    //! | Offset                             | Length   | Value      | Description                                                   |
    //! |------------------------------------|----------|------------|---------------------------------------------------------------|
    //! | 0                                  | 1        | 0x00       | Status: 0 for OK                                              |
    //! | 1                                  | 3        | 0x000000   | zeroed bytes for padding                                      |
    //! | 4                                  | 4        | N          | Number of USB devices from peer; 0 means none.                |
    //! | 8                                  |          |            | From now on the N devices are described, if any.              |
    //! |                                    | 8        | P          | len(path): The length of the next field in bytes.             |
    //! | 0x10                               | P <= 256 |            | path: Path of the device on the peer.                         |
    //! | 0x10 + P                           | 8        | I          | len(busid): The length of the next field in bytes.            |
    //! | 0x18 + P                           | I <= 32  |            | busid: Bus ID of the USB device.                              |
    //! | 0x18 + P + I                       | 4        |            | busnum                                                        |
    //! | 0x1C + P + I                       | 4        |            | devnum                                                        |
    //! | 0x20 + P + I                       | 4        |            | speed                                                         |
    //! | 0x24 + P + I                       | 2        |            | idVendor                                                      |
    //! | 0x26 + P + I                       | 2        |            | idProduct                                                     |
    //! | 0x28 + P + I                       | 2        |            | bcdDevice                                                     |
    //! | 0x2A + P + I                       | 1        |            | bDeviceClass                                                  |
    //! | 0x2B + P + I                       | 1        |            | bDeviceSubClass                                               |
    //! | 0x2C + P + I                       | 1        |            | bDeviceProtocol                                               |
    //! | 0x2D + P + I                       | 1        |            | bConfigurationValue                                           |
    //! | 0x2E + P + I                       | 1        |            | bNumConfigurations                                            |
    //! | 0x2F + P + I                       | 1        | T          | bNumInterfaces                                                |
    //! | 0x30 + P + I                       |          | m_0        | From now on each interface is described T times:              |
    //! |                                    | 1        |            | bInterfaceClass                                               |
    //! | 0x31 + P + I                       | 1        |            | bInterfaceSubClass                                            |
    //! | 0x32 + P + I                       | 1        |            | bInterfaceProtocol                                            |
    //! | 0x8 + i*(P + I + 0x28) + m_(i-1)*3 | 1        |            | The second USB device starts at i=1 with the len(path) field. |
 
    use thiserror::Error;
    use zerocopy::network_endian::U16;
    use zerocopy_derive::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

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
}

pub const BUS_ID_SIZE: usize = 32;
pub const VERSION: Version = Version(0x0200);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Version(u16);

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#06x}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Request {
    ListUsbDevices,
    Borrow(UsbDeviceId),
}

pub type Response<T> = Result<T, Error>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct UsbDeviceId {
    pub bus_number: u8,
    pub device_addr: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct BusId<'a>(pub std::borrow::Cow<'a, lstr::LimitedStr<32>>);

impl Deref for BusId<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsbDeviceInfo<'a> {
    pub id: UsbDeviceId,
    pub bus_id: BusId<'a>,
    pub vendor_id: u16,
    pub product_id: u16,
    pub class: u8,
    pub subclass: u8,
    pub protocol: u8,
    pub interfaces: Vec<InterfaceInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceInfo {
    pub interface_number: u8,
    pub class: u8,
    pub subclass: u8,
    pub protocol: u8,
}

#[derive(Debug, Error, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum Error {
    #[error("request failed")]
    Failed = 1,
    #[error("device busy (exported)")]
    DevBusy,
    #[error("device in error state")]
    DevErr,
    #[error("device not found")]
    NoDev,
    #[error("unexpected request")]
    UnexpectedReq,
    #[error("unexpected request")]
    UnexpectedResp,
    #[error("unexpected version - client: {client}, server: {server}")]
    VersionMismatch { client: Version, server: Version },
}

/*
I want to send ISO packets using QUIC
datagrams, not with QUIC streams.
ISO packets are best effort and unreliable
so I might as well do the same thing.
*/

#[cfg(test)]
mod tests {}
