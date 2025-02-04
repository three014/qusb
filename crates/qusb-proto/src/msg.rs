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
//! All message data must be sent in little endian,
//! AND every complete message must be aligned to 4 bytes.
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
//! | 10     | 6      | 0          | Padding for align(8)
//!
//! Response::ListDevices:
//!
//! | Offset                    | Length   | Value      | Description                                                   |
//! |---------------------------|----------|------------|---------------------------------------------------------------|
//! | 0                         | 1        | 0x00       | Status: 0 for OK                                              |
//! | 1                         | 7        | 0          | Padding for align(8)                                          |
//! | 8                         |          |            | UsbDeviceInfoHeader                                           |
//! |                           |          |            | UsbInterfaceInfo * padded_num_interfaces                      |
//!
//! Non-zero status response:
//!
//! | Offset                    | Length   | Value      | Description                                                   |
//! |---------------------------|----------|------------|---------------------------------------------------------------|
//! | 0                         | 1        | 0x00       | Status: Nonzero status                                        |
//! | 1                         | 3        | 0          | Padding for align(8) (technically)
//! | 4                         | 1        |            | Major number (only if status == VersionMismatch)              |
//! | 5                         | 1        |            | Minor number (only if status == VersionMismatch)              |
//! | 7                         | 3        |            | Patch number (only if status == VersionMismatch)              |
//!
//! Response::BorrowDevice:
//!
//! | Offset                    | Length   | Value      | Description                                                   |
//! |---------------------------|----------|------------|---------------------------------------------------------------|
//! | 0                         | 1        | 0x00       | Status: 0 for OK                                              |
//! | 1                         | 7        | 0          | Padding for status
//!
//!

use std::{
    ffi::OsStr,
    io,
    mem::{offset_of, size_of},
    os::unix::ffi::OsStrExt,
    path::Path,
};

use thiserror::Error;
use zerocopy::{try_transmute, FromBytes, IntoBytes, TryFromBytes};
use zerocopy_derive::*;

use crate::{GetSliceLen, GetSliceLenErr};

pub const BUS_ID_MAX_LEN: u8 = 32;
pub const PATH_MAX_LEN: u16 = 256;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u16,
}

impl Version {
    pub const fn is_compat(&self, other: &Self) -> bool {
        self.major == other.major && self.minor == other.minor
    }
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
    LendDevice,
}

#[derive(
    Debug,
    Error,
    Clone,
    Copy,
    IntoBytes,
    FromZeros,
    KnownLayout,
    Immutable,
    Unaligned,
    PartialEq,
    Eq,
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
    #[error("timed out")]
    Timeout,
    #[error("invalid data from peer")]
    Proto,
}

#[derive(
    Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, PartialEq, Eq, Hash,
)]
#[repr(C)]
pub struct UsbDeviceId {
    pub bus_number: u8,
    pub device_addr: u8,
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
#[repr(C, align(8))]
struct ReqListDevices {
    version: Version,
    req: Request,
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
#[repr(C, align(8))]
struct ReqBorrowDevice {
    version: Version,
    req: Request,
    id: UsbDeviceId,
    _padding: [u8; 6],
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
#[repr(C, align(8))]
struct ReqLendDevice {
    version: Version,
    req: Request,
    id: UsbDeviceId,
    _padding: [u8; 6],
}

#[inline]
pub async fn req_list_devices<W: tokio::io::AsyncWrite + Unpin>(mut tx: W) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let request = ReqListDevices {
        version: crate::QUSB_VER,
        req: Request::ListDevices,
    };
    tx.write_all_buf(&mut request.as_bytes()).await
}

#[inline]
pub async fn req_borrow<W: tokio::io::AsyncWrite + Unpin>(
    mut tx: W,
    id: UsbDeviceId,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let request = ReqBorrowDevice {
        version: crate::QUSB_VER,
        req: Request::BorrowDevice,
        id,
        _padding: Default::default(),
    };
    tx.write_all_buf(&mut request.as_bytes()).await
}

#[inline]
pub async fn req_lend<W: tokio::io::AsyncWrite + Unpin>(
    mut tx: W,
    id: UsbDeviceId,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let request = ReqLendDevice {
        version: crate::QUSB_VER,
        req: Request::LendDevice,
        id,
        _padding: Default::default(),
    };
    tx.write_all_buf(&mut request.as_bytes()).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct UsbInterfaceInfo {
    pub b_interface_number: u8,
    pub b_interface_class: u8,
    pub b_interface_subclass: u8,
    pub b_interface_protocol: u8,
}

/// USB connection speed
#[derive(
    Default,
    Debug,
    Copy,
    Clone,
    Eq,
    PartialOrd,
    Ord,
    PartialEq,
    Hash,
    FromZeros,
    IntoBytes,
    KnownLayout,
    Immutable,
    Unaligned,
)]
#[non_exhaustive]
#[repr(u8)]
pub enum Speed {
    #[default]
    Unknown = 0,

    /// Low speed (1.5 Mbit)
    Low = 1,

    /// Full speed (12 Mbit)
    Full,

    /// High speed (480 Mbit)
    High,

    /// Super speed (5000 Mbit)
    Super,

    /// Super speed (10000 Mbit)
    SuperPlus,
}

impl Speed {
    pub fn from_u8(num: u8) -> Self {
        try_transmute!(num).unwrap_or_default()
    }
}

pub trait SendUsbDeviceInfo {
    fn get(&self) -> &UsbDeviceInfoHeader;
    fn interfaces_with_padding(&self) -> &[UsbInterfaceInfo];
    fn interfaces(&self) -> &[UsbInterfaceInfo];
}

#[derive(FromZeros, KnownLayout, Immutable)]
#[repr(C)]
pub struct UsbDeviceInfo {
    pub header: UsbDeviceInfoHeader,
    pub interfaces: [UsbInterfaceInfo],
}

impl UsbDeviceInfo {
    pub fn path(&self) -> Option<&Path> {
        self.header
            .path
            .get(..usize::from(self.header.path_len))
            .map(OsStr::from_bytes)
            .map(Path::new)
    }

    pub fn bus_id(&self) -> Option<&OsStr> {
        self.header
            .bus_id
            .get(..usize::from(self.header.bus_id_len))
            .map(OsStr::from_bytes)
    }
}

impl GetSliceLen for UsbDeviceInfo {
    type Header = UsbDeviceInfoHeader;

    fn get_slice_len(buf: &[u8]) -> Result<usize, GetSliceLenErr> {
        const SIZE_OF_BASE: usize = size_of::<UsbDeviceInfoHeader>();
        if SIZE_OF_BASE > buf.len() {
            return Err(GetSliceLenErr::BufferShort {
                num_bytes_needed: SIZE_OF_BASE - buf.len(),
            });
        }

        const HEADER_OFFSET: usize = offset_of!(UsbDeviceInfo, header);
        const LEN_OFFSET: usize =
            HEADER_OFFSET + offset_of!(UsbDeviceInfoHeader, padded_num_interfaces);
        let padded_len = usize::from(buf[LEN_OFFSET]);

        let size_of_t = padded_len * size_of::<UsbInterfaceInfo>() + SIZE_OF_BASE;
        if size_of_t > buf.len() {
            Err(GetSliceLenErr::BufferShort {
                num_bytes_needed: size_of_t - buf.len(),
            })
        } else {
            Ok(padded_len)
        }
    }
}

#[derive(Debug, Clone, KnownLayout, Immutable, IntoBytes, FromZeros)]
#[repr(C, align(8))]
pub struct UsbDeviceInfoHeader {
    pub path: [u8; PATH_MAX_LEN as usize],
    pub bus_id: [u8; BUS_ID_MAX_LEN as usize],
    pub path_len: u16,
    pub id_vendor: u16,
    pub id_product: u16,
    pub bcd_device: u16,
    pub bus_id_len: u8,
    pub busnum: u8,
    pub devnum: u8,
    pub speed: Speed,
    pub b_device_class: u8,
    pub b_device_subclass: u8,
    pub b_device_protocol: u8,
    pub b_configuration_value: u8,
    pub b_num_configurations: u8,
    pub b_num_interfaces: u8,
    pub padded_num_interfaces: u8,
    pub _padding: [u8; 5],
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Immutable, IntoBytes, FromZeros, KnownLayout, Unaligned,
)]
#[repr(u8)]
pub enum Command {
    CmdSubmit = 0,
    CmdUnlink = 1,
    CmdPort = 2,
    RetSubmit = 3,
    RetUnlink = 4,
    RetPort = 5,
}

/// A Qusb frame header.
///
/// Some things to note:
///
/// - `total_frame_len` is the number of bytes for the entire
///   transmitted frame divided by 8. A frame is guaranteed to
///   be aligned to 8 bytes.
/// - `status` is read by the requester, and written to by the responder.
#[derive(Debug, Clone, KnownLayout, Immutable, FromZeros, IntoBytes)]
#[repr(C, align(8))]
pub struct Header {
    pub total_frame_len: u16,
    pub command: Command,
    pub status: Status,
    pub seqnum: u32,
}

/// A URB frame header
///
/// Some things to note:
///
/// - `transfer_actual_len` will be less than or equal to the
///   real transfer length, which will always be aligned to 8 bytes.
/// - The length of the data after this header is determined
///   by the `transfer_actual_len` and `num_isos`, but only
///   when the frame has a:
///   - [`Out`] endpoint when status is [`Pending`], or
///   - [`In`] endpoint when status is not [`Pending`]
///
/// [`Out`]: vhci::usbfs::Dir::Out
/// [`In`]: vhci::usbfs::Dir::In
/// [`Pending`]: vhci::Status::Pending
#[derive(Debug, Clone, KnownLayout, Immutable, IntoBytes, FromZeros)]
#[repr(C, align(8))]
pub struct UrbHeader {
    pub actual_transfer_len: u16,
    // pub transfer_padded_len: u16,
    pub iso_packet_count: u16,
    pub endpoint: vhci::ioctl::Endpoint,
    pub kind: vhci::ioctl::UrbType,
    pub interval: u16,
    pub status: vhci::Status,
    pub flags: u16,
    pub num_errors: u16,
    pub ctrl_packet: vhci::ioctl::IocSetupPacket,
    // pub _padding: [u8; 5],
}

#[derive(KnownLayout, Immutable, FromZeros)]
#[repr(C)]
pub struct UrbFrame {
    pub header: UrbHeader,
    pub data: [u8],
}

impl GetSliceLen for UrbFrame {
    type Header = UrbHeader;

    fn get_slice_len(buf: &[u8]) -> Result<usize, GetSliceLenErr> {
        const BASE_LEN: usize = size_of::<UrbHeader>();

        let (header, rest) = UrbHeader::try_ref_from_prefix(buf)
            .map_err(|err| GetSliceLenErr::from_convert_err(BASE_LEN, err))?;

        let transfer_padded_len = header.actual_transfer_len.next_multiple_of(8) as usize;
        let num_isos = header.iso_packet_count as usize;
        let iso_byte_len = num_isos * size_of::<vhci::ioctl::IocIsoPacketData>();
        let is_out = vhci::usbfs::Dir::Out == header.endpoint.direction();
        let is_pending = vhci::Status::Pending == header.status;

        let valid = (is_out && is_pending) || (!is_out && !is_pending);

        // If transfer has real data to send, then we count the
        // padded transfer length and number of isochronous packets
        let required_byte_len = (transfer_padded_len + iso_byte_len) * valid as usize;
        if required_byte_len > rest.len() {
            Err(GetSliceLenErr::BufferShort {
                num_bytes_needed: required_byte_len - rest.len(),
            })
        } else {
            Ok(required_byte_len - BASE_LEN)
        }
    }
}

#[derive(KnownLayout, Immutable, FromZeros)]
#[repr(C)]
pub struct QusbFrame {
    pub header: Header,
    pub data: [u8],
}

impl GetSliceLen for QusbFrame {
    type Header = Header;

    fn get_slice_len(buf: &[u8]) -> Result<usize, GetSliceLenErr> {
        const BASE_LEN: usize = size_of::<Header>();

        let (frame_len, _) =
            u16::read_from_prefix(buf).map_err(|_| GetSliceLenErr::BufferShort {
                num_bytes_needed: BASE_LEN - buf.len(),
            })?;

        let frame_len = (frame_len * 8) as usize;

        if frame_len > buf.len() {
            Err(GetSliceLenErr::BufferShort {
                num_bytes_needed: frame_len - buf.len(),
            })
        } else {
            Ok(frame_len - BASE_LEN)
        }
    }
}

#[derive(KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct DeviceDescriptor {
    pub b_length: u8,
    pub b_descriptor_type: u8,
    pub bcd_usb: u16,
    pub b_device_class: u8,
    pub b_device_subclass: u8,
    pub b_device_protocol: u8,
    pub b_max_packet_size0: u8,
    pub id_vendor: u16,
    pub id_product: u16,
    pub bcd_device: u16,
    pub i_manufacturer: u8,
    pub i_product: u8,
    pub i_serial_number: u8,
    pub b_num_configurations: u8,
}

pub mod mass_storage {
    use zerocopy::little_endian::U32;
    use zerocopy_derive::*;

    #[derive(Debug, Clone, FromBytes, IntoBytes, Unaligned, KnownLayout, Immutable)]
    #[repr(C)]
    pub struct CommandBlockWrapper {
        pub d_cbw_signature: [u8; 4],
        pub d_cbw_tag: U32,
        pub d_cbw_data_transfer_length: U32,
        pub bm_cbw_flags: u8,
        pub b_cbw_lun: u8,
        pub b_cbw_cb_length: u8,
        pub cbw_cb: [u8; 16],
    }

    #[derive(Debug, Clone, FromBytes, IntoBytes, Unaligned, KnownLayout, Immutable)]
    #[repr(C)]
    pub struct CommandStatusWrapper {
        pub d_cbw_signature: [u8; 4],
        pub d_cbw_tag: U32,
        pub d_cbw_data_residue: U32,
        pub bm_cbw_status: u8,
    }
}

// CONST TESTS FOR ALIGNMENT
// All complete messages must be aligned to 8 bytes.

const _: [u8; ((size_of::<ReqListDevices>() % 8 == 0) as usize) - 1] = [];

const _: [u8; ((size_of::<ReqBorrowDevice>() % 8 == 0) as usize) - 1] = [];

const _: [u8; ((size_of::<ReqLendDevice>() % 8 == 0) as usize) - 1] = [];
