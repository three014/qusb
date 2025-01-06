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
//! | 1                         | 7        | 0          | zeroed bytes for align(8)                                     |
//! |                           |          |            | From now on the devices are described, if any.                |
//! | 4                         | 2        | P          | len(path): The length of the next field in bytes.             |
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
//! |                           | 1        |            | bConfigurationValue                                           |
//! |                           | 1        |            | bNumConfigurations                                            |
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

use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::Path};

use thiserror::Error;
use zerocopy::try_transmute;
use zerocopy_derive::*;

use crate::GetSliceLen;

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
    fn get_slice_len(buf: &[u8]) -> Option<usize> {
        let size_of_base = std::mem::size_of::<UsbDeviceInfoHeader>();
        if size_of_base > buf.len() {
            return None;
        }

        const HEADER_OFFSET: usize = std::mem::offset_of!(UsbDeviceInfo, header);
        const LEN_OFFSET: usize =
            HEADER_OFFSET + std::mem::offset_of!(UsbDeviceInfoHeader, padded_num_interfaces);
        let len = buf[LEN_OFFSET];
        Some(len as usize)
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
    RetSubmit = 2,
    RetUnlink = 3,
}

#[derive(KnownLayout, Immutable, FromZeros, IntoBytes)]
#[repr(C, align(8))]
pub struct Header {
    pub seqnum: u64,
    pub dev_id: UsbDeviceId,
    pub command: Command,
    pub status: Status,
    pub _padding: [u8; 4],
}

#[derive(KnownLayout, Immutable, IntoBytes, TryFromBytes)]
#[repr(C, align(8))]
pub struct UrbHeader {
    pub setup_packet: vhci::ioctl::IocSetupPacket,
    pub interval: u16,
    pub flags: u16,
    pub num_errors: u16,
    pub address: vhci::ioctl::Address,
    pub endpoint: vhci::ioctl::Endpoint,
    pub status: vhci::Status,
    pub kind: vhci::ioctl::UrbType,
    pub _padding: [u8; 3],
}

#[derive(KnownLayout, Immutable, FromBytes)]
#[repr(C, align(8))]
pub struct Transfer {
    pub header: TransferHeader,
    pub buf: [u8],
}

#[derive(Debug, Clone, Copy, Immutable, KnownLayout, IntoBytes, FromBytes)]
#[repr(C)]
pub struct TransferHeader {
    pub aligned_len: u16,
    pub actual_len: u16,
    pub _padding: [u8; 4],
}

impl GetSliceLen for Transfer {
    fn get_slice_len(buf: &[u8]) -> Option<usize> {
        if std::mem::size_of::<TransferHeader>() > buf.len() {
            return None;
        }

        const LEN_OFFSET: usize = std::mem::offset_of!(TransferHeader, aligned_len);
        let len_bytes = &buf[LEN_OFFSET..LEN_OFFSET + size_of::<u16>()];
        let len = u16::from_le_bytes(len_bytes.try_into().unwrap());
        Some(len.into())
    }
}

#[derive(KnownLayout, Immutable, IntoBytes, FromBytes, Clone, Copy, Debug)]
#[repr(C)]
pub struct IsoPacketHeader {
    pub len: u16,
    pub _padding: [u8; 6],
}

#[derive(KnownLayout, Immutable, FromBytes)]
#[repr(C, align(8))]
pub struct IsoPacketData {
    pub header: IsoPacketHeader,
    pub buf: [vhci::ioctl::IocIsoPacketData],
}

impl GetSliceLen for IsoPacketData {
    fn get_slice_len(buf: &[u8]) -> Option<usize> {
        if std::mem::size_of::<IsoPacketHeader>() > buf.len() {
            return None;
        }

        const LEN_OFFSET: usize = std::mem::offset_of!(IsoPacketHeader, len);
        let len_bytes = &buf[LEN_OFFSET..LEN_OFFSET + size_of::<u16>()];
        let len = u16::from_le_bytes(len_bytes.try_into().unwrap());
        Some(len.into())
    }
}

#[derive(KnownLayout, Immutable, FromBytes)]
#[repr(C, align(8))]
pub struct IsoPacketGiveback {
    pub header: IsoPacketHeader,
    pub buf: [vhci::ioctl::IocIsoPacketGiveback],
}

impl GetSliceLen for IsoPacketGiveback {
    fn get_slice_len(buf: &[u8]) -> Option<usize> {
        if std::mem::size_of::<IsoPacketHeader>() > buf.len() {
            return None;
        }

        const LEN_OFFSET: usize = std::mem::offset_of!(IsoPacketHeader, len);
        let len_bytes = &buf[LEN_OFFSET..LEN_OFFSET + size_of::<u16>()];
        let len = u16::from_le_bytes(len_bytes.try_into().unwrap());
        Some(len.into())
    }
}

pub trait SendUrb {
    fn urb(&self) -> &UrbHeader;
    fn transfer(&self) -> &Transfer;
    fn transfer_mut(&mut self) -> &mut Transfer;
    fn iso_packets_tx(&self) -> &IsoPacketData;
    fn iso_packets_tx_mut(&mut self) -> &mut IsoPacketData;
    fn iso_packets_rx(&self) -> &IsoPacketGiveback;
    fn iso_packets_rx_mut(&mut self) -> &mut IsoPacketGiveback;
}

impl<T: SendUrb> SendUrb for &mut T {
    fn urb(&self) -> &UrbHeader {
        T::urb(self)
    }

    fn transfer(&self) -> &Transfer {
        T::transfer(self)
    }

    fn transfer_mut(&mut self) -> &mut Transfer {
        T::transfer_mut(self)
    }

    fn iso_packets_tx(&self) -> &IsoPacketData {
        T::iso_packets_tx(self)
    }

    fn iso_packets_tx_mut(&mut self) -> &mut IsoPacketData {
        T::iso_packets_tx_mut(self)
    }

    fn iso_packets_rx(&self) -> &IsoPacketGiveback {
        T::iso_packets_rx(self)
    }

    fn iso_packets_rx_mut(&mut self) -> &mut IsoPacketGiveback {
        T::iso_packets_rx_mut(self)
    }
}
