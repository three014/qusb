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

use std::{
    marker::PhantomData,
    mem::{align_of, zeroed},
};

use thiserror::Error;
use zerocopy::{
    little_endian::{I32, U16, U32, U64},
    transmute_ref, try_transmute, try_transmute_ref, Immutable, IntoBytes, KnownLayout,
    TryFromBytes, Unalign, Unaligned,
};
use zerocopy_derive::*;

use crate::GetSliceLen;

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
}

#[derive(
    Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, PartialEq, Eq, Hash,
)]
#[repr(C)]
pub struct UsbDeviceId {
    pub bus_number: u8,
    pub device_addr: u8,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
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

#[derive(TryFromBytes, KnownLayout, Immutable, Unaligned, IntoBytes)]
#[repr(C, packed)]
pub struct UsbDeviceInfoRx {
    pub path_len: U16,
    pub path: [u8; 256],
    pub bus_id_len: u8,
    pub bus_id: [u8; 32],
    pub busnum: u8,
    pub devnum: u8,
    pub speed: Speed,
    pub id_vendor: U16,
    pub id_product: U16,
    pub bcd_device: U16,
    pub b_device_class: u8,
    pub b_device_subclass: u8,
    pub b_device_protocol: u8,
    pub b_configuration_value: u8,
    pub b_num_configurations: u8,
    pub b_num_interfaces: u8,
    pub interfaces: [UsbInterfaceInfo],
}

impl GetSliceLen for UsbDeviceInfoRx {
    fn get_slice_len(buf: &[u8]) -> Option<usize> {
        let size_of_base = 306;
        if buf.len() < size_of_base {
            None
        } else {
            let start = size_of_base - std::mem::size_of::<u8>();
            let len = buf[start];
            Some(len as usize)
        }
    }
}

impl SendUsbDeviceInfo for UsbDeviceInfoRx {
    fn get(&self) -> &UsbDeviceInfoTx {
        debug_assert_eq!(align_of::<UsbDeviceInfoTx>(), align_of::<u8>());
        debug_assert_eq!(align_of_val(self), align_of::<u8>());
        debug_assert_eq!(align_of::<UsbDeviceInfoTx>(), align_of_val(self));
        assert_eq!(self.interfaces.len(), self.b_num_interfaces as usize);
        assert_eq!(
            size_of::<UsbDeviceInfoTx>(),
            size_of_val(self) - (size_of::<UsbInterfaceInfo>() * self.b_num_interfaces as usize)
        );

        let bytes = &self.as_bytes()[..std::mem::size_of::<UsbDeviceInfoTx>()];
        UsbDeviceInfoTx::try_ref_from_bytes(bytes)
            .expect("the two structs should match in everything, including fields")
    }

    fn interfaces(&self) -> &[UsbInterfaceInfo] {
        &self.interfaces[..self.b_num_interfaces as usize]
    }
}

#[derive(Debug, Clone, KnownLayout, Immutable, IntoBytes, FromZeros, Unaligned)]
#[repr(C)]
pub struct UsbDeviceInfoTx {
    pub path_len: U16,
    pub path: [u8; 256],
    pub bus_id_len: u8,
    pub bus_id: [u8; 32],
    pub busnum: u8,
    pub devnum: u8,
    pub speed: Speed,
    pub id_vendor: U16,
    pub id_product: U16,
    pub bcd_device: U16,
    pub b_device_class: u8,
    pub b_device_subclass: u8,
    pub b_device_protocol: u8,
    pub b_configuration_value: u8,
    pub b_num_configurations: u8,
    pub b_num_interfaces: u8,
}

pub trait SendUsbDeviceInfo {
    fn get(&self) -> &UsbDeviceInfoTx;
    fn interfaces(&self) -> &[UsbInterfaceInfo];
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Immutable, IntoBytes, TryFromBytes, KnownLayout, Unaligned,
)]
#[repr(u8)]
pub enum Command {
    CmdSubmit = 1,
    CmdUnlink = 2,
    RetSubmit = 3,
    RetUnlink = 4,
}

#[derive(KnownLayout, Immutable, Unaligned, TryFromBytes, IntoBytes)]
#[repr(C)]
pub struct Header {
    seqnum: U64,
    dev_id: UsbDeviceId,
    command: Command,
    status: Status,
    _padding: [u8; 4],
}

#[derive(Default, Clone, Copy, Immutable, IntoBytes, TryFromBytes, KnownLayout, Unaligned)]
#[repr(C)]
pub struct IsoPacketDescriptor {
    offset: U32,
    length: U32,
    actual_length: U32,
    status: Unalign<vhci::Status>,
}

#[derive(KnownLayout, Immutable, IntoBytes, TryFromBytes, Unaligned)]
#[repr(C)]
pub struct UrbHeader {
    urb: Unalign<vhci::ioctl::IocUrb>,
    num_errors: U16,
    num_isos: U16,
    buf_len: U16,
    _padding: [u8; 6],
}

pub trait SendUrb {
    fn urb(&self) -> &UrbHeader;
    fn transfer(&self) -> &[u8];
    fn iso_packets(&self) -> &[IsoPacketDescriptor];
}
