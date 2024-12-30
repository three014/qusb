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
    network_endian::{I32, U16, U32, U64},
    transmute_ref, try_transmute, TryFromBytes, Unalign,
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
    Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, PartialEq, Eq,
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

#[derive(Debug, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
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
        assert_eq!(align_of::<UsbDeviceInfoTx>(), align_of::<u8>());
        assert_eq!(align_of_val(self), align_of::<u8>());
        assert_eq!(align_of::<UsbDeviceInfoTx>(), align_of_val(self));
        assert_eq!(self.interfaces.len(), self.b_num_interfaces as usize);
        assert_eq!(
            size_of::<UsbDeviceInfoTx>(),
            size_of_val(self) - (size_of::<UsbInterfaceInfo>() * self.b_num_interfaces as usize)
        );

        let ptr: *const u8 = std::ptr::from_ref(self).cast();

        // SAFETY: Verified that:
        // - alignment for both structs match the required 1-byte alignment for zerocopy
        // - sizeof::<UsbDeviceInfoRx>() matches sizeof::<UsbDeviceInfoTx>() when dyn-slice is removed
        // - length of dyn slice is equal to self.b_num_interfaces
        let bytes =
            unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<UsbDeviceInfoTx>()) };
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, Unaligned, KnownLayout, Immutable,
)]
#[repr(C)]
pub struct UrbReqHeader {
    seqnum: U64,
    command: Command,
    devid: U32,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, Unaligned, KnownLayout, Immutable,
)]
#[repr(C)]
pub struct UrbRespHeader {
    seqnum: U64,
    command: Command,
    status: Status,
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Immutable, IntoBytes, TryFromBytes, KnownLayout,
)]
#[repr(i32)]
pub enum IsoStatus {
    #[default]
    Success = 0x00000000,
    Pending = 0x10000001,
    ShortPacket = 0x10000002,
    Error = 0x7ff00000,
    Canceled = 0x30000001,
    TimedOut = 0x30000002,
    DeviceDisabled = 0x71000001,
    DeviceDisconnected = 0x71000002,
    BitStuff = 0x72000001,
    Crc = 0x72000002,
    NoResponse = 0x72000003,
    Babble = 0x72000004,
    Stall = 0x74000001,
    BufferOverrun = 0x72100001,
    BufferUnderrun = 0x72100002,
    AllIsoPacketsFailed = 0x78000001,
}

#[derive(Default, Clone, Copy, Immutable, IntoBytes, TryFromBytes, KnownLayout, Unaligned)]
#[repr(C)]
pub struct IsoPacketDescriptor {
    offset: U32,
    length: U32,
    actual_length: U32,
    status: Unalign<IsoStatus>,
}

#[derive(Immutable, IntoBytes, TryFromBytes, KnownLayout, Unaligned)]
#[repr(C)]
pub struct IsoPackets {
    descriptors: [IsoPacketDescriptor],
}

#[derive(KnownLayout, Immutable, IntoBytes, TryFromBytes, Unaligned)]
#[repr(transparent)]
pub struct Urb<O: zerocopy::ByteOrder> {
    inner: Unalign<vhci::ioctl::IocUrb>,
    _order: PhantomData<O>,
}

impl From<vhci::ioctl::IocUrb> for Urb<zerocopy::NativeEndian> {
    fn from(value: vhci::ioctl::IocUrb) -> Self {
        Self {
            inner: Unalign::new(value),
            _order: PhantomData,
        }
    }
}

impl From<Urb<zerocopy::NetworkEndian>> for Urb<zerocopy::NativeEndian> {
    fn from(value: Urb<zerocopy::NetworkEndian>) -> Self {
        #[cfg(target_endian = "little")]
        {
            let mut urb = value.inner.into_inner();
            urb.buffer_length = urb.buffer_length.to_le();
            urb.flags = urb.flags.to_le();
            urb.interval = urb.interval.to_le();
            urb.packet_count = urb.packet_count.to_le();
            urb.setup_packet.w_index = urb.setup_packet.w_index.to_le();
            urb.setup_packet.w_value = urb.setup_packet.w_value.to_le();
            urb.setup_packet.w_length = urb.setup_packet.w_length.to_le();
            Self {
                inner: Unalign::new(urb),
                _order: PhantomData,
            }
        }
        #[cfg(not(target_endian = "little"))]
        panic!("big endian not supported!")
    }
}

impl From<Urb<zerocopy::NativeEndian>> for Urb<zerocopy::NetworkEndian> {
    fn from(value: Urb<zerocopy::NativeEndian>) -> Self {
        #[cfg(target_endian = "little")]
        {
            let mut urb = value.inner.into_inner();
            urb.buffer_length = urb.buffer_length.to_be();
            urb.flags = urb.flags.to_be();
            urb.interval = urb.interval.to_be();
            urb.packet_count = urb.packet_count.to_be();
            urb.setup_packet.w_index = urb.setup_packet.w_index.to_be();
            urb.setup_packet.w_value = urb.setup_packet.w_value.to_be();
            urb.setup_packet.w_length = urb.setup_packet.w_length.to_be();

            Self {
                inner: Unalign::new(urb),
                _order: PhantomData,
            }
        }
        #[cfg(not(target_endian = "little"))]
        panic!("big endian not supported!")
    }
}
