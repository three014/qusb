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
//! AND every complete message must be aligned to 8 bytes.

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
#[repr(u8)]
pub enum Req {
    ListDevices {
        _padding: [u8; 3],
    } = 0,
    BorrowDevice {
        _padding: [u8; 1],
        dev_id: UsbDeviceId,
    } = 1,
    LendDevice {
        data_rate: DataRate,
        dev_id: UsbDeviceId,
    } = 2,
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
pub struct ReqFrame {
    pub version: Version,
    pub req: Req,
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
#[repr(u16)]
pub enum VersionOpt {
    Some(Version),
    None([u8; 4]),
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(u8)]
pub enum DataRate {
    Low = 0,
    Full,
    High,
}

#[derive(Debug, Clone, Copy, FromZeros, IntoBytes, KnownLayout, Immutable)]
#[repr(u8)]
pub enum Resp {
    ListDevices {
        _padding: [u8; 7],
    } = 0,
    BorrowDevice {
        data_rate: DataRate,
        _padding: [u8; 6],
    } = 1,
    LendDevice {
        _padding: [u8; 7],
    },
    Failure {
        stat: Status,
        ver: VersionOpt,
    } = 3,
}

#[inline]
pub async fn send_req<W: tokio::io::AsyncWrite + Unpin>(mut tx: W, req: Req) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    tx.write_all_buf(
        &mut ReqFrame {
            version: crate::QUSB_VER,
            req,
        }
        .as_bytes(),
    )
    .await
}

#[inline]
pub async fn send_resp<W: tokio::io::AsyncWrite + Unpin>(mut tx: W, resp: Resp) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    tx.write_all_buf(&mut resp.as_bytes()).await
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

#[derive(FromZeros, KnownLayout, Immutable, IntoBytes)]
#[repr(C, packed)]
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
#[repr(C)]
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
#[derive(Debug, Clone, Copy, KnownLayout, Immutable, FromZeros, IntoBytes)]
#[repr(C)]
pub struct Header {
    pub total_frame_len: u16,
    pub command: Command,
    pub status: Status,
    pub seqnum: u32,
}

/// A URB frame header
///
/// # How to fill in this header
///
/// VHCI (borrowing) side:
///
/// `actual_transfer_len` must be filled with the length of the transfer buffer
/// specified by the borrowing side. This is not necessarily the same as the length
/// of the buffer that will be sent over the wire. See the safety section for more info.
///
/// `iso_packet_count` must be filled with the number of isochronous packet
/// descriptors for this transfer.
///
/// `endpoint` must be the real endpoint number for this transfer, including the
/// direction set at the highest bit.
///
/// `kind` must be set to the type of transfer.
///
/// `interval` must be set if the transfer is isochronous or interrupt.
///
/// `status` MUST be set to [`vhci::Status::Pending`].
///
/// `flags` should be set to the bit-and of ~0x04 to prevent DMA mapping.
///
/// `num_errors` should be set to 0.
///
/// `ctrl_packet` can be left alone if not a control transfer. Otherwise, it must
/// be set to the exact setup packet specified by the borrowing side and
/// `actual_transfer_len` should be equal to the `w_length` value of the setup packet.
///
/// Libusb (lending) side:
///
/// `actual_transfer_len` must be filled with the number of bytes read by the USB device
/// or written to the buffer from the USB device. For isochronous transfers, this value needs
/// to be the same as the value received by the borrower. For all other transfers, note that
/// this value may not necessarily be the same as the length of the buffer that will be sent
/// over the wire. See the safety section for more info.
///
/// `status` MUST NEVER be set to [`vhci::Status::Pending`]. It should instead be set to the
/// result of the completed transfer, or [`vhci::Status::Canceled`] if canceled by the borrower.
///
/// `ctrl_packet` does not need to be modified on the lender's side.
///
/// # Safety
///
/// To reduce the size of the entire frame, we only send a transfer buffer when the other
/// side needs to read that data. Therefore, the real length of the buffer sent over the
/// wire must be calculated from the information in this header.
///
/// When sending a request frame, we do not send a transfer buffer if we're expecting data
/// from the USB device (aka, an IN transfer). In that same scenario, we would expect a reply
/// with a transfer buffer.
///
/// When receiving a request frame, we expect a transfer buffer if we're supposed to write data
/// to the USB device (aka, an OUT transfer). In that same scenario, we would not reply with
/// a transfer buffer.
///
/// HOWEVER, because the VHCI expects return URBs to contain the number of bytes written to the
/// USB device or read from the USB device, we still need to convey that information to the borrower,
/// even if we're not sending over any actual data.
///
/// Below is a working example on how to determine the buffer's length. Note that we only use
/// the length of the transfer to calculate the buffer's length if there was data to be read.
/// Isochronous packets are always sent as they arrive.
///
/// ```
/// use vhci::{Status, usbfs::Dir, ioctl::IocIsoPacketData};
///
/// let urb_header = /* get frame from somewhere */;
/// let padded_transfer_len = urb_header.actual_transfer_len.next_multiple_of(8) as usize;
/// let iso_byte_len = urb_header.iso_packet_count as usize * size_of::<IocIsoPacketData>();
/// let is_out = Dir::Out == urb_header.endpoint.direction();
/// let is_pending = Status::Pending == urb_header.status;
///
/// let has_transfer = (is_out && is_pending) || (!is_out && !is_pending);
///
/// let real_buffer_len = padded_transfer_len * (has_transfer as usize) + iso_byte_len;
/// ```
#[derive(Debug, Clone, KnownLayout, Immutable, IntoBytes, FromZeros)]
#[repr(C)]
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

/*
struct UrbRequestHeader {
    actual_transfer_len: u16,
    kind_and_iso_packet_count: KindAndPacketCnt,
    endpoint: vhci::ioctl::Endpoint,
    _padding: [u8; 4],
    ctrl_packet: vhci::ioctl::IocSetupPacket,
}
Request: 2 + 1 + 1 + 4 + 8 = 16

struct UrbReplyHeader {
    actual_transfer_len: u16,
    kind_and_iso_packet_count: KindAndPacketCnt,
    endpoint: vhci::ioctl::Endpoint,
    status: vhci::Status,
}
Reply: 2 + 1 + 1 + 4 = 8

We can calculate `num_errors` on the fly if we need to.

*/

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrbKind {
    Iso(u8),
    Int,
    Ctrl,
    Bulk,
}

const PKT_MASK: u8 = 0b01111111;
const MAX_PKTS: u8 = 0b01111111;
const KIND_MASK: u8 = 0b10000011;
const KIND_ISO: u8 = 0b00000000;
const KIND_INT: u8 = 0b10000000;
const KIND_CTRL: u8 = 0b10000001;
const KIND_BULK: u8 = 0b10000010;

/// The URB type and (optionally) the number of
/// isochronous packet descriptors, now packed into one byte.
///
/// The motivation is that if we limit the number of isochronous packets
/// to 127 or less, then we can use the high bits of a u8 to store
/// the URB type while reserving the lower bits for the packet count.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, IntoBytes, KnownLayout, Immutable, Unaligned, FromBytes,
)]
#[repr(transparent)]
pub struct PackedKind {
    inner: u8,
}

impl PackedKind {
    #[inline]
    pub const fn iso(num_pkts: u8) -> Self {
        assert!(MAX_PKTS >= num_pkts);

        Self { inner: num_pkts }
    }

    #[inline]
    pub const fn int() -> Self {
        Self { inner: KIND_INT }
    }

    #[inline]
    pub const fn ctrl() -> Self {
        Self { inner: KIND_CTRL }
    }

    #[inline]
    pub const fn bulk() -> Self {
        Self { inner: KIND_BULK }
    }

    #[inline]
    pub const fn get(&self) -> UrbKind {
        let num_pkts = self.inner & PKT_MASK;
        let kind = self.inner & KIND_MASK;
        match kind {
            KIND_ISO..=0b11 => UrbKind::Iso(num_pkts),
            KIND_INT => UrbKind::Int,
            KIND_CTRL => UrbKind::Ctrl,
            KIND_BULK => UrbKind::Bulk,
            _ => unreachable!(),
        }
    }
}

impl UrbHeader {
    pub const fn padded_transfer_len(&self) -> usize {
        let actual_transfer_len = self.actual_transfer_len as usize;
        actual_transfer_len.next_multiple_of(size_of::<u64>())
    }

    pub const fn is_out(&self) -> bool {
        matches!(self.endpoint.direction(), vhci::usbfs::Dir::Out)
    }

    pub const fn is_pending(&self) -> bool {
        matches!(self.status, vhci::Status::Pending)
    }

    pub const fn iso_byte_len(&self) -> usize {
        let num_iso_pkts = self.iso_packet_count as usize;
        num_iso_pkts * size_of::<vhci::ioctl::IocIsoPacketData>()
    }

    /// Returns `true` if this header is part of a reply frame,
    /// `false` otherwise.
    pub const fn is_reply(&self) -> bool {
        !self.is_pending()
    }
}

#[derive(KnownLayout, Immutable, FromZeros, IntoBytes)]
#[repr(C, packed)]
pub struct UrbFrame {
    pub header: UrbHeader,
    pub data: [u8],
}

impl UrbFrame {
    pub const fn header(&self) -> UrbHeader {
        // SAFETY: UrbHeader pointer is valid if UrbFrame is valid,
        //         which it is.
        unsafe { (&raw const self.header).read_unaligned() }
    }
}

impl GetSliceLen for UrbFrame {
    type Header = UrbHeader;

    fn get_slice_len(buf: &[u8]) -> Result<usize, GetSliceLenErr> {
        const BASE_LEN: usize = size_of::<UrbHeader>();

        let (header, rest) = UrbHeader::try_ref_from_prefix(buf)
            .map_err(|err| GetSliceLenErr::from_convert_err(BASE_LEN, err))?;

        let padded_transfer_len = header.padded_transfer_len();
        let iso_byte_len = header.iso_byte_len();
        let is_out = header.is_out();
        let is_reply = header.is_reply();

        let has_transfer = (is_out && !is_reply) || (!is_out && !is_reply);

        // If transfer has real data to send, then we count the
        // padded transfer length. We always count the iso packets.
        let required_byte_len = padded_transfer_len * has_transfer as usize + iso_byte_len;
        if required_byte_len > rest.len() {
            Err(GetSliceLenErr::BufferShort {
                num_bytes_needed: required_byte_len - rest.len(),
            })
        } else {
            Ok(required_byte_len - BASE_LEN)
        }
    }
}

#[derive(KnownLayout, Immutable, FromZeros, IntoBytes)]
#[repr(C, packed)]
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
