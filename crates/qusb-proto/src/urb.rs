use std::ops::Index;

use bytes::Buf;
use zerocopy::{
    network_endian::{I32, U32},
    IntoBytes, Unalign,
};
use zerocopy_derive::*;

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
    Debug, Clone, Copy, PartialEq, Eq, Immutable, IntoBytes, TryFromBytes, KnownLayout, Unaligned,
)]
#[repr(u8)]
pub enum Dir {
    Out = 0,
    In = 1,
}

#[derive(Debug, Clone, Copy, Immutable, TryFromBytes, KnownLayout, IntoBytes, Unaligned)]
#[repr(C)]
pub struct BasicHeader {
    pub command: Command,
    pub seqnum: U32,
    pub dev_id: U32,
    pub direction: Dir,
    pub endpoint: I32,
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

#[derive(Immutable, IntoBytes, TryFromBytes, Unaligned, KnownLayout)]
#[repr(C)]
pub struct IsoUrbWithoutPackets {
    dev_id: U32,
    direction: Dir,
    endpoint: I32,
    is_asap: bool,
    start_frame: I32,
    interval: I32,
    error_count: I32,
    num_packets: i8,
}

pub struct IsoUrbSend<'a, 'b> {
    cursor: usize,
    iso_urb: &'a IsoUrbWithoutPackets,
    packets: &'b [IsoPacketDescriptor],
}

impl<'a, 'b> IsoUrbSend<'a, 'b> {
    pub const fn new(
        iso_urb: &'a IsoUrbWithoutPackets,
        packets: &'b [IsoPacketDescriptor],
    ) -> Option<Self> {
        if iso_urb.num_packets as usize != packets.len() {
            None
        } else {
            Some(Self {
                cursor: 0,
                iso_urb,
                packets,
            })
        }
    }
}

impl Buf for IsoUrbSend<'_, '_> {
    fn remaining(&self) -> usize {
        let index = extract_all_but_top_bit(self.cursor);

        let total_size = self.iso_urb.as_bytes().len() + self.packets.as_bytes().len();
        total_size - index
    }

    fn chunk(&self) -> &[u8] {
        // Allows our two fields to be indexable
        let fields = [self.iso_urb.as_bytes(), self.packets.as_bytes()];

        // The top bit of the cursor tells us the field index - either 0 or 1
        let field = extract_top_bit(self.cursor);

        // We then calculate the current index of the current field's byte vector like so:
        // 1. Extract the lower 63 bits of the cursor - this will equal the current index
        //    of the entire buffer.
        // 2. Multiply the length of iso_urb's byte vector with our current field index (0 or 1).
        // 3. Subtract that length from the current index
        let index = extract_all_but_top_bit(self.cursor) - (self.iso_urb.as_bytes().len() * field);
        &fields[field][index..]
    }

    fn advance(&mut self, cnt: usize) {
        let new_index = extract_all_but_top_bit(self.cursor) + cnt;
        let (_, same_field) = new_index.overflowing_sub(self.iso_urb.as_bytes().len());
        self.cursor = ((!same_field as usize) << 63) | new_index;
    }
}

#[derive(Immutable, TryFromBytes, Unaligned, KnownLayout, IntoBytes)]
#[repr(C, packed)]
pub struct IsoUrbRecv {
    urb: IsoUrbWithoutPackets,
    packets: [IsoPacketDescriptor],
}

const fn extract_top_bit(num: usize) -> usize {
    num >> 63
}

const fn extract_all_but_top_bit(num: usize) -> usize {
    num & !(1 << 63)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use zerocopy::{IntoBytes, TryFromBytes};

    use super::*;

    #[test]
    fn iso_convert_send_to_recv() {
        let iso = IsoUrbWithoutPackets {
            dev_id: 0.into(),
            direction: Dir::Out,
            endpoint: 0.into(),
            num_packets: 1,
            is_asap: false,
            start_frame: 0.into(),
            interval: 10.into(),
            error_count: 0.into(),
        };

        let packets = [IsoPacketDescriptor::default()];

        let mut buf = IsoUrbSend::new(&iso, &packets).unwrap();
        let mut vec: Vec<u8> = vec![];

        while buf.has_remaining() {
            let written = vec.write(buf.chunk()).unwrap();
            buf.advance(written);
        }

        let _r = IsoUrbRecv::try_ref_from_bytes(&vec).unwrap();
        assert_eq!(_r.packets[0].status.get(), IsoStatus::Success);
    }

    #[test]
    fn iso_packet_buffer() {
        let packets: &[IsoPacketDescriptor] = &[
            IsoPacketDescriptor::default(),
            IsoPacketDescriptor::default(),
        ];

        let _bytes = packets.as_bytes();

        // Create a type that can be fed to a Bytes container
        // This will let us use references to slices, and
        // maybes let us work with async stuff a bit better
    }
}
