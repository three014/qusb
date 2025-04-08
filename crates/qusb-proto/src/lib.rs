use std::io;

use compio_io::AsyncRead;
use data::{Data, Ring};
use msg::{QusbFrame, UrbFrame};
use thiserror::Error;
use unpacked::{Frame, Seq};
use zerocopy::{Immutable, KnownLayout, TryFromBytes};

pub mod data;
pub mod msg;
pub mod unpacked {
    //! Counterparts for the types expressed
    //! in the [`qusb-proto::msg`] module.
    //!
    //! When parsing an object from a stream
    //! of bytes, users of this crate can store
    //! the results into these data types
    //! for that 'Rusty' feeling we all know
    //! and love.
    use crate::{data::Data, msg::UrbFrame};

    /// A `qusb` sequence number.
    /// This number is unique for every
    /// frame of data exchanged between two
    /// peers, making this a good use for
    /// keys in hashmaps and tree structures.
    pub type Seqnum = u32;

    /// Encodes a piece of data
    /// with the corresponding sequence
    /// number for bookkeeping.
    #[derive(Debug)]
    pub struct Seq<T> {
        pub seqnum: Seqnum,
        pub data: T,
    }

    impl<T: Clone> Clone for Seq<T> {
        fn clone(&self) -> Self {
            Self {
                seqnum: self.seqnum,
                data: self.data.clone(),
            }
        }
    }

    impl<T: Copy> Copy for Seq<T> {}

    /// A `qusb` frame for use when exchanging
    /// USB data between a borrower and lender.
    /// Anything a peer needs to say to another
    /// peer can be expressed in this enum.
    pub enum Frame {
        Urb(Seq<Data<UrbFrame>>),
        PortReset(Seqnum),
        Unlink(Seqnum),
    }
    
    /// Identifies the URB (USB Request Block)
    /// type, including the number of packet
    /// descriptors if the type is Isochronous.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum UrbKind {
        Iso(u8),
        Int,
        Ctrl,
        Bulk,
    }
}
mod utils {
    pub const fn align(val: usize, alignment: usize) -> usize {
        (val + (alignment - 1)) & !(alignment - 1)
    }
}

pub const QUSB_VER: msg::Version = msg::Version {
    major: 0,
    minor: 4,
    patch: 0,
};

pub const BUS_ID_SIZE: usize = 32;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum GetSliceLenErr {
    #[error("impl is not confident that the obtained length is valid for the slice")]
    NoConfidence,
    /// Buffer is too short to read the slice length.
    #[error("buffer is too short to read the slice length (missing >{num_bytes_needed} bytes")]
    BufferShort { num_bytes_needed: usize },
}

impl GetSliceLenErr {
    pub fn from_convert_err<A, Dst: ?Sized, V>(
        needed_size: usize,
        err: zerocopy::ConvertError<A, zerocopy::SizeError<&[u8], Dst>, V>,
    ) -> Self {
        use zerocopy::ConvertError::*;
        match err {
            Alignment(_) | Validity(_) => GetSliceLenErr::NoConfidence,
            Size(src) => GetSliceLenErr::BufferShort {
                num_bytes_needed: needed_size - src.into_src().len(),
            },
        }
    }
}

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
    type Header: TryFromBytes + KnownLayout + Immutable + Sized;
    type Data;

    /// If `buf` was some DST `T`, then this
    /// function returns the number of elements
    /// in the slice at the end of the DST instance.
    ///
    /// The implementer should return an error if the
    /// buffer was shorter than std::mem::size_of::<T>()
    /// or if they have reason to believe their value
    /// is incorrect.
    ///
    /// # Assumptions
    ///
    /// - The bytes of `buf` are in little endian
    /// - `buf` may not be exactly the length of
    ///   the DST and its trailing slice.
    fn get_slice_len(buf: &[u8]) -> Result<usize, GetSliceLenErr>;
    fn header(&self) -> Self::Header;
}

/// An error that, if received, means you should
/// stop the session and abort everything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[repr(u8)]
pub enum AbortableError {
    #[error("data does not make sense for received frame")]
    InvalidData = 1 << 4,
    #[error("remote device disconnected")]
    DeviceDisconnected,
    #[error("device rejected transfer due to invalid format")]
    Proto,
    #[error("peer did not understand us (proto error)")]
    Oops,
    #[error("???")]
    Other,
}

impl AbortableError {
    pub const fn as_proto(&self) -> msg::Status {
        match self {
            AbortableError::InvalidData => msg::Status::Proto,
            AbortableError::DeviceDisconnected => msg::Status::NoDev,
            AbortableError::Proto => msg::Status::DevErr,
            AbortableError::Oops => msg::Status::Failed,
            AbortableError::Other => msg::Status::Unexpected,
        }
    }
}

/// An error that should be reported to the VHCI driver
/// so it can decide the next steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[repr(u8)]
pub enum ReportableError {
    #[error("device timed out")]
    TimedOut = 2,
    #[error("transfer cancelled")]
    Cancelled,
    #[error("device could not fulfill request")]
    Stall,
    #[error("device returned more data than expected (congrats now you have UB)")]
    Overflow,
    #[error("not enough bandwidth to support requested mode")]
    NotEnoughBandwidth,
}

/// Describes the errors that can
/// occur during a USB transfer submission.
///
/// Note that abortable errors don't require a peer to
/// notify the other of the error; the peer should instead
/// attempt to close the connection as soon as possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransferError {
    #[error(transparent)]
    Reportable(#[from] ReportableError),
    #[error(transparent)]
    Abortable(#[from] AbortableError),
}

/// Describes the errors that can occur
/// while trying to receive data from a peer.
///
/// For now, the only things that can go wrong are
/// the usual I/O errors, as well as receiving
/// mangled data which should not be further read.
#[derive(Debug, Error)]
pub enum RecvError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("received garbage")]
    CorruptedData,
}

/// Receives data until enough bytes are
/// read to split apart a new frame.
///
/// Returns `Err(None)` if the remote peer
/// closes the connection properly. Otherwise,
/// returns an I/O error or reports on garbage
/// data from the peer.
pub async fn recv_frame<R: AsyncRead + Unpin>(
    mut rx: R,
    buf: &mut Ring,
) -> Result<Data<QusbFrame>, Option<RecvError>> {
    use data::ReadError::*;
    let mut min_len = size_of::<msg::Header>();
    loop {
        buf.fill_until(&mut rx, min_len)
            .await
            .map_err(RecvError::from)?
            .ok_or(None)?;

        min_len = match buf.claim_dst() {
            Ok(frame) => return Ok(frame),
            Err(CorruptedData) => {
                return Err(Some(RecvError::CorruptedData));
            }
            Err(BufferShort { num_bytes_needed }) => buf.len() + num_bytes_needed,
        }
    }
}

pub fn parse_frame(frame: Data<QusbFrame>) -> Result<Frame, AbortableError> {
    use msg::Command::*;
    use msg::Status::*;
    use msg::Header;
    match frame.get().header.command {
        RetSubmit => {
            let (Header { status, seqnum, .. }, data) = frame.split::<UrbFrame>();
            match status {
                Success => Ok(Frame::Urb(Seq {
                    seqnum,
                    data: data.ok_or(AbortableError::InvalidData)?,
                })),
                Failed => unimplemented!(),
                DevBusy => unimplemented!(),
                DevErr => Err(AbortableError::Proto),
                NoDev => Err(AbortableError::DeviceDisconnected),
                Unexpected => unimplemented!(),
                VersionMismatch => unimplemented!(),
                Timeout => unimplemented!(),
                Proto => Err(AbortableError::Oops),
            }
        }
        CmdSubmit => {
            let (Header { seqnum, .. }, data) = frame.split::<UrbFrame>();
            Ok(Frame::Urb(Seq {
                seqnum,
                data: data.ok_or(AbortableError::InvalidData)?,
            }))
        }
        CmdUnlink => {
            let seqnum = frame.get().header.seqnum;
            Ok(Frame::Unlink(seqnum))
        }
        CmdPort => {
            let seqnum = frame.get().header.seqnum;
            Ok(Frame::PortReset(seqnum))
        }
        RetPort => {
            let Header { status, seqnum, .. } = frame.get().header();
            match status {
                Success => Ok(Frame::PortReset(seqnum)),
                Failed => unimplemented!(),
                DevBusy => unimplemented!(),
                DevErr => Err(AbortableError::Proto),
                NoDev => Err(AbortableError::DeviceDisconnected),
                Unexpected => unimplemented!(),
                VersionMismatch => unimplemented!(),
                Timeout => unimplemented!(),
                Proto => Err(AbortableError::Oops),
            }
        }
    }
}
