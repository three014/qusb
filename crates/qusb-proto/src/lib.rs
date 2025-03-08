use thiserror::Error;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub mod data;
pub mod msg;

pub const QUSB_VER: msg::Version = msg::Version {
    major: 0,
    minor: 3,
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
            Alignment(_) | Validity(_) => {
                GetSliceLenErr::NoConfidence
            }
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
    Self: KnownLayout<PointerMetadata = usize> + IntoBytes,
{
    type Header: TryFromBytes + KnownLayout + Immutable + Sized + IntoBytes;

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
    /// - The bytes of `buf` are in little endian
    /// - `buf` may not be exactly the length of
    ///   the DST and its trailing slice.
    fn get_slice_len(buf: &[u8]) -> Result<usize, GetSliceLenErr>;
}

#[cfg(test)]
mod tests {}
