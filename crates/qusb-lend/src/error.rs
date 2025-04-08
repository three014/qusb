use std::io;

use proto::{
    AbortableError, RecvError,
    msg::{Command, Endpoint, Header, Status, TransferKind, UsbDeviceId, compress_frame_len},
    unpacked::Seq,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SeqError {
    #[error("device failed to reset")]
    Reset,
    #[error("{kind:?} transfer failed on endpoint {endpoint}: {err}")]
    Transfer {
        kind: TransferKind,
        endpoint: Endpoint,
        err: AbortableError,
    },
}

impl SeqError {
    pub const fn as_command(&self) -> Command {
        match self {
            SeqError::Reset => Command::RetPort,
            SeqError::Transfer { .. } => Command::RetSubmit,
        }
    }

    pub const fn as_status(&self) -> Status {
        match self {
            SeqError::Reset => Status::NoDev,
            SeqError::Transfer { err: status, .. } => status.as_proto(),
        }
    }

    // pub const fn status(&self) -> Option<vhci::Status> {
    //     match self {
    //         SeqError::Reset => None,
    //         SeqError::Transfer { status, .. } => Some(*status),
    //     }
    // }

    // pub const fn kind(&self) -> Option<UrbType> {
    //     match self {
    //         SeqError::Reset => None,
    //         SeqError::Transfer { kind, .. } => Some(*kind),
    //     }
    // }
}

#[derive(Debug, Clone, Copy)]
pub struct UsbError {
    pub id: UsbDeviceId,
    pub seq: Seq<SeqError>,
}

impl UsbError {
    pub const fn as_header(&self) -> Header {
        Header {
            total_frame_len: compress_frame_len(size_of::<Header>()),
            command: self.seq.data.as_command(),
            status: self.seq.data.as_status(),
            seqnum: self.seq.seqnum,
        }
    }
}

impl std::fmt::Display for UsbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dev {} seq {}: {}",
            self.id, self.seq.seqnum, self.seq.data
        )
    }
}

impl std::error::Error for UsbError {}

#[derive(Debug, Error)]
pub enum Error {
    #[error("error on {0}")]
    Usb(#[from] UsbError),
    #[error("error while receiving data from peer: {0}")]
    Recv(#[from] RecvError),
    #[error("error while sending data to peer: {0}")]
    Send(#[from] io::Error),
}
