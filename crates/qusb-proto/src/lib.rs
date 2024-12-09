use std::ops::Deref;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod utils {
    pub use lstr;
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
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    struct CommandOld: u16 {
        const OP_REQUEST = 0x80 << 8;
        const OP_REPLY = 0x00 << 8;

        const OP_IMPORT = 0x03;
        const OP_REQ_IMPORT = Self::OP_REQUEST.bits() | Self::OP_IMPORT.bits();
        const OP_REP_IMPORT = Self::OP_REPLY.bits() | Self::OP_IMPORT.bits();

        const OP_DEVLIST = 0x05;
        const OP_REQ_DEVLIST = Self::OP_REQUEST.bits() | Self::OP_DEVLIST.bits();
        const OP_REP_DEVLIST = Self::OP_REPLY.bits() | Self:: OP_DEVLIST.bits();

        const OP_EXPORT = 0x06;
        const OP_REQ_EXPORT = Self::OP_REQUEST.bits() | Self::OP_EXPORT.bits();
        const OP_REP_EXPORT = Self::OP_REPLY.bits() | Self::OP_EXPORT.bits();
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u16)]
pub enum ResponseStatus {
    Success = 0x00,
    Failed = 0x01,
    DevBusy = 0x02,
    DevErr = 0x03,
    NoDev = 0x04,
    Unexpected = 0x05,
}

impl core::fmt::Display for ResponseStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ResponseStatus::Success => write!(f, "Request succeeded"),
            ResponseStatus::Failed => write!(f, "Request failed"),
            ResponseStatus::DevBusy => write!(f, "Device busy (exported)"),
            ResponseStatus::DevErr => write!(f, "Device in error state"),
            ResponseStatus::NoDev => write!(f, "Device not found"),
            ResponseStatus::Unexpected => write!(f, "Unexpected response"),
        }
    }
}
*/

#[cfg(test)]
mod tests {}
