use std::ops::Deref;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use lstr;

pub mod urb;

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
I want to send ISO packets using QUIC
datagrams, not with QUIC streams.
ISO packets are best effort and unreliable
so I might as well do the same thing.
*/

#[cfg(test)]
mod tests {}
