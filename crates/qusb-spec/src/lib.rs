use nohash_hasher::IsEnabled;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::num::NonZeroU64;
use thiserror::Error;

pub const BUS_ID_SIZE: usize = 32;
pub const VERSION: Version = Version(0x0200);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Version(u16);

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#06x}", self.0)
    }
}

pub trait RequestTrait: Sized + Serialize + DeserializeOwned {
    type Response: Sized + Serialize + DeserializeOwned;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    ListUsbDevices,
    ImportUsbDevice(UsbDeviceId),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ListUsbDevices;

pub type Response<T> = Result<T, Error>;

impl RequestTrait for Request {
    type Response = UsbDeviceInfo;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsbDeviceId {
    pub bus_number: u8,
    pub device_addr: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedDevice {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsbDevices {
    list: Vec<UsbDeviceInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsbDeviceInfo {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ReqId(NonZeroU64);
impl IsEnabled for ReqId {}

impl ReqId {
    fn new() -> Self {
        Self(rand::random())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
#[repr(transparent)]
pub struct PayloadId(pub NonZeroU64);
impl IsEnabled for PayloadId {}

#[derive(Debug, Error, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Header {
    pub version: u16,
    pub stream_type: StreamType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StreamType {
    Ctrl,
    Data(PayloadId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Dir {
    Bi = 0,
    Uni = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub id: ReqId,
    pub payload: Result<Option<(PayloadId, Dir)>, Error>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Req {
    pub id: ReqId,
    pub op: Operation,
}

impl Req {
    pub fn new(op: Operation) -> Self {
        Self {
            id: ReqId::new(),
            op,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    ListDevices,
    ImportDevice(UsbDeviceId),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Control {
    Request(Req),
    Response(Resp),
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
