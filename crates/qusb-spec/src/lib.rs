use std::{borrow::Cow, marker::PhantomData, ops::Deref};

use bitflags::bitflags;
use serde::{
    de::{DeserializeOwned, Visitor},
    ser::SerializeStruct,
    Deserialize, Serialize,
};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    pub struct Command: u16 {
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

const VERSION: u16 = 0x0111;

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

pub trait Request {
    const COMMAND: Command;
}

pub struct QusbReq<T: Request> {
    req: T,
}

impl<T> Serialize for QusbReq<T>
where
    T: Request + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("QusbReq", 4)?;
        state.serialize_field("version", &VERSION)?;
        state.serialize_field("command", &T::COMMAND)?;
        state.serialize_field("status", &ResponseStatus::Success)?;
        state.serialize_field("req", &self.req)?;
        state.end()
    }
}

impl<'de, T> Deserialize<'de> for QusbReq<T>
where
    T: Request + DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Version,
            Command,
            Status,
            Req,
        }

        struct QusbReqVisitor<T>(PhantomData<T>);
        impl<'de, T> Visitor<'de> for QusbReqVisitor<T>
        where
            T: Request + DeserializeOwned,
        {
            type Value = QusbReq<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a valid QusbReq containing a VERSION, COMMAND, 0 STATUS, and REQUEST",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let version: u16 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let _command: Command = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let _status: ResponseStatus = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let req: T = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;

                // Check version
                if version != VERSION {
                    Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(version.into()),
                        &self,
                    ))
                } else {
                    Ok(QusbReq { req })
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut version = None;
                let mut command = None;
                let mut status = None;
                let mut req = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(map.next_value()?);
                        }
                        Field::Command => {
                            if command.is_some() {
                                return Err(serde::de::Error::duplicate_field("command"));
                            }
                            command = Some(map.next_value()?);
                        }
                        Field::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        Field::Req => {
                            if req.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            req = Some(map.next_value()?);
                        }
                    }
                }

                let version: u16 =
                    version.ok_or_else(|| serde::de::Error::missing_field("version"))?;
                let _command: Command =
                    command.ok_or_else(|| serde::de::Error::missing_field("command"))?;
                let _status: ResponseStatus =
                    status.ok_or_else(|| serde::de::Error::missing_field("status"))?;
                let req: T = req.ok_or_else(|| serde::de::Error::missing_field("req"))?;

                // Check version
                if version != VERSION {
                    Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(version.into()),
                        &self,
                    ))
                } else {
                    Ok(QusbReq { req })
                }
            }
        }

        const FIELDS: &[&str] = &["version", "command", "status", "req"];
        deserializer.deserialize_struct("QusbReq", FIELDS, QusbReqVisitor(PhantomData))
    }
}

struct Import<'a> {
    bus_id: Cow<'a, str>,
}

impl Request for Import<'_> {
    const COMMAND: Command = Command::OP_REQ_IMPORT;
}

#[cfg(test)]
mod tests {}
