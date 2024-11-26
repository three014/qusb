use std::borrow::{Borrow, Cow};
use bitflags::bitflags;
use serde::{
    de::{self, Visitor},
    ser::SerializeStruct,
    Deserialize, Serialize,
};

pub const BUS_ID_SIZE: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct BusId<'a>(pub Cow<'a, LimitedStr<BUS_ID_SIZE>>);

#[derive(Debug, Clone)]
pub enum QusbReq<'a> {
    ListDevices,
    ImportDevice(BusId<'a>),
}

impl<'a> Serialize for QusbReq<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ser = serializer.serialize_struct("QusbReq", 2)?;
        ser.serialize_field("version", &VERSION)?;
        ser.serialize_field("req", self)?;
        ser.end()
    }
}

impl<'de> Deserialize<'de> for QusbReq<'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field { Version, Req }

        #[derive(Debug, Clone, Deserialize)]
        pub enum Inner {
            ListDevices,
            ImportDevice(BusId<'static>),
        }

        impl From<Inner> for QusbReq<'static> {
            fn from(value: Inner) -> Self {
                match value {
                    Inner::ListDevices => Self::ListDevices,
                    Inner::ImportDevice(bus_id) => Self::ImportDevice(bus_id),
                }
            }
        }
        
        struct QusbReqVisitor;
        impl<'de> Visitor<'de> for QusbReqVisitor {
            type Value = QusbReq<'static>;

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::SeqAccess<'de>, {
                let version: u16 = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let req: Inner = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;

                if version != VERSION {
                    Err(de::Error::invalid_value(de::Unexpected::Unsigned(version.into()), &self))
                } else {
                    Ok(req.into())
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: de::MapAccess<'de>, {
                let mut version = None;
                let mut req = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Version => {
                            if version.is_some() {
                                return Err(de::Error::duplicate_field("version"));
                            }
                            version = Some(map.next_value()?)
                        }
                        Field::Req => {
                            if req.is_some() {
                                return Err(de::Error::duplicate_field("req"));
                            }
                            req = Some(map.next_value()?)
                        }
                    }
                }

                let version: u16 = version.ok_or_else(|| de::Error::missing_field("version"))?;
                let req: Inner = req.ok_or_else(|| de::Error::missing_field("req"))?;

                if version != VERSION {
                    Err(de::Error::invalid_value(de::Unexpected::Unsigned(version.into()), &self))
                } else {
                    Ok(req.into())
                }
            }

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a valid QusbReq containing a VERSION and REQUEST")
            }
        }

        const FIELDS: &[&str] = &["version", "req"];
        deserializer.deserialize_struct("QusbReq", FIELDS, QusbReqVisitor)
    }
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct LimitedStr<const MAX_LENGTH: usize>(str);

impl<const MAX_LENGTH: usize> Borrow<LimitedStr<MAX_LENGTH>> for LimitedString<MAX_LENGTH> {
    fn borrow(&self) -> &LimitedStr<MAX_LENGTH> {
        unsafe { LimitedStr::<MAX_LENGTH>::from_str_unchecked(self.0.borrow()) }
    }
}

impl<const MAX_LENGTH: usize> LimitedStr<MAX_LENGTH> {
    pub const fn from_str(s: &str) -> Option<&Self> {
        if s.len() <= MAX_LENGTH {
            Some(unsafe { Self::from_str_unchecked(s) })
        } else {
            None
        }
    }

    pub const unsafe fn from_str_unchecked(s: &str) -> &Self {
        union StrRepr<'a, const MAX_LENGTH: usize> {
            normal_str: &'a str,
            limited_str: &'a LimitedStr<MAX_LENGTH>,
        }
        unsafe { StrRepr::<MAX_LENGTH> { normal_str: s }.limited_str }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct LimitedString<const MAX_LENGTH: usize>(String);

impl<const MAX_LENGTH: usize> ToOwned for LimitedStr<MAX_LENGTH> {
    type Owned = LimitedString<MAX_LENGTH>;

    fn to_owned(&self) -> Self::Owned {
        LimitedString(self.0.to_owned())
    }
}

impl<const MAX_LENGTH: usize> LimitedString<MAX_LENGTH> {
    pub fn from_string(s: String) -> Result<Self, String> {
        if s.len() <= MAX_LENGTH {
            Ok(Self(s))
        } else {
            Err(s)
        }
    }
}

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

const VERSION: u16 = 0x0112;

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

#[cfg(test)]
mod tests {}
