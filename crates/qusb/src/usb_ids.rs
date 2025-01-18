use core::fmt;
use std::{
    collections::HashMap,
    fs,
    hash::Hash,
    io,
    num::ParseIntError,
    path::Path,
    str::FromStr,
};

use nohash_hasher::IsEnabled;

use crate::utils::SimpleMap;

#[derive(Debug, Clone)]
struct Inner {
    vendor: SimpleMap<VendorKey, Box<str>>,
    product: SimpleMap<ProductKey, Box<str>>,
    class: SimpleMap<ClassKey, Box<str>>,
    subclass: SimpleMap<SubclassKey, Box<str>>,
    protocol: SimpleMap<ProtocolKey, Box<str>>,
}

#[derive(Debug, Clone)]
pub struct UsbIds {
    inner: Inner,
}

impl UsbIds {
    fn parse(buf: &str) -> UsbIds {
        let mut names = Inner::with_capacity(64);
        let mut last_state = LastState::Start;

        for (line, _num) in buf.lines().zip(1usize..) {
            if can_skip(line) {
                continue;
            }

            if line.contains("L ") {
                last_state = LastState::Lang;
                continue;
            }

            if let Some((key, text)) = parse_class(line) {
                if names.class.insert(key, text).is_some() {
                    // Print message about duplicate vendor spec?
                }
                last_state = LastState::Class(key);
                continue;
            }

            if let Some((key, text)) = parse_vendor(line) {
                if names.vendor.insert(key, text).is_some() {
                    // Etc...
                }
                last_state = LastState::Vendor(key);
                continue;
            }

            if line.contains("HUT ") {
                last_state = LastState::Hut;
                continue;
            }

            match last_state {
                LastState::Start | LastState::Lang | LastState::Hut => {}
                LastState::Class(ClassKey(class)) => {
                    if let Some((key, text)) = parse_subclass(line, class) {
                        if names.subclass.insert(key, text).is_some() {
                            // Err...
                        }
                        last_state = LastState::Subclass(key);
                    }
                }
                LastState::Subclass(SubclassKey { class, subclass }) => {
                    if let Some((key, text)) = parse_subclass(line, class) {
                        if names.subclass.insert(key, text).is_some() {
                            // Err...
                        }
                        last_state = LastState::Subclass(key);
                    } else if let Some((key, text)) = parse_protocol(line, class, subclass) {
                        if names.protocol.insert(key, text).is_some() {
                            // Err...
                        }
                    }
                }
                LastState::Vendor(VendorKey(vendor))
                | LastState::Product(ProductKey { vendor, product: _ }) => {
                    if let Some((key, text)) = parse_product(line, vendor) {
                        if names.product.insert(key, text).is_some() {
                            // Print message about duplicate vendor spec?
                        }
                        last_state = LastState::Product(key);
                    }
                }
            }
        }

        UsbIds { inner: names }
    }

    pub fn vendor(&self, vendor: u16) -> Option<&str> {
        self.inner.vendor(vendor)
    }

    pub fn product(&self, vendor: u16, product: u16) -> Option<&str> {
        self.inner.product(vendor, product)
    }

    pub fn class(&self, class: u8) -> Option<&str> {
        self.inner.class(class)
    }

    pub fn subclass(&self, class: u8, subclass: u8) -> Option<&str> {
        self.inner.subclass(class, subclass)
    }

    pub fn protocol(&self, class: u8, subclass: u8, protocol: u8) -> Option<&str> {
        self.inner.protocol(class, subclass, protocol)
    }

    pub fn product_display<'a: 'b, 'b>(&'a self, vendor: u16, product: u16) -> Product<'b> {
        Product {
            product_str: self.product(vendor, product),
            product,
            vendor_str: self.vendor(vendor),
            vendor,
        }
    }

    pub fn class_display<'a: 'b, 'b>(&'a self, class: u8, subclass: u8, protocol: u8) -> Class<'b> {
        Class {
            class_str: self.class(class),
            class,
            subclass_str: self.subclass(class, subclass),
            subclass,
            protocol_str: self.protocol(class, subclass, protocol),
            protocol,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Class<'a> {
    class_str: Option<&'a str>,
    class: u8,
    subclass_str: Option<&'a str>,
    subclass: u8,
    protocol_str: Option<&'a str>,
    protocol: u8,
}

impl fmt::Display for Class<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.class == 0 && self.subclass == 0 && self.protocol == 0 {
            write!(f, "(Defined at Interface level)")?;
        } else {
            let class = self.class_str.unwrap_or("unknown class");
            let subclass = self.subclass_str.unwrap_or("unknown subclass");
            let protocol = self.protocol_str.unwrap_or("unknown protocol");

            write!(f, "{class} / {subclass} / {protocol} ")?;
        }

        write!(
            f,
            "({:02x}/{:02x}/{:02x})",
            self.class, self.subclass, self.protocol
        )
    }
}

#[derive(Debug, Clone)]
pub struct Product<'a> {
    product_str: Option<&'a str>,
    product: u16,
    vendor_str: Option<&'a str>,
    vendor: u16,
}

impl fmt::Display for Product<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prod = self.product_str.unwrap_or("unknown product");
        let vend = self.vendor_str.unwrap_or("unknown vendor");

        write!(
            f,
            "{vend} : {prod} ({:04x}:{:04x})",
            self.vendor, self.product
        )
    }
}

impl Inner {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vendor: SimpleMap::with_capacity_and_hasher(capacity, Default::default()),
            product: HashMap::with_capacity_and_hasher(capacity, Default::default()),
            class: HashMap::with_capacity_and_hasher(capacity, Default::default()),
            subclass: SimpleMap::with_capacity_and_hasher(capacity, Default::default()),
            protocol: SimpleMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }

    pub fn vendor(&self, vendor: u16) -> Option<&str> {
        self.vendor.get(&VendorKey(vendor)).map(Box::as_ref)
    }

    pub fn product(&self, vendor: u16, product: u16) -> Option<&str> {
        self.product
            .get(&ProductKey { vendor, product })
            .map(Box::as_ref)
    }

    pub fn class(&self, class: u8) -> Option<&str> {
        self.class.get(&ClassKey(class)).map(Box::as_ref)
    }

    pub fn subclass(&self, class: u8, subclass: u8) -> Option<&str> {
        self.subclass
            .get(&SubclassKey { class, subclass })
            .map(Box::as_ref)
    }

    pub fn protocol(&self, class: u8, subclass: u8, protocol: u8) -> Option<&str> {
        self.protocol
            .get(&ProtocolKey {
                class,
                subclass,
                protocol,
            })
            .map(Box::as_ref)
    }
}

enum LastState {
    Start,
    Lang,
    Class(ClassKey),
    Subclass(SubclassKey),
    Vendor(VendorKey),
    Product(ProductKey),
    Hut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VendorKey(u16);

impl IsEnabled for VendorKey {}
impl Hash for VendorKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u16(self.0)
    }
}

impl FromStr for VendorKey {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(VendorKey(u16::from_str_radix(s, 16)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProductKey {
    vendor: u16,
    product: u16,
}

impl ProductKey {
    fn from_str_and_vendor(s: &str, vendor: u16) -> Result<Self, ParseIntError> {
        Ok(ProductKey {
            vendor,
            product: u16::from_str_radix(s, 16)?,
        })
    }
}

impl IsEnabled for ProductKey {}
impl std::hash::Hash for ProductKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let vendor = self.vendor as u32;
        let product = self.product as u32;
        let key: u32 = (vendor << 16) | product;
        state.write_u32(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClassKey(u8);

impl IsEnabled for ClassKey {}
impl Hash for ClassKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u8(self.0)
    }
}

impl FromStr for ClassKey {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ClassKey(u8::from_str_radix(s, 16)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SubclassKey {
    class: u8,
    subclass: u8,
}
impl SubclassKey {
    fn from_str_and_class(s: &str, class: u8) -> Result<SubclassKey, ParseIntError> {
        Ok(SubclassKey {
            class,
            subclass: u8::from_str_radix(s, 16)?,
        })
    }
}

impl IsEnabled for SubclassKey {}
impl std::hash::Hash for SubclassKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let class = self.class as u16;
        let subclass = self.class as u16;
        let key: u16 = (class << 8) | subclass;
        state.write_u16(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtocolKey {
    class: u8,
    subclass: u8,
    protocol: u8,
}

impl ProtocolKey {
    fn from_str_class_and_subclass(
        s: &str,
        class: u8,
        subclass: u8,
    ) -> Result<ProtocolKey, ParseIntError> {
        Ok(ProtocolKey {
            class,
            subclass,
            protocol: u8::from_str_radix(s, 16)?,
        })
    }
}

impl IsEnabled for ProtocolKey {}
impl std::hash::Hash for ProtocolKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let class = self.class as u32;
        let subclass = self.class as u32;
        let protocol = self.class as u32;
        let key: u32 = (class << 16) | (subclass << 8) | protocol;
        state.write_u32(key)
    }
}

fn can_skip(line: &str) -> bool {
    line.is_empty()
        || line.starts_with('#')
        || line.starts_with("PHYSDES ")
        || line.starts_with("PHY ")
        || line.starts_with("BIAS ")
        || line.starts_with("AT ")
        || line.starts_with("HCC ")
        || line.starts_with("HID ")
        || line.starts_with("R ")
        || line.starts_with("VT")
}

fn parse_value<F, T>(possible: &str, f: F) -> Option<(T, Box<str>)>
where
    F: Fn(&str) -> Result<T, ParseIntError>,
{
    possible
        .split_once(' ')
        .and_then(|(key_token, rest)| f(key_token).ok().map(|key| (key, Box::from(rest.trim()))))
}

fn parse_class(line: &str) -> Option<(ClassKey, Box<str>)> {
    parse_value(line.strip_prefix("C ")?, str::parse::<ClassKey>)
}

fn parse_product(line: &str, vendor: u16) -> Option<(ProductKey, Box<str>)> {
    parse_value(line.strip_prefix('\t')?, |token| {
        ProductKey::from_str_and_vendor(token, vendor)
    })
}

fn parse_subclass(line: &str, class: u8) -> Option<(SubclassKey, Box<str>)> {
    parse_value(line.strip_prefix('\t')?, |token| {
        SubclassKey::from_str_and_class(token, class)
    })
}

fn parse_protocol(line: &str, class: u8, subclass: u8) -> Option<(ProtocolKey, Box<str>)> {
    parse_value(line.strip_prefix("\t\t")?, |token| {
        ProtocolKey::from_str_class_and_subclass(token, class, subclass)
    })
}

fn parse_vendor(line: &str) -> Option<(VendorKey, Box<str>)> {
    parse_value(line, str::parse::<VendorKey>)
}

pub fn parse<P>(path: P) -> io::Result<UsbIds>
where
    P: AsRef<Path>,
{
    let reader = fs::read_to_string(path)?;
    Ok(UsbIds::parse(&reader))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_vendor_works() {
        parse_vendor("0001  Fry's Electronics").unwrap();
        parse_vendor("0002  Ingram").unwrap();
        parse_vendor("0003  Club Mac").unwrap();
    }

    #[test]
    fn parse_vendor_hex() {
        parse_vendor("001f  Walmart").unwrap();
    }

    #[test]
    fn parse_product_works() {
        parse_product("\t7778  Counterfeit flash drive [Kingston]", 1).unwrap();
    }
}
