use std::{
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    num::ParseIntError,
    str::FromStr,
};

use clap::{Parser, Subcommand};
use qusb::BoundedU8;

const DEFAULT_PORT: u16 = 7002;

#[derive(Debug, Parser)]
#[command(name = "qusb")]
#[command(about = "A USB over QUIC implementation", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

const ANY_DEFAULT_PORT: SocketAddr =
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, DEFAULT_PORT, 0, 0));
const ANY: SocketAddr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0));
const DEFAULT_NUM_VHCI_PORTS: NumVhciPorts = NumVhciPorts(BoundedU8::new(4).unwrap());

#[derive(Debug, Subcommand)]
pub enum Commands {
    Serve {
        #[arg(value_name = "BIND", default_value_t = ANY_DEFAULT_PORT)]
        bind: SocketAddr,
        #[arg(long, default_value_t = false)]
        make_self_signed: bool,
        #[arg(short = 'n', long, value_name = "NUM", default_value_t = DEFAULT_NUM_VHCI_PORTS)]
        /// Number of USB devices that this instance of Qusb can borrow.
        num_vhci_ports: NumVhciPorts,
    },
    Borrow {
        #[arg(short, long, value_name = "BIND", default_value_t = ANY)]
        bind: SocketAddr,
        /// Allow client to connect without verifying
        /// peer certificates.
        #[arg(long, default_value_t = false)]
        allow_insecure: bool,
        #[arg(value_name = "PEER_ADDR")]
        connect: String,
        #[arg(value_name = "BUS:DEV")]
        dev_id: DeviceId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct NumVhciPorts(pub BoundedU8<1, 32>);

impl std::fmt::Display for NumVhciPorts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.get())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumVhciPortsErr<const MIN: u8, const MAX: u8> {
    OutOfBounds,
    ParseIntError(ParseIntError),
}

impl<const MIN: u8, const MAX: u8> std::error::Error for NumVhciPortsErr<MIN, MAX> {}

impl<const MIN: u8, const MAX: u8> std::fmt::Display for NumVhciPortsErr<MIN, MAX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumVhciPortsErr::OutOfBounds => {
                write!(f, "number of ports must be between {} and {}", MIN, MAX)
            }
            NumVhciPortsErr::ParseIntError(int) => int.fmt(f),
        }
    }
}

impl FromStr for NumVhciPorts {
    type Err = NumVhciPortsErr<1, 31>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let num_vhci_ports = s.parse::<u8>().map_err(NumVhciPortsErr::ParseIntError)?;
        match BoundedU8::<1, 32>::new(num_vhci_ports) {
            Some(value) => Ok(NumVhciPorts(value)),
            None => Err(NumVhciPortsErr::OutOfBounds),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceId {
    bus: u8,
    dev: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceIdParseError {
    MissingColon,
    ParseIntError(ParseIntError),
}

impl std::error::Error for DeviceIdParseError {}

impl std::fmt::Display for DeviceIdParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceIdParseError::MissingColon => {
                write!(f, "Missing colon separator between bus and device number")
            }
            DeviceIdParseError::ParseIntError(int) => int.fmt(f),
        }
    }
}

impl FromStr for DeviceId {
    type Err = DeviceIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (bus, dev) = s
            .trim()
            .split_once(':')
            .ok_or(DeviceIdParseError::MissingColon)?;
        let bus = bus
            .parse::<u8>()
            .map_err(DeviceIdParseError::ParseIntError)?;
        let dev = dev
            .parse::<u8>()
            .map_err(DeviceIdParseError::ParseIntError)?;
        Ok(Self { bus, dev })
    }
}
