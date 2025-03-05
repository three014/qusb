use blocking::CtrlReq;
use bytes::BytesMut;
use rusb_async::UsbMemMut;
use vhci::{
    ioctl::{self, IocSetupPacket},
    usbfs::Request,
};

pub(super) mod blocking;

pub enum CtrlKind {
    Blocking(CtrlReq),
    Async(IocSetupPacket),
}

impl CtrlKind {
    pub const fn parse(setup_pkt: IocSetupPacket) -> Self {
        match setup_pkt.req() {
            Request::STANDARD_INTERFACE_SET_INTERFACE => {
                CtrlKind::Blocking(CtrlReq::SetInterface {
                    setting: setup_pkt.value() as u8,
                    interface: setup_pkt.index() as u8,
                })
            }
            Request::STANDARD_DEVICE_SET_CONFIGURATION => CtrlKind::Blocking(CtrlReq::SetConfig {
                desired: setup_pkt.value() as u8,
            }),
            Request::STANDARD_ENDPOINT_CLEAR_FEATURE => CtrlKind::Blocking(CtrlReq::ClearStall {
                endpoint: setup_pkt.index() as u8,
            }),
            _ => CtrlKind::Async(setup_pkt),
        }
    }
}

#[derive(Debug)]
pub enum ResultData {
    In(UsbMemMut),
    Out { bytes_transferred: usize },
}

#[derive(Debug)]
pub struct Iso {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub interval: u16,
    pub raw_iso_buf: BytesMut,
    pub num_errors: u16,
    pub num_iso_packets: u16,
    pub status: vhci::Status,
}

#[derive(Debug)]
pub struct Int {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub interval: u16,
    pub status: vhci::Status,
}

#[derive(Debug)]
pub struct Ctrl {
    pub res: ResultData,
    pub status: vhci::Status,
}

#[derive(Debug)]
pub struct Bulk {
    pub res: ResultData,
    pub endpoint: ioctl::Endpoint,
    pub status: vhci::Status,
}
