use std::sync::Mutex;

use proto::msg::UsbDeviceId;
use rusb::UsbContext;
use tracing::{debug, error, trace, trace_span, warn};

const NUM_INTERFACES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceStatus {
    /// Qusb has exclusive access to
    /// this interface.
    Claimed,
    /// Qusb has not claimed this interface.
    /// Requests to this interface will fail.
    Unclaimed,
    /// A kernel driver has access to this
    /// interface and must be detached to
    /// claim the interface.
    _DriverClaimed,
    /// This interface does not exist for this
    /// device.
    NotFound,
}

pub struct Handle {
    device: rusb::DeviceHandle<rusb::Context>,
    interfaces: Mutex<[InterfaceStatus; NUM_INTERFACES]>,
}

impl Handle {
    #[inline]
    pub fn as_device(&self) -> &rusb::DeviceHandle<rusb::Context> {
        &self.device
    }

    #[inline]
    pub fn active_config(&self) -> rusb::Result<u8> {
        self.device.active_configuration()
    }

    /// # Safety
    ///
    /// Setting the device to the same config as its current config
    /// will cause a lightweight reset, which might not be desired
    /// nor expected.
    fn set_config_inner(&self, config: u8) -> rusb::Result<()> {
        #[inline]
        fn is_claimed((interface, status): (usize, &mut InterfaceStatus)) -> Option<u8> {
            match status {
                InterfaceStatus::Claimed => {
                    *status = InterfaceStatus::Unclaimed;
                    Some(interface as u8)
                }
                _ => None,
            }
        }

        #[inline]
        fn is_unclaimed((interface, status): (usize, &mut InterfaceStatus)) -> Option<u8> {
            match status {
                InterfaceStatus::Unclaimed => {
                    *status = InterfaceStatus::Claimed;
                    Some(interface as u8)
                }
                _ => None,
            }
        }

        self.interfaces
            .lock()
            .unwrap()
            .iter_mut()
            .enumerate()
            .filter_map(is_claimed)
            .try_for_each(|interface| self.device.release_interface(interface as u8))?;
        self.device.set_active_configuration(config)?;
        self.interfaces
            .lock()
            .unwrap()
            .iter_mut()
            .enumerate()
            .filter_map(is_unclaimed)
            .try_for_each(|interface| self.device.claim_interface(interface as u8))
    }

    #[inline]
    pub fn set_config(&self, config: u8) -> rusb::Result<()> {
        if config == self.active_config()? {
            return Ok(());
        }

        self.set_config_inner(config)
    }

    fn claim_interface_inner(&self, interface: u8) -> rusb::Result<()> {
        self.device.claim_interface(interface)?;
        self.interfaces.lock().unwrap()[interface as usize] = InterfaceStatus::Claimed;
        Ok(())
    }

    pub fn claim_interface(&self, interface: u8) -> rusb::Result<()> {
        if self
            .interfaces
            .lock()
            .unwrap()
            .get(interface as usize)
            .is_some_and(|status| InterfaceStatus::Claimed == *status)
        {
            return Ok(());
        }

        self.claim_interface_inner(interface)
    }

    #[inline]
    pub fn set_alt_setting(&self, interface: u8, setting: u8) -> rusb::Result<()> {
        self.device.set_alternate_setting(interface, setting)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        _ = self.device.set_auto_detach_kernel_driver(true);
    }
}

pub fn open(dev_id: UsbDeviceId) -> rusb::Result<Handle> {
    let mut ctx = rusb::Context::new()?;

    let span = trace_span!("libusb");
    ctx.set_log_level(rusb::LogLevel::None);
    ctx.set_log_callback(
        Box::new(move |level, msg| {
            let _enter = span.enter();
            match level {
                rusb::LogLevel::None => (),
                rusb::LogLevel::Error => error!("{}", msg.trim_end()),
                rusb::LogLevel::Warning => warn!("{}", msg.trim_end()),
                rusb::LogLevel::Info => debug!("{}", msg.trim_end()),
                rusb::LogLevel::Debug => trace!("{}", msg.trim_end()),
            }
        }),
        rusb::LogCallbackMode::Context,
    );

    let dev = ctx
        .devices()?
        .iter()
        .find(|dev| dev_id.bus_number == dev.bus_number() && dev_id.device_addr == dev.address())
        .ok_or(rusb::Error::NoDevice)?;

    let handle = dev.open()?;
    handle.set_auto_detach_kernel_driver(false)?;
    handle.reset()?;

    let mut interfaces = [InterfaceStatus::NotFound; NUM_INTERFACES];
    for (interface, status) in interfaces.iter_mut().enumerate() {
        *status = match handle.kernel_driver_active(interface as u8) {
            Ok(true) => {
                handle.detach_kernel_driver(interface as u8)?;
                handle.claim_interface(interface as u8)?;
                InterfaceStatus::Claimed
            }
            Ok(false) => match handle.claim_interface(interface as u8) {
                Ok(_) => InterfaceStatus::Claimed,
                Err(rusb::Error::NotFound) => InterfaceStatus::NotFound,
                Err(err) => todo!("handle this err: {err}"),
            },
            Err(_) => InterfaceStatus::NotFound,
        }
    }

    let handle = Handle {
        device: handle,
        interfaces: Mutex::new(interfaces),
    };

    const BASE_CONFIG: u8 = 1;
    handle.set_config(BASE_CONFIG)?;

    Ok(handle)
}
