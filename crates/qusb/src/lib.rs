use compio::quic::{Connection, ConnectionError, RecvStream, SendStream};
use futures_concurrency::{future::Race, stream::Merge};
use futures_lite::{stream, StreamExt};
use futures_util::SinkExt;
use operator::{BorrowDevice, LendDevice, SendDevices, ServerResp};
use proto::{
    data::{IterDst, IterMutDst, ReadError, Ring},
    msg::{self, BUS_ID_MAX_LEN, PATH_MAX_LEN},
};
use std::{
    io,
    net::{Ipv6Addr, SocketAddr},
    num::NonZeroU64,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
};
use std::{net::IpAddr, os::unix::ffi::OsStrExt, pin::pin};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn, Instrument};
use usb_ids::UsbIds;
use utils::mpsc;
use zerocopy::FromZeros;

pub use vhci::utils::BoundedU8;

mod operator;
mod stub;
mod usb_ids;
mod utils;

static USB_IDS: LazyLock<UsbIds> =
    LazyLock::new(|| usb_ids::parse(Path::new("./usb-ids")).unwrap());

pub type Result<T> = std::result::Result<T, Error>;

pub fn usb_ids() -> &'static UsbIds {
    USB_IDS.deref()
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("unsupported qusb protocol version (their version: {0}, our version: {ver})", ver = proto::QUSB_VER)]
    VersionMismatch(msg::Version),
    #[error("device with id {0:?} not found")]
    DevNotFound(msg::UsbDeviceId),
    #[error("request failed on the server side")]
    ReqFailed,
    #[error("unknown data from peer")]
    Unknown,
}

// impl From<quinn::StoppedError> for Error {
//     fn from(value: quinn::StoppedError) -> Self {
//         Error::Io(value.into())
//     }
// }

// impl From<quinn::ConnectionError> for Error {
//     fn from(value: quinn::ConnectionError) -> Self {
//         Error::Io(value.into())
//     }
// }

// impl From<quinn::WriteError> for Error {
//     fn from(value: quinn::WriteError) -> Self {
//         Error::Io(value.into())
//     }
// }

// impl From<quinn::ReadError> for Error {
//     fn from(value: quinn::ReadError) -> Self {
//         Error::Io(value.into())
//     }
// }

// impl From<quinn::ClosedStream> for Error {
//     fn from(value: quinn::ClosedStream) -> Self {
//         Error::Io(value.into())
//     }
// }

impl From<RusbError> for Error {
    fn from(value: RusbError) -> Self {
        match value.kind {
            rusb::Error::Io => todo!(),
            rusb::Error::InvalidParam => Error::Io(io::ErrorKind::InvalidInput.into()),
            rusb::Error::Access => Error::Io(io::ErrorKind::PermissionDenied.into()),
            rusb::Error::NoDevice | rusb::Error::NotFound => Error::DevNotFound(value.dev_id),
            rusb::Error::Busy => Error::Io(io::ErrorKind::ResourceBusy.into()),
            rusb::Error::Timeout => Error::Io(io::ErrorKind::TimedOut.into()),
            rusb::Error::Overflow => todo!(),
            rusb::Error::Pipe => Error::Io(io::ErrorKind::BrokenPipe.into()),
            rusb::Error::Interrupted => Error::Io(io::ErrorKind::Interrupted.into()),
            rusb::Error::NoMem => Error::Io(io::ErrorKind::OutOfMemory.into()),
            rusb::Error::NotSupported => Error::Io(io::ErrorKind::Unsupported.into()),
            rusb::Error::BadDescriptor => todo!(),
            rusb::Error::Other => Error::Io(io::ErrorKind::Other.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RusbError {
    kind: rusb::Error,
    dev_id: msg::UsbDeviceId,
}

impl From<RusbError> for msg::Status {
    fn from(value: RusbError) -> Self {
        match value.kind {
            rusb::Error::Io => msg::Status::Failed,
            rusb::Error::InvalidParam => msg::Status::Unexpected,
            rusb::Error::Access => msg::Status::Failed,
            rusb::Error::NoDevice | rusb::Error::NotFound => msg::Status::NoDev,
            rusb::Error::Busy => msg::Status::DevBusy,
            rusb::Error::Timeout => msg::Status::Timeout,
            rusb::Error::BadDescriptor => msg::Status::DevErr,
            _ => msg::Status::Unexpected,
        }
    }
}

pub(crate) struct UrbWithIsoData<'a> {
    pub handle: vhci::ioctl::UrbHandle,
    pub header: &'a msg::UrbHeader,
    pub transfer: &'a mut [u8],
    pub iso_data: &'a mut [vhci::ioctl::IocIsoPacketData],
}

impl vhci::Urb for UrbWithIsoData<'_> {
    fn kind(&self) -> vhci::ioctl::UrbType {
        self.header.kind
    }

    fn handle(&self) -> vhci::ioctl::UrbHandle {
        self.handle
    }

    fn status(&self) -> vhci::Status {
        self.header.status
    }

    fn dir(&self) -> vhci::usbfs::Dir {
        self.header.endpoint.direction()
    }

    fn bytes_transferred(&self) -> u16 {
        self.header.actual_transfer_len
    }
}

impl vhci::TransferMut for UrbWithIsoData<'_> {
    fn transfer_mut(&mut self) -> &mut [u8] {
        self.transfer
    }
}

impl vhci::IsoPacketDataMut for UrbWithIsoData<'_> {
    fn iso_packet_data_mut(&mut self) -> &mut [vhci::ioctl::IocIsoPacketData] {
        self.iso_data
    }
}

#[derive(Debug)]
pub(crate) struct _UrbWithIsoGiveback<'a> {
    pub handle: vhci::ioctl::UrbHandle,
    pub header: &'a msg::UrbHeader,
    pub transfer: &'a mut [u8],
    pub iso_giveback: &'a mut [vhci::ioctl::IocIsoPacketGiveback],
}

impl vhci::Urb for _UrbWithIsoGiveback<'_> {
    fn kind(&self) -> vhci::ioctl::UrbType {
        self.header.kind
    }

    fn handle(&self) -> vhci::ioctl::UrbHandle {
        self.handle
    }

    fn status(&self) -> vhci::Status {
        self.header.status
    }

    fn dir(&self) -> vhci::usbfs::Dir {
        self.header.endpoint.direction()
    }

    fn bytes_transferred(&self) -> u16 {
        self.header.actual_transfer_len
    }
}

impl vhci::TransferMut for _UrbWithIsoGiveback<'_> {
    fn transfer_mut(&mut self) -> &mut [u8] {
        self.transfer
    }
}

impl vhci::IsoPacketGivebackMut for _UrbWithIsoGiveback<'_> {
    fn iso_packet_giveback_mut(&mut self) -> &mut [vhci::ioctl::IocIsoPacketGiveback] {
        self.iso_giveback
    }

    fn error_count(&self) -> u16 {
        self.header.num_errors
    }
}

#[derive(Debug)]
pub struct UsbDevices(Ring);

impl UsbDevices {
    pub fn iter(&self) -> IterDst<'_, msg::UsbDeviceInfo> {
        self.0.iter_dst()
    }

    pub fn iter_mut(&mut self) -> IterMutDst<'_, msg::UsbDeviceInfo> {
        self.0.iter_mut_dst()
    }
}

#[derive(Debug)]
pub struct Session {
    conn: compio::quic::Connection,
    dev: stub::Controller,
}

impl Session {
    fn new(conn: compio::quic::Connection, dev: stub::Controller) -> Self {
        Self { conn, dev }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.conn.remote_address()
    }

    #[tracing::instrument(skip_all, level = "trace")]
    #[must_use]
    pub async fn accept_stream(&self) -> Result<ServerResp<SendStream, RecvStream>> {
        let (mut tx, mut rx) = self.conn.accept_bi().await.unwrap();
        let mut buf = Ring::with_capacity(32);

        buf.fill_until(&mut rx, size_of::<msg::ReqFrame>()).await?;

        let msg::ReqFrame { version, req } = match buf.read() {
            Ok(frame) => frame,
            Err(ReadError::CorruptedData) => {
                let response = msg::Resp::Failure {
                    stat: msg::Status::Proto,
                    ver: msg::VersionOpt::Some(proto::QUSB_VER),
                };
                msg::send_resp(&mut tx, response).await?;
                return Err(Error::Unknown);
            }
            Err(ReadError::BufferShort { .. }) => {
                // TODO: This can be possible if client just didn't
                // send enough data, then closed the connection.
                unreachable!("we should have read enough data to get the version")
            }
        };
        if !version.is_compat(&proto::QUSB_VER) {
            let response = msg::Resp::Failure {
                stat: msg::Status::VersionMismatch,
                ver: msg::VersionOpt::Some(proto::QUSB_VER),
            };
            msg::send_resp(&mut tx, response).await?;
            return Err(Error::VersionMismatch(version));
        }

        let req = match req {
            msg::Req::ListDevices { .. } => ServerResp::ListDevices(SendDevices::new(tx)),
            msg::Req::BorrowDevice { dev_id, .. } => {
                // TODO: Use global context instead
                let device = match operator::open_device(dev_id) {
                    Ok(handle) => {
                        let data_rate = match handle.device().speed() {
                            rusb::Speed::Unknown | rusb::Speed::Low => msg::DataRate::Low,
                            rusb::Speed::Full => msg::DataRate::Full,
                            rusb::Speed::High | rusb::Speed::Super | rusb::Speed::SuperPlus => {
                                msg::DataRate::High
                            }
                            _ => unimplemented!("there's a new usb speed?"),
                        };

                        let response = msg::Resp::BorrowDevice {
                            data_rate,
                            _padding: Default::default(),
                        };
                        msg::send_resp(&mut tx, response).await?;

                        handle
                    }
                    Err(err) => {
                        let err = RusbError { kind: err, dev_id };
                        let ret_err = Error::from(err);
                        let status = msg::Status::from(err);
                        let response = msg::Resp::Failure {
                            stat: status,
                            ver: msg::VersionOpt::None(Default::default()),
                        };

                        msg::send_resp(&mut tx, response).await?;
                        return Err(ret_err);
                    }
                };
                ServerResp::BorrowDevice(LendDevice::new(tx, rx, buf, device, dev_id))
            }
            msg::Req::LendDevice { dev_id, data_rate } => {
                let response = msg::Resp::LendDevice {
                    _padding: Default::default(),
                };
                msg::send_resp(&mut tx, response).await?;

                ServerResp::LendDevice(BorrowDevice::new(
                    tx,
                    rx,
                    buf,
                    self.dev.clone(),
                    data_rate,
                    dev_id,
                ))
            }
        };

        Ok(req)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn req_list_devices(&self) -> Result<UsbDevices> {
        let (tx, mut rx) = self
            .conn
            .open_bi()
            .inspect_err(|err| error!("{err}"))
            .unwrap();
        trace!("established stream with stream id {:?}", tx.id());
        let mut buf = Ring::with_capacity(1024);

        let req = msg::Req::ListDevices {
            _padding: Default::default(),
        };
        msg::send_req(tx, req).await?;

        // Arbitrary number that's probably more than what's needed for the
        // number of usb devices the other machine has connected.
        buf.fill_until(&mut rx, 8192).await?;
        trace!("finished reading from remote stream");
        let resp = match buf.read() {
            Ok(resp) => resp,
            Err(ReadError::CorruptedData) => {
                unimplemented!()
            }
            Err(ReadError::BufferShort { .. }) => return Err(Error::Unknown),
        };

        match resp {
            msg::Resp::ListDevices { .. } => { /* success */ }
            msg::Resp::Failure {
                stat: msg::Status::VersionMismatch,
                ver: msg::VersionOpt::Some(theirs),
            } => {
                return Err(Error::VersionMismatch(theirs));
            }
            resp => unimplemented!("why did the peer send this? {resp:?}"),
        }

        Ok(UsbDevices(buf))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    #[must_use]
    pub async fn req_borrow(
        &self,
        id: msg::UsbDeviceId,
    ) -> Result<BorrowDevice<SendStream, RecvStream>> {
        let (mut tx, mut rx) = self
            .conn
            .open_bi()
            .inspect_err(|err| {
                warn! {
                    %err,
                    "err while opening new request with peer"
                }
            })
            .unwrap();
        let mut buf = Ring::with_capacity(1024);

        trace!("established stream with stream id {:?}", tx.id());

        let req = msg::Req::BorrowDevice {
            dev_id: id,
            _padding: Default::default(),
        };
        msg::send_req(&mut tx, req).await?;

        trace!(
            "sent request to borrow device {id:?} from peer @ {:?}",
            self.conn.remote_address()
        );

        buf.fill_until(&mut rx, size_of::<msg::Resp>()).await?;

        trace!("received enough bytes for a status from peer");

        let resp = match buf.read() {
            Ok(resp) => resp,
            Err(ReadError::CorruptedData) => {
                unimplemented!()
            }
            Err(ReadError::BufferShort { .. }) => {
                unimplemented!()
            }
        };
        let data_rate = match resp {
            msg::Resp::BorrowDevice { data_rate, .. } => data_rate,
            msg::Resp::Failure {
                stat: msg::Status::VersionMismatch | msg::Status::Proto,
                ver: msg::VersionOpt::Some(theirs),
            } => return Err(Error::VersionMismatch(theirs)),
            msg::Resp::Failure {
                stat: msg::Status::DevBusy,
                ..
            } => return Err(Error::Io(io::ErrorKind::ResourceBusy.into())),
            msg::Resp::Failure {
                stat: msg::Status::NoDev,
                ..
            } => return Err(Error::DevNotFound(id)),
            resp => unimplemented!("why did the peer send this? {resp:?}"),
        };

        let client = BorrowDevice::new(tx, rx, buf, self.dev.clone(), data_rate, id);
        Ok(client)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    #[must_use]
    pub async fn req_lend(
        &self,
        id: msg::UsbDeviceId,
    ) -> Result<LendDevice<SendStream, RecvStream>> {
        let (mut tx, mut rx) = self
            .conn
            .open_bi()
            .inspect_err(|err| {
                warn! {
                    %err,
                    "err while opening new request with peer"
                }
            })
            .unwrap();
        let mut buf = Ring::with_capacity(1024);
        trace!("established stream with stream id {:?}", tx.id());

        // TODO: Use global context instead
        let device = match operator::open_device(id) {
            Ok(handle) => {
                let data_rate = match handle.device().speed() {
                    rusb::Speed::Unknown | rusb::Speed::Low => msg::DataRate::Low,
                    rusb::Speed::Full => msg::DataRate::Full,
                    rusb::Speed::High | rusb::Speed::Super | rusb::Speed::SuperPlus => {
                        msg::DataRate::High
                    }
                    _ => unimplemented!(),
                };

                let req = msg::Req::LendDevice {
                    dev_id: id,
                    data_rate,
                };
                msg::send_req(&mut tx, req).await?;

                handle
            }
            Err(err) => {
                let err = RusbError {
                    kind: err,
                    dev_id: id,
                };
                return Err(Error::from(err));
            }
        };

        trace!(
            "sent request to lend device {id:?} to peer @ {:?}",
            self.conn.remote_address()
        );

        buf.fill_until(&mut rx, size_of::<msg::Resp>()).await?;

        trace!("received enough bytes for a status from peer");

        let resp = match buf.read() {
            Ok(resp) => resp,
            Err(ReadError::CorruptedData) => {
                unimplemented!()
            }
            Err(ReadError::BufferShort { .. }) => {
                unimplemented!()
            }
        };

        match resp {
            msg::Resp::LendDevice { .. } => (),
            msg::Resp::Failure {
                stat: msg::Status::VersionMismatch | msg::Status::Proto,
                ver: msg::VersionOpt::Some(theirs),
            } => return Err(Error::VersionMismatch(theirs)),
            msg::Resp::Failure {
                stat: msg::Status::DevBusy,
                ..
            } => return Err(Error::Io(io::ErrorKind::ResourceBusy.into())),
            msg::Resp::Failure {
                stat: msg::Status::NoDev,
                ..
            } => return Err(Error::DevNotFound(id)),
            resp => unimplemented!("why did the peer say this? {resp:?}"),
        }

        let client = LendDevice::new(tx, rx, buf, device, id);
        Ok(client)
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: compio::quic::Endpoint,
    dev: stub::Controller,
}

impl Client {
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn connect(&self, peer_addr: SocketAddr, peer_name: &str) -> Result<Session> {
        let conn = self
            .endpoint
            .connect(peer_addr, peer_name, None)
            .unwrap()
            .await
            .unwrap();
        // let conn2 = conn.clone();
        // compio::runtime::spawn(async move {
        //     let mut interval = compio::runtime::time::interval(Duration::from_secs(30));
        //     loop {
        //         tokio::select! {
        //             _ = conn2.closed() => {
        //                 break;
        //             }
        //             _ = interval.tick() => {
        //                 let stats = conn2.stats();
        //                 trace!("{stats:?}");
        //             }
        //         }
        //     }
        // });
        Ok(Session::new(conn, self.dev.clone()))
    }
}

#[derive(Debug)]
pub(crate) struct UsbDeviceWrapper {
    info: msg::UsbDeviceInfoHeader,
    interfaces: Box<[msg::UsbInterfaceInfo]>,
}

impl msg::SendUsbDeviceInfo for UsbDeviceWrapper {
    fn get(&self) -> &msg::UsbDeviceInfoHeader {
        &self.info
    }

    fn interfaces_with_padding(&self) -> &[msg::UsbInterfaceInfo] {
        &self.interfaces
    }

    fn interfaces(&self) -> &[msg::UsbInterfaceInfo] {
        &self.interfaces[..self.info.b_num_interfaces as usize]
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn get_usb_devices() -> io::Result<
    std::iter::FilterMap<
        impl Iterator<Item = nusb::DeviceInfo>,
        impl FnMut(nusb::DeviceInfo) -> Option<UsbDeviceWrapper>,
    >,
> {
    let iterator = nusb::list_devices()?.filter_map(|info| -> Option<UsbDeviceWrapper> {
        let device = info
            .open()
            .inspect_err(|err| trace!("skipping usb device due to {err}"))
            .ok()?;
        let mut interfaces: Vec<msg::UsbInterfaceInfo> = info
            .interfaces()
            .map(|iface| msg::UsbInterfaceInfo {
                b_interface_number: iface.interface_number(),
                b_interface_class: iface.class(),
                b_interface_subclass: iface.subclass(),
                b_interface_protocol: iface.protocol(),
            })
            .collect();
        let b_num_interfaces = interfaces.len().try_into().unwrap();
        if interfaces.len() % 2 == 1 {
            interfaces.push(msg::UsbInterfaceInfo::new_zeroed());
        }
        let interfaces: Box<[msg::UsbInterfaceInfo]> = interfaces.into_boxed_slice();
        let b_configuration_value = device
            .active_configuration()
            .map(|conf| conf.configuration_value())
            .unwrap_or_default();
        let b_num_configurations = device.configurations().count() as u8;
        let path = info.sysfs_path().as_os_str().as_bytes();
        let path_len = path.len();
        let bus_id = info.sysfs_path().file_name().map(|name| name.as_bytes())?;
        let bus_id_len = bus_id.len();
        let info = msg::UsbDeviceInfoHeader {
            path_len: u16::try_from(path_len)
                .unwrap_or_default()
                .clamp(0, PATH_MAX_LEN),
            path: {
                let mut arr = [0; PATH_MAX_LEN as usize];
                let len = path_len.clamp(0, PATH_MAX_LEN as usize);
                arr[..len].copy_from_slice(&path[..len]);
                arr
            },
            bus_id_len: u8::try_from(bus_id_len)
                .unwrap_or_default()
                .clamp(0, BUS_ID_MAX_LEN),
            bus_id: {
                let mut arr = [0; BUS_ID_MAX_LEN as usize];
                let len = bus_id_len.clamp(0, BUS_ID_MAX_LEN as usize);
                arr[..len].copy_from_slice(&bus_id[..len]);
                arr
            },
            busnum: info.bus_number(),
            devnum: info.device_address(),
            speed: info
                .speed()
                .map(|speed| msg::Speed::from_u8(speed as u8))
                .unwrap_or_default(),
            id_vendor: info.vendor_id(),
            id_product: info.product_id(),
            bcd_device: info.device_version(),
            b_device_class: info.class(),
            b_device_subclass: info.subclass(),
            b_device_protocol: info.protocol(),
            b_configuration_value,
            b_num_configurations,
            b_num_interfaces,
            padded_num_interfaces: interfaces.len().try_into().unwrap(),
            _padding: [0; 5],
        };

        Some(UsbDeviceWrapper { info, interfaces })
    });
    Ok(iterator)
}

struct ReqHandler {
    vhci: stub::Controller,
    incoming: compio::quic::Incoming,
    cancel_token: CancellationToken,
}

impl ReqHandler {
    const fn new(
        vhci: stub::Controller,
        incoming: compio::quic::Incoming,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            vhci,
            incoming,
            cancel_token,
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn open_session(self) -> Result<()> {
        enum Event {
            Conn(std::result::Result<Connection, ConnectionError>),
            Cancelled,
        }

        let connect = async { Event::Conn(self.incoming.await) };
        let cancel = async {
            self.cancel_token.cancelled().await;
            Event::Cancelled
        };
        let conn = match (connect, cancel).race().await {
            Event::Conn(connection) => connection.unwrap(),
            Event::Cancelled => return Ok(()),
        };

        info!(
            "established new session with {} - RTT {:?}",
            conn.remote_address(),
            conn.rtt()
        );
        let session = Session::new(conn.clone(), self.vhci);
        while conn.close_reason().is_none() && !self.cancel_token.is_cancelled() {
            match session.accept_stream().await? {
                ServerResp::ListDevices(operator) => {
                    operator.send_device_list(get_usb_devices).await?;
                }
                ServerResp::BorrowDevice(operator) => {
                    operator.lend(self.cancel_token.clone()).await?
                }
                ServerResp::LendDevice(operator) => {
                    operator.borrow(self.cancel_token.clone()).await?
                }
            }
        }

        Ok(())
    }
}

pub(crate) type ServerTaskResult = (NonZeroU64, Result<()>);

#[derive(Debug)]
pub struct Server {
    endpoint: compio::quic::Endpoint,
    dev: stub::Controller,
}

impl Server {
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn serve(self) -> ServerHandle {
        let cancel_for_handle = CancellationToken::new();
        let cancel_for_serve = cancel_for_handle.clone();
        let fut = async move {
            let (task_tx, task_rx) = mpsc::channel(0);
            let mut task_rx = task_rx.into_stream();
            let task_tx = task_tx.into_sink();
            info!("Server ready to accept new connections");

            enum Event {
                Incoming(compio::quic::Incoming),
                Cancelled,
                Task(ServerTaskResult),
            }

            let incoming = stream::unfold(self.endpoint, |endpoint| async {
                let incoming = endpoint.wait_incoming().await?;
                Some((Event::Incoming(incoming), endpoint))
            });
            let cancel = stream::once_future(async {
                cancel_for_serve.cancelled().await;
                Event::Cancelled
            });
            let task = (&mut task_rx).map(Event::Task);

            let mut events = pin!((incoming, cancel, task).merge());
            while let Some(event) = events.next().await {
                match event {
                    Event::Incoming(incoming) => {
                        debug!("incoming connection from {}", incoming.remote_address());
                        let handler =
                            ReqHandler::new(self.dev.clone(), incoming, cancel_for_serve.clone());
                        let mut task_tx = task_tx.clone();
                        compio::runtime::spawn(async move {
                            static ID: AtomicU64 = AtomicU64::new(1);
                            let id = ID.fetch_add(1, Ordering::Relaxed);
                            let id = NonZeroU64::new(id).unwrap();
                            let result = handler.open_session().await;
                            _ = task_tx.send((id, result)).await;
                        })
                        .detach();
                    }
                    Event::Task((id, Ok(_))) => {
                        info!("session {id} completed successfully");
                    }
                    Event::Task((id, Err(err))) => {
                        warn! { %err, "session {id} failed" };
                    }
                    Event::Cancelled => break,
                }
            }

            cancel_for_serve.cancel();
            drop(task_tx);
            while let Some(result) = task_rx.next().await {
                match result {
                    (id, Ok(_)) => info!("session {id} completed successfully"),
                    (id, Err(cause)) => warn! { %cause, "session {id} failed" },
                }
            }

            Ok(())
        }
        .in_current_span();

        let handle = compio::runtime::spawn(fut);
        ServerHandle {
            handle,
            cancel_token: cancel_for_handle,
        }
    }
}

pub struct ServerHandle {
    handle: compio::runtime::JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
}

impl ServerHandle {
    pub async fn shutdown(self) -> std::result::Result<Result<()>, Box<dyn std::any::Any + Send + 'static>> {
        self.cancel_token.cancel();
        self.handle.await
    }
}

// bitflags! {
//     struct Features: u8 {
//         /// Controls whether Qusb can borrow other peers' USB devices.
//         ///
//         /// Must have the USB Stub kernel module loaded or else
//         /// Qusb will fail to start.
//         const BORROWER = 0b0001;

//         /// Controls whether Qusb can lend USB devices to other peers.
//         const LENDER = 0b0010;

//         /// Controls whether Qusb can contact other peers.
//         const CLIENT = 0b0100;

//         /// Controls whether Qusb can accept connections from peers.
//         const SERVER = 0b1000;
//     }
// }

#[tracing::instrument(skip_all, level = "trace")]
pub async fn peer(
    server_tls: rustls::ServerConfig,
    client_tls: rustls::ClientConfig,
    bind: Option<SocketAddr>,
    transport: compio::quic::TransportConfig,
    num_ports: BoundedU8<1, 32>,
) -> (Client, Server) {
    let addr = bind.unwrap_or(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0));
    let mut endpoint =
        compio::quic::ServerBuilder::new_with_rustls_server_config(server_tls).bind(addr).await.unwrap();
    let mut client_cfg = compio::quic::ClientBuilder::new_with_rustls_client_config(client_tls).build();
    client_cfg.transport_config(Arc::new(transport));
    endpoint.default_client_config = Some(client_cfg);
    let dev = stub::Controller::start(num_ports).unwrap();

    (
        Client {
            endpoint: endpoint.clone(),
            dev: dev.clone(),
        },
        Server { endpoint, dev },
    )
}
