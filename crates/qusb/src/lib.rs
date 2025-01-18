use bytes::Buf;
use operator::{ClientBorrowDevice, ClientReq, LendDevice, SendDevices, ServerResp};
use proto::{
    data::{IterDst, IterMutDst, ReadError, Ring},
    msg::{self, BUS_ID_MAX_LEN, PATH_MAX_LEN},
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use std::os::unix::ffi::OsStrExt;
use std::{
    io,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    ops::Deref,
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, trace, warn};
use usb_ids::UsbIds;
use utils::{align_to_usize, CloseStream};
use vhci::utils::BoundedU8;
use zerocopy::{FromZeros, IntoBytes};

pub use quinn::rustls;
pub use rcgen;

mod operator;
mod stub;
mod usb_ids;
pub mod utils;

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

impl From<quinn::StoppedError> for Error {
    fn from(value: quinn::StoppedError) -> Self {
        Error::Io(value.into())
    }
}

impl From<quinn::ConnectionError> for Error {
    fn from(value: quinn::ConnectionError) -> Self {
        Error::Io(value.into())
    }
}

impl From<quinn::WriteError> for Error {
    fn from(value: quinn::WriteError) -> Self {
        Error::Io(value.into())
    }
}

impl From<quinn::ReadError> for Error {
    fn from(value: quinn::ReadError) -> Self {
        Error::Io(value.into())
    }
}

impl From<quinn::ClosedStream> for Error {
    fn from(value: quinn::ClosedStream) -> Self {
        Error::Io(value.into())
    }
}

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

pub struct UrbWithIsoData<'a> {
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

    fn endpoint(&self) -> vhci::ioctl::Endpoint {
        self.header.endpoint
    }
}

impl vhci::TransferMut for UrbWithIsoData<'_> {
    fn transfer_mut(&mut self) -> &mut [u8] {
        &mut self.transfer[..self.header.transfer_actual_len as usize]
    }
}

impl vhci::IsoPacketDataMut for UrbWithIsoData<'_> {
    fn iso_packet_data_mut(&mut self) -> &mut [vhci::ioctl::IocIsoPacketData] {
        self.iso_data
    }
}

#[derive(Debug)]
pub struct UrbWithIsoGiveback<'a> {
    pub handle: vhci::ioctl::UrbHandle,
    pub header: &'a msg::UrbHeader,
    pub transfer: &'a mut [u8],
    pub iso_giveback: &'a mut [vhci::ioctl::IocIsoPacketGiveback],
}

impl vhci::Urb for UrbWithIsoGiveback<'_> {
    fn kind(&self) -> vhci::ioctl::UrbType {
        self.header.kind
    }

    fn handle(&self) -> vhci::ioctl::UrbHandle {
        self.handle
    }

    fn status(&self) -> vhci::Status {
        self.header.status
    }

    fn endpoint(&self) -> vhci::ioctl::Endpoint {
        self.header.endpoint
    }
}

impl vhci::TransferMut for UrbWithIsoGiveback<'_> {
    fn transfer_mut(&mut self) -> &mut [u8] {
        &mut self.transfer[..self.header.transfer_actual_len as usize]
    }
}

impl vhci::IsoPacketGivebackMut for UrbWithIsoGiveback<'_> {
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
    conn: quinn::Connection,
    dev: stub::Controller,
}

impl Session {
    fn new(conn: quinn::Connection, dev: stub::Controller) -> Self {
        Self { conn, dev }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.conn.remote_address()
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn accept_stream(&self) -> Result<ServerResp<quinn::SendStream, quinn::RecvStream>> {
        let (mut tx, mut rx) = self.conn.accept_bi().await?;
        let mut buf = Ring::with_capacity(32);

        const SIZE_OF_REQUEST: usize = size_of::<msg::Version>() + size_of::<msg::Request>();
        buf.fill_until(&mut rx, SIZE_OF_REQUEST).await?;

        let version: msg::Version = match buf.read() {
            Ok(version) => version,
            Err(ReadError::CorruptedData) => {
                unreachable!("msg::Version has FromBytes impl, and should be aligned")
            }
            Err(ReadError::BufferShort { .. }) => {
                unreachable!("we should have read enough data to get the version")
            }
        };
        if !version.is_compat(&proto::QUSB_VER) {
            let mut response = msg::Status::VersionMismatch
                .as_bytes()
                .chain(&[0u8; 7][..])
                .chain(proto::QUSB_VER.as_bytes())
                .chain(&[0u8; 4][..]);
            tx.write_all_buf(&mut response).await?;
            tx.close().await?;
            return Err(Error::VersionMismatch(version));
        }

        let req = match buf.read::<msg::Request>().and_then(|req| match req {
            msg::Request::ListDevices => Ok::<_, ReadError>(ClientReq::ListDevices),
            msg::Request::BorrowDevice => Ok(ClientReq::BorrowDevice(buf.read()?)),
        }) {
            Ok(ClientReq::ListDevices) => ServerResp::ListDevices(SendDevices::new(tx)),
            Ok(ClientReq::BorrowDevice(device)) => {
                buf.consume(6);

                ServerResp::BorrowDevice(LendDevice::new(tx, rx, buf, device))
            }
            Err(ReadError::CorruptedData) => {
                let mut response = msg::Status::Proto.as_bytes().chain(&[0u8; 7][..]);
                tx.write_all_buf(&mut response).await?;
                tx.close().await?;
                return Err(Error::Unknown);
            }
            Err(ReadError::BufferShort { .. }) => todo!(),
        };

        Ok(req)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn req_list_devices(&self) -> Result<UsbDevices> {
        let (mut tx, mut rx) = self
            .conn
            .open_bi()
            .await
            .inspect_err(|err| error!("{err}"))?;
        trace!("established stream with stream id {:?}", tx.id());
        let mut buf = Ring::with_capacity(1024);

        let version = proto::QUSB_VER;
        let req = msg::Request::ListDevices;

        let mut request = version.as_bytes().chain(req.as_bytes());
        tx.write_all_buf(&mut request).await?;
        drop(tx);

        while 0 == buf.fill_with_reader(&mut rx).await? {}
        trace!("finished reading from remote stream");
        let status = match buf.read() {
            Ok(status) => status,
            Err(ReadError::CorruptedData) => {
                unreachable!("msg::Status is FromBytes, and should be aligned")
            }
            Err(ReadError::BufferShort { .. }) => return Err(Error::Unknown),
        };
        buf.consume(7);
        match status {
            msg::Status::Success => {}
            msg::Status::NoDev | msg::Status::DevBusy | msg::Status::DevErr => unreachable!(),
            msg::Status::Unexpected | msg::Status::Failed => {
                return Err(Error::ReqFailed);
            }
            msg::Status::VersionMismatch => {
                buf.fill_until(&mut rx, align_to_usize(size_of::<msg::Version>()))
                    .await?;
                let their_version = buf.read::<msg::Version>().unwrap();
                return Err(Error::VersionMismatch(their_version));
            }
            msg::Status::Timeout => todo!(),
            msg::Status::Proto => todo!(),
        }

        Ok(UsbDevices(buf))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn borrow_device(
        &self,
        id: msg::UsbDeviceId,
    ) -> Result<ClientBorrowDevice<quinn::SendStream, quinn::RecvStream>> {
        let (mut tx, mut rx) = self.conn.open_bi().await.inspect_err(|err| {
            warn! {
                %err,
                "err while opening new request with peer"
            }
        })?;
        let mut buf = Ring::with_capacity(1024);

        trace!("established stream with stream id {:?}", tx.id());

        let version = proto::QUSB_VER;
        let req = msg::Request::BorrowDevice;

        let mut req = version
            .as_bytes()
            .chain(req.as_bytes())
            .chain(id.as_bytes())
            .chain(&[0u8; 6][..]);

        debug_assert_eq!(req.remaining() % size_of::<u64>(), 0);

        tx.write_all_buf(&mut req).await?;
        trace!(
            "sent request to borrow device {id:?} from peer @ {:?}",
            self.conn.remote_address()
        );

        buf.fill_until(&mut rx, align_to_usize(size_of::<msg::Status>()))
            .await?;

        trace!("received enough bytes for a status from peer");

        let status = match buf.read() {
            Ok(status) => status,
            Err(ReadError::CorruptedData) => {
                unreachable!("msg::Status is FromBytes, and should be aligned")
            }
            Err(ReadError::BufferShort { .. }) => {
                unreachable!("we should have read enough bytes to read status")
            }
        };
        buf.consume(7);
        match status {
            msg::Status::Success => (),
            msg::Status::Failed => return Err(Error::ReqFailed),
            msg::Status::DevBusy => return Err(Error::Io(io::ErrorKind::ResourceBusy.into())),
            msg::Status::DevErr => todo!(),
            msg::Status::NoDev => {
                return Err(Error::DevNotFound(id));
            }
            msg::Status::Unexpected => {
                return Err(io::Error::from(io::ErrorKind::InvalidData).into())
            }
            msg::Status::VersionMismatch => {
                buf.fill_until(&mut rx, align_to_usize(size_of::<msg::Version>()))
                    .await?;
                let their_version = buf.read::<msg::Version>().unwrap();
                return Err(Error::VersionMismatch(their_version));
            }
            msg::Status::Timeout => todo!(),
            msg::Status::Proto => todo!(),
        }

        let client = ClientBorrowDevice::new(tx, rx, buf, self.dev.clone(), id);
        Ok(client)
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: quinn::Endpoint,
    dev: stub::Controller,
}

impl Client {
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn connect(&self, peer_addr: SocketAddr, peer_name: &str) -> Result<Session> {
        let conn = self.endpoint.connect(peer_addr, peer_name).unwrap().await?;
        Ok(Session::new(conn, self.dev.clone()))
    }
}

#[derive(Debug)]
pub struct UsbDeviceWrapper {
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
    incoming: quinn::Incoming,
}

impl ReqHandler {
    const fn new(vhci: stub::Controller, incoming: quinn::Incoming) -> Self {
        Self { vhci, incoming }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn run(self) -> Result<()> {
        let conn = self.incoming.await.inspect_err(|err| warn! { %err })?;
        trace!(
            "established new session with {} - RTT {:?}",
            conn.remote_address(),
            conn.rtt()
        );
        let session = Session::new(conn.clone(), self.vhci);
        while conn.close_reason().is_none() {
            match session.accept_stream().await? {
                ServerResp::ListDevices(operator) => {
                    operator.resp_list_devices(get_usb_devices).await?;
                }
                ServerResp::BorrowDevice(operator) => operator.lend2().await?,
            }
        }

        Ok(())
    }
}

pub type ServerTaskResult =
    std::result::Result<(tokio::task::Id, Result<()>), tokio::task::JoinError>;

#[derive(Debug)]
pub struct Server {
    endpoint: quinn::Endpoint,
    dev: stub::Controller,
}

impl Server {
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn serve(self) -> ServerHandle {
        let cancel_for_handle = tokio_util::sync::CancellationToken::new();
        let cancel_for_serve = cancel_for_handle.clone();
        let handle = tokio::spawn(async move {
            let mut set = tokio::task::JoinSet::new();
            info!("Server ready to accept new connections");

            async fn check_for_completed_task(
                set: &mut tokio::task::JoinSet<Result<()>>,
            ) -> Option<ServerTaskResult> {
                if set.is_empty() {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                set.join_next_with_id().await
            }

            enum Event {
                Incoming(Option<quinn::Incoming>),
                Cancel,
                MaybeCompletedTask(Option<ServerTaskResult>),
            }

            loop {
                let event = tokio::select! {
                    incoming = self.endpoint.accept() => {
                        Event::Incoming(incoming)
                    },
                    _ = cancel_for_serve.cancelled() => {
                        Event::Cancel
                    },
                    maybe_complete = check_for_completed_task(&mut set) => {
                        Event::MaybeCompletedTask(maybe_complete)
                    }
                };

                match event {
                    Event::Incoming(Some(incoming)) => {
                        debug!("Incoming connection from {}", incoming.remote_address());
                        set.spawn(ReqHandler::new(self.dev.clone(), incoming).run());
                    }
                    Event::MaybeCompletedTask(Some(Ok((id, Ok(_))))) => {
                        info!("session {id} completed successfully")
                    }
                    Event::MaybeCompletedTask(Some(Ok((id, Err(cause))))) => {
                        warn! { %cause, "session {id} failed" }
                    }
                    Event::MaybeCompletedTask(Some(Err(cause))) => error! { %cause },
                    Event::Incoming(None) | Event::Cancel => break,
                    _ => (),
                }
            }

            while let Some(result) = set.join_next_with_id().await {
                match result {
                    Ok((id, Ok(_))) => info!("session {id} completed successfully"),
                    Ok((id, Err(cause))) => warn! { %cause, "session {id} failed" },
                    Err(cause) => error! { %cause },
                }
            }

            Ok(())
        });
        ServerHandle {
            handle,
            cancel_token: cancel_for_handle,
        }
    }
}

pub struct ServerHandle {
    handle: tokio::task::JoinHandle<Result<()>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl ServerHandle {
    pub async fn shutdown(self) -> std::result::Result<Result<()>, tokio::task::JoinError> {
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
pub fn peer(
    server_tls: rustls::ServerConfig,
    client_tls: rustls::ClientConfig,
    bind: Option<SocketAddr>,
    transport: quinn::TransportConfig,
    num_ports: BoundedU8<1, 32>,
) -> (Client, Server) {
    let server_tls =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_tls).unwrap()));
    let mut client_tls =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_tls).unwrap()));
    client_tls.transport_config(Arc::new(transport));

    let mut endpoint = quinn::Endpoint::server(
        server_tls,
        bind.unwrap_or(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into()),
    )
    .unwrap();
    endpoint.set_default_client_config(client_tls);

    let dev = stub::Controller::start(num_ports).unwrap();

    (
        Client {
            endpoint: endpoint.clone(),
            dev: dev.clone(),
        },
        Server { endpoint, dev },
    )
}
