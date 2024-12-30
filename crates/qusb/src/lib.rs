use iso::{Demuxer, Handle};
use proto::lstr;
use proto::{
    data::{IterDst, IterMutDst, Ring},
    msg,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use state::{ClientBorrowDevice, ClientReq, ServerLendDevice, ServerListDevices, ServerResp};
use std::os::unix::ffi::OsStrExt;
use std::{
    io,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    ops::Deref,
    path::Path,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use usb_ids::UsbIds;
use utils::CloseStream;
use vhci::utils::BoundedU8;
use zerocopy::network_endian::U16;
use zerocopy::IntoBytes;

pub use quinn::rustls;
pub use rcgen;

mod dev;
mod iso;
mod state {
    use std::io;

    use proto::{data::Ring, msg};
    use tokio::io::{AsyncRead, AsyncWrite};
    use zerocopy::{network_endian::U16, IntoBytes};

    use crate::{dev, iso, utils, Error};

    pub enum ClientReq {
        ListDevices,
        BorrowDevice(msg::UsbDeviceId),
    }

    pub struct ClientBorrowDevice<W, R> {
        tx: W,
        rx: R,
        buf: Ring,
        iso_tx: iso::Sender,
        iso_rx: iso::Receiver,
        vhci: dev::Controller,
    }

    impl<W, R> ClientBorrowDevice<W, R> {
        pub fn new(
            tx: W,
            rx: R,
            buf: Ring,
            iso_tx: iso::Sender,
            iso_rx: iso::Receiver,
            vhci: dev::Controller,
        ) -> Self {
            Self {
                tx,
                rx,
                buf,
                iso_tx,
                iso_rx,
                vhci,
            }
        }
    }

    impl<W, R> ClientBorrowDevice<W, R>
    where
        W: AsyncWrite,
        R: AsyncRead,
    {
        pub async fn run(self) {}
    }

    pub enum ServerResp<W, R> {
        ListDevices(ServerListDevices<W>),
        BorrowDevice(ServerLendDevice<W, R>),
    }

    pub struct ServerLendDevice<W, R> {
        tx: W,
        rx: R,
        buf: Ring,
        iso_tx: iso::Sender,
        iso_rx: iso::Receiver,
        device_id: msg::UsbDeviceId,
    }

    impl<W, R> ServerLendDevice<W, R> {
        pub fn new(
            tx: W,
            rx: R,
            buf: Ring,
            iso_tx: iso::Sender,
            iso_rx: iso::Receiver,
            device_id: msg::UsbDeviceId,
        ) -> Self {
            Self {
                tx,
                rx,
                buf,
                iso_tx,
                iso_rx,
                device_id,
            }
        }
    }

    pub struct ServerListDevices<W> {
        tx: W,
    }

    impl<W> ServerListDevices<W> {
        pub fn new(tx: W) -> Self {
            Self { tx }
        }
    }

    impl<W> ServerListDevices<W>
    where
        W: AsyncWrite + utils::CloseStream + Unpin,
    {
        pub async fn resp_list_devices<'a, I, T>(
            self,
            iter: impl Fn() -> io::Result<I>,
        ) -> Result<(), Error>
        where
            I: Iterator<Item = T>,
            T: msg::SendUsbDeviceInfo,
        {
            use tokio::io::AsyncWriteExt;
            let mut tx = self.tx;

            let devices = match iter() {
                Ok(devices) => {
                    tx.write_all(msg::Status::Success.as_bytes()).await?;
                    tx.write_all(&[0, 0, 0]).await?;
                    devices
                }
                Err(err) => {
                    tx.write_all(msg::Status::Failed.as_bytes()).await?;
                    return Err(err.into());
                }
            };

            for usb in devices {
                let main_info = usb.get();
                assert_eq!(main_info.b_num_interfaces as usize, usb.interfaces().len());
                tx.write_all(main_info.as_bytes()).await?;
                tx.write_all(usb.interfaces().as_bytes()).await?;
            }

            tx.close().await.map_err(Error::from)
        }
    }
}
mod usb_ids;
pub mod utils;

static USB_IDS: LazyLock<UsbIds> =
    LazyLock::new(|| usb_ids::parse(Path::new("./usb-ids")).unwrap());

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

#[derive(Debug)]
pub struct UsbDevices(Ring);

impl UsbDevices {
    pub fn iter(&self) -> IterDst<'_, msg::UsbDeviceInfoRx> {
        self.0.iter_dst()
    }

    pub fn iter_mut(&mut self) -> IterMutDst<'_, msg::UsbDeviceInfoRx> {
        self.0.iter_mut_dst()
    }
}

fn init_iso(conn: quinn::Connection) -> Handle {
    let (register_tx, register_rx) = mpsc::channel(8);
    let (disconnect_tx, disconnect_rx) = mpsc::channel(8);
    let demux_conn = conn.clone();
    let handle = tokio::spawn(async move {
        Demuxer {
            register_rx,
            disconnect_rx,
            conn: demux_conn,
        }
        .run()
        .await
    });

    Handle {
        handle,
        register_tx,
        disconnect_tx,
        conn,
    }
}

#[derive(Debug)]
pub struct Session {
    conn: quinn::Connection,
    iso: Arc<Mutex<Option<Handle>>>,
    dev: dev::Controller,
}

impl Session {
    fn new(conn: quinn::Connection, dev: dev::Controller) -> Self {
        Self {
            conn,
            iso: Arc::new(Mutex::new(None)),
            dev,
        }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.conn.remote_address()
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn accept_stream(
        &self,
    ) -> Result<ServerResp<quinn::SendStream, quinn::RecvStream>, Error> {
        let (mut tx, mut rx) = self.conn.accept_bi().await?;
        let mut buf = Ring::with_capacity(32);

        buf.read_into_from_reader(&mut rx).await?;
        let version: msg::Version = buf.read()?;

        if version.major != proto::QUSB_VER.major || version.minor != proto::QUSB_VER.minor {
            tx.write_all(msg::Status::VersionMismatch.as_bytes())
                .await?;
            tx.write_all(proto::QUSB_VER.as_bytes()).await?;
            tx.close().await?;
            return Err(Error::VersionMismatch(version));
        }

        let req = match buf.read::<msg::Request>().and_then(|req| match req {
            msg::Request::ListDevices => Ok::<_, io::Error>(ClientReq::ListDevices),
            msg::Request::BorrowDevice => Ok(ClientReq::BorrowDevice(buf.read()?)),
        }) {
            Ok(ClientReq::ListDevices) => ServerResp::ListDevices(ServerListDevices::new(tx)),
            Ok(ClientReq::BorrowDevice(device)) => {
                let mut slot = self.iso.lock().unwrap();
                if slot
                    .as_ref()
                    .is_none_or(|&Handle { ref handle, .. }| handle.is_finished())
                {
                    *slot = None;
                }

                match slot
                    .get_or_insert_with(|| init_iso(self.conn.clone()))
                    .make_channel(rx.id())
                {
                    Some((iso_tx, iso_rx)) => ServerResp::BorrowDevice(ServerLendDevice::new(
                        tx, rx, buf, iso_tx, iso_rx, device,
                    )),
                    // One way this can happen is if the demuxer wasn't finished
                    // when we checked, but then finished right after.
                    // TODO: Have this process run in a short loop so we can try to
                    // initialize the demuxer again.
                    None => return Err(Error::Io(io::ErrorKind::BrokenPipe.into())),
                }
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                tx.write_all(msg::Status::Unexpected.as_bytes()).await?;
                tx.close().await?;
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };

        Ok(req)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn req_list_devices(&self) -> Result<UsbDevices, Error> {
        let (mut tx, mut rx) = self
            .conn
            .open_bi()
            .await
            .inspect_err(|err| tracing::error!("{err}"))?;
        tracing::trace!("established stream with stream id {:?}", tx.id());
        let mut buf = Ring::with_capacity(1024);

        let version = proto::QUSB_VER;
        let req = msg::Request::ListDevices;

        tx.write_all(version.as_bytes())
            .await
            .inspect_err(|err| tracing::error!("{err}"))?;
        tx.write_all(req.as_bytes())
            .await
            .inspect_err(|err| tracing::error!("{err}"))?;
        drop(tx);

        while 0
            == buf
                .read_into_from_reader(&mut rx)
                .await
                .inspect_err(|err| tracing::error!("{err}"))?
        {}
        tracing::trace!("finished reading from remote stream");
        let status = buf.read()?;
        match status {
            msg::Status::Success => {
                buf.consume(&[0u8, 0u8, 0u8]);
            }
            msg::Status::Failed => return Err(Error::ReqFailed),
            msg::Status::DevBusy => todo!(),
            msg::Status::DevErr => todo!(),
            msg::Status::NoDev => todo!(),
            msg::Status::Unexpected => {
                return Err(io::Error::from(io::ErrorKind::InvalidData).into())
            }
            msg::Status::VersionMismatch => {
                let their_version = buf.read::<msg::Version>()?;
                return Err(Error::VersionMismatch(their_version));
            }
        }

        Ok(UsbDevices(buf))
    }

    pub async fn borrow_device(
        &self,
        id: msg::UsbDeviceId,
    ) -> Result<ClientBorrowDevice<quinn::SendStream, quinn::RecvStream>, Error> {
        let (mut tx, mut rx) = self.conn.open_bi().await?;
        let mut buf = Ring::with_capacity(1024);

        let version = proto::QUSB_VER;
        let req = msg::Request::BorrowDevice;

        tx.write_all(version.as_bytes()).await?;
        tx.write_all(req.as_bytes()).await?;
        tx.write_all(id.as_bytes()).await?;

        while buf.len() < std::mem::size_of::<msg::Status>() {
            buf.read_into_from_reader(&mut rx).await?;
        }

        let status = buf.read()?;
        match status {
            msg::Status::Success => {}
            msg::Status::Failed => return Err(Error::ReqFailed),
            msg::Status::DevBusy => todo!(),
            msg::Status::DevErr => todo!(),
            msg::Status::NoDev => {
                return Err(Error::DevNotFound(id));
            }
            msg::Status::Unexpected => {
                return Err(io::Error::from(io::ErrorKind::InvalidData).into())
            }
            msg::Status::VersionMismatch => {
                let their_version = buf.read::<msg::Version>()?;
                return Err(Error::VersionMismatch(their_version));
            }
        }

        let mut slot = self.iso.lock().unwrap();
        if slot
            .as_ref()
            .is_none_or(|&Handle { ref handle, .. }| handle.is_finished())
        {
            *slot = None;
        }

        let client = match slot
            .get_or_insert_with(|| init_iso(self.conn.clone()))
            .make_channel(tx.id())
        {
            Some((iso_tx, iso_rx)) => {
                ClientBorrowDevice::new(tx, rx, buf, iso_tx, iso_rx, self.dev.clone())
            }
            // One way this can happen is if the demuxer wasn't finished
            // when we checked, but then finished right after.
            // TODO: Have this process run in a short loop so we can try to
            // initialize the demuxer again.
            None => return Err(Error::Io(io::ErrorKind::BrokenPipe.into())),
        };

        Ok(client)
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: quinn::Endpoint,
    dev: dev::Controller,
}

impl Client {
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn connect(&self, peer_addr: SocketAddr, peer_name: &str) -> Result<Session, Error> {
        let conn = self.endpoint.connect(peer_addr, peer_name).unwrap().await?;
        Ok(Session::new(conn, self.dev.clone()))
    }
}

#[derive(Debug)]
pub struct UsbDeviceWrapper {
    info: msg::UsbDeviceInfoTx,
    interfaces: Box<[msg::UsbInterfaceInfo]>,
}

impl msg::SendUsbDeviceInfo for UsbDeviceWrapper {
    fn get(&self) -> &msg::UsbDeviceInfoTx {
        &self.info
    }

    fn interfaces(&self) -> &[msg::UsbInterfaceInfo] {
        &self.interfaces
    }
}

fn get_usb_devices() -> io::Result<
    std::iter::FilterMap<
        impl Iterator<Item = nusb::DeviceInfo>,
        impl FnMut(nusb::DeviceInfo) -> Option<UsbDeviceWrapper>,
    >,
> {
    let iterator = nusb::list_devices()?.filter_map(|info| -> Option<UsbDeviceWrapper> {
        let device = info
            .open()
            .inspect_err(|err| tracing::trace!("skipping usb device due to {err}"))
            .ok()?;
        let interfaces: Box<[msg::UsbInterfaceInfo]> = info
            .interfaces()
            .map(|iface| msg::UsbInterfaceInfo {
                b_interface_number: iface.interface_number(),
                b_interface_class: iface.class(),
                b_interface_subclass: iface.subclass(),
                b_interface_protocol: iface.protocol(),
            })
            .collect();
        let b_num_interfaces = interfaces.len().try_into().unwrap();
        let b_configuration_value = device
            .active_configuration()
            .map(|conf| conf.configuration_value())
            .unwrap_or_default();
        let b_num_configurations = device.configurations().count() as u8;
        let path = info.sysfs_path().as_os_str().as_bytes().try_into().ok()?;
        let bus_id = info
            .sysfs_path()
            .file_name()
            .map(|name| name.as_bytes().try_into().ok())
            .flatten()?;
        let info = msg::UsbDeviceInfoTx {
            path,
            path_len: U16::new(path.len().try_into().unwrap()),
            bus_id,
            bus_id_len: bus_id.len().try_into().unwrap(),
            busnum: info.bus_number(),
            devnum: info.device_address(),
            speed: info
                .speed()
                .map(|speed| msg::Speed::from_u8(speed as u8))
                .unwrap_or_default(),
            id_vendor: U16::new(info.vendor_id()),
            id_product: U16::new(info.product_id()),
            bcd_device: U16::new(info.device_version()),
            b_device_class: info.class(),
            b_device_subclass: info.subclass(),
            b_device_protocol: info.protocol(),
            b_configuration_value,
            b_num_configurations,
            b_num_interfaces,
        };

        Some(UsbDeviceWrapper { info, interfaces })
    });
    Ok(iterator)
}

struct ReqHandler {
    vhci: dev::Controller,
    incoming: quinn::Incoming,
}

impl ReqHandler {
    const fn new(vhci: dev::Controller, incoming: quinn::Incoming) -> Self {
        Self { vhci, incoming }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn run(self) -> Result<(), Error> {
        let conn = self.incoming.await?;
        tracing::trace!(
            "established new session with {} - RTT {:?}",
            conn.remote_address(),
            conn.rtt()
        );
        let session = Session::new(conn, self.vhci);
        match session.accept_stream().await? {
            ServerResp::ListDevices(state) => state.resp_list_devices(get_usb_devices).await,
            ServerResp::BorrowDevice(state) => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct Server {
    endpoint: quinn::Endpoint,
    dev: dev::Controller,
}

impl Server {
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn serve(self) -> ServerHandle {
        let cancel_for_handle = tokio_util::sync::CancellationToken::new();
        let cancel_for_serve = cancel_for_handle.clone();
        let handle = tokio::spawn(async move {
            let mut set = tokio::task::JoinSet::new();
            tracing::info!("Server ready to accept new connections");

            async fn check_for_completed_task(
                set: &mut tokio::task::JoinSet<Result<(), Error>>,
            ) -> Option<Result<(tokio::task::Id, Result<(), Error>), tokio::task::JoinError>>
            {
                if set.is_empty() {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                set.join_next_with_id().await
            }

            enum Event {
                Incoming(Option<quinn::Incoming>),
                Cancel,
                MaybeCompletedTask(
                    Option<Result<(tokio::task::Id, Result<(), Error>), tokio::task::JoinError>>,
                ),
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
                        tracing::debug!("Incoming connection from {}", incoming.remote_address());
                        set.spawn(ReqHandler::new(self.dev.clone(), incoming).run());
                    }
                    Event::Incoming(None) | Event::Cancel => break,
                    Event::MaybeCompletedTask(Some(Ok((id, Ok(_))))) => {
                        tracing::info!("session {id} completed successfully")
                    }
                    Event::MaybeCompletedTask(Some(Ok((id, Err(cause))))) => {
                        tracing::warn! { %cause, "session {id} failed" }
                    }
                    Event::MaybeCompletedTask(Some(Err(_err))) => todo!(),
                    _ => (),
                }
            }

            while let Some(result) = set.join_next_with_id().await {
                match result {
                    Ok((id, Ok(_))) => tracing::info!("Session {id} completed successfully"),
                    Ok((id, Err(cause))) => tracing::warn! { %cause, "Session {id} failed" },
                    Err(_) => todo!(),
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
    handle: tokio::task::JoinHandle<Result<(), Error>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl ServerHandle {
    pub async fn shutdown(self) -> Result<Result<(), Error>, tokio::task::JoinError> {
        self.cancel_token.cancel();
        self.handle.await
    }
}

// pub async fn list_devices(state: State<state::ServerDecideResp>) -> Result<(), Error> {
//     let mut stream = state.list_devices();

//     for device in nusb::list_devices()? {
//         let to_send = proto::UsbDeviceInfo {
//             id: proto::UsbDeviceId {
//                 bus_number: device.bus_number(),
//                 device_addr: device.device_address(),
//             },
//             bus_id: proto::BusId(std::borrow::Cow::Borrowed(
//                 device
//                     .sysfs_path()
//                     .file_name()
//                     .unwrap()
//                     .to_str()
//                     .unwrap()
//                     .try_into()
//                     .unwrap(),
//             )),
//             vendor_id: device.vendor_id(),
//             product_id: device.product_id(),
//             class: device.class(),
//             subclass: device.subclass(),
//             protocol: device.protocol(),
//             interfaces: device
//                 .interfaces()
//                 .map(|int| proto::InterfaceInfo {
//                     interface_number: int.interface_number(),
//                     class: int.subclass(),
//                     subclass: int.subclass(),
//                     protocol: int.protocol(),
//                 })
//                 .collect(),
//         };

//         stream.send_device_info(&to_send).await?;
//     }

//     stream.finish().await
// }

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

    let dev = dev::Controller::start(num_ports).unwrap();

    (
        Client {
            endpoint: endpoint.clone(),
            dev: dev.clone(),
        },
        Server { endpoint, dev },
    )
}
