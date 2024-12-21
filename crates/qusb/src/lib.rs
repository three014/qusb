use iso::{Demuxer, Handle};
use proto::zerocopy::IntoBytes;
use proto::{
    data::{IterDst, IterMutDst, Ring},
    msg,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use state2::{ClientReq, ServerLendDevice, ServerListDevices, ServerResp};
use std::{
    future::Future,
    io,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    ops::Deref,
    path::Path,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use usb_ids::UsbIds;
use vhci::utils::BoundedU8;

pub use quinn::rustls;
pub use rcgen;

mod dev;
mod iso;
mod state2 {
    use std::{future::Future, io};

    use proto::{data::Ring, msg};
    use tokio::io::{AsyncRead, AsyncWrite};
    use zerocopy::{network_endian::U16, IntoBytes};

    use crate::{
        dev,
        iso,
        Error,
    };

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
        pub async fn run(self) {

        }
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
        W: AsyncWrite,
    {
        pub async fn resp_list_devices<'a, F, Fut, I, T>(mut self, iter: F) -> Result<(), Error>
        where
            W: AsyncWrite + Unpin,
            F: FnOnce() -> Fut,
            Fut: Future<Output = io::Result<I>>,
            I: Iterator<Item = &'a T>,
            T: msg::SendUsbDeviceInfo + 'a,
        {
            use tokio::io::AsyncWriteExt;
            let tx = &mut self.tx;

            let devices = match iter().await {
                Ok(devices) => {
                    tx.write(msg::Status::Success.as_bytes()).await?;
                    tx.write(&[0, 0, 0]).await?;
                    devices
                },
                Err(err) => {
                    tx.write_all(msg::Status::Failed.as_bytes()).await?;
                    return Err(err.into());
                }
            };

            for usb in devices {
                let path_len = U16::new(usb.path().len() as u16);
                let bus_id_len = U16::new(usb.bus_id().len() as u16);
                let scratch = [0; 256];
                let b_scratch = [0; 32];

                tx.write_all(path_len.as_bytes()).await?;
                tx.write_all(usb.path().as_bytes()).await?;
                tx.write_all(&scratch[path_len.get() as usize..]).await?;
                tx.write_all(bus_id_len.as_bytes()).await?;
                tx.write_all(usb.bus_id().as_bytes()).await?;
                tx.write_all(&b_scratch[bus_id_len.get() as usize..])
                    .await?;
                tx.write_all(usb.busnum().as_bytes()).await?;
                tx.write_all(usb.devnum().as_bytes()).await?;
                tx.write_all(usb.speed().as_bytes()).await?;
                tx.write_all(usb.id_vendor().as_bytes()).await?;
                tx.write_all(usb.id_product().as_bytes()).await?;
                tx.write_all(usb.bcd_device().as_bytes()).await?;
                tx.write_all(usb.b_device_class().as_bytes()).await?;
                tx.write_all(usb.b_device_subclass().as_bytes()).await?;
                tx.write_all(usb.b_device_protocol().as_bytes()).await?;
                tx.write_all(usb.b_configuration_value().as_bytes()).await?;
                tx.write_all(usb.b_num_configurations().as_bytes()).await?;
                tx.write_all(usb.b_num_interfaces().as_bytes()).await?;

                for int in usb.interfaces() {
                    tx.write_all(int.as_bytes()).await?;
                }
            }

            Ok(())
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

pub struct UsbDevices(Ring);

impl UsbDevices {
    pub fn iter(&self) -> IterDst<'_, msg::UsbDeviceInfo> {
        self.0.iter_dst()
    }

    pub fn iter_mut(&mut self) -> IterMutDst<'_, msg::UsbDeviceInfo> {
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
}

impl Session {
    fn new(conn: quinn::Connection) -> Self {
        Self {
            conn,
            iso: Arc::new(Mutex::new(None)),
        }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.conn.remote_address()
    }

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
            tx.finish()?;
            tx.stopped().await?;
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
                    None => return Err(Error::Io(io::ErrorKind::BrokenPipe.into())),
                }
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                tx.write_all(msg::Status::Unexpected.as_bytes()).await?;
                tx.finish()?;
                tx.stopped().await?;
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };

        Ok(req)
    }

    // // #[tracing::instrument(skip_all, level = "trace")]
    // pub async fn open_stream(&self) -> Result<State<state::ClientReq>, Error> {
    //     let (tx, rx) = self.conn.open_bi().await?;

    //     let iso_handle = self
    //         .iso
    //         .lock()
    //         .unwrap()
    //         .get_or_insert_with(|| init_iso(self.conn.clone()));

    //     let idle = State::new_client(tx, rx);
    //     let state = idle.verify_version().await?;
    //     Ok(state)
    // }

    pub async fn req_list_devices(&self) -> Result<UsbDevices, Error> {
        let (mut tx, mut rx) = self.conn.open_bi().await?;
        let mut buf = Ring::with_capacity(1024);

        let version = proto::QUSB_VER;
        let req = msg::Request::ListDevices;

        tx.write_all(version.as_bytes()).await?;
        tx.write_all(req.as_bytes()).await?;
        drop(tx);

        while 0 == buf.read_into_from_reader(&mut rx).await? {}
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

    pub async fn borrow_device(&self, id: msg::UsbDeviceId) -> Result<(), Error> {
        todo!()
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
        Ok(Session::new(conn))
    }
}

#[derive(Debug)]
pub struct Server {
    endpoint: quinn::Endpoint,
    dev: dev::Controller,
}

impl Server {
    // #[tracing::instrument(skip_all, level = "debug")]
    pub fn serve<F, Fut>(self, session_handler: F) -> ServerHandle
    where
        F: FnOnce(quinn::SendStream, quinn::RecvStream, tokio_util::sync::CancellationToken) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
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

            loop {
                tokio::select! {
                    incoming = self.endpoint.accept() => {
                        if let Some(incoming) = incoming {
                            let cancel_for_session = cancel_for_serve.clone();
                            let handle = session_handler.clone();
                            tracing::debug!("Incoming connection from {}", incoming.remote_address());
                            set.spawn(async move {
                                let conn = incoming.await?;
                                tracing::trace!("Established new session with {} - RTT {:?}", conn.remote_address(), conn.rtt());
                                let session = Session::new(conn);
                                // handle(session, cancel_for_session).await

                                todo!()
                            });
                        } else {
                            break;
                        }
                    },
                    _ = cancel_for_serve.cancelled() => {
                        break;
                    },

                    maybe_complete = check_for_completed_task(&mut set) => {
                        match maybe_complete {
                            Some(Ok((id, Ok(_)))) => tracing::info!("Session {id} completed successfully"),
                            Some(Ok((id, Err(cause)))) => tracing::warn! { %cause, "Session {id} failed" },
                            Some(Err(_)) => todo!(),
                            _ => (),
                        }
                    }
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
