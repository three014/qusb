use pipe::{IsoDemuxer, IsoHandle};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use state::State;
use std::{
    future::Future,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    ops::Deref,
    path::Path,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use usb_ids::UsbIds;
use utils::BoundedU8;

pub use quinn::rustls;
pub use rcgen;

mod dev;
mod pipe;
mod state;
mod stream;
mod usb_ids;
pub mod utils;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Serde(#[from] postcard::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported qusb protocol version (their version: {0}, our version: {ver})", ver = proto::VERSION)]
    VersionMismatch(proto::Version),
    #[error("device with id {0:?} not found")]
    DevNotFound(proto::UsbDeviceId),
}

static USB_IDS: LazyLock<UsbIds> =
    LazyLock::new(|| usb_ids::parse(Path::new("./usb-ids")).unwrap());

pub fn usb_ids() -> &'static UsbIds {
    USB_IDS.deref()
}

#[derive(Debug)]
pub struct Session {
    conn: quinn::Connection,
    iso: Arc<Mutex<Option<IsoHandle>>>,
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

    // #[tracing::instrument(skip_all, level = "trace")]
    pub async fn open_stream(&self) -> Result<State<state::ClientReq>, Error> {
        let mut slot = self.iso.lock().unwrap();
        if slot
            .as_ref()
            .is_none_or(|&IsoHandle { ref handle, .. }| handle.is_finished())
        {
            let (register_tx, register_rx) = mpsc::channel(8);
            let (disconnect_tx, disconnect_rx) = mpsc::channel(8);
            let conn = self.conn.clone();
            let handle = tokio::spawn(async move {
                IsoDemuxer {
                    register_rx,
                    disconnect_rx,
                    conn,
                }
                .run()
                .await
            });

            *slot = Some(IsoHandle {
                handle,
                register_tx,
                disconnect_tx,
            })
        }

        // TODO: Move state stuff out of this crate into proto crate

        let (tx, rx) = self
            .conn
            .open_bi()
            .await
            .map_err(|err| Error::from(std::io::Error::from(err)))?;
        let idle = State::new_client(tx, rx);
        let state = idle.verify_version().await?;
        Ok(state)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn accept_stream(&self) -> Result<State<state::ServerGetReq>, Error> {
        let (tx, rx) = self
            .conn
            .accept_bi()
            .await
            .map_err(|err| Error::from(std::io::Error::from(err)))?;
        let listening = State::new_server(tx, rx);
        Ok(listening.verify_version().await?)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn list_devices(&self) -> Result<Vec<proto::UsbDeviceInfo<'static>>, Error> {
        let mut client = self.open_stream().await.unwrap().list_devices().await?;

        tracing::trace!("Requested 'ListDevices'");
        let mut devices = vec![];
        while let Some(dev) = client.next().await? {
            tracing::trace!("Got a device from server: {:?}", dev);
            devices.push(dev);
        }
        tracing::trace!("Finished receiving devices from the server");
        Ok(devices)
    }

    pub async fn borrow_device(&mut self, id: proto::UsbDeviceId) -> Result<(), Error> {
        use vhci::*;
        let client = self.open_stream().await?.borrow_device(id).await?;

        let mut addr = 0xff;
        let mut stat = PortStat {
            status: PortStatus::empty(),
            change: PortChange::empty(),
            index: Port::new(1).unwrap(),
            flags: PortFlag::empty(),
        };

        // We unfortunately need two queues; one for submitted urbs
        // and one for the return values of urbs
        //
        //

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
        let conn = self
            .endpoint
            .connect(peer_addr, peer_name)
            .unwrap()
            .await
            .map_err(std::io::Error::from)?;
        Ok(Session::new(conn))
    }
}

#[derive(Debug)]
pub struct Server {
    endpoint: quinn::Endpoint,
    dev: dev::Controller,
}

impl Server {
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn serve<F, Fut>(self, session_handler: F) -> ServerHandle
    where
        F: FnOnce(Session, tokio_util::sync::CancellationToken) -> Fut + Clone + Send + 'static,
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
                                let conn = incoming.await.map_err(|err| Error::Io(err.into()))?;
                                tracing::trace!("Established new session with {} - RTT {:?}", conn.remote_address(), conn.rtt());
                                let session = Session::new(conn);
                                handle(session, cancel_for_session).await
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

#[tracing::instrument(skip_all, level = "trace")]
pub async fn handle_list_devices(state: State<state::ServerDecideResp>) -> Result<(), Error> {
    let mut stream = state.list_devices();

    for device in nusb::list_devices()? {
        let to_send = proto::UsbDeviceInfo {
            id: proto::UsbDeviceId {
                bus_number: device.bus_number(),
                device_addr: device.device_address(),
            },
            bus_id: proto::BusId(std::borrow::Cow::Borrowed(
                device
                    .sysfs_path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )),
            vendor_id: device.vendor_id(),
            product_id: device.product_id(),
            class: device.class(),
            subclass: device.subclass(),
            protocol: device.protocol(),
            interfaces: device
                .interfaces()
                .map(|int| proto::InterfaceInfo {
                    interface_number: int.interface_number(),
                    class: int.subclass(),
                    subclass: int.subclass(),
                    protocol: int.protocol(),
                })
                .collect(),
        };

        stream.send_device_info(&to_send).await?;
    }

    stream.finish().await
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

    let dev = dev::Controller::start(BoundedU8::new(4).unwrap()).unwrap();

    (
        Client {
            endpoint: endpoint.clone(),
            dev: dev.clone(),
        },
        Server { endpoint, dev },
    )
}
