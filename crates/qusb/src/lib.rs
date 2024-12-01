// use bitflags::bitflags;
use crate::utils::SkipServerVerification;
use futures_util::StreamExt;
use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls,
};
use serde::{de::DeserializeOwned, Serialize};
use state::State;
use std::{
    future::Future, net::{Ipv6Addr, SocketAddr, SocketAddrV6}, ops::Deref, os::unix::ffi::OsStrExt, path::Path, sync::{Arc, LazyLock}
};
use tokio::io::BufReader;
use usb_ids::UsbIds;

mod state;
mod stream;
mod usb_ids;
mod utils;

pub type Sender<T> = stream::Sender<T, quinn::SendStream>;
pub type Receiver<T> = stream::Receiver<T, BufReader<quinn::RecvStream>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Serde(#[from] postcard::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported qusb protocol version (their version: {0}, our version: {ver})", ver = spec::VERSION)]
    VersionMismatch(spec::Version),
}

static USB_IDS: LazyLock<UsbIds> =
    LazyLock::new(|| usb_ids::parse(Path::new("./usb-ids")).unwrap());

pub fn usb_ids() -> &'static UsbIds {
    USB_IDS.deref()
}

#[derive(Debug)]
pub struct Session {
    conn: quinn::Connection,
}

impl Session {
    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn open_stream(&self) -> Result<State<state::ClientReq>, Error> {
        let (tx, rx) = self
            .conn
            .open_bi()
            .await
            .map_err(|err| Error::from(std::io::Error::from(err)))?;
        let idle = State::new_client(tx, rx);
        Ok(idle.verify_version().await?)
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

    const fn new(conn: quinn::Connection) -> Self {
        Self { conn }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub async fn list_devices(&self) -> Result<Vec<spec::UsbDeviceInfo<'static>>, Error> {
        let client = self.open_stream().await.unwrap();

        let mut list_devices = client.list_devices().await?;
        tracing::trace!("Requested 'ListDevices'");
        let mut devices = vec![];
        while let Some(dev) = list_devices.next().await? {
            tracing::trace!("Got a device from server: {:?}", dev);
            devices.push(dev);
        }
        tracing::trace!("Finished receiving devices from the server");
        Ok(devices)
    }
}

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

    (
        Client {
            endpoint: endpoint.clone(),
        },
        Server { endpoint },
    )
}

pub struct Client {
    endpoint: quinn::Endpoint,
}

impl Client {
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn connect(&self, peer_addr: SocketAddr, peer_name: &str) -> Session {
        let conn = self
            .endpoint
            .connect(peer_addr, peer_name)
            .unwrap()
            .await
            .unwrap();
        Session::new(conn)
    }
}

#[derive(Debug)]
pub struct Server {
    endpoint: quinn::Endpoint,
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

#[tracing::instrument(skip_all, level = "trace")]
pub async fn handle_list_devices(state: State<state::ServerDecideResp>) -> Result<(), Error> {
    let mut stream = state.list_devices();

    for device in nusb::list_devices()? {
        let to_send = spec::UsbDeviceInfo {
            id: spec::UsbDeviceId {
                bus_number: device.bus_number(),
                device_addr: device.device_address(),
            },
            bus_id: spec::BusId(std::borrow::Cow::Borrowed(
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
                .map(|int| spec::InterfaceInfo {
                    interface_number: int.interface_number(),
                    class: int.subclass(),
                    subclass: int.subclass(),
                    protocol: int.protocol(),
                })
                .collect(),
        };

        stream.send_device(&to_send).await?;
    }

    stream.finish().await
}

pub struct ServerHandle {
    handle: tokio::task::JoinHandle<Result<(), Error>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl ServerHandle {
    pub async fn shutdown(self) {
        self.cancel_token.cancel();
        self.handle.await.unwrap().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn list_devices_works() {
        let (server, cert) = utils::make_self_signed();
        let mut certs = rustls::RootCertStore::empty();
        certs.add(cert).unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(certs))
            .with_no_client_auth();
        let (client, server) = peer(
            server,
            client,
            Some("127.0.0.1:7640".parse().unwrap()),
            quinn::TransportConfig::default(),
        );

        let handle = server.serve(|session, _cancel_handle| async move {
            let stream = session.accept_stream().await.unwrap();
            tracing::trace!("Accepted new stream");

            let (req, stream) = stream.recv_req().await.unwrap();
            tracing::trace!("Received request from client: {req:?}");
            match req {
                spec::Request::ListUsbDevices => {
                    handle_list_devices(stream).await?;
                }
                spec::Request::ImportUsbDevice(_) => panic!("Not what this test is for"),
            }

            tracing::trace!("Finished serving req");
            Ok(())
        });

        {
            let session = client
                .connect("127.0.0.1:7640".parse().unwrap(), "localhost")
                .await;
            tracing::info!("Connected to {}", session.conn.remote_address());

            let devs = session.list_devices().await.unwrap();
        }

        handle.shutdown().await;
    }
}
