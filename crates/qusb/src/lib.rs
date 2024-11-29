// use bitflags::bitflags;
use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    path::Path,
    sync::{Arc, LazyLock},
};
use tokio::io::BufReader;
use usb_ids::UsbIds;

mod stream;
mod state;
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

#[derive(Debug)]
pub struct Session {
    conn: quinn::Connection,
}

impl Session {
    pub async fn open_stream<R: DeserializeOwned>(
        &self,
    ) -> Result<(state::ReqSender, state::RespReceiver<R>), Error> {
        let (tx, rx) = self.conn.open_bi().await.map_err(|err| Error::Io(err.into()))?;
        let (tx, rx) = stream::new::<spec::Version, spec::Response<()>>(tx, rx);

        let tx = state::VersionSender(tx).send().await?;
        let mut rx = state::RespReceiver::<()>(rx);
        if let Err(_err) = rx.recv().await? {
            panic!("something happened")
        }
        Ok((tx, rx.convert()))
    }

    pub async fn accept_stream<T: Serialize>(
        &self,
    ) -> Result<(state::RespSender<T>, state::ReqReceiver), quinn::ConnectionError> {
        let (tx, rx) = self.conn.accept_bi().await?;
        let (tx, rx) = stream::new::<spec::Response<()>, spec::Version>(tx, rx);

        let rx = state::VersionReceiver(rx).recv().await.unwrap();
        let mut tx = state::RespSender(tx);
        tx.send_data(()).await.unwrap();
        Ok((tx.convert(), rx))
    }

    const fn new(conn: quinn::Connection) -> Result<Self, quinn::ConnectionError> {
        Ok(Self { conn })
    }
}

pub async fn list_devices(peer_addr: SocketAddr, peer_name: &str) -> Result<Vec<spec::UsbDeviceInfo>, Error> {
    let client = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    let server = {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
        let priv_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], priv_key.into())
        .unwrap()
    };

    let peer = Peer::new(server, client, None, quinn::TransportConfig::default()).unwrap();
    let session = peer.connect(peer_addr, peer_name).await;
    let (tx, mut rx) = session.open_stream::<spec::UsbDeviceInfo>().await.unwrap();

    let _ = tx.send::<()>(spec::Request::ListUsbDevices).await.unwrap();

    let mut devices = vec![];
    while let Ok(dev) = rx.recv().await.unwrap() {
        devices.push(dev);
    }

    Ok(devices)
}

#[derive(Debug, Clone)]
pub struct Peer {
    endpoint: quinn::Endpoint,
}

impl Peer {
    pub fn new(
        server_tls: rustls::ServerConfig,
        client_tls: rustls::ClientConfig,
        bind_addr: Option<SocketAddr>,
        transport_cfg: quinn::TransportConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let server_tls = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server_tls).unwrap(),
        ));
        let mut client_tls =
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_tls).unwrap()));
        client_tls.transport_config(Arc::new(transport_cfg));

        let mut endpoint = quinn::Endpoint::server(
            server_tls,
            bind_addr.unwrap_or(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into()),
        )
        .unwrap();
        endpoint.set_default_client_config(client_tls);

        Ok(Peer { endpoint })
    }

    pub async fn serve(&self) {
        while let Some(incoming) = self.endpoint.accept().await {
            tokio::spawn(async move {
                // let session = Session::accept(incoming).await;

                // Two things:
                // 1. I gotta fix the request line to work with
                //    responses, not just requests.
                // 2. I'd like to use a channel-type system to
                //    allow the caller to send/recv data to
                //    this session, which ultimately I don't want
                //    to go anywhere.
            });
        }
    }

    pub async fn connect(&self, peer_addr: SocketAddr, peer_name: &str) -> Session {
        let conn = self.endpoint.connect(peer_addr, peer_name).unwrap().await.unwrap();
        Session::new(conn).unwrap()
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

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
