use bitflags::bitflags;
use quinn::{rustls, ClientConfig, Endpoint, ServerConfig};
use std::{net::SocketAddr, sync::Arc};

#[derive(Debug)]
pub struct Config {
    server: ServerConfig,
    client: ClientConfig,
    bind: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct QusbPeer {
    endpoint: Endpoint,
}

impl QusbPeer {
    pub fn bind(config: Config) -> Self {
        let mut endpoint = Endpoint::server(config.server, config.bind).unwrap();
        endpoint.set_default_client_config(config.client);
        Self { endpoint }
    }

    pub async fn serve(&self) {
        while let Some(incoming) = self.endpoint.accept().await {
            tokio::spawn(async move {
                let conn = incoming.await.unwrap();
                loop {
                    let stream = conn.accept_bi().await;
                    let (mut send, mut recv) = match stream {
                        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                            return Ok(());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                        Ok(s) => s,
                    };
                    tokio::spawn(async move {
                        let req = recv.read_to_end(64 * 1024).await.unwrap();
                    });
                }
            });
        }
    }

    pub async fn query_peer(&self, peer_addr: SocketAddr, peer_name: &str) {
        let conn = self.endpoint.connect(peer_addr, peer_name).unwrap().await.unwrap();

        let (send, recv) = conn.open_bi().await.unwrap();

        // Ask the peer for what devices they have to offer
    }
}

bitflags! {
    struct Features: u8 {
        /// Controls whether Qusb can borrow other peers' USB devices.
        ///
        /// Must have the USB Stub kernel module loaded or else
        /// Qusb will fail to start.
        const BORROWER = 0b0001;

        /// Controls whether Qusb can lend USB devices to other peers.
        const LENDER = 0b0010;

        /// Controls whether Qusb can contact other peers.
        const CLIENT = 0b0100;

        /// Controls whether Qusb can accept connections from peers.
        const SERVER = 0b1000;
    }
}

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
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
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
