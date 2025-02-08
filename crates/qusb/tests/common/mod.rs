use quinn::rustls;
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use vhci::utils::BoundedU8;

/// A custom certificate that accepts any and all certificates it sees.
///
/// Do not use in production environments.
#[derive(Debug)]
pub struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
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

/// Convenience function for making self-signed certificates.
///
/// Probably don't use in production environments?
pub fn make_self_signed() -> (
    quinn::rustls::ServerConfig,
    rustls::pki_types::CertificateDer<'static>,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let priv_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert_der.clone()], priv_key.into())
    .unwrap();
    (server, cert_der)
}

pub fn addr(port: u16) -> SocketAddr {
    (Ipv4Addr::new(127, 0, 0, 1), port).into()
}

pub fn dummy_server(addr: SocketAddr) -> qusb::Server {
    let (server, cert) = make_self_signed();
    let mut certs = rustls::RootCertStore::empty();
    certs.add(cert).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(Arc::new(certs))
        .with_no_client_auth();
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(10)));

    let (_, server) = qusb::peer(
        server,
        client,
        Some(addr),
        transport,
        BoundedU8::new(4).unwrap(),
    );

    server
}

pub fn dummy_trusting_client(addr: SocketAddr) -> qusb::Client {
    let (server, _) = make_self_signed();
    let client = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(10)));

    let (client, _) = qusb::peer(
        server,
        client,
        Some(addr),
        transport,
        BoundedU8::new(4).unwrap(),
    );

    client
}

pub fn localhost(addr: SocketAddr) -> (qusb::Client, qusb::Server) {
    let (server, cert) = make_self_signed();
    let mut certs = rustls::RootCertStore::empty();
    certs.add(cert).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(Arc::new(certs))
        .with_no_client_auth();
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(10)));

    qusb::peer(
        server,
        client,
        Some(addr),
        transport,
        BoundedU8::new(4).unwrap(),
    )
}
