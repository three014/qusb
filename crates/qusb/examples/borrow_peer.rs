use std::{
    io::BufWriter,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use compio::net::ToSocketAddrsAsync;
use futures_concurrency::future::Race;
use mimalloc::MiMalloc;
use proto::msg;
use qusb::BoundedU8;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[compio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    let log_path = "borrow_self_dev.log";
    let log_file = std::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();
    _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .parse("none,borrow_self=info,qusb=trace")
                .unwrap(),
        )
        .with_writer(Mutex::new(BufWriter::with_capacity(64, log_file)))
        .try_init();

    let _guard = tracing::info_span!("main");

    let addr = "[::]:7002".parse().unwrap();
    let client = make_client(addr).await;
    let ctrl_c = compio::signal::ctrl_c();

    let addr = "quesadilla.garden.lan:7002"
        .to_socket_addrs_async()
        .await
        .unwrap()
        .next()
        .unwrap();
    let session = client.connect(addr, "quesadilla.garden.lan").await.unwrap();
    info!("connected to {}", session.remote_address());

    let dev = msg::UsbDeviceId {
        bus_number: 3,
        device_addr: 8,
    };
    let usb = session.req_borrow(dev).await.unwrap();
    let cancel_token = CancellationToken::new();
    let timer = compio::runtime::time::sleep(Duration::from_secs(60));
    let mut handle = compio::runtime::spawn(usb.borrow(cancel_token.clone()));

    enum Event {
        Cancelled,
        Completed,
    }

    let cancel = async {
        ctrl_c.await.unwrap();
        Event::Cancelled
    };
    let timer = async {
        timer.await;
        Event::Cancelled
    };
    let completion = {
        let handle = &mut handle;
        async {
            handle.await.unwrap().unwrap();
            Event::Completed
        }
    };

    match (cancel, timer, completion).race().await {
        Event::Cancelled => {
            tracing::info!("borrow cancelled, waiting for session to complete");
            cancel_token.cancel();
            handle.await.unwrap().unwrap();
        }
        Event::Completed => {}
    }

    info!("client done!");
}

/// Convenience function for making self-signed certificates.
///
/// Probably don't use in production environments?
fn make_self_signed() -> (
    rustls::ServerConfig,
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

async fn make_client(addr: SocketAddr) -> qusb::Client {
    let (server, _) = make_self_signed();
    let verifier = SkipServerVerification::new();
    let client = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    let mut transport = compio::quic::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(10)));

    let (client, _) = qusb::peer(
        server,
        client,
        Some(addr),
        transport,
        BoundedU8::new(4).unwrap(),
    )
    .await;

    client
}

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
