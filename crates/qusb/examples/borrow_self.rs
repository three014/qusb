use std::{
    io::BufWriter,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_concurrency::future::Race;
use proto::msg;
use qusb::BoundedU8;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const DEV: msg::UsbDeviceId = msg::UsbDeviceId {
    bus_number: 1,
    device_addr: 17,
};

fn main() {
    let proactor = compio::driver::ProactorBuilder::new()
        // .sqpoll_idle(Duration::from_millis(1))
        .clone();
    compio::runtime::RuntimeBuilder::new()
        .event_interval(29)
        .with_proactor(proactor)
        .build()
        .unwrap()
        .block_on(async_main());
}

async fn async_main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    let log_path = "borrow_self.log";
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
        .with_line_number(false)
        .with_file(false)
        .compact()
        .with_writer(Mutex::new(BufWriter::with_capacity(196, log_file)))
        .try_init();

    let _guard = tracing::info_span!("main");

    let addr = "127.0.0.1:7002".parse().unwrap();
    let (client, server) = make_pair(addr).await;
    let server = server.serve();
    let ctrl_c = compio::signal::ctrl_c();

    {
        let session = client.connect(addr, "localhost").await.unwrap();
        info!("connected to {}", session.remote_address());

        let usb = session.req_borrow(DEV).await.unwrap();
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
                _ = handle.await.unwrap().inspect_err(|err| error! { %err });
                Event::Completed
            }
        };

        match (cancel, timer, completion).race().await {
            Event::Cancelled => {
                tracing::info!("borrow cancelled, waiting for session to complete");
                cancel_token.cancel();
                _ = handle.await.unwrap().inspect_err(|err| error! { %err });
            }
            Event::Completed => {}
        }

        info!("client done!");
    }

    info!("waiting for server to complete");
    server.shutdown().await.unwrap().unwrap();
    info!("server done!");
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

async fn make_pair(addr: SocketAddr) -> (qusb::Client, qusb::Server) {
    let (server, cert) = make_self_signed();
    let mut certs = rustls::RootCertStore::empty();
    certs.add(cert).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(Arc::new(certs))
        .with_no_client_auth();
    let mut transport = compio::quic::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(10)));

    qusb::peer(
        server,
        client,
        Some(addr),
        transport,
        BoundedU8::new(4).unwrap(),
    )
    .await
}
