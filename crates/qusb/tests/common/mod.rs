use quinn::rustls;
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use vhci::utils::BoundedU8;

pub fn addr(port: u16) -> SocketAddr {
    (Ipv4Addr::new(127, 0, 0, 1), port).into()
}

pub fn setup(addr: SocketAddr) -> (qusb::Client, qusb::Server) {
    let (server, cert) = qusb::utils::make_self_signed();
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
