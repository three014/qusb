use std::{
    fs,
    io::{stdout, BufWriter},
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::{arg, Command};
use qusb::{
    quinn,
    rustls::{self, pki_types::pem::PemObject},
    BoundedU8,
};
use rcgen;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_keep_alive(Duration::from_secs(60))
        .build()?;

    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let matches = cli().get_matches();

    let log_path = "qusb-cli.log";
    let _log_file = std::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(log_path)?;
    _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::builder().parse("rusb=trace,qusb=trace").unwrap())
        .with_line_number(true)
        .with_writer(Mutex::new(BufWriter::with_capacity(128, _log_file)))
        .try_init();

    match matches.subcommand() {
        Some(("serve", sub_matches)) => {
            let bind: SocketAddr = sub_matches
                .get_one("bind_addr")
                .cloned()
                .unwrap_or_else(|| "[::]:7400".parse().unwrap());
            let make_self_signed: bool = sub_matches.get_flag("make_self_signed");
            let allow_insecure: bool = sub_matches.get_flag("allow_insecure");
            let num_ports = BoundedU8::new(4).unwrap();
            let conf_dir = match sub_matches.get_one::<String>("config_dir") {
                Some(dir) => PathBuf::from(dir),
                None => std::env::current_dir()?,
            };

            let (server_cfg, cert) = if make_self_signed {
                let (cfg, cert_key) = make_self_signed_cfg();
                let key_pair = cert_key.key_pair.serialize_pem();
                let cert = cert_key.cert.pem();
                fs::write("server.pem", cert)?;
                fs::write("server.key", key_pair)?;
                (cfg, cert_key)
            } else {
                let key_pair = fs::read(conf_dir.join("server.key"))?;
                let cert =
                    rustls::pki_types::CertificateDer::pem_file_iter(conf_dir.join("server.pem"))?;
                todo!()
            };

            println!("config directory: {}", conf_dir.display());
            println!(
                "bind address: {bind}",
            );
            println!("make and use self-signed certificate: {make_self_signed}");
            println!("allow insecure connections: {allow_insecure}");

            let mut certs = rustls::RootCertStore::empty();
            certs
                .add(rustls::pki_types::CertificateDer::from(cert.cert))
                .unwrap();
            let client = rustls::ClientConfig::builder()
                .with_root_certificates(Arc::new(certs))
                .with_no_client_auth();
            let mut transport = quinn::TransportConfig::default();
            transport.keep_alive_interval(Some(Duration::from_secs(10)));

            let (_, server) = qusb::peer(server_cfg, client, Some(bind), transport, num_ports);

            let handle = server.serve();
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for event");

            handle.shutdown().await.unwrap()?;
            Ok(())
        }
        _ => todo!(),
    }
}

fn cli() -> Command {
    Command::new("qusb")
        .about("A USB/IP implementation using the QUIC protocol.")
        .author("Aaron Perez, aap7640@gmail.com")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("serve")
                .about("Listen for requests from client peers")
                .arg(
                    arg!(bind_addr: [BIND_ADDR] "Local address to listen for requests")
                        .value_parser(SocketAddr::from_str),
                )
                .arg(arg!(config_dir: -f --"conf-dir" [DIR] "Directory to store configuration files, client/server certificates, etc."))
                .arg(arg!(make_self_signed: --"use-self-signed-certs" "Create and sign certificates for this session only. The created certificates are output in the current working directory."))
                .arg(arg!(allow_insecure: --"allow-insecure" "Allow connecting to peers without verifying the peer's certificate chain"))
        )
}

/// Convenience function for making self-signed certificates.
///
/// Probably don't use in production environments?
pub fn make_self_signed_cfg() -> (rustls::ServerConfig, rcgen::CertifiedKey) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let priv_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone()], priv_key.into())
    .unwrap();
    (server, cert)
}

mod danger {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use qusb::{quinn, rustls, BoundedU8};

    pub fn dummy_trusting_client(
        addr: Option<SocketAddr>,
        num_ports: BoundedU8<1, 32>,
    ) -> qusb::Client {
        let (server, _) = super::make_self_signed_cfg();
        let client = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(Duration::from_secs(10)));

        let (client, _) = qusb::peer(server, client, addr, transport, num_ports);

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
}
