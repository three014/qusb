use std::{
    fs,
    io::stdout,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::{arg, Command};
use qusb::{quinn, rustls, BoundedU8};
use rcgen;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_keep_alive(Duration::from_secs(60))
        .build()?;

    rt.block_on(async_main())
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
        .with_env_filter(EnvFilter::builder().parse("none,qusb=trace").unwrap())
        .with_line_number(true)
        .with_writer(Mutex::new(stdout()))
        .try_init();

    match matches.subcommand() {
        Some(("serve", sub_matches)) => {
            let bind: Option<SocketAddr> = sub_matches.get_one("bind_addr").cloned();
            let make_self_signed: bool = sub_matches.get_flag("make_self_signed");
            let num_ports = BoundedU8::new(4).unwrap();
            let conf_dir = if let Some(dir) = sub_matches.get_one::<String>("config_dir") {
                PathBuf::from(dir)
            } else {
                std::env::current_dir()?
            };

            let (server_cfg, cert) = if make_self_signed {
                let (cfg, cert_key) = make_self_signed_cfg();
                let key_pair = cert_key.key_pair.serialize_pem();
                let cert = cert_key.cert.pem();
                fs::write("server.pem", cert + &key_pair)?;
                (cfg, cert_key)
            } else {
                let cert_path = {
                    let mut dir = conf_dir.clone();
                    dir.set_file_name("server.pem");
                    dir
                };
                let cert_key = fs::read(cert_path)?;
                todo!()
            };

            println!("config directory: {}", conf_dir.display());
            println!(
                "bind address: {}",
                bind.unwrap_or("[::]:7400".parse().unwrap())
            );
            println!("make and use self-signed certificate: {}", make_self_signed);

            let mut certs = rustls::RootCertStore::empty();
            certs
                .add(rustls::pki_types::CertificateDer::from(cert.cert))
                .unwrap();
            let client = rustls::ClientConfig::builder()
                .with_root_certificates(Arc::new(certs))
                .with_no_client_auth();
            let mut transport = quinn::TransportConfig::default();
            transport.keep_alive_interval(Some(Duration::from_secs(10)));

            let (_, server) = qusb::peer(server_cfg, client, bind, transport, num_ports);

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
                .arg(arg!(make_self_signed: --"use-self-signed-certs" "Create and sign certificates for this session only. The created certificates are output in the current working directory."))
                .arg(arg!(config_dir: -f --"conf-dir" [DIR] "Directory to store configuration files, client/server certificates, etc."))
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
