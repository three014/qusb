use bitflags::bitflags;
use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls,
};
use rand::random;
use std::{
    collections::HashMap,
    hash::BuildHasherDefault,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    path::Path,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufStream, BufWriter,
};
use usb_ids::UsbIds;

mod usb_ids;

static USB_IDS: LazyLock<UsbIds> =
    LazyLock::new(|| usb_ids::parse(Path::new("./usb-ids")).unwrap());

#[derive(Debug)]
struct RWStream {
    tx: quinn::SendStream,
    rx: quinn::RecvStream,
}

impl AsyncRead for RWStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.rx).poll_read(cx, buf)
    }
}

impl AsyncWrite for RWStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        <quinn::SendStream as AsyncWrite>::poll_write(Pin::new(&mut self.tx), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        <quinn::SendStream as AsyncWrite>::poll_flush(Pin::new(&mut self.tx), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        <quinn::SendStream as AsyncWrite>::poll_shutdown(Pin::new(&mut self.tx), cx)
    }
}

#[derive(Debug)]
struct ReqLine {
    tx: Mutex<BufWriter<quinn::SendStream>>,
    rx: Mutex<BufReader<quinn::RecvStream>>,
}

#[derive(Debug)]
struct Session {
    conn: quinn::Connection,
    req_line: ReqLine,
    req_data: Mutex<HashMap<spec::ReqId, spec::Req, nohash_hasher::BuildNoHashHasher<spec::ReqId>>>,
    unclaimed_unidata_lines: HashMap<
        spec::PayloadId,
        BufReader<quinn::RecvStream>,
        nohash_hasher::BuildNoHashHasher<spec::PayloadId>,
    >,
    unclaimed_bidata_lines: HashMap<
        spec::PayloadId,
        BufStream<RWStream>,
        nohash_hasher::BuildNoHashHasher<spec::PayloadId>,
    >,
}

impl Session {
    pub async fn send_req(&self, op: spec::Operation) -> spec::ReqId {
        let id = spec::ReqId(random());
        let req = postcard::to_vec_cobs::<_, 32>(&spec::Req { id, op }).unwrap();
        let mut tx = self.req_line.tx.lock().unwrap();
        let mut written = 0;
        while written < req.len() {
            written += tx.write(&req[written..]).await.unwrap();
        }
        tx.flush().await.unwrap();

        id
    }

    pub async fn recv_req(&self, id: spec::ReqId) -> spec::Req {
        if let Some(req) = self.req_data.lock().unwrap().remove(&id) {
            return req;
        }

        let mut buf = vec![0; 256];
        loop {
            buf.clear();
            let _read = self
                .req_line
                .rx
                .lock()
                .unwrap()
                .read_until(0, &mut buf)
                .await
                .unwrap();

            let req: spec::Req = postcard::from_bytes_cobs(&mut buf).unwrap();
            if req.id == id {
                return req;
            }
            self.req_data.lock().unwrap().insert(id, req);
        }
    }

    async fn connect(conn: quinn::Connecting) -> Self {
        let conn = conn.await.unwrap();
        let (mut tx, rx) = conn.open_bi().await.unwrap();
        let header = postcard::to_vec_cobs::<_, 32>(&spec::Header {
            version: spec::VERSION,
            stream_type: spec::StreamType::Req,
        })
        .unwrap();
        let mut written = 0;
        while written < header.len() {
            written += tx.write(&header[written..]).await.unwrap();
        }

        Self {
            conn,
            req_line: ReqLine {
                tx: Mutex::new(BufWriter::with_capacity(1024, tx)),
                rx: Mutex::new(BufReader::with_capacity(1024, rx)),
            },
            req_data: Mutex::new(HashMap::with_hasher(BuildHasherDefault::default())),
            unclaimed_unidata_lines: HashMap::with_hasher(BuildHasherDefault::default()),
            unclaimed_bidata_lines: HashMap::with_hasher(BuildHasherDefault::default()),
        }
    }

    async fn accept(conn: quinn::Incoming) -> Self {
        let conn = conn.await.unwrap();
        let (tx, rx) = conn.accept_bi().await.unwrap();

        let mut rx = BufReader::with_capacity(1024, rx);
        let mut buf = vec![0; 128];
        let _read = rx.read_until(0, &mut buf).await.unwrap();
        let header: spec::Header = postcard::from_bytes_cobs(&mut buf).unwrap();
        if header.version != spec::VERSION {
            panic!("Wrong version!");
        }

        if header.stream_type != spec::StreamType::Req {
            panic!("First stream was not a request!");
        }

        Self {
            conn,
            req_line: ReqLine {
                tx: Mutex::new(BufWriter::with_capacity(1024, tx)),
                rx: Mutex::new(rx),
            },
            req_data: Mutex::new(HashMap::with_hasher(BuildHasherDefault::default())),
            unclaimed_unidata_lines: HashMap::with_hasher(BuildHasherDefault::default()),
            unclaimed_bidata_lines: HashMap::with_hasher(BuildHasherDefault::default()),
        }
    }
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
        let conn = self
            .endpoint
            .connect(peer_addr, peer_name)
            .unwrap()
            .await
            .unwrap();

        let (mut send, mut recv) = conn.open_bi().await.unwrap();

        // Ask the peer for what devices they have to offer
        // let req = QusbReq {
        //     version: spec::VERSION,
        //     req: Req::ListDevices,
        // };

        // let mut buf = postcard::to_vec::<_, 64>(&req).unwrap();
        // let mut written = 0;
        // while written < buf.len() {
        //     written += send.write(&buf[written..]).await.unwrap();
        // }
        // send.finish().unwrap();

        // let mut read = 0;
        // while let Some(bytes) = recv.read(&mut buf[read..]).await.unwrap() {

        //     read += bytes;
        // }
        // buf.truncate(read);

        // let resp = postcard::from_bytes::<QusbResp>(&*buf).unwrap();

        // if resp.version != spec::VERSION {
        //     panic!("Wrong verison! Them: {}, Us: {}", resp.version, spec::VERSION);
        // }

        // let payload_id = resp.resp.unwrap();

        // let a = conn.accept_uni().await.unwrap();

        // All streams need to include the Qusb Header:
        // Version: u16
        // Id: u64
        //
        // QusbPeer will (try to) act as a middleware between
        // all streams in a connection by storing new connections
        // in a lost+found.
        //
        // Let's imagine a connection with streams A and B. Stream A
        // gets told by its peer that there's more data in a new stream.
        // Stream A waits for the new stream, but upon connecting, sees
        // that it's the wrong stream and it doesn't have the right
        // data. So, Stream A gives this new stream "C" back to QusbPeer.
        // Meanwhile, Stream B got told by its peer that there's more
        // data in a new stream, so it waits for a stream. However,
        // Stream B can ask QusbPeer for a stream that matches the
        // data it needs, which in this case was actually Stream C.
        // QusbPeer then gives Stream C to Stream B.
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
