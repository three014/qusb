use bitflags::bitflags;
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

mod usb_ids;
mod utils;
mod stream;

pub type Sender<T> = stream::Sender<T, quinn::SendStream>;
pub type Receiver<T> = stream::Receiver<spec::Response<T>, BufReader<quinn::RecvStream>>;

mod state {
    use serde::{de::DeserializeOwned, Serialize};
    use tokio::io::BufReader;
    use crate::{stream, Error};

    pub struct VersionSender(pub(crate) crate::Sender<spec::Version>);
    impl VersionSender {
        pub async fn send(mut self) -> Result<ReqSender, Error> {
            self.0.send(&spec::VERSION).await?;
            Ok(ReqSender(self.0.convert()))
        }
    }

    pub struct VersionReceiver(pub(crate) stream::Receiver<spec::Version, BufReader<quinn::RecvStream>>);
    impl VersionReceiver {
        pub async fn recv<T: DeserializeOwned>(mut self) -> Result<stream::Receiver<T, BufReader<quinn::RecvStream>>, Error> {
            let version = self.0.recv().await?;
            if version != spec::VERSION {
                Err(Error::VersionMismatch(version))
            } else {
                Ok(self.0.convert::<T>())
            }
        }
    }

    pub struct RespSender<T: Serialize>(crate::Sender<spec::Response<T>>);
    impl<T: Serialize> RespSender<T> {
        pub async fn send_data(&mut self, data: T) -> Result<(), Error> {
            self.0.send(&Ok(data)).await
        }

        pub async fn send_err(mut self, data: spec::Error) -> Result<(), Error> {
            self.0.send(&Err(data)).await
        }

        pub fn convert<R: Serialize>(self) -> RespSender<R> {
            RespSender(self.0.convert())
        }
    }

    pub struct RespReceiver<T: DeserializeOwned>(pub(crate) crate::Receiver<T>);
    impl<T: DeserializeOwned> RespReceiver<T> {
        pub async fn recv(&mut self) -> Result<spec::Response<T>, Error> {
            self.0.recv().await
        }

        pub fn convert<R: DeserializeOwned>(self) -> RespReceiver<R> {
            RespReceiver(self.0.convert())
        }
    }

    pub struct ReqSender(crate::Sender<spec::Request>);
    impl ReqSender {
        pub async fn send<T: Serialize>(mut self, req: spec::Request) -> Result<crate::Sender<T>, Error> {
            self.0.send(&req).await?;
            Ok(self.0.convert::<T>())
        }
    }

    // TODO: Add ReqReceiver
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Serde(#[from] postcard::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported protocol version (their version: {0}, our version: {ver})", ver = spec::VERSION)]
    VersionMismatch(spec::Version)
}

static USB_IDS: LazyLock<UsbIds> =
    LazyLock::new(|| usb_ids::parse(Path::new("./usb-ids")).unwrap());


#[derive(Debug)]
struct Session {
    conn: quinn::Connection,
}

impl Session {
    pub async fn open_stream<T: Serialize, R: DeserializeOwned>(
        &self,
    ) -> Result<(state::ReqSender, state::RespReceiver<R>), quinn::ConnectionError> {
        let (tx, rx) = self.conn.open_bi().await?;
        let (tx, rx) = stream::new::<spec::Version, spec::Response<()>>(tx, rx);

        let tx = state::VersionSender(tx).send().await.unwrap();
        let mut rx = state::RespReceiver::<()>(rx);
        if let Err(_err) = rx.recv().await.unwrap() {
            panic!("something happened")
        }
        Ok((tx, rx.convert()))
    }

    pub async fn accept_stream<T: Serialize, R: DeserializeOwned>(
        &self,
    ) -> Result<(state::RespSender<T>, Receiver<R>), quinn::ConnectionError> {
        let (tx, rx) = self.conn.accept_bi().await?;
        // Ok(stream::new(tx, rx))
        todo!()
    }
    // pub async fn open_ctrl(&self) -> ctrl::Requester {
    //     let (tx, rx) = self.conn.open_bi().await.unwrap();
    //     ctrl::ConnectingReq::new(tx, rx).handshake().await
    // }

    // pub async fn accept_ctrl(&self) -> ctrl::Responder {
    //     let (tx, rx) = self.conn.accept_bi().await.unwrap();
    //     ctrl::ConnectingResp::new(tx, rx).handshake().await
    // }

    // pub async fn send_req(&self, req: spec::Req) -> spec::ReqId {
    //     // let id = req.id;
    //     let req = postcard::to_vec_cobs::<_, 32>(&spec::Control::Request(req)).unwrap();
    //     let mut tx = self.ctrl.tx.lock().unwrap();
    //     let mut written = 0;
    //     while written < req.len() {
    //         written += tx.write(&req[written..]).await.unwrap();
    //     }
    //     tx.flush().await.unwrap();

    //     //id
    //     todo!()
    // }

    // pub async fn send_resp_bi(
    //     &self,
    //     resp: spec::Resp,
    // ) -> Option<(quinn::SendStream, quinn::RecvStream)> {
    //     let data_tx = if let Ok(Some((payload, dir))) = &resp.payload {
    //         match dir {
    //             spec::Dir::Bi => {
    //                 let header = postcard::to_vec_cobs::<_, 32>(&spec::Header {
    //                     version: spec::VERSION,
    //                     stream_type: spec::StreamType::Data(*payload),
    //                 })
    //                 .unwrap();
    //                 let (mut tx, rx) = self.conn.open_bi().await.unwrap();
    //                 let mut written = 0;
    //                 while written < header.len() {
    //                     written += tx.write(&header[written..]).await.unwrap();
    //                 }
    //                 Some((tx, rx))
    //             }
    //             _ => panic!("Wrong direction"),
    //         }
    //     } else {
    //         None
    //     };
    //     let resp = postcard::to_vec_cobs::<_, 32>(&spec::Control::Response(resp)).unwrap();
    //     let mut tx = self.ctrl.tx.lock().unwrap();
    //     let mut written = 0;
    //     while written < resp.len() {
    //         written += tx.write(&resp[written..]).await.unwrap();
    //     }
    //     tx.flush().await.unwrap();

    //     data_tx
    // }

    // pub async fn send_resp_uni(&self, resp: spec::Resp) -> Option<quinn::SendStream> {
    //     let data_tx = if let Ok(Some((payload, dir))) = &resp.payload {
    //         match dir {
    //             spec::Dir::Uni => {
    //                 let header = postcard::to_vec_cobs::<_, 32>(&spec::Header {
    //                     version: spec::VERSION,
    //                     stream_type: spec::StreamType::Data(*payload),
    //                 })
    //                 .unwrap();
    //                 let mut tx = self.conn.open_uni().await.unwrap();
    //                 let mut written = 0;
    //                 while written < header.len() {
    //                     written += tx.write(&header[written..]).await.unwrap();
    //                 }
    //                 Some(tx)
    //             }
    //             _ => panic!("Wrong direction"),
    //         }
    //     } else {
    //         None
    //     };
    //     let resp = postcard::to_vec_cobs::<_, 32>(&spec::Control::Response(resp)).unwrap();
    //     let mut tx = self.ctrl.tx.lock().unwrap();
    //     let mut written = 0;
    //     while written < resp.len() {
    //         written += tx.write(&resp[written..]).await.unwrap();
    //     }
    //     tx.flush().await.unwrap();

    //     data_tx
    // }

    // pub async fn recv_resp_uni(&self, id: spec::ReqId) -> Option<BufReader<quinn::RecvStream>> {
    //     // BUG: What happens when function call "A" checks for their response object "A_r" as
    //     //      function call "B" is currently receiving "A_r", then putting
    //     //      "A_r" into the unclaimed bin after checking.
    //     let resp = if let Some(resp) = self.ctrl_data.resp.lock().unwrap().remove(&id) {
    //         resp
    //     } else {
    //         let mut buf = Vec::with_capacity(256);
    //         loop {
    //             buf.clear();
    //             let _read = self
    //                 .ctrl
    //                 .rx
    //                 .lock()
    //                 .unwrap()
    //                 .read_until(0, &mut buf)
    //                 .await
    //                 .unwrap();

    //             match postcard::from_bytes_cobs(&mut buf).unwrap() {
    //                 spec::Control::Request(req) => {
    //                     // self.ctrl_data.req.lock().unwrap().insert(id, req);
    //                 }
    //                 spec::Control::Response(resp) => {
    //                     // if resp.id == id {
    //                     //     break resp;
    //                     // }
    //                     // self.ctrl_data.resp.lock().unwrap().insert(id, resp);
    //                 }
    //             }
    //         }
    //     };

    //     if let Some((resp_payload_id, dir)) = resp.payload.unwrap() {
    //         match dir {
    //             spec::Dir::Uni => loop {
    //                 let mut rx = BufReader::new(self.conn.accept_uni().await.unwrap());
    //                 let mut buf = Vec::new();
    //                 let _read = rx.read_until(0, &mut buf).await.unwrap();

    //                 let header: spec::Header = postcard::from_bytes_cobs(&mut buf).unwrap();
    //                 if header.version != spec::VERSION {
    //                     panic!("wrong version!");
    //                 }

    //                 match header.stream_type {
    //                     spec::StreamType::Data(new_stream_id) => {
    //                         if new_stream_id == resp_payload_id {
    //                             break Some(rx);
    //                         } else {
    //                             self.unclaimed_unidata_lines
    //                                 .lock()
    //                                 .unwrap()
    //                                 .insert(new_stream_id, rx);
    //                         }
    //                     }
    //                     _ => panic!("expected a data stream"),
    //                 }
    //             },
    //             _ => panic!("wrong direction"),
    //         }
    //     } else {
    //         None
    //     }
    // }

    // pub async fn recv_req(&self, id: spec::ReqId) -> spec::Req {
    //     if let Some(req) = self.ctrl_data.req.lock().unwrap().remove(&id) {
    //         req
    //     } else {
    //         let mut buf = Vec::with_capacity(256);
    //         loop {
    //             buf.clear();
    //             let _read = self
    //                 .ctrl
    //                 .rx
    //                 .lock()
    //                 .unwrap()
    //                 .read_until(0, &mut buf)
    //                 .await
    //                 .unwrap();

    //             match postcard::from_bytes_cobs(&mut buf).unwrap() {
    //                 spec::Control::Request(req) => {
    //                     // if req.id == id {
    //                     //     return req;
    //                     // }
    //                     // self.ctrl_data.req.lock().unwrap().insert(id, req);
    //                 }
    //                 spec::Control::Response(resp) => {
    //                     // self.ctrl_data.resp.lock().unwrap().insert(id, resp);
    //                 }
    //             }
    //         }
    //     }
    // }

    // async fn connect(conn: quinn::Connecting) -> Self {
    //     let conn = conn.await.unwrap();
    //     let (mut tx, rx) = conn.open_bi().await.unwrap();
    //     let header = postcard::to_vec_cobs::<_, 32>(&spec::Header {
    //         version: spec::VERSION,
    //         stream_type: spec::StreamType::Ctrl,
    //     })
    //     .unwrap();
    //     let mut written = 0;
    //     while written < header.len() {
    //         written += tx.write(&header[written..]).await.unwrap();
    //     }

    //     Self {
    //         conn,
    //         ctrl: ControlLine {
    //             tx: Mutex::new(BufWriter::with_capacity(1024, tx)),
    //             rx: Mutex::new(BufReader::with_capacity(1024, rx)),
    //         },
    //         ctrl_data: ControlData {
    //             req: Mutex::new(HashMap::with_hasher(BuildHasherDefault::default())),
    //             resp: Mutex::new(HashMap::with_hasher(BuildHasherDefault::default())),
    //         },
    //         unclaimed_unidata_lines: Mutex::new(
    //             HashMap::with_hasher(BuildHasherDefault::default()),
    //         ),
    //         unclaimed_bidata_lines: Mutex::new(HashMap::with_hasher(BuildHasherDefault::default())),
    //     }
    // }

    async fn new(conn: quinn::Connection) -> Result<Self, quinn::ConnectionError> {
        Ok(Self { conn })
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
