use std::{pin::Pin, sync::Arc};

use quinn::rustls;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
pub use vhci::utils::*;

use crate::Error;

// #[derive(Debug)]
// pub struct BorrowedBuffer<'a> {
//     pub data: &'a mut [u8],
//     pub index: usize,
// }

// impl Buf for BorrowedBuffer<'_> {
//     #[tracing::instrument(level = "trace")]
//     fn remaining(&self) -> usize {
//         self.data.len() - self.index
//     }

//     #[tracing::instrument(level = "trace")]
//     fn chunk(&self) -> &[u8] {
//         &self.data[self.index..]
//     }

//     #[tracing::instrument(level = "trace")]
//     fn advance(&mut self, cnt: usize) {
//         if cnt > self.remaining() {
//             panic!("cannot advance buffer that far")
//         }
//         self.index += cnt;
//     }
// }

#[tracing::instrument(level = "trace", skip(writer))]
pub async fn serialize_into_writer<T: Serialize + std::fmt::Debug, W: AsyncWrite + Unpin>(
    item: &T,
    writer: &mut W,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    let msg = loop {
        // tracing::trace!("Trying to serialize a message to the buffer");
        match postcard::to_slice_cobs(item, buf) {
            Ok(msg) => break msg,
            Err(postcard::Error::SerializeBufferFull) => {
                // tracing::trace!("Buffer got full, resizing the buffer and trying again");
                buf.resize(buf.len() + 512, 0);
            }
            Err(err) => Err(err)?,
        }
    };
    // tracing::trace!("Success! Now I'll try to write this message into the buffer");
    writer.write_all(msg).await?;
    buf.clear();
    Ok(())
}

#[tracing::instrument(level = "trace", skip(reader))]
pub async fn deserialize_from_reader<
    T: DeserializeOwned + std::fmt::Debug,
    R: AsyncBufRead + Sized + Unpin,
>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Option<T>, Error> {
    tracing::trace!("Trying to read a message from the buffer");
    if 0 == reader.read_until(0, buf).await? {
        tracing::trace!("No bytes read! Must be EOF");
        return Ok(None);
    }
    tracing::trace!("Success! buf: {buf:?} - Now attempting to deserialize the message");
    let recv: T = postcard::from_bytes_cobs(buf)?;
    buf.clear();
    Ok(Some(recv))
}

#[derive(Debug)]
pub struct RWStream {
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
