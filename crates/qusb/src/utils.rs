use crate::rustls;
use nohash_hasher::BuildNoHashHasher;
use std::{collections::HashMap, future::Future, hash::Hash, io, sync::Arc};
use tokio::sync::oneshot;

pub type SimpleMap<K, V> = HashMap<K, V, BuildNoHashHasher<K>>;

pub struct NoHash<T>(pub T);
impl<T: Clone> Clone for NoHash<T> {
    fn clone(&self) -> Self {
        NoHash(self.0.clone())
    }
}
impl<T: Copy> Copy for NoHash<T> {}
impl<T: Hash> Hash for NoHash<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl<T: Eq> Eq for NoHash<T> {}
impl<T: PartialEq> PartialEq for NoHash<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl nohash_hasher::IsEnabled for NoHash<quinn::StreamId> {}

pub struct Ctrl<S, R = (), E = std::io::Error> {
    pub data: S,
    pub(crate) tx: oneshot::Sender<Result<R, E>>,
}

impl<S, R, E> Ctrl<S, R, E> {
    pub(crate) fn new(data: S) -> (oneshot::Receiver<Result<R, E>>, Ctrl<S, R, E>) {
        let (tx, rx) = oneshot::channel();
        let ctrl = Self { data, tx };
        (rx, ctrl)
    }
}

pub trait CloseStream {
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

impl CloseStream for quinn::SendStream {
    async fn close(&mut self) -> io::Result<()> {
        self.finish().map_err(io::Error::from)?;
        self.stopped().await.map_err(io::Error::from)?;
        Ok(())
    }
}

// /// A synchronous sleep function that uses
// /// spinlocking for small sleep durations.
// ///
// /// Credit goes to [Blat Blatnik](https://blog.bearcats.nl/accurate-sleep-function/)
// /// for the implementation.
// pub(crate) fn precise_sleep(clock: &quanta::Clock, mut seconds: f64) {
//     let mut estimate = 5e-3;
//     let mut mean = 5e-3;
//     let mut m2 = 0.0;
//     let mut count = 1;

//     while seconds > estimate {
//         let start = clock.now();
//         std::thread::sleep(std::time::Duration::from_millis(1));

//         let observed = start.elapsed().as_secs_f64();
//         seconds -= observed;

//         count += 1;
//         let delta = observed - mean;
//         mean += delta / count as f64;
//         m2 += delta * (observed - mean);
//         let stddev = (m2 / (count - 1) as f64).sqrt();
//         estimate = mean + stddev;
//     }

//     // spin lock
//     let start = clock.now();
//     while start.elapsed().as_secs_f64() < seconds {}
// }

// /// Serializes an object that implements [`serde::Serialize`] into an async writer.
// /// Uses a user-provided buffer to prevent unneeded allocations, but does reallocate
// /// the buffer if not big enough to hold the serialized data.
// #[tracing::instrument(level = "trace", skip(writer))]
// pub(crate) async fn serialize_into_writer<T, W>(
//     item: &T,
//     writer: &mut W,
//     buf: &mut Vec<u8>,
// ) -> Result<(), Error>
// where
//     T: Serialize + std::fmt::Debug,
//     W: AsyncWrite + Unpin,
// {
//     let msg = loop {
//         match postcard::to_slice_cobs(item, buf) {
//             Ok(msg) => break msg,
//             Err(postcard::Error::SerializeBufferFull) => {
//                 buf.resize(buf.len() + 512, 0);
//             }
//             Err(err) => Err(err)?,
//         }
//     };
//     writer.write_all(msg).await?;
//     buf.clear();
//     Ok(())
// }

// /// Deserializes an object that implements [`serde::Deserialize`] from an async bufreader.
// ///
// /// Returns `None` if the reader doesn't provide any data.
// #[tracing::instrument(level = "trace", skip(reader))]
// pub(crate) async fn deserialize_from_reader<T, R>(
//     reader: &mut R,
//     buf: &mut Vec<u8>,
// ) -> Result<Option<T>, Error>
// where
//     T: DeserializeOwned + std::fmt::Debug,
//     R: AsyncBufRead + Sized + Unpin,
// {
//     if 0 == reader.read_until(0, buf).await? {
//         return Ok(None);
//     }
//     let recv: T = postcard::from_bytes_cobs(buf)?;
//     buf.clear();
//     Ok(Some(recv))
// }

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
