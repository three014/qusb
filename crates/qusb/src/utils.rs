use crate::rustls;
use nohash_hasher::BuildNoHashHasher;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, f64::consts::SQRT_2, sync::Arc, time::Duration};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
pub use vhci::utils::{ClosedBoundedI16, OpenBoundedU8, TimeoutMillis};

use crate::Error;

pub type SimpleMap<K, V> = HashMap<K, V, BuildNoHashHasher<K>>;

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

/*
void preciseSleep(double seconds) {
    using namespace std;
    using namespace std::chrono;

    static double estimate = 5e-3;
    static double mean = 5e-3;
    static double m2 = 0;
    static int64_t count = 1;

    while (seconds > estimate) {
        auto start = high_resolution_clock::now();
        this_thread::sleep_for(milliseconds(1));
        auto end = high_resolution_clock::now();

        double observed = (end - start).count() / 1e9;
        seconds -= observed;

        ++count;
        double delta = observed - mean;
        mean += delta / count;
        m2   += delta * (observed - mean);
        double stddev = sqrt(m2 / (count - 1));
        estimate = mean + stddev;
    }

    // spin lock
    auto start = high_resolution_clock::now();
    while ((high_resolution_clock::now() - start).count() / 1e9 < seconds);
}
*/

pub(crate) fn precise_sleep(mut seconds: f64) {
    let clock = quanta::Clock::new();
    let mut estimate = 5e-3;
    let mut mean = 5e-3;
    let mut m2 = 0.0;
    let mut count = 1;

    while seconds > estimate {
        let start = clock.now();
        std::thread::sleep(std::time::Duration::from_millis(1));

        let observed = start.elapsed().as_secs_f64();
        seconds -= observed;

        count += 1;
        let delta = observed - mean;
        mean += delta / count as f64;
        m2 += delta * (observed - mean);
        let stddev = (m2 / (count - 1) as f64).sqrt();
        estimate = mean + stddev;
    }

    // spin lock
    let start = clock.now();
    while start.elapsed().as_secs_f64() < seconds {}
}

#[tracing::instrument(level = "trace", skip(writer))]
pub(crate) async fn serialize_into_writer<T: Serialize + std::fmt::Debug, W: AsyncWrite + Unpin>(
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
pub(crate) async fn deserialize_from_reader<
    T: DeserializeOwned + std::fmt::Debug,
    R: AsyncBufRead + Sized + Unpin,
>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Option<T>, Error> {
    // tracing::trace!("Trying to read a message from the buffer");
    if 0 == reader.read_until(0, buf).await? {
        // tracing::trace!("No bytes read! Must be EOF");
        return Ok(None);
    }
    // tracing::trace!("Success! buf: {buf:?} - Now attempting to deserialize the message");
    let recv: T = postcard::from_bytes_cobs(buf)?;
    buf.clear();
    Ok(Some(recv))
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
