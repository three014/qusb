use std::{convert::Infallible, io};

use bytes::{Buf, Bytes};
use tokio::sync::mpsc;
use zerocopy::{self, little_endian::U64, FromBytes};

use crate::utils::{Ctrl, NoHash, SimpleMap};

#[derive(Debug)]
pub struct Handle {
    pub handle: tokio::task::JoinHandle<std::io::Result<()>>,
    pub register_tx: mpsc::Sender<Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>, Infallible>>,
    pub disconnect_tx: mpsc::Sender<Ctrl<quinn::StreamId, (), Infallible>>,
    pub conn: quinn::Connection,
}

impl Handle {
    pub fn make_channel(&self, id: quinn::StreamId) -> Option<(Sender, Receiver)> {
        let (rx, ctrl) = Ctrl::new(id);
        self.register_tx.blocking_send(ctrl).ok()?;
        let iso_rx = rx.blocking_recv().ok()?.unwrap();
        let iso_tx = self.conn.clone();

        Some((Sender { iso_tx }, Receiver { iso_rx }))
    }
}

pub struct Sender {
    iso_tx: quinn::Connection,
}

pub struct Receiver {
    iso_rx: mpsc::Receiver<Bytes>,
}

pub struct Demuxer {
    pub register_rx: mpsc::Receiver<Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>, Infallible>>,
    pub disconnect_rx: mpsc::Receiver<Ctrl<quinn::StreamId, (), Infallible>>,
    pub conn: quinn::Connection,
}

impl Demuxer {
    pub async fn run(self) -> std::io::Result<()> {
        let Self {
            mut register_rx,
            mut disconnect_rx,
            conn,
        } = self;

        let mut mailer = SimpleMap::<NoHash<quinn::StreamId>, mpsc::Sender<Bytes>>::default();

        enum Event {
            Register(Option<Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>, Infallible>>),
            Datagram(Result<Bytes, quinn::ConnectionError>),
            Disconnect(Option<Ctrl<quinn::StreamId, (), Infallible>>),
        }

        loop {
            let select = tokio::select! {
                req = register_rx.recv() => {
                    Event::Register(req)
                }
                datagram = conn.read_datagram() => {
                    Event::Datagram(datagram)
                }
                req = disconnect_rx.recv() => {
                    Event::Disconnect(req)
                }
            };

            match select {
                Event::Register(Some(Ctrl { data: id, tx })) => {
                    let (iso_tx, iso_rx) = mpsc::channel(32);
                    mailer.insert(NoHash(id), iso_tx);
                    if tx.send(Ok(iso_rx)).is_err() {
                        mailer.remove(&NoHash(id));
                    }
                }
                Event::Datagram(Ok(mut bytes)) => {
                    if let Ok((id, _)) = U64::read_from_prefix(&bytes) {
                        bytes.advance(std::mem::size_of_val(&id));
                        let id = NoHash(quinn::StreamId(id.get()));
                        if let Some(tx) = mailer.get(&id) {
                            if tx.send(bytes).await.is_err() {
                                mailer.remove(&id);
                            }
                        }
                    }
                }
                Event::Disconnect(Some(Ctrl { data: id, tx })) => {
                    mailer.remove(&NoHash(id));
                    let _ = tx.send(Ok(()));
                }
                Event::Register(None) | Event::Disconnect(None) => break,
                Event::Datagram(Err(err)) => return Err(io::Error::from(err)),
            }
        }
        Ok(())
    }
}
