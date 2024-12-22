use std::convert::Infallible;

use bytes::{Buf, Bytes};
use tokio::sync::mpsc;
use zerocopy::{self, FromBytes};

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
            Register(Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>, Infallible>),
            Datagram(Bytes),
            Disconnect(Ctrl<quinn::StreamId, (), Infallible>),
        }

        loop {
            let select = tokio::select! {
                req = register_rx.recv() => {
                    if let Some(register) = req {
                        Event::Register(register)
                    } else {
                        break;
                    }
                }
                datagram = conn.read_datagram() => {
                    match datagram {
                        Ok(bytes) => Event::Datagram(bytes),
                        Err(err) => return Err(std::io::Error::from(err)),
                    }
                }
                req = disconnect_rx.recv() => {
                    if let Some(disconnect) = req {
                        Event::Disconnect(disconnect)
                    } else {
                        break;
                    }
                }
            };

            match select {
                Event::Register(Ctrl { data: id, tx }) => {
                    let (iso_tx, iso_rx) = mpsc::channel(32);
                    mailer.insert(NoHash(id), iso_tx);
                    if tx.send(Ok(iso_rx)).is_err() {
                        mailer.remove(&NoHash(id));
                    }
                }
                Event::Datagram(mut bytes) => {
                    if let Ok((id, _)) = zerocopy::network_endian::U64::read_from_prefix(&bytes) {
                        bytes.advance(std::mem::size_of_val(&id));
                        let id = NoHash(quinn::StreamId(id.get()));
                        if let Some(tx) = mailer.get(&id) {
                            if tx.send(bytes).await.is_err() {
                                mailer.remove(&id);
                            }
                        }
                    }
                }
                Event::Disconnect(Ctrl { data: id, tx }) => {
                    mailer.remove(&NoHash(id));
                    let _ = tx.send(Ok(()));
                }
            }
        }
        Ok(())
    }
}
