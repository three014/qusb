use tokio::sync::mpsc;
use tokio_util::bytes::Bytes;

use crate::utils::Ctrl;

pub struct Sender {
    tx: quinn::SendStream,
    iso_tx: quinn::Connection,
}

impl Sender {
    fn foo(&self) {
        self.tx.id();
    }
}

pub struct Receiver {
    rx: quinn::RecvStream,
    iso_rx: mpsc::Receiver<Bytes>
}

pub struct Operator {
    register_rx: mpsc::Receiver<Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>>>,
    disconnect_rx: mpsc::Receiver<Ctrl<quinn::StreamId>>,
    conn: quinn::Connection,
}

impl Operator {
    pub(crate) async fn run(self) -> std::io::Result<()> {
        let Self { register_rx, disconnect_rx, conn } = self;
        todo!()
    }
}
