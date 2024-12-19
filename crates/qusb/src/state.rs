use std::convert::Infallible;

use crate::{utils::Ctrl, Error};
use tokio::{io::BufReader, sync::mpsc};
use tokio_util::bytes::Bytes;

pub struct ClientIdle;
pub struct ClientReq;
pub struct ClientListDevices;
pub struct ClientBorrowDev(proto::UsbDeviceInfo<'static>);
pub struct ServerListening;
pub struct ServerGetReq;
pub struct ServerDecideResp(proto::Request);
pub struct ServerListDevices;

pub struct ClientIdle2 {
    reg_tx: mpsc::Sender<Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>, Infallible>>,
    dereg_tx: mpsc::Sender<Ctrl<quinn::StreamId, (), Infallible>>,
}

pub struct State2<
    S,
    W = crate::stream::Sender<quinn::SendStream>,
    R = crate::stream::Receiver<BufReader<quinn::RecvStream>>,
> {
    _s: S,
    tx: W,
    rx: R,
}

impl State2<ClientIdle2> {
    pub fn new_client(
        tx: quinn::SendStream,
        rx: quinn::RecvStream,
        reg_tx: mpsc::Sender<Ctrl<quinn::StreamId, mpsc::Receiver<Bytes>, Infallible>>,
        dereg_tx: mpsc::Sender<Ctrl<quinn::StreamId, (), Infallible>>,
    ) -> Self {
        Self {
            _s: ClientIdle2 { reg_tx, dereg_tx },
            tx: crate::stream::Sender::new(tx),
            rx: crate::stream::Receiver::new(BufReader::with_capacity(1024, rx)),
        }
    }

}

pub struct State<S> {
    _s: S,
    tx: crate::stream::Sender<quinn::SendStream>,
    rx: crate::stream::Receiver<BufReader<quinn::RecvStream>>,
}

impl State<ClientIdle> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn new_client(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        tracing::trace!("New client-side stream ready to go!");
        Self {
            _s: ClientIdle,
            tx: crate::stream::Sender::new(tx),
            rx: crate::stream::Receiver::new(BufReader::with_capacity(1024, rx)),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn verify_version(self) -> Result<State<ClientReq>, Error> {
        let Self { _s, mut tx, mut rx } = self;
        tx.send(&proto::VERSION).await?;
        let result = rx
            .recv::<proto::Response<()>>()
            .await?
            .expect("Server should return a response in this state");
        let next_state = match result {
            Ok(_) => State {
                _s: ClientReq,
                tx,
                rx,
            },
            Err(proto::Error::VersionMismatch { client: _, server }) => {
                Err(Error::VersionMismatch(server))?
            }
            _ => unreachable!("Not a valid error in this state"),
        };
        tracing::trace!("Verified that client and server have the same version");
        Ok(next_state)
    }
}

impl State<ClientReq> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn list_devices(self) -> Result<State<ClientListDevices>, Error> {
        let Self { _s, mut tx, rx } = self;
        tx.send(&proto::Request::ListUsbDevices).await?;
        tracing::trace!("Sent request to list available USB devices to server");
        tx.as_writer_mut().finish().unwrap();
        Ok(State {
            _s: ClientListDevices,
            tx,
            rx,
        })
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn borrow_device(
        self,
        id: proto::UsbDeviceId,
    ) -> Result<State<ClientBorrowDev>, Error> {
        let Self { _s, mut tx, mut rx } = self;
        tx.send(&proto::Request::Borrow(id)).await?;
        tracing::trace!("Sent request to attach USB device with this id: {id:?}");
        let resp = rx
            .recv()
            .await?
            .expect("Should receive a response from server");
        match resp {
            proto::Response::Ok(dev) => Ok(State {
                _s: ClientBorrowDev(dev),
                tx,
                rx,
            }),
            proto::Response::Err(proto::Error::NoDev) => Err(Error::DevNotFound(id)),
            _ => todo!(),
        }
    }
}

impl State<ClientListDevices> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn next(&mut self) -> Result<Option<proto::UsbDeviceInfo<'static>>, Error> {
        match self.rx.recv().await? {
            Some(dev) => Ok(Some(dev)),
            None => {
                // self.rx.as_reader_mut().get_mut().received_reset().await.unwrap();
                Ok(None)
            }
        }
    }
}

impl State<ClientBorrowDev> {
    pub fn dev(&self) -> &proto::UsbDeviceInfo {
        &self._s.0
    }
}

impl State<ServerListening> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn new_server(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        tracing::trace!("New server-side stream ready to go!");
        Self {
            _s: ServerListening,
            tx: crate::stream::Sender::new(tx),
            rx: crate::stream::Receiver::new(BufReader::with_capacity(1024, rx)),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn verify_version(self) -> Result<State<ServerGetReq>, Error> {
        let Self { _s, mut tx, mut rx } = self;
        let version = rx
            .recv::<proto::Version>()
            .await?
            .expect("We can't get here without the client sending a proper message");
        if version != proto::VERSION {
            tx.send::<proto::Response<()>>(&proto::Response::Err(proto::Error::VersionMismatch {
                client: version,
                server: proto::VERSION,
            }))
            .await?;
            Err(Error::VersionMismatch(version))
        } else {
            tx.send::<proto::Response<()>>(&proto::Response::Ok(()))
                .await?;
            tracing::trace!("Verified that client and server have the same version");
            Ok(State {
                _s: ServerGetReq,
                tx,
                rx,
            })
        }
    }
}

impl State<ServerGetReq> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn recv_req(self) -> Result<State<ServerDecideResp>, Error> {
        let Self { _s, tx, mut rx } = self;
        let req = rx
            .recv::<proto::Request>()
            .await?
            .expect("Client should have sent a request");
        tracing::trace!("Received a request from client");
        Ok(State {
            _s: ServerDecideResp(req),
            tx,
            rx,
        })
    }
}

impl State<ServerDecideResp> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn list_devices(self) -> State<ServerListDevices> {
        State {
            _s: ServerListDevices,
            tx: self.tx,
            rx: self.rx,
        }
    }

    pub fn req(&self) -> proto::Request {
        self._s.0
    }
}

impl State<ServerListDevices> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn send_device_info(&mut self, dev: &proto::UsbDeviceInfo<'_>) -> Result<(), Error> {
        self.tx.send(&dev).await?;
        tracing::trace!("Sent a message to client");
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn finish(mut self) -> Result<(), Error> {
        self.tx.as_writer_mut().finish().unwrap();
        tracing::trace!("Finished responding to client req");
        self.tx.as_writer_mut().stopped().await.unwrap();
        Ok(())
    }
}

// pub struct VersionSender(pub(crate) crate::Sender<proto::Version>);
// impl VersionSender {
//     pub async fn send(mut self) -> Result<ReqSender, Error> {
//         self.0.send(&proto::VERSION).await?;
//         Ok(ReqSender(self.0.convert()))
//     }
// }

// pub struct VersionReceiver(pub(crate) crate::Receiver<proto::Version>);
// impl VersionReceiver {
//     pub async fn recv(mut self) -> Result<ReqReceiver, Error> {
//         let version = self.0.recv().await?.unwrap();
//         if version != proto::VERSION {
//             Err(Error::VersionMismatch(version))
//         } else {
//             Ok(ReqReceiver(self.0.convert()))
//         }
//     }
// }

// pub struct RespSender<T: Serialize + std::fmt::Debug>(pub(crate) crate::Sender<proto::Response<T>>);
// impl<T: Serialize + std::fmt::Debug> RespSender<T> {
//     pub async fn send_data(&mut self, data: T) -> Result<(), Error> {
//         self.0.send(&Ok(data)).await
//     }

//     pub async fn send_err(mut self, data: proto::Error) -> Result<(), Error> {
//         self.0.send(&Err(data)).await
//     }

//     pub fn convert<R: Serialize + std::fmt::Debug>(self) -> RespSender<R> {
//         RespSender(self.0.convert())
//     }
// }

// pub struct RespReceiver<T: DeserializeOwned + std::fmt::Debug>(
//     pub(crate) crate::Receiver<proto::Response<T>>,
// );
// impl<T: DeserializeOwned + std::fmt::Debug> RespReceiver<T> {
//     pub async fn recv(&mut self) -> Result<Option<proto::Response<T>>, Error> {
//         self.0.recv().await
//     }

//     pub fn convert<R: DeserializeOwned + std::fmt::Debug>(self) -> RespReceiver<R> {
//         RespReceiver(self.0.convert())
//     }
// }

// pub struct ReqSender(crate::Sender<proto::Request>);
// impl ReqSender {
//     pub async fn send<T: Serialize + std::fmt::Debug>(
//         mut self,
//         req: proto::Request,
//     ) -> Result<crate::Sender<T>, Error> {
//         self.0.send(&req).await?;
//         Ok(self.0.convert::<T>())
//     }
// }

// pub struct ReqReceiver(crate::Receiver<proto::Request>);
// impl ReqReceiver {
//     pub async fn recv<T: DeserializeOwned + std::fmt::Debug>(
//         mut self,
//     ) -> Result<(proto::Request, crate::Receiver<T>), Error> {
//         let req = self.0.recv().await?;
//         Ok((req, self.0.convert()))
//     }
// }
