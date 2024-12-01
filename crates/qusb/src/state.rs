use std::marker::PhantomData;

use crate::Error;
use tokio::io::BufReader;

pub struct ClientIdle;
pub struct ClientReq;
pub struct ClientListDevices;
pub struct ServerListening;
pub struct ServerGetReq;
pub struct ServerDecideResp;
pub struct ServerListDevices;

pub struct State<T> {
    _p: PhantomData<T>,
    tx: crate::stream::Sender2<quinn::SendStream>,
    rx: crate::stream::Receiver2<BufReader<quinn::RecvStream>>,
}

impl State<ClientIdle> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn new_client(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        tracing::trace!("New client-side stream ready to go!");
        Self {
            _p: PhantomData,
            tx: crate::stream::Sender2::new(tx),
            rx: crate::stream::Receiver2::new(BufReader::with_capacity(1024, rx)),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn verify_version(self) -> Result<State<ClientReq>, Error> {
        let Self { _p, mut tx, mut rx } = self;
        tx.send(&spec::VERSION).await?;
        let result = rx
            .recv::<spec::Response<()>>()
            .await?
            .expect("Server should return a response in this state");
        let next_state = match result {
            Ok(_) => State {
                _p: PhantomData,
                tx,
                rx,
            },
            Err(spec::Error::VersionMismatch { client: _, server }) => {
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
        let Self { _p, mut tx, rx } = self;
        tx.send(&spec::Request::ListUsbDevices).await?;
        tracing::trace!("Sent request to list available USB devices to server");
        tx.as_writer_mut().finish().unwrap();
        Ok(State {
            _p: PhantomData,
            tx,
            rx,
        })
    }
}

impl State<ClientListDevices> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn next(&mut self) -> Result<Option<spec::UsbDeviceInfo<'static>>, Error> {
        match self.rx.recv().await? {
            Some(dev) => Ok(Some(dev)),
            None => {
                // self.rx.as_reader_mut().get_mut().received_reset().await.unwrap();
                Ok(None)
            },
        }
    }
}

impl State<ServerListening> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn new_server(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        tracing::trace!("New server-side stream ready to go!");
        Self {
            _p: PhantomData,
            tx: crate::stream::Sender2::new(tx),
            rx: crate::stream::Receiver2::new(BufReader::with_capacity(1024, rx)),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn verify_version(self) -> Result<State<ServerGetReq>, Error> {
        let Self { _p, mut tx, mut rx } = self;
        let version = rx
            .recv::<spec::Version>()
            .await?
            .expect("We can't get here without the client sending a proper message");
        if version != spec::VERSION {
            tx.send::<spec::Response<()>>(&spec::Response::Err(spec::Error::VersionMismatch {
                client: version,
                server: spec::VERSION,
            }))
            .await?;
            Err(Error::VersionMismatch(version))
        } else {
            tx.send::<spec::Response<()>>(&spec::Response::Ok(()))
                .await?;
            tracing::trace!("Verified that client and server have the same version");
            Ok(State {
                _p: PhantomData,
                tx,
                rx,
            })
        }
    }
}

impl State<ServerGetReq> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn recv_req(self) -> Result<(spec::Request, State<ServerDecideResp>), Error> {
        let Self { _p, tx, mut rx } = self;
        let req = rx
            .recv::<spec::Request>()
            .await?
            .expect("Client should have sent a request");
        tracing::trace!("Received a request from client");
        Ok((
            req,
            State {
                _p: PhantomData,
                tx,
                rx,
            },
        ))
    }
}

impl State<ServerDecideResp> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn list_devices(self) -> State<ServerListDevices> {
        State {
            _p: PhantomData,
            tx: self.tx,
            rx: self.rx,
        }
    }
}

impl State<ServerListDevices> {
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn send_device(&mut self, dev: &spec::UsbDeviceInfo<'_>) -> Result<(), Error> {
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

// pub struct VersionSender(pub(crate) crate::Sender<spec::Version>);
// impl VersionSender {
//     pub async fn send(mut self) -> Result<ReqSender, Error> {
//         self.0.send(&spec::VERSION).await?;
//         Ok(ReqSender(self.0.convert()))
//     }
// }

// pub struct VersionReceiver(pub(crate) crate::Receiver<spec::Version>);
// impl VersionReceiver {
//     pub async fn recv(mut self) -> Result<ReqReceiver, Error> {
//         let version = self.0.recv().await?.unwrap();
//         if version != spec::VERSION {
//             Err(Error::VersionMismatch(version))
//         } else {
//             Ok(ReqReceiver(self.0.convert()))
//         }
//     }
// }

// pub struct RespSender<T: Serialize + std::fmt::Debug>(pub(crate) crate::Sender<spec::Response<T>>);
// impl<T: Serialize + std::fmt::Debug> RespSender<T> {
//     pub async fn send_data(&mut self, data: T) -> Result<(), Error> {
//         self.0.send(&Ok(data)).await
//     }

//     pub async fn send_err(mut self, data: spec::Error) -> Result<(), Error> {
//         self.0.send(&Err(data)).await
//     }

//     pub fn convert<R: Serialize + std::fmt::Debug>(self) -> RespSender<R> {
//         RespSender(self.0.convert())
//     }
// }

// pub struct RespReceiver<T: DeserializeOwned + std::fmt::Debug>(
//     pub(crate) crate::Receiver<spec::Response<T>>,
// );
// impl<T: DeserializeOwned + std::fmt::Debug> RespReceiver<T> {
//     pub async fn recv(&mut self) -> Result<Option<spec::Response<T>>, Error> {
//         self.0.recv().await
//     }

//     pub fn convert<R: DeserializeOwned + std::fmt::Debug>(self) -> RespReceiver<R> {
//         RespReceiver(self.0.convert())
//     }
// }

// pub struct ReqSender(crate::Sender<spec::Request>);
// impl ReqSender {
//     pub async fn send<T: Serialize + std::fmt::Debug>(
//         mut self,
//         req: spec::Request,
//     ) -> Result<crate::Sender<T>, Error> {
//         self.0.send(&req).await?;
//         Ok(self.0.convert::<T>())
//     }
// }

// pub struct ReqReceiver(crate::Receiver<spec::Request>);
// impl ReqReceiver {
//     pub async fn recv<T: DeserializeOwned + std::fmt::Debug>(
//         mut self,
//     ) -> Result<(spec::Request, crate::Receiver<T>), Error> {
//         let req = self.0.recv().await?;
//         Ok((req, self.0.convert()))
//     }
// }
