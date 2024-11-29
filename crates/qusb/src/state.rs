use crate::Error;
use serde::{de::DeserializeOwned, Serialize};

pub struct VersionSender(pub(crate) crate::Sender<spec::Version>);
impl VersionSender {
    pub async fn send(mut self) -> Result<ReqSender, Error> {
        self.0.send(&spec::VERSION).await?;
        Ok(ReqSender(self.0.convert()))
    }
}

pub struct VersionReceiver(pub(crate) crate::Receiver<spec::Version>);
impl VersionReceiver {
    pub async fn recv(mut self) -> Result<ReqReceiver, Error> {
        let version = self.0.recv().await?;
        if version != spec::VERSION {
            Err(Error::VersionMismatch(version))
        } else {
            Ok(ReqReceiver(self.0.convert()))
        }
    }
}

pub struct RespSender<T: Serialize>(pub(crate) crate::Sender<spec::Response<T>>);
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

pub struct RespReceiver<T: DeserializeOwned>(pub(crate) crate::Receiver<spec::Response<T>>);
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
    pub async fn send<T: Serialize>(
        mut self,
        req: spec::Request,
    ) -> Result<crate::Sender<T>, Error> {
        self.0.send(&req).await?;
        Ok(self.0.convert::<T>())
    }
}

pub struct ReqReceiver(crate::Receiver<spec::Request>);
impl ReqReceiver {
    pub async fn recv<T: DeserializeOwned>(
        mut self,
    ) -> Result<(spec::Request, crate::Receiver<T>), Error> {
        let req = self.0.recv().await?;
        Ok((req, self.0.convert()))
    }
}
