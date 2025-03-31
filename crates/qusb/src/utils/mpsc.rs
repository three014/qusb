pub type AsyncSender<T> = flume::r#async::SendSink<'static, T>;
pub type AsyncReceiver<T> = flume::r#async::RecvStream<'static, T>;
pub type Sender<T> = flume::Sender<T>;
pub type Receiver<T> = flume::Receiver<T>;

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    flume::bounded(capacity)
}
