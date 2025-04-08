use proto::unpacked::Seq;

use crate::Ctrl;

pin_project_lite::pin_project! {
    #[project = CtrlKindProj]
    pub enum Kind<F1, F2, F3, F4> {
        Async { #[pin] fut: F1 },
        SetInterface { #[pin] fut: F2 },
        SetConfig { #[pin] fut: F3 },
        ClearStall { #[pin] fut: F4 },
    }
}

impl<F1, F2, F3, F4> Future for Kind<F1, F2, F3, F4>
where
    F1: Future<Output = Seq<Ctrl>>,
    F2: Future<Output = Seq<Ctrl>>,
    F3: Future<Output = Seq<Ctrl>>,
    F4: Future<Output = Seq<Ctrl>>,
{
    type Output = Seq<Ctrl>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.project() {
            CtrlKindProj::Async { fut } => fut.poll(cx),
            CtrlKindProj::SetInterface { fut } => fut.poll(cx),
            CtrlKindProj::SetConfig { fut } => fut.poll(cx),
            CtrlKindProj::ClearStall { fut } => fut.poll(cx),
        }
    }
}
