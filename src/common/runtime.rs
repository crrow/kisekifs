use std::future::Future;

use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tracing::debug;

static GLOBAL_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    debug!("start tokio runtime");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap()
});

pub(crate) fn handle() -> tokio::runtime::Handle {
    GLOBAL_RUNTIME.handle().clone()
}

pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    GLOBAL_RUNTIME.spawn(future)
}

#[allow(dead_code)]
pub(crate) fn spawn_blocking<F, R>(func: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    GLOBAL_RUNTIME.spawn_blocking(func)
}

#[allow(dead_code)]
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    GLOBAL_RUNTIME.block_on(future)
}
