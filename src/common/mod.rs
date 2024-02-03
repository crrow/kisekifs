use tracing_subscriber::{layer::SubscriberExt, Registry};
pub mod err;
pub(crate) mod readable_size;
pub(crate) mod runtime;

#[allow(dead_code)]
pub(crate) fn install_fmt_log() {
    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    let subscriber = Registry::default().with(stdout_log);
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");
}
