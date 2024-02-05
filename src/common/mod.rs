use opendal::Operator;
use tracing_subscriber::{layer::SubscriberExt, Registry};
pub mod err;
pub mod readable_size;
pub(crate) mod runtime;

#[allow(dead_code)]
pub(crate) fn install_fmt_log() {
    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    let subscriber = Registry::default().with(stdout_log);
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");
}

pub(crate) fn new_memory_sto() -> Operator {
    let builder = opendal::services::Memory::default();
    Operator::new(builder).unwrap().finish()
}
