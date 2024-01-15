use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    // Add any errors that you expect to occur during runtime
    #[snafu(display("component {} occur error: {}", component, source))]
    GenericError {
        component: &'static str,
        source: Box<dyn std::error::Error>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
