use snafu::{Location, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source: tokio::task::JoinError,
    },

    StorageErr {
        source: kiseki_storage::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
