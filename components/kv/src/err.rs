use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    Unknown {
        #[snafu(implicit)]
        location: Location,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    TokioJoinError {
        #[snafu(implicit)]
        location: Location,
        source: tokio::task::JoinError,
    },

    #[cfg(feature = "kv-rocksdb")]
    RocksdbError {
        #[snafu(implicit)]
        location: Location,
        source: rocksdb::Error,
    },
}
