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

    #[cfg(feature = "meta-rocksdb")]
    RocksdbError {
        #[snafu(implicit)]
        location: Location,
        source: rocksdb::Error,
    },

    // Model Error
    ModelError {
        #[snafu(implicit)]
        location: Location,
        source: model_err::Error,
    },

    // Setting
    #[snafu(display("FileSystem has not been initialized yet. Location: {}", location))]
    UninitializedEngine {
        #[snafu(implicit)]
        location: Location,
    },
    InvalidSetting {
        #[snafu(implicit)]
        location: Location,
        key: Vec<u8>,
    },
}

pub mod model_err {
    use std::string::FromUtf8Error;

    use kiseki_types::ino::Ino;
    use snafu::{location, Location, Snafu};

    #[derive(Debug)]
    pub enum ModelKind {
        Attr,
        EntryInfo,
        Symlink,
        Setting,
        Counter,
        ChunkSlices,
    }

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        NotFound {
            kind: ModelKind,
            key: Vec<u8>,
        },
        Corruption {
            kind: ModelKind,
            key: Vec<u8>,
            source: bincode::Error,
        },
        CorruptionString {
            kind: ModelKind,
            key: Vec<u8>,
            source: FromUtf8Error,
        },
    }

    impl Error {
        pub fn is_not_found(&self) -> bool {
            matches!(self, Error::NotFound { .. })
        }
    }

    impl From<Error> for super::Error {
        fn from(e: Error) -> Self {
            super::Error::ModelError {
                location: location!(),
                source: e,
            }
        }
    }
}
