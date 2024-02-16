use snafu::{location, Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    Unknown {
        #[snafu(implicit)]
        location: Location,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    UnsupportedMetaDSN {
        #[snafu(implicit)]
        location: Location,
        dsn: String,
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
    #[snafu(display("Model error: {:?}, {:?}", source, location))]
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

    #[snafu(display("Invalid setting: {:?}, {:?}", String::from_utf8_lossy(key.as_slice()).to_string(), location))]
    InvalidSetting {
        #[snafu(implicit)]
        location: Location,
        key: Vec<u8>,
    },

    LibcError {
        #[snafu(implicit)]
        location: Location,
        errno: libc::c_int,
    },
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::ModelError { source, .. } if source.is_not_found())
    }
}

pub mod model_err {
    use snafu::Snafu;

    #[derive(Debug)]
    pub enum ModelKind {
        Attr,
        DEntry,
        Symlink,
        Setting,
        Counter,
        ChunkSlices,
        DirStat,
    }

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("Not found: {:?}", String::from_utf8_lossy(key.as_slice()).to_string()))]
        NotFound { kind: ModelKind, key: Vec<u8> },
        Corruption {
            kind: ModelKind,
            key: String,
            source: bincode::Error,
        },
        CorruptionString {
            kind: ModelKind,
            key: String,
            reason: String,
        },
    }

    impl Error {
        pub fn is_not_found(&self) -> bool {
            matches!(self, Error::NotFound { .. })
        }
    }
}
