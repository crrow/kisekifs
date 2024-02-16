use snafu::{Location, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source: tokio::task::JoinError,
    },
    OpenDalError {
        #[snafu(implicit)]
        location: Location,
        source: opendal::Error,
    },
    StorageError {
        source: kiseki_storage::err::Error,
    },
    MetaError {
        source: kiseki_meta::Error,
    },

    // ====VFS====
    LibcError {
        errno: libc::c_int,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<kiseki_meta::Error> for Error {
    fn from(value: kiseki_meta::Error) -> Self {
        Self::MetaError { source: value }
    }
}
impl From<kiseki_storage::err::Error> for Error {
    fn from(value: kiseki_storage::err::Error) -> Self {
        Self::StorageError { source: value }
    }
}

impl kiseki_types::ToErrno for Error {
    fn to_errno(&self) -> kiseki_types::Errno {
        match self {
            Self::LibcError { errno, .. } => *errno,
            _ => libc::EINTR,
        }
    }
}
