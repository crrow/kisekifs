use std::time::Duration;

use kiseki_storage::error::Error;
use libc::c_int;
use snafu::{Location, Snafu};

use kiseki_types::ino::Ino;

use crate::{common, common::err::ToErrno, meta::MetaError, vfs::FH};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum VFSError {
    // ====Storage====
    #[snafu(display("cannot read empty block"))]
    ReadEmptyBlock,
    #[snafu(display("object storage error: {source}"))]
    ErrObjectStorage {
        source: opendal::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("no more space in cache dir {}", cache_dir))]
    ErrStageNoMoreSpace {
        cache_dir: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("OpenDAL operator failed"))]
    OpenDal {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: opendal::Error,
    },
    CacheIO {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },
    FailedToHandleSystime {
        #[snafu(implicit)]
        location: Location,
        source: std::time::SystemTimeError,
    },

    // ====workers====
    #[snafu(display("Failed to join handle"))]
    Join {
        #[snafu(source)]
        error: tokio::task::JoinError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Worker {} is stopped", id))]
    WorkerStopped {
        id: u32,
        #[snafu(implicit)]
        location: Location,
    },
    // ====VFS====
    #[snafu(display("invalid file handle {}", ino))]
    InvalidIno {
        ino: Ino,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("failed to acquire next slice id"))]
    FailedToGetNextSliceID {
        #[snafu(implicit)]
        location: Location,
        source: opendal::Error,
    },
    #[snafu(display("this file reader is invalid {ino}, {fh}"))]
    ThisFileReaderIsClosing {
        ino: Ino,
        fh: FH,
        #[snafu(implicit)]
        location: Location,
    },
    ErrMeta {
        source: MetaError,
    },
    ErrLIBC {
        kind: c_int,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("try acquire lock timeout {:?}", timeout))]
    ErrTimeout {
        timeout: Duration,
    },
    ErrStorage {
        source: kiseki_storage::error::Error,
    },
}

impl From<VFSError> for common::err::Error {
    fn from(value: VFSError) -> Self {
        match value {
            VFSError::ErrMeta { source } => common::err::Error::_MetaError { source },
            _ => common::err::Error::_VFSError { source: value },
        }
    }
}

impl From<MetaError> for VFSError {
    fn from(value: MetaError) -> Self {
        Self::ErrMeta { source: value }
    }
}

impl From<kiseki_storage::error::Error> for VFSError {
    fn from(value: Error) -> Self {
        Self::ErrStorage { source: value }
    }
}

impl ToErrno for VFSError {
    fn to_errno(&self) -> c_int {
        match self {
            VFSError::ErrMeta { source } => source.to_errno(),
            VFSError::ErrLIBC { kind, .. } => kind.to_owned(),
            VFSError::ErrTimeout { .. } => libc::ETIMEDOUT,
            _ => libc::EINTR,
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, VFSError>;
