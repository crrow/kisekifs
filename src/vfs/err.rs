use std::time::Duration;

use libc::c_int;
use snafu::{Location, Snafu};

use crate::{
    common,
    common::err::ToErrno,
    meta::{types::Ino, MetaError},
    vfs::FH,
};

// FIXME: its ugly

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum StorageError {
    #[snafu(display("unexpected end of file"))]
    EOF,
    #[snafu(display("out of memory: {source}"))]
    OOM {
        source: datafusion_common::DataFusionError,
    },
    #[snafu(display("object storage error: {source}"))]
    ObjectStorageError { source: opendal::Error },

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
}

impl From<StorageError> for VFSError {
    fn from(value: StorageError) -> Self {
        VFSError::ErrLIBC { kind: libc::EINTR }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum VFSError {
    ErrMeta {
        source: MetaError,
    },
    ErrLIBC {
        kind: c_int,
    },
    #[snafu(display("try acquire lock timeout {:?}", timeout))]
    ErrTimeout {
        timeout: Duration,
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

impl ToErrno for VFSError {
    fn to_errno(&self) -> c_int {
        match self {
            VFSError::ErrMeta { source } => source.to_errno(),
            VFSError::ErrLIBC { kind } => kind.to_owned(),
            VFSError::ErrTimeout { .. } => libc::ETIMEDOUT,
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, VFSError>;
