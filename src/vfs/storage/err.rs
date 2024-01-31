use snafu::{Location, Snafu};

use crate::{meta::types::Ino, vfs::FH};

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
}

impl From<StorageError> for crate::vfs::err::VFSError {
    fn from(value: StorageError) -> Self {
        crate::vfs::err::VFSError::ErrLIBC { kind: libc::EINTR }
    }
}

pub(crate) type Result<T> = std::result::Result<T, StorageError>;
