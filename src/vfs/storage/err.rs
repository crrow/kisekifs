use snafu::Snafu;

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
}

impl From<StorageError> for crate::vfs::err::VFSError {
    fn from(value: StorageError) -> Self {
        crate::vfs::err::VFSError::ErrLIBC { kind: libc::EINTR }
    }
}

pub(crate) type Result<T> = std::result::Result<T, StorageError>;
