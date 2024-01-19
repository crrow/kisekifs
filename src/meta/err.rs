use libc::c_int;
use opendal::ErrorKind;
use snafu::Snafu;
use tracing::error;

use crate::{common::err::ToErrno, meta::types::Ino};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MetaError {
    #[snafu(display("meta has not been initialized yet"))]
    ErrMetaHasNotBeenInitializedYet,
    #[snafu(display("invalid format version"))]
    ErrInvalidFormatVersion,
    #[snafu(display("failed to parse scheme: {}: {}", got, source))]
    FailedToParseScheme { source: opendal::Error, got: String },
    #[snafu(display("failed to open operator: {}", source))]
    FailedToOpenOperator { source: opendal::Error },
    #[snafu(display("bad access permission for inode:{inode}, want:{want}, grant:{grant}"))]
    ErrBadAccessPerm { inode: Ino, want: u8, grant: u8 },
    #[snafu(display("inode {inode} is not a directory"))]
    ErrNotDir { inode: Ino },
    #[snafu(display("failed to deserialize: {source}"))]
    ErrBincodeDeserializeFailed { source: bincode::Error },
    #[snafu(display("failed to read {key} from sto: {source}"))]
    ErrFailedToReadFromSto { key: String, source: opendal::Error },
    #[snafu(display("failed to write {key} into sto: {source}"))]
    ErrFailedToWriteToSto { key: String, source: opendal::Error },
    #[snafu(display("failed to list by opendal: {source}"))]
    ErrOpendalList { source: opendal::Error },
    #[snafu(display("failed to mknod: {kind}"))]
    ErrMknod { kind: libc::c_int },
    #[snafu(display("failed to do counter: {source}"))]
    ErrFailedToDoCounter { source: opendal::Error },
}

impl From<MetaError> for crate::common::err::Error {
    fn from(value: MetaError) -> Self {
        Self::MetaError { source: value }
    }
}

// TODO: review the errno mapping
impl ToErrno for MetaError {
    fn to_errno(&self) -> c_int {
        match self {
            MetaError::FailedToParseScheme { .. } => libc::EINVAL,
            MetaError::FailedToOpenOperator { .. } => libc::EIO,
            MetaError::ErrBadAccessPerm { .. } => libc::EACCES,
            MetaError::ErrNotDir { .. } => libc::ENOTDIR,
            MetaError::ErrBincodeDeserializeFailed { .. } => libc::EIO,
            MetaError::ErrFailedToReadFromSto { source, .. } => match source.kind() {
                ErrorKind::NotFound => libc::ENOENT,
                _ => {
                    error!("failed to read from sto: {}", source);
                    libc::EIO
                }
            },
            MetaError::ErrOpendalList { .. } => libc::EIO,
            MetaError::ErrInvalidFormatVersion => libc::EBADF, // TODO: review
            MetaError::ErrMknod { kind } => *kind,
            MetaError::ErrFailedToDoCounter { .. } => libc::EIO,
            MetaError::ErrFailedToWriteToSto { .. } => libc::EIO,
            MetaError::ErrMetaHasNotBeenInitializedYet => {
                error!("meta has not been initialized yet");
                libc::EIO
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, MetaError>;
