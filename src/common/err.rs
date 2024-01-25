use libc::c_int;
use snafu::Snafu;

/// Errors that can be converted to a raw OS error (errno)
pub trait ToErrno {
    fn to_errno(&self) -> libc::c_int;
}

#[derive(Debug, Snafu)]
pub enum Error {
    MetaError { source: crate::meta::MetaError },
    VFSError { source: crate::vfs::VFSError },
    ChunkError { source: crate::chunk::ChunkError },
}

impl ToErrno for Error {
    fn to_errno(&self) -> c_int {
        match self {
            Error::MetaError { source } => source.to_errno(),
            Error::VFSError { source } => source.to_errno(),
            Error::ChunkError { source } => source.to_errno(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
