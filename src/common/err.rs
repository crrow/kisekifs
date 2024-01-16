use crate::fuse::InodeError;
use libc::c_int;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    // Add any errors that you expect to occur during runtime
    #[snafu(display("component {} occur error: {}", component, source))]
    GenericError {
        component: &'static str,
        source: Box<dyn std::error::Error>,
    },
    MetaError {
        source: crate::meta::MetaError,
    },
    VFSError {
        source: crate::vfs::VFSError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can be converted to a raw OS error (errno)
pub trait ToErrno {
    fn to_errno(&self) -> libc::c_int;
}
