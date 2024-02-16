pub mod attr;
pub mod entry;
pub mod ino;
pub mod internal_nodes;
pub mod setting;
pub mod slice;
pub mod stat;

pub use fuser::{FileType, Request};
pub use libc::c_int as Errno;

/// Errors that can be converted to a raw OS error (errno)
pub trait ToErrno {
    fn to_errno(&self) -> Errno;
}
