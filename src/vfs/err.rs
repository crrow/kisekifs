use std::time::Duration;

use libc::c_int;
use snafu::Snafu;

use crate::{common, common::err::ToErrno, meta::MetaError};

// FIXME: its ugly

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
