use snafu::Snafu;

use crate::common::err::ToErrno;

pub type Result<T> = std::result::Result<T, ChunkError>;
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ChunkError {
    #[snafu(display("operate opendal store failed : {}", source))]
    StoErr { source: opendal::Error },
    #[snafu(display("invalid input {msg}"))]
    ErrInvalidInput { msg: String },

    #[snafu(display("general error : {}", source))]
    General {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<ChunkError> for crate::common::err::Error {
    fn from(value: ChunkError) -> Self {
        crate::common::err::Error::ChunkError { source: value }
    }
}

impl ToErrno for ChunkError {
    fn to_errno(&self) -> libc::c_int {
        libc::EINTR
    }
}
