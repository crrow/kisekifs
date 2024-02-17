use libc::c_int;
use snafu::{location, Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    Unknown {
        #[snafu(implicit)]
        location: Location,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl kiseki_types::ToErrno for Error {
    fn to_errno(&self) -> c_int {
        match self {
            Error::Unknown { .. } => libc::EINTR,
        }
    }
}
