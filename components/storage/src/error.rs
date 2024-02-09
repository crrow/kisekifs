use snafu::{Location, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("OpenDAL operator failed"))]
    OpenDal {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: opendal::Error,
    },

    UnknownIOError {
        #[snafu(implicit)]
        location: Location,
        source: std::io::Error,
    },

    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source: tokio::task::JoinError,
    },

    FlushBlockFailed {
        #[snafu(implicit)]
        location: Location,
    },

    InvalidSliceBufferWriteOffset {
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
