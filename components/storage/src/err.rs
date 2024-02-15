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

    DiskPoolMmapError {
        #[snafu(implicit)]
        location: Location,
        source: fmmap::error::Error,
    },

    #[snafu(display("no more space in cache dir {}", cache_dir))]
    ErrStageNoMoreSpace {
        cache_dir: String,
        #[snafu(implicit)]
        location: Location,
    },

    MetaError {
        #[snafu(implicit)]
        location: Location,
        source: kiseki_meta::err::Error,
    },
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::OpenDal { error, .. } if error.kind() == opendal::ErrorKind::NotFound)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
