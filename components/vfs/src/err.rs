use kiseki_common::FH;
use kiseki_types::ino::Ino;
use snafu::{Location, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source: tokio::task::JoinError,
    },

    StorageErr {
        source: kiseki_storage::err::Error,
        #[snafu(implicit)]
        location: Location,
    },

    // ====VFS====
    #[snafu(display("invalid file handle {}", ino))]
    InvalidIno {
        ino: Ino,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("this file reader is invalid {ino}, {fh}"))]
    ThisFileReaderIsClosing {
        ino: Ino,
        fh: FH,
        #[snafu(implicit)]
        location: Location,
    },
    LibcError {
        errno: libc::c_int,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
