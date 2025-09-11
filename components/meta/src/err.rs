// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use kiseki_types::ToErrno;
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    Unknown {
        #[snafu(implicit)]
        location: Location,
        source:   Box<dyn std::error::Error + Send + Sync>,
    },
    UnsupportedMetaDSN {
        #[snafu(implicit)]
        location: Location,
        dsn:      String,
    },

    TokioJoinError {
        #[snafu(implicit)]
        location: Location,
        source:   tokio::task::JoinError,
    },

    #[cfg(feature = "meta-rocksdb")]
    RocksdbError {
        #[snafu(implicit)]
        location: Location,
        source:   rocksdb::Error,
    },

    // Model Error
    #[snafu(display("Model error: {:?}, {:?}", source, location))]
    ModelError {
        #[snafu(implicit)]
        location: Location,
        source:   model_err::Error,
    },

    // Setting
    #[snafu(display("FileSystem has not been initialized yet. Location: {}", location))]
    UninitializedEngine {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid setting: {:?}, {:?}", String::from_utf8_lossy(key.as_slice()).to_string(), location))]
    InvalidSetting {
        #[snafu(implicit)]
        location: Location,
        key:      Vec<u8>,
    },

    LibcError {
        #[snafu(implicit)]
        location: Location,
        errno:    libc::c_int,
    },
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::ModelError { source, .. } if source.is_not_found())
    }
}

pub mod model_err {
    use snafu::Snafu;

    #[derive(Debug)]
    pub enum ModelKind {
        Attr,
        DEntry,
        Symlink,
        Setting,
        Counter,
        ChunkSlices,
        DirStat,
        HardLinkCount,
        Sustained,
        DeleteInode,
    }

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        NotFound {
            kind: ModelKind,
            key:  String,
        },
        Corruption {
            kind:   ModelKind,
            key:    String,
            source: bincode::Error,
        },
        CorruptionString {
            kind:   ModelKind,
            key:    String,
            reason: String,
        },
    }

    impl Error {
        pub fn is_not_found(&self) -> bool { matches!(self, Error::NotFound { .. }) }
    }
}

impl ToErrno for Error {
    fn to_errno(&self) -> libc::c_int {
        match self {
            Error::Unknown { .. } => libc::EINTR,
            Error::UnsupportedMetaDSN { .. } => libc::EINTR,
            Error::TokioJoinError { .. } => libc::EINTR,
            #[cfg(feature = "meta-rocksdb")]
            Error::RocksdbError { .. } => libc::EINTR,
            Error::ModelError { source, .. } => {
                if source.is_not_found() {
                    libc::ENOENT
                } else {
                    libc::EINTR
                }
            }
            Error::UninitializedEngine { .. } => libc::EINTR,
            Error::InvalidSetting { .. } => libc::EINTR,
            Error::LibcError { errno, .. } => *errno,
        }
    }
}
