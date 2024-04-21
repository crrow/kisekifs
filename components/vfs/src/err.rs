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

use kiseki_types::slice::SliceKey;
use snafu::{Location, Snafu};
use tracing::error;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source:   tokio::task::JoinError,
    },
    OpenDalError {
        #[snafu(implicit)]
        location: Location,
        source:   opendal::Error,
    },

    ObjectStorageError {
        #[snafu(implicit)]
        location: Location,
        source:   kiseki_utils::object_storage::ObjectStorageError,
    },

    ObjectBlockNotFound {
        #[snafu(implicit)]
        location: Location,
        key:      SliceKey,
    },

    StorageError {
        source: kiseki_storage::err::Error,
    },
    MetaError {
        source: kiseki_meta::Error,
    },

    // ====VFS====
    LibcError {
        errno:    libc::c_int,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<kiseki_meta::Error> for Error {
    fn from(value: kiseki_meta::Error) -> Self { Self::MetaError { source: value } }
}

impl From<kiseki_storage::err::Error> for Error {
    fn from(value: kiseki_storage::err::Error) -> Self { Self::StorageError { source: value } }
}

impl kiseki_types::ToErrno for Error {
    fn to_errno(&self) -> kiseki_types::Errno {
        match self {
            Self::LibcError { errno, .. } => {
                error!("libc error: {}", errno);
                *errno
            }
            Self::MetaError { source } => {
                error!("meta error: {}", source);
                source.to_errno()
            }
            _ => panic!("unhandled error type in to_errno {}", self),
        }
    }
}
