// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{fmt::Display, path::PathBuf, str::FromStr, time::Duration};

use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

use crate::err::Result;

/// Atime (Access Time):
/// Every file has three timestamps:
/// atime (access time): The last time the file was read or accessed.
/// mtime (modification time): The last time the file's content was modified.
/// ctime (change time): The last time the file's metadata (e.g., permissions,
/// owner) was changed.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum AccessTimeMode {
    /// Disables atime updates entirely.
    /// Reading a file doesn't update its atime timestamp.
    /// Improves performance, especially for frequently accessed files.
    /// Can make it difficult to determine when a file was last accessed.
    Never,
    /// Default atime mode on many Linux systems.
    /// Updates atime only if:
    /// The file's atime is older than its mtime or ctime.
    /// The file has been accessed more than a certain time threshold (usually 1
    /// day). Balances performance and access time tracking.
    Relative,
    /// Always updates atime whenever a file is read.
    /// Accurately tracks file access times.
    /// Can impact performance, especially on storage systems with slow write
    /// speeds.
    Everytime,
}

impl Default for AccessTimeMode {
    fn default() -> Self { Self::Never }
}

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct MetaConfig {
    pub dsn: String,

    pub read_only:        bool,
    /// The duration to reuse open file without checking update (0 means disable
    /// this feature)
    pub open_cache:       Duration,
    /// max number of open files to cache (soft limit, 0 means unlimited)
    pub open_cache_limit: usize,
    /// [skip_dir_mtime] skip updating attribute of a directory if the mtime
    /// difference is smaller than this value
    pub skip_dir_mtime:   Duration,
}

impl MetaConfig {
    pub fn with_dsn(&mut self, dsn: &str) -> &mut Self {
        self.dsn = dsn.to_string();
        self
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            dsn:              kiseki_common::KISEKI_DEBUG_META_ADDR.to_string(),
            read_only:        false,
            open_cache:       Duration::default(),
            open_cache_limit: 10_000,
            skip_dir_mtime:   Duration::from_millis(100),
        }
    }
}
