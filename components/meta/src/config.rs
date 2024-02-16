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
    fn default() -> Self {
        Self::Never
    }
}
#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct MetaConfig {
    pub dsn: String,

    // update ctime
    pub strict: bool,
    pub retries: usize,
    pub max_deletes: usize,
    pub skip_dir_nlink: usize,
    pub case_insensitive: bool,
    pub read_only: bool,
    // disable background jobs
    pub no_bg_job: bool,
    pub open_cache: Duration,
    // max number of files to cache (soft limit)
    pub open_cache_limit: usize,
    pub heartbeat: Duration,
    pub mount_point: PathBuf,
    // mount a sub-directory as root
    // pub sub_dir: Option<PathBuf>,
    pub atime_mode: AccessTimeMode,
    pub dir_stat_flush_period: Duration,
    pub skip_dir_mtime: Duration,
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
            // dsn: "rocksdb://:tmp/kiseki.meta".to_string(),
            dsn: "/tmp/kiseki.meta".to_string(),
            strict: true,
            retries: 10,
            max_deletes: 2,
            skip_dir_nlink: 0,
            case_insensitive: false,
            read_only: false,
            no_bg_job: false,
            open_cache: Duration::default(),
            open_cache_limit: 0,
            heartbeat: Duration::from_secs(12),
            mount_point: PathBuf::new(),
            atime_mode: Default::default(),
            dir_stat_flush_period: Duration::from_secs(1),
            skip_dir_mtime: Default::default(),
        }
    }
}

const MIN_CLIENT_VERSION: &str = "1";
const MAX_META_VERSION: usize = 1;
