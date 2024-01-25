/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use clap::ValueEnum;
use opendal::Scheme;
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use snafu::ResultExt;
use tracing::info;

use crate::meta::{
    engine::MetaEngine,
    err::{MetaError, Result},
};

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
    // connect info
    #[serde(serialize_with = "serialize_scheme")]
    #[serde(deserialize_with = "deserialize_scheme")]
    pub scheme: Scheme,
    pub scheme_config: HashMap<String, String>,
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
    pub sub_dir: Option<PathBuf>,
    pub atime_mode: AccessTimeMode,
    pub dir_stat_flush_period: Duration,
    pub skip_dir_mtime: Duration,
}

fn serialize_scheme<S>(s: &Scheme, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&s.to_string())
}

fn deserialize_scheme<'de, D>(deserializer: D) -> std::result::Result<Scheme, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Scheme::from_str(&s)
        .map_err(|e| D::Error::custom(format!("failed to parse scheme: {}: {}", s, e.to_string())))
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            scheme: Scheme::Sled,
            scheme_config: {
                let mut map = HashMap::new();
                map.insert("datadir".to_string(), "/tmp/kiseki-meta".to_string());
                map
            },
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
            sub_dir: None,
            atime_mode: Default::default(),
            dir_stat_flush_period: Duration::from_secs(1),
            skip_dir_mtime: Default::default(),
        }
    }
}

impl MetaConfig {
    pub(crate) fn verify(&mut self) -> Result<()> {
        todo!()
    }
    pub fn open(self) -> Result<MetaEngine> {
        info!(
            "try to open meta on {:?}, {:?}",
            self.scheme, &self.scheme_config
        );
        let m = MetaEngine::open(self)?;
        info!("open {} successfully.", &m.info());
        Ok(m)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Format {
    // unimplemented
    pub uuid: Option<String>,
    pub storage: Option<String>,
    pub storage_class: Option<String>,
    pub bucket: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub shards: Option<u64>,
    pub hash_prefix: Option<bool>,
    pub encrypt_key: Option<String>,
    pub encrypt_algo: Option<String>,
    pub key_encrypted: Option<bool>,
    pub upload_limit: Option<isize>,
    pub download_limit: Option<isize>,
    pub min_client_version: Option<String>,
    pub max_client_version: Option<String>,

    pub dir_stats: bool,
    pub meta_version: usize,
    pub name: String,
    pub block_size: u64,
    pub compression: Option<Compression>,
    pub capacity_in_bytes: u64,
    pub inodes: u64,
    pub trash_days: u64,
}

const MIN_CLIENT_VERSION: &str = "1";
const MAX_META_VERSION: usize = 1;

impl Default for Format {
    fn default() -> Self {
        Self {
            uuid: None,
            storage: None,
            storage_class: None,
            bucket: None,
            access_key: None,
            secret_key: None,
            session_token: None,
            shards: None,
            hash_prefix: None,
            encrypt_key: None,
            encrypt_algo: None,
            key_encrypted: None,
            upload_limit: None,
            download_limit: None,
            min_client_version: None,
            max_client_version: None,
            meta_version: MAX_META_VERSION,
            dir_stats: true, // TODO: review it
            name: "".to_string(),
            block_size: 0,
            compression: None,
            capacity_in_bytes: 0,
            inodes: 0,
            trash_days: 0,
        }
    }
}

impl Format {
    #[inline]
    pub fn format_key_str() -> String {
        String::from("setting")
    }

    pub fn parse_from<R: AsRef<[u8]>>(r: R) -> std::result::Result<Self, bincode::Error> {
        let format: Format = bincode::deserialize(r.as_ref())?;
        Ok(format)
    }

    pub fn check_version(&self) -> std::result::Result<(), MetaError> {
        return Ok(());
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    Lz4,
    Zstd,
}

impl Display for Compression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::Lz4 => write!(f, "lz4"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_config() {
        let c = MetaConfig::default();
        let json_str = serde_json::to_string(&c).unwrap();
        let c2: MetaConfig = serde_json::from_str(&json_str).unwrap();
        assert_eq!(c, c2)
    }
}
