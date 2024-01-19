use std::{collections::HashMap, path::PathBuf, str::FromStr, time::Duration};

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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Format {
    pub name: String,
    pub uuid: String,
    pub storage: String,
    pub storage_class: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub block_size: u64,
    pub compression: String,
    pub shards: u64,
    pub hash_prefix: bool,
    pub capacity: u64,
    pub inodes: u64,
    pub encrypt_key: String,
    pub encrypt_algo: String,
    pub key_encrypted: bool,
    pub upload_limit: isize,
    pub download_limit: isize,
    pub trash_days: isize,
    pub meta_version: isize,
    pub min_client_version: String,
    pub max_client_version: String,
    pub dir_stats: bool,
}

impl Format {
    #[inline]
    pub fn format_key_str() -> String {
        String::from("format")
    }

    pub fn parse_from<R: AsRef<[u8]>>(r: R) -> std::result::Result<Self, bincode::Error> {
        let format: Format = bincode::deserialize(r.as_ref())?;
        Ok(format)
    }

    pub fn check_version(&self) -> std::result::Result<(), MetaError> {
        return Ok(());
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
