use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
// Config for clients.
use crate::common;
use crate::meta::{types, MetaEngine, MetaError::ErrInvalidFormatVersion};
use common::err::Result;
use opendal::Scheme;
use serde::__private::de::IdentifierDeserializer;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::{ResultExt, Snafu, Whatever};
use std::time::Duration;
use tracing::info;

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
    pub sub_dir: PathBuf,
    pub atime_mode: types::AccessTimeMode,
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
            sub_dir: PathBuf::new(),
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

    pub fn check_version(&self) -> std::result::Result<(), crate::meta::MetaError> {
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
