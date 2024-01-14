// Config for clients.
use super::err::Result;
use crate::common;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
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
    pub mount_point: String,
    pub sub_dir: String,
    pub atime_mode: common::AccessTimeMode,
    pub dir_stat_flush_period: Duration,
    pub skip_dir_mtime: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            strict: true,
            retries: 10,
            max_deletes: 2,
            skip_dir_nlink: 0,
            case_insensitive: false,
            read_only: false,
            no_bg_job: false,
            open_cache: Default::default(),
            open_cache_limit: 0,
            heartbeat: Duration::from_secs(12),
            mount_point: "".to_string(),
            sub_dir: "".to_string(),
            atime_mode: Default::default(),
            dir_stat_flush_period: Duration::from_secs(1),
            skip_dir_mtime: Default::default(),
        }
    }
}

impl Config {
    pub(crate) fn verify(&mut self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Deserialize, Serialize)]
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
