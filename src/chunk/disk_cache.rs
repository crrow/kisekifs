use std::{
    hash::Hash,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};

use bytes::Bytes;
use dashmap::DashMap;
use opendal::Operator;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};

use crate::chunk::{
    err::{GeneralSnafu, Result},
    page::UnsafePageView,
    ChunkError,
};

const STAGING_DIR: &str = "rawstaging";
const CACHE_DIR: &str = "raw";

#[derive(Debug)]
pub(crate) struct Config {
    free_ratio: f32,
    has_prefix: bool,
    block_size: usize,
    // if none, we don't cache anything.
    // MiB
    capacity: Option<usize>,
    // if true, we only cache full block.
    only_full_block: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            free_ratio: 0.1, // 10%
            has_prefix: false,
            block_size: 0,
            capacity: Some(100 << 10),
            only_full_block: false,
        }
    }
}

// pub(crate) struct CacheManager(Arc<Vec<Mutex<CacheStore>>>);

#[derive(Debug)]
struct CacheStore {
    // input arguments
    op: Arc<Operator>,
    config: Config,
    root: PathBuf,

    // runtime status
    stage_full: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    pages: DashMap<u64, Bytes>,
}
struct CacheEntry<K> {
    hash: u64,
    _mark: PhantomData<K>,
}

impl CacheStore {
    fn new<P: AsRef<Path>>(p: P, config: Config, op: Arc<Operator>) -> Self {
        let dir = p.as_ref().to_path_buf();
        let stage_full = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        Self {
            op,
            config,
            root: dir,
            stage_full,
            cancel_token,
            pages: Default::default(),
        }
    }
    async fn run(&self) {
        let mut free_space_checker = FreeSpaceChecker::new(
            self.root.clone(),
            self.stage_full.clone(),
            self.cancel_token.clone(),
        );
        free_space_checker.run();
    }

    // When write slice finish its job, it will transfer the ownership of the block
    // to the cache.
    fn stage(&mut self, key: &str, data: Vec<u8>) -> Result<PathBuf> {
        if self.stage_full.load(Ordering::SeqCst) {
            // return Err(ChunkError::CacheFull.into());
        }
        let stage_path = self.stage_path(key);

        todo!()
    }

    pub fn should_cache(&self, size: usize) -> bool {
        self.config.only_full_block || size < self.config.block_size
    }

    fn stage_path(&self, key: &str) -> PathBuf {
        self.root.join(STAGING_DIR).join(key)
    }

    fn cache_path(&self, key: &str) -> PathBuf {
        self.root.join(CACHE_DIR).join(key)
    }

    // we don't care error in this function.
    fn cache<K: AsRef<str>>(&self, key: K, data: Vec<u8>, force: bool)
    where
        K: Hash,
    {
        if self.config.capacity.is_none() {
            return;
        }
        let key = String::from(key.as_ref());
        if self.stage_full.load(Ordering::SeqCst) {
            debug!(
                "Caching directory is full ({}), drop ({}) ({} bytes)",
                self.root.to_string_lossy().to_string(),
                key,
                data.len()
            );
            return;
        }
        // if self.pages.contains_key(&key) {
        //     return;
        // }
    }
}

#[derive(Debug)]
struct DiskUsage {
    //  Total disk space in bytes
    total: u64,
    // free: Available free space in bytes
    free: u64,
    // files: Total number of inodes (file system data structures)
    files: u64,
    // ffree: Number of free inodes
    ffree: u64,
    _dir: PathBuf,
}

impl DiskUsage {
    fn new<P: AsRef<Path>>(p: P) -> Result<Self> {
        let p = p.as_ref();
        let v = Self::get_raw_disk_usage(p)?;
        let total = v.f_blocks as u64 * v.f_bsize as u64;
        let free = v.f_bavail as u64 * v.f_bsize as u64;
        let files = v.f_files as u64;
        let ffree = v.f_ffree as u64;
        Ok(Self {
            total,
            free,
            files,
            ffree,
            _dir: p.to_path_buf(),
        })
    }

    fn fetch(&mut self) -> Result<()> {
        let v = Self::get_raw_disk_usage(&self._dir)?;
        self.total = v.f_blocks as u64 * v.f_bsize as u64;
        self.free = v.f_bavail as u64 * v.f_bsize as u64;
        self.files = v.f_files as u64;
        self.ffree = v.f_ffree as u64;
        Ok(())
    }

    // Overall free space ratio (free space / total space)
    fn free_space_ratio(&self) -> f32 {
        self.free as f32 / self.total as f32
    }

    // Free inode ratio (free inodes / total inodes)
    fn free_inode_ratio(&self) -> f32 {
        self.ffree as f32 / self.files as f32
    }

    fn get_raw_disk_usage<P: AsRef<Path>>(p: P) -> Result<rustix::fs::StatFs> {
        let v = rustix::fs::statfs(p.as_ref()).map_err(|e| ChunkError::General {
            source: Box::new(e),
        })?;
        Ok(v)
    }
}

#[derive(Debug)]
struct FreeSpaceStatus {
    stage_full: bool,
    raw_full: bool,
}

#[derive(Debug)]
struct FreeSpaceChecker {
    dir: PathBuf,
    stage_full: Arc<AtomicBool>,
    cancel_token: CancellationToken,
}

impl FreeSpaceChecker {
    fn new(dir: PathBuf, stage_full: Arc<AtomicBool>, cancel_token: CancellationToken) -> Self {
        Self {
            dir,
            stage_full,
            cancel_token,
        }
    }

    #[instrument(skip_all)]
    fn run(mut self) {
        let token = self.cancel_token;
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        debug!("check free space");
                        // TODO: in this case, we should check the free ratio,
                        // if it exceeds the config value, we should evict some
                        // cache value.
                        self.stage_full.store(true, Ordering::SeqCst);
                    }
                    _ = token.cancelled() => {
                        debug!("cancel free space checker");
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{layer::SubscriberExt, Registry};

    use super::*;

    #[test]
    fn get_disk_usage() {
        let mut ds = DiskUsage::new("/tmp").unwrap();
        println!("{}, {}", ds.free_space_ratio(), ds.free_inode_ratio());
        ds.fetch().unwrap();
        println!("{}, {}", ds.free_space_ratio(), ds.free_inode_ratio());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        let stdout_log = tracing_subscriber::fmt::layer().pretty();
        let subscriber = Registry::default().with(stdout_log);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set global subscriber");

        let tempdir = tempfile::tempdir().unwrap();
        let op = Arc::new(
            Operator::new(opendal::services::Memory::default())
                .unwrap()
                .finish(),
        );

        let config = Config {
            free_ratio: 0.1,
            has_prefix: false,
            block_size: 0,
            capacity: Some(1024 * 1024 * 1024),
            only_full_block: false,
        };

        let cs = CacheStore::new(&tempdir, config, op);
        cs.run().await;
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        cs.cancel_token.cancel();

        assert_eq!(cs.stage_full.load(Ordering::SeqCst), true);
    }
}
