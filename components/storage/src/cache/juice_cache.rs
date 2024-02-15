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

use std::path::{Path, PathBuf};
use std::{
    fmt::{Debug, Formatter},
    fs,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::cache::Cache;
use crate::err::{ErrStageNoMoreSpaceSnafu, OpenDalSnafu, Result, UnknownIOSnafu};
use async_trait::async_trait;
use crc32fast::Hasher;
use dashmap::{DashMap, DashSet};
use futures::{FutureExt, TryStreamExt};
use kiseki_types::slice::{SliceID, SliceKey, EMPTY_SLICE_KEY};
use kiseki_utils::{readable_size::ReadableSize, runtime};
use opendal::{Builder, Lister, Operator as ObjectStorage, Reader};
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::{
    io::AsyncWriteExt,
    select,
    sync::{mpsc, watch, Notify},
    time::Instant,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, instrument, trace, warn};
// use crate::vfs::{
//     err::{
//         CacheIOSnafu, ErrStageNoMoreSpaceSnafu, FailedToHandleSystimeSnafu, OpenDalSnafu, Result,
//     },
//     storage::cache::Cache,
//     VFSError,
// };

const CACHE_SIZE_PADDING: usize = 4096;
const MAX_EXPIRE_CNT: usize = 1000;
const RAW_CACHE_DIR: &'static str = "raw";
const STAGE_CACHE_DIR: &'static str = "stage";

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CacheEviction {
    Disable, // disable the eviction
    Random,
}

pub struct JuiceFileCacheBuilder {
    pub cache_dir: String,
    pub capacity: ReadableSize,
    pub free_ratio: f32,
    pub add_checksum: bool,
    pub cache_eviction: CacheEviction,
    pub cache_expire: Duration,
    pub max_pending_cnt: usize,
    // only when we enable the write back, then we need the uploader.
    pub write_back_uploader: Option<mpsc::Sender<(SliceKey, String)>>,
}

impl JuiceFileCacheBuilder {
    pub fn new<P: AsRef<str>>(path: P) -> Self {
        JuiceFileCacheBuilder {
            cache_dir: path.as_ref().to_string(),
            capacity: ReadableSize::gb(1),
            free_ratio: 0.1, // 10 %
            add_checksum: false,
            cache_eviction: CacheEviction::Random,
            cache_expire: Duration::from_secs(1),
            max_pending_cnt: 5,
            write_back_uploader: None,
        }
    }
    pub fn build(self) -> Result<Arc<dyn Cache>> {
        let jc = self.inner_build()?;
        Ok(Arc::new(jc))
    }

    pub(crate) fn inner_build(self) -> Result<JuiceFileCache> {
        debug!("create juice file cache at {:?}", &self.cache_dir);
        let local_store =
            kiseki_utils::object_storage::new_fs_store(&self.cache_dir).context(OpenDalSnafu)?;
        let (sender, receiver) = mpsc::channel(self.max_pending_cnt);
        let cancel_token = CancellationToken::new();
        let tracker = TaskTracker::new();
        let (cleanup_full_tx, cleanup_full_rx) = watch::channel(EMPTY_SLICE_KEY);
        let fc = Arc::new(JuiceFileCacheInner {
            cache_root_dir: self.cache_dir,
            add_checksum: self.add_checksum,
            free_ratio: self.free_ratio,
            capacity: self.capacity.as_bytes() as usize,
            cache_eviction: self.cache_eviction,
            cache_expire: self.cache_expire,
            local_store,
            flush_finished_notify: Arc::new(Default::default()),
            used: Arc::new(Default::default()),
            stage_full: Arc::new(Default::default()),
            raw_full: Arc::new(Default::default()),
            memory_usage: Arc::new(Default::default()),
            pending_set: DashSet::with_capacity(self.max_pending_cnt),
            memory_index: Default::default(),
            stage_index: Default::default(),
            cancel_token,
            disk_item_tx: sender,
            stage_uploader_tx: self.write_back_uploader,
            task_tracker: tracker,
            in_cleanup_full: Arc::new(Default::default()),
            cleanup_full_finished_notify: Arc::new(Default::default()),
            cleanup_full_tx,
        });

        let flush_task = FlushTask {
            fc: fc.clone(),
            rx: receiver,
            cancel_token: fc.cancel_token.clone(),
            notify: fc.flush_finished_notify.clone(),
        };

        let free_space_checker = FreeSpaceChecker {
            fc: fc.clone(),
            cancel_token: fc.cancel_token.clone(),
            cleanup_full_rx,
        };

        let handle = &runtime::handle();
        fc.task_tracker.spawn_on(flush_task.run(), handle);
        fc.task_tracker.spawn_on(free_space_checker.run(), handle);

        // Once we spawned everything, we close the tracker.
        fc.task_tracker.close();

        Ok(JuiceFileCache(fc))
    }

    pub fn with_capacity(mut self, capacity: ReadableSize) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_free_ratio(mut self, free_ratio: f32) -> Self {
        self.free_ratio = free_ratio;
        self
    }
}

fn cache_file_path(cache_file_dir: &str, slice_id: SliceID) -> String {
    join_path(cache_file_dir, &format!("{}", slice_id))
}

fn join_path(parent: &str, child: &str) -> String {
    let output = format!("{}/{}", parent, child);
    opendal::raw::normalize_path(&output)
}

fn get_sys_time_in_secs() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[derive(Debug, Clone)]
struct CacheIndexValue {
    block_size: usize,
    access_time: u64,
    // the path of the block on the local store.
    path: String,
    link: bool,
}

#[derive(Debug, Clone)]
struct StageIndexValue {
    block_size: usize,
    access_time: u64,
    // the path of the block on the local store.
    path: String,
    in_cache: bool, // if the stage data also in the cache.
}

pub(crate) struct JuiceFileCache(Arc<JuiceFileCacheInner>);

impl Clone for JuiceFileCache {
    fn clone(&self) -> Self {
        JuiceFileCache(self.0.clone())
    }
}

impl Debug for JuiceFileCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JuiceFileCache")
    }
}

#[async_trait]
impl Cache for JuiceFileCache {
    async fn cache(&self, key: SliceKey, block: Arc<Vec<u8>>) -> bool {
        // we expose weak cache for low latency,
        // so we reject the request if the cache is in cleanup.
        self.0.cache_weak(key.clone(), block).await
    }

    async fn get(&self, key: SliceKey) -> Option<Reader> {
        self.0.get(key).await
    }

    async fn wait_on_all_flush_finish(&self) {
        self.0.wait_on_all_flush_finish().await
    }

    async fn close(&self) {
        self.0.close().await
    }

    async fn remove(&self, key: SliceKey) {
        self.0.remove(key.clone()).await
    }

    async fn stage(&self, key: SliceKey, data: Arc<Vec<u8>>, keep_cache: bool) -> Result<()> {
        self.0.stage(key, data, keep_cache).await
    }
}

/// The JuiceFS file cache implementation.
pub(crate) struct JuiceFileCacheInner {
    cache_root_dir: String,
    add_checksum: bool,
    free_ratio: f32,
    capacity: usize,
    cache_eviction: CacheEviction,
    cache_expire: Duration,

    local_store: ObjectStorage,

    // for test and benchmark, tell the caller that we have finished the flush.
    flush_finished_notify: Arc<Notify>,
    // used represents the total size we stored in cache dir.
    // if we store in stage, then we don't calculate the size.
    used: Arc<AtomicUsize>,
    stage_full: Arc<AtomicBool>,
    raw_full: Arc<AtomicBool>, // TODO: rename me
    memory_index: DashMap<SliceKey, Arc<CacheIndexValue>>,
    stage_index: DashMap<SliceKey, Arc<StageIndexValue>>,
    cancel_token: CancellationToken,
    // if the slice is in the pending set, we won't cache it again.
    // when the background task is processing the block, we remove
    // the key from the pending set.
    pending_set: DashSet<SliceKey>,
    // check how many memory use while we are caching the block.
    memory_usage: Arc<AtomicUsize>,

    disk_item_tx: mpsc::Sender<DiskItem>,
    stage_uploader_tx: Option<mpsc::Sender<(SliceKey, String)>>,

    task_tracker: TaskTracker,
    // we may call the cleanup manually, so maintain the state to reject the new requests.
    in_cleanup_full: Arc<AtomicBool>,
    cleanup_full_finished_notify: Arc<Notify>,
    cleanup_full_tx: watch::Sender<SliceKey>,
}

impl JuiceFileCacheInner {
    /// Load a block from cache.
    pub(crate) async fn get(self: &Arc<Self>, key: SliceKey) -> Option<Reader> {
        if !self.memory_index.contains_key(&key) {
            return None;
        }

        let file_path = self.cache_path(key);
        match self.get_reader(&file_path).await {
            Ok(Some(reader)) => {
                return Some(reader);
            }
            Err(e) => {
                if !e.is_not_found() {
                    warn!("Failed to get file for key {:?}", file_path);
                };
            }
            Ok(None) => {}
        }

        // We remove the file from the index.
        if let Some(e) = self.memory_index.remove(&key) {
            if e.1.block_size > 0 && !e.1.link {
                self.used
                    .fetch_sub(e.1.block_size + CACHE_SIZE_PADDING, Ordering::AcqRel);
            }
        }
        None
    }

    /// FIXME: handle the checksum case.
    /// FIXME: we may handle the case when we return the reader, but then it
    /// starts to clean up.
    async fn get_reader(&self, file_path: &str) -> Result<Option<Reader>> {
        if self
            .local_store
            .is_exist(file_path)
            .await
            .context(OpenDalSnafu)?
        {
            Ok(Some(
                self.local_store
                    .reader(file_path)
                    .await
                    .context(OpenDalSnafu)?,
            ))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn cache(self: &Arc<Self>, key: SliceKey, block: Arc<Vec<u8>>) -> bool {
        self.do_cache(key, block, false).await
    }

    pub(crate) async fn cache_weak(self: &Arc<Self>, key: SliceKey, block: Arc<Vec<u8>>) -> bool {
        self.do_cache(key, block, true).await
    }

    async fn do_cache(
        self: &Arc<Self>,
        key: SliceKey,
        block: Arc<Vec<u8>>,
        weak: bool, // weak means we can reject the request if the cache is in cleanup.
    ) -> bool {
        // check the memory index
        if self.memory_index.contains_key(&key) {
            return false;
        }
        // check the memory buffer
        if self.pending_set.contains(&key) {
            return false;
        }
        // reject the new requests if the cache is full.
        if self.raw_full.load(Ordering::Acquire) {
            return false;
        }
        // check if in cleanup.
        if self.in_cleanup_full.load(Ordering::Acquire) {
            if weak {
                return false;
            }
            // wait the cleanup finished
            let cancel_token = self.cancel_token.clone();
            while self.in_cleanup_full.load(Ordering::Acquire) {
                select! {
                    // wait the cleanup finished
                    _ = self.cleanup_full_finished_notify.notified() => {}
                    _ = cancel_token.cancelled() => {
                        debug!("cache is cancelled");
                        return false;
                    }
                }
            }
        }

        let block_len = block.len();
        // cache the memory buf.
        self.pending_set.insert(key);
        if let Err(e) = self.disk_item_tx.send(DiskItem { key, block }).await {
            // failed to send the block to the flush task, discard it
            debug!("failed to cache block to disk: {e}, discard it {}", key);
            self.pending_set.remove(&key);
            return false;
        };
        true
    }

    async fn add_memory_index(
        self: &Arc<Self>,
        key: SliceKey,
        block_len: usize,
        access_time: u64,
        link: bool,
    ) {
        if let Some(old) = self.memory_index.get(&key) {
            self.used
                .fetch_sub(old.block_size + CACHE_SIZE_PADDING, Ordering::AcqRel);
        }

        self.memory_index.insert(
            key,
            Arc::new(CacheIndexValue {
                block_size: block_len,
                access_time,
                path: self.cache_path(key),
                link,
            }),
        );
        if block_len > 0 && !link {
            self.used
                .fetch_add(block_len + CACHE_SIZE_PADDING, Ordering::AcqRel);
        }
        let used = self.used.load(Ordering::Acquire);
        if used > self.capacity && !matches!(self.cache_eviction, CacheEviction::Disable) {
            debug!("cache is full {}, start to cleanup", used);
            self.notify_to_cleanup_full(key).await;
        }
    }

    async fn del_cache_index(self: &Arc<Self>, key: SliceKey, link: bool) {
        if let Some(old) = self.memory_index.remove(&key) {
            if old.1.block_size > 0 && !link {
                self.used
                    .fetch_sub(old.1.block_size + CACHE_SIZE_PADDING, Ordering::AcqRel);
            }
        }
    }

    pub(crate) async fn cleanup_full(self: &Arc<Self>) {
        self.notify_to_cleanup_full(SliceKey::random()).await;
        // TODO: wait on cleanup full finished.
    }

    async fn notify_to_cleanup_full(self: &Arc<Self>, changed: SliceKey) {
        if self.in_cleanup_full.load(Ordering::Acquire) {
            return;
        }

        if let Err(e) = self.cleanup_full_tx.send(changed) {
            warn!("failed to notify the cleanup full: {:?}", e);
        }
    }

    /// Stage the slice data to local store.
    pub(crate) async fn stage(
        self: &Arc<Self>,
        key: SliceKey,
        data: Arc<Vec<u8>>,
        keep_in_cache: bool,
    ) -> Result<()> {
        if self.stage_full.load(Ordering::Acquire) {
            return ErrStageNoMoreSpaceSnafu {
                cache_dir: self.cache_root_dir.clone(),
            }
            .fail();
        }

        let stage_path = self.stage_path(key);
        let block_size = data.len();
        persistent_block(
            self.local_store.clone(),
            &stage_path,
            data,
            self.add_checksum,
        )
        .await?;
        // update the index.
        let idx = Arc::new(StageIndexValue {
            block_size,
            access_time: get_sys_time_in_secs(),
            path: stage_path.clone(),
            in_cache: keep_in_cache,
        });
        if keep_in_cache {
            let cache_path = self.cache_path(key);
            std::os::unix::fs::symlink(&stage_path, &cache_path).context(UnknownIOSnafu)?;
            self.add_memory_index(key, block_size, idx.access_time, true)
                .await;
        }
        self.stage_index.insert(key, idx);

        Ok(())
    }

    /// When the cache is full, we should upload the staging data to the remote.
    async fn upload_staging(self: &Arc<Self>) {
        let uploader = match self.stage_uploader_tx {
            Some(ref tx) => tx.clone(),
            None => return,
        };
        let (br, fr) = self.cur_free_ratio();
        let mut to_free = if br < self.free_ratio || fr < self.free_ratio {
            let total = if let Some(du) = get_disk_usage(&self.cache_root_dir) {
                du.total
            } else {
                1
            };
            let min = if br > fr { fr } else { br };
            (total as f64 * self.free_ratio as f64 - min as f64) as usize
        } else {
            0usize
        };

        let mut cnt = 0;
        let mut last_slice_key = EMPTY_SLICE_KEY;
        let mut last_sidx = Arc::new(StageIndexValue {
            block_size: 0,
            access_time: 0,
            path: "".to_string(),
            in_cache: false,
        });
        // for each two random keys, then compare the access time, upload the older one
        for e in self.stage_index.iter() {
            // pick the bigger one if they were accessed within the same minute
            if cnt == 0
                || last_sidx.access_time / 60 > e.access_time / 60
                || last_sidx.access_time / 60 == e.access_time / 60
                    && last_sidx.block_size > e.block_size
            {
                last_slice_key = e.key().clone();
                last_sidx = e.value().clone();
            }
            cnt += 1;
            if cnt > 1 {
                cnt = 0;
                if let Err(e) = uploader
                    .send((last_slice_key, last_sidx.path.clone()))
                    .await
                {
                    warn!("failed to send stage data to the uploader: {:?}", e);
                    continue;
                }
                to_free -= last_sidx.block_size + CACHE_SIZE_PADDING;
            }

            if to_free <= 0 {
                break;
            }
        }
        if cnt > 0 {
            if let Err(e) = uploader
                .send((last_slice_key, last_sidx.path.clone()))
                .await
            {
                warn!("failed to send stage data to the uploader: {:?}", e);
            }
        }
    }

    /// This function will wait on cleanup, and all background tasks to finish.
    pub(crate) async fn close(self: &Arc<Self>) {
        debug!("closing file cache");
        if self.in_cleanup_full.load(Ordering::Acquire) {
            debug!("wait on cleanup finished");
            self.cleanup_full_finished_notify.notified().await;
        }
        self.cancel_token.cancel();
        self.task_tracker.wait().await;
        debug!("file cache is closed");
    }

    pub(crate) async fn wait_on_all_flush_finish(self: &Arc<Self>) {
        let start = Instant::now();
        debug!("wait on {} all flush finish", &self.cache_root_dir);
        while !self.pending_set.is_empty() {
            // wait the flush task to notify me that one flush finished.
            self.flush_finished_notify.notified().await;
        }
        debug!(
            "{} all flush finished, cost: {:?}",
            &self.cache_root_dir,
            start.elapsed()
        );
    }
}

// ===== Remove
impl JuiceFileCacheInner {
    pub(crate) async fn remove(self: &Arc<Self>, key: SliceKey) {
        tokio::join!(self.remove_cache(key), self.remove_stage(key),);
    }

    pub(crate) async fn remove_cache(self: &Arc<Self>, key: SliceKey) {
        // check if the block is in the pending set.
        if self.pending_set.remove(&key).is_some() {
            // the block is in the pending set, we can return now
            return;
        };

        // the slice has been cached, clean it.
        if let Some((_, iv)) = self.memory_index.remove(&key) {
            if let Err(e) = self.local_store.delete(&iv.path).await {
                warn!("failed to delete cache file: {:?}", e);
            }
            if !iv.link {
                self.used
                    .fetch_sub(iv.block_size + CACHE_SIZE_PADDING, Ordering::AcqRel);
            }
        }
    }

    pub(crate) async fn remove_stage(self: &Arc<Self>, key: SliceKey) {
        if let Some(e) = self.stage_index.remove(&key) {
            if e.1.in_cache {
                self.memory_index.remove(&key);
            }
            if let Err(e) = self.local_store.delete(&e.1.path).await {
                warn!("failed to delete stage file: {:?}", e);
            }
        }
    }
}

// ===== Fetches
impl JuiceFileCacheInner {
    fn cache_path(self: &Arc<Self>, key: SliceKey) -> String {
        join_path(
            &join_path(&self.cache_root_dir, RAW_CACHE_DIR),
            &format!("{}", key.gen_path_for_local_sto()),
        )
    }

    fn cache_dir(self: &Arc<Self>) -> String {
        let output = format!("{}/{}/", self.cache_root_dir, RAW_CACHE_DIR);
        opendal::raw::normalize_path(&output)
    }

    fn stage_path(self: &Arc<Self>, key: SliceKey) -> String {
        join_path(
            &join_path(&self.cache_root_dir, STAGE_CACHE_DIR),
            &format!("{}", key.gen_path_for_local_sto()),
        )
    }

    fn stage_dir(self: &Arc<Self>) -> String {
        let output = format!("{}/{}/", self.cache_root_dir, STAGE_CACHE_DIR);
        opendal::raw::normalize_path(&output)
    }

    pub(crate) fn cur_free_ratio(self: &Arc<Self>) -> (f32, f32) {
        get_free_ratio(&self.cache_root_dir)
    }

    pub(crate) async fn memory_usage(self: &Arc<Self>) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }
}

// ===== Recover
impl JuiceFileCacheInner {
    /// Recovers the index from local store.
    pub(crate) async fn recover(self: &Arc<Self>) -> Result<()> {
        if self.stage_uploader_tx.is_some() {
            tokio::try_join!(self.recover_cache(), self.recover_stage())?;
        } else {
            self.recover_cache().await?;
        }
        return Ok(());
    }

    async fn recover_cache(self: &Arc<Self>) -> Result<()> {
        let now = Instant::now();
        let cache_dir = self.stage_dir();
        let mut lister = self
            .local_store
            .lister_with(&cache_dir)
            .await
            .context(OpenDalSnafu)?;
        let (mut total_size, mut total_cnt) = (0, 0);
        while let Some(entry) = lister.try_next().await.context(OpenDalSnafu)? {
            let meta = entry.metadata();
            if !meta.is_file() {
                continue;
            }
            let slice_key = match SliceKey::try_from(entry.name()) {
                Ok(slice_key) => slice_key,
                Err(e) => {
                    warn!("invalid cache file name: {:?}, {:?}", entry.name(), e);
                    continue;
                }
            };

            let metadata = fs::metadata(entry.path()).context(UnknownIOSnafu)?;
            let block_size = slice_key.block_size;
            assert_eq!(
                block_size as u64,
                meta.content_length(),
                "invalid block size {}",
                entry.path()
            );
            let atime = metadata
                .accessed()
                .context(UnknownIOSnafu)?
                .duration_since(UNIX_EPOCH)
                .expect("failed to handle system time")
                .as_secs();
            total_size += block_size;
            total_cnt += 1;
            self.add_memory_index(slice_key, block_size, atime, metadata.is_symlink())
                .await;
        }

        debug!(
            "Recovered file cache, num_keys: {}, num_bytes: {}, cost: {:?}",
            total_size,
            total_cnt,
            now.elapsed()
        );

        Ok(())
    }

    /// In case of the cache is full and then the service is shutdown,
    /// we need to upload the staging data to the remote when we
    /// start the service again.
    ///
    /// This scanner only run once when the service is started.
    ///
    /// We discard the error inside scanner, just hope the freeSpaceChecker
    /// can do its job.
    async fn recover_stage(self: &Arc<Self>) -> Result<()> {
        debug!("start recover stage data");
        let uploader_tx = match self.stage_uploader_tx {
            Some(ref tx) => tx.clone(),
            None => panic!("stage uploader tx is not set"),
        };
        let start = Instant::now();
        let stage_dir = self.stage_dir();
        let mut lister = self
            .local_store
            .lister(&stage_dir)
            .await
            .context(OpenDalSnafu)?;
        let mut count = 0;
        let mut staged_size = 0;

        while let Some(mut de) = select! {
            r =  lister.try_next() => {
                match r {
                    Ok(v) => {v},
                    Err(_) => None
                }
            }
            _ = self.cancel_token.cancelled() => {
                None
            }
        } {
            // TODO: check the validation of the stage path
            if !matches!(de.metadata().mode(), opendal::EntryMode::FILE) {
                continue;
            }
            let name = de.name();
            let slice_key = match SliceKey::try_from(name) {
                Ok(slice_key) => slice_key,
                Err(e) => {
                    warn!("invalid stage file name: {:?}, {:?}", name, e);
                    continue;
                }
            };
            if slice_key.block_size == 0 {
                // possible ?
                continue;
            }

            staged_size += slice_key.block_size;
            if let Err(e) = uploader_tx.send((slice_key, de.path().to_string())).await {
                panic!("failed to send stage data to the uploader: {:?}", e);
            }
            count += 1;
        }

        debug!(
            "Found {} staging blocks ({} bytes) in {} with {:?}",
            count,
            staged_size,
            self.cache_root_dir,
            start.elapsed(),
        );

        Ok(())
    }
}

// 1. The proportion of free space available on the disk relative to its total
//    capacity.
// 2. The ratio of free space to the number of files on the disk.
fn get_free_ratio(path: &str) -> (f32, f32) {
    if let Some(usage) = get_disk_usage(path) {
        let total = usage.total as f32;
        let free = usage.free as f32;
        let files = usage.files as f32;
        let ffree = usage.ffree as f32;
        return (free / total, ffree / files);
    }
    (0.0, 0.0)
}

struct DiskUsage {
    // total capacity of the disk
    total: u64,
    // free capacity of the disk
    free: u64,
    // Total number of inodes on the filesystem
    files: u64,
    // Number of free inodes available for use
    ffree: u64,
}

impl Default for DiskUsage {
    fn default() -> Self {
        Self {
            total: 1,
            free: 1,
            files: 1,
            ffree: 1,
        }
    }
}

fn get_disk_usage(path: &str) -> Option<DiskUsage> {
    if let Ok(stat) = rustix::fs::statvfs(path) {
        return Some(DiskUsage {
            total: stat.f_blocks * stat.f_bsize,
            free: stat.f_bavail * stat.f_bsize,
            files: stat.f_files,
            ffree: stat.f_ffree,
        });
    }
    None
}

async fn persistent_block(
    local_store: ObjectStorage,
    path: &str,
    block: Arc<Vec<u8>>,
    add_checksum: bool,
) -> Result<()> {
    debug!("persistent block to {}", path);
    let mut writer = local_store.writer(path).await.context(OpenDalSnafu)?;
    writer
        .write_all(block.as_slice())
        .await
        .context(UnknownIOSnafu)?;

    if add_checksum {
        let mut hasher = Hasher::new();
        hasher.update(block.as_slice());
        writer
            .write_u32(hasher.finalize())
            .await
            .context(UnknownIOSnafu)?;
    }
    writer.close().await.context(OpenDalSnafu)?;

    Ok(())
}

// DiskItem represents a block on disk.
struct DiskItem {
    key: SliceKey,
    block: Arc<Vec<u8>>,
}

/// The background job to flush the cache to the local store.
struct FlushTask {
    fc: Arc<JuiceFileCacheInner>,
    rx: mpsc::Receiver<DiskItem>,
    cancel_token: CancellationToken,
    notify: Arc<Notify>,
}

impl FlushTask {
    #[instrument(skip(self))]
    async fn run(mut self) {
        debug!("flush task is started");
        let fc = self.fc;
        loop {
            select! {
                _ = self.cancel_token.cancelled() => {
                    debug!("flush task is cancelled");
                    return;
                }
                disk_item = self.rx.recv() => {
                    if let Some(disk_item) = disk_item {
                        debug!("flush task received a flush block req: {}", disk_item.key);
                        let sto = fc.local_store.clone();
                        let path = fc.cache_path(disk_item.key);
                        let block_len = disk_item.block.len();
                        let slice_id = disk_item.key;
                        if !fc.pending_set.contains(&slice_id) {
                            // someone cancel this block, we should not cache it.
                            // update the memory usage.
                            fc.flush_finished_notify.notify_one();
                            fc.memory_usage.fetch_sub(block_len, Ordering::AcqRel);
                            continue;
                        }
                        // do the real work
                        if persistent_block(sto.clone(), &path, disk_item.block, fc.add_checksum)
                            .await.is_ok() {
                            fc.add_memory_index(slice_id, block_len, get_sys_time_in_secs(), false).await;
                        }
                        // remove the block from memory buffer
                        if fc.pending_set.remove(&disk_item.key).is_none() {
                            // When we just persistent the block to the disk,
                            // but then the block is already removed by other thread.
                            // we should remove the block we just added to the disk.
                            let _ = sto.delete(&path).await;
                            // we need to remove the item from memory index, since
                            // we just update the index when we successfully persistent
                            // the block to the disk.
                            fc.memory_index.remove(&slice_id);
                        };
                        fc.flush_finished_notify.notify_one();
                        fc.memory_usage.fetch_sub(block_len, Ordering::AcqRel);
                    }
                }
            }
        }
    }
}

/// FreeSpaceChecker is used for checking the free space of the disk.
struct FreeSpaceChecker {
    fc: Arc<JuiceFileCacheInner>,
    cancel_token: CancellationToken,
    cleanup_full_rx: watch::Receiver<SliceKey>,
}

impl FreeSpaceChecker {
    #[instrument(skip(self))]
    async fn run(mut self) {
        debug!("free space checker is started");
        let mut check_disk_usage_interval = tokio::time::interval(Duration::from_secs(1));
        let mut check_expire_interval =
            tokio::time::interval(Duration::min(Duration::from_secs(60), self.fc.cache_expire));
        loop {
            select! {
                _ = check_disk_usage_interval.tick() => {
                    self.check_free_ratio().await;
                },
                _ = check_expire_interval.tick() => {
                    self.check_expire().await;
                },
                _ = self.cleanup_full_rx.changed() => {
                    assert!(self.fc.in_cleanup_full.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok());
                    debug!("free space checker is notified, we need to do cleanup full");
                    self.clean_up_full().await;
                    continue;
                },
                _ = self.cancel_token.cancelled() => {
                    debug!("free space checker is cancelled");
                    return;
                }
            }
        }
    }

    async fn check_expire(&mut self) {
        let cut_off = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error getting Unix timestamp")
            .as_secs()
            - self.fc.cache_expire.as_secs();

        let mut freed = 0;
        let mut to_expire = vec![];
        for (idx, e) in self.fc.memory_index.iter().enumerate() {
            if idx >= MAX_EXPIRE_CNT {
                break;
            }
            if e.access_time < cut_off {
                freed += e.block_size + CACHE_SIZE_PADDING;
                to_expire.push((e.key().clone(), e.path.clone()));
            }
        }
        let new_used = self.fc.used.load(Ordering::Acquire) - freed;
        if !to_expire.is_empty() {
            debug!(
                "Cleanup expired cache ({}): {} blocks ({:.1} MB), expired {} blocks ({:.1} MB)",
                &self.fc.cache_root_dir,
                self.fc.memory_index.len(),
                new_used as f64 / 1024.0 / 1024.0,
                to_expire.len(),
                freed as f64 / 1024.0 / 1024.0
            );
        }
        if self
            .fc
            .local_store
            .remove(
                to_expire
                    .into_iter()
                    .map(|(sid, path)| {
                        self.fc.memory_index.remove(&sid);
                        path
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .is_err()
        {
            warn!("{} failed to remove expired cache", &self.fc.cache_root_dir);
        }
    }

    async fn check_free_ratio(&mut self) {
        let (bfr, ffr) = get_free_ratio(&self.fc.cache_root_dir);
        let stage_full = bfr < self.fc.free_ratio / 2.0 || ffr < self.fc.free_ratio / 2.0;
        let raw_full = bfr < self.fc.free_ratio || ffr < self.fc.free_ratio;
        // update the state back, make we can reject the new requests.
        self.fc.stage_full.store(stage_full, Ordering::Release);
        self.fc.raw_full.store(raw_full, Ordering::Release);
        let cancel_token = self.cancel_token.clone();

        // check if we need to clean all.
        if raw_full && !matches!(self.fc.cache_eviction, CacheEviction::Disable) {
            trace!(
                "Cleanup cache when checking free space {}: free ratio ({:.2}%), space usage ({:.2}%), inodes usage ({:.2}%)",
                &self.fc.cache_root_dir,
                self.fc.free_ratio * 100.0,
                bfr * 100.0,
                ffr * 100.0
            );
            select! {
                _ = cancel_token.cancelled() => {
                    debug!("free space checker is cancelled");
                    return;
                }
                _ = self.clean_up_full() => {
                    debug!("cleanup full finished");
                }
            }
            // we still raw_full, we won't accept new requests.
            if self.fc.raw_full.load(Ordering::Acquire) && self.fc.stage_uploader_tx.is_some() {
                select! {
                    _ = self.cancel_token.cancelled() => {
                        debug!("free space checker is cancelled");
                        return;
                    }
                    _ = self.fc.upload_staging() => {}
                }
            }
        }
    }

    async fn clean_up_full(&mut self) {
        defer!({
            self.fc.in_cleanup_full.store(false, Ordering::Release);
            self.fc.cleanup_full_finished_notify.notify_waiters();
        });
        let free_ratio = self.fc.free_ratio;
        let cache_dir = self.fc.cache_root_dir.clone();

        let mut goal = self.fc.capacity * 95 / 100;
        // Returns an approximate number of entries in this cache.
        let mut num = self.fc.memory_index.len() as isize * 99 / 100;
        // make sure we have enough free space after cleanup
        let (bfr, ffr) = get_free_ratio(&self.fc.cache_root_dir);
        let mut du_cache = None;
        let mut used_cache = None;
        if bfr < free_ratio {
            let du = get_disk_usage(&cache_dir).unwrap_or_default();
            let to_free = ((free_ratio - bfr) * du.total as f32) as usize;
            du_cache = Some(du);
            let used = self.fc.used.load(Ordering::Acquire);
            used_cache = Some(used);
            if to_free > used {
                goal = 0;
            } else if used - to_free < goal {
                goal = used - to_free;
            }
        }
        if ffr < free_ratio {
            let du = du_cache.unwrap_or_else(|| get_disk_usage(&cache_dir).unwrap_or_default());
            let to_free = ((free_ratio - ffr) * du.files as f32) as isize;
            let cnt = self.fc.memory_index.len() as isize;
            if to_free > cnt {
                num = 0;
            } else {
                num = cnt - to_free
            };
        }

        let mut to_del = vec![];
        let mut cnt: usize = 0;
        let mut freed: usize = 0;
        let now = get_sys_time_in_secs();
        let cutoff = now - self.fc.cache_expire.as_secs();
        let mut last_key = EMPTY_SLICE_KEY;
        let mut last_value = Arc::new(CacheIndexValue {
            block_size: 0,
            access_time: 0,
            path: "".to_string(),
            link: false,
        });
        let mut new_used = used_cache.unwrap_or_else(|| self.fc.used.load(Ordering::Acquire));
        // for each two random keys, then compare the access time, evict the older one
        for e in self.fc.memory_index.iter() {
            let k = e.key();
            let v = e.value();
            if !matches!(self.fc.cache_eviction, CacheEviction::Disable) && v.access_time < cutoff {
                last_key = k.clone();
                last_value = v.clone();
                cnt += 1;
            } else if cnt == 0 || last_value.access_time > v.access_time {
                last_key = k.clone();
                last_value = v.clone();
            }
            cnt += 1;
            if cnt > 1 {
                let size = last_value.block_size + CACHE_SIZE_PADDING;
                freed += size;
                new_used -= size;
                to_del.push((last_key, v.clone()));
                cnt = 0;
                if (self.fc.memory_index.len() as isize) < num && new_used < goal {
                    self.fc.used.store(new_used, Ordering::Release);
                    break;
                };
            }
        }
        if to_del.len() > 0 {
            debug!(
                "cleanup cache {}, {} blocks {} MB, freed {} blocks {} MB",
                &cache_dir,
                self.fc.memory_index.len(),
                self.fc.used.load(Ordering::Acquire) >> 20,
                to_del.len(),
                freed >> 20,
            );
        }
        let handles = to_del
            .iter()
            .map(|(sid, iv)| {
                self.fc.memory_index.remove(sid);
                debug!("delete cache file: {}", iv.path);
                self.fc.local_store.delete(&iv.path)
            })
            .collect::<Vec<_>>();
        // wait for all the delete operations finished
        for r in futures::future::join_all(handles).await {
            if let Err(e) = r {
                warn!("failed to delete cache file: {:?}", e);
            }
        }
        // re calculate the free ratio
        let (br, fr) = self.fc.cur_free_ratio();
        let full = br < self.fc.free_ratio || fr < self.fc.free_ratio;
        self.fc.raw_full.store(full, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use kiseki_utils::logger::install_fmt_log;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_full() {
        install_fmt_log();

        let cache = JuiceFileCacheBuilder::default()
            .with_capacity(ReadableSize::mb(1))
            .with_free_ratio(0.5)
            .inner_build()
            .unwrap();
        basic(cache.clone()).await;
        cache.0.cleanup_full().await;
        cache.close().await;
    }

    async fn basic(cache: JuiceFileCache) {
        assert!(cache.get(SliceKey::random()).await.is_none());
        struct CacheReq {
            slice_key: SliceKey,
            block: Vec<u8>,
            stage: bool,
        }

        let cache_reqs = (0..1024)
            .map(|_| {
                let key = SliceKey::random();
                let block = (0..1024).map(|_| rand::random::<u8>()).collect();
                CacheReq {
                    slice_key: key,
                    block,
                    stage: rand::random::<bool>(),
                }
            })
            .collect::<Vec<_>>();

        for req in cache_reqs.iter() {
            if req.stage {
                cache
                    .stage(req.slice_key, Arc::new(req.block.clone()), false)
                    .await
                    .unwrap()
            } else {
                cache
                    .cache(req.slice_key, Arc::new(req.block.clone()))
                    .await;
            }
        }

        cache.wait_on_all_flush_finish().await;

        for req in cache_reqs.iter() {
            let mut reader = cache.get(req.slice_key).await;
            if let Some(r) = reader.as_mut() {
                let mut buf = vec![];
                r.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, req.block);
            }
        }

        assert_eq!(cache.0.pending_set.len(), 0);
    }
}
