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

use std::{
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use crc32fast::Hasher;
use dashmap::{DashMap, DashSet};
use futures::FutureExt;
use opendal::{Builder, Operator as ObjectStorage, Reader};
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

use crate::{
    common::{readable_size::ReadableSize, runtime},
    meta::types::{random_slice_id, SliceID},
    vfs::{
        err::{CacheIOSnafu, OpenDalSnafu, Result},
        storage::cache::Cache,
        VFSError,
    },
};

const CACHE_SIZE_PADDING: isize = 4096;
const MAX_EXPIRE_CNT: usize = 1000;
const RAW_CACHE_DIR: &'static str = "raw";

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
}

impl Default for JuiceFileCacheBuilder {
    fn default() -> Self {
        let tempdir = tempfile::tempdir().expect("should fail when creating temp dir");
        let dir = tempdir
            .as_ref()
            .to_str()
            .expect("should fail when converting to str");
        JuiceFileCacheBuilder {
            cache_dir: dir.to_string(),
            capacity: ReadableSize::gb(1),
            free_ratio: 0.1, // 10 %
            add_checksum: false,
            cache_eviction: CacheEviction::Random,
            cache_expire: Duration::from_secs(1),
            max_pending_cnt: 5,
        }
    }
}

impl JuiceFileCacheBuilder {
    pub fn build(self) -> Result<Arc<dyn Cache>> {
        let jc = self.inner_build()?;
        Ok(Arc::new(jc))
    }

    pub(crate) fn inner_build(self) -> Result<JuiceFileCache> {
        debug!("create juice file cache at {}", &self.cache_dir);
        let local_store = new_fs_store(&self.cache_dir)?;
        let (sender, receiver) = mpsc::channel(self.max_pending_cnt);
        let cancel_token = CancellationToken::new();
        let tracker = TaskTracker::new();
        let (cleanup_full_tx, cleanup_full_rx) = watch::channel(0);
        let fc = Arc::new(JuiceFileCacheInner {
            cache_root_dir: self.cache_dir,
            add_checksum: self.add_checksum,
            free_ratio: self.free_ratio,
            capacity: self.capacity.as_bytes() as isize,
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
            cancel_token,
            disk_item_tx: sender,
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
fn new_fs_store(path: &str) -> Result<ObjectStorage> {
    let temp_dir = format!("{}-temp", path);
    let mut builder = opendal::services::Fs::default();
    builder.root(path);
    // when doesn't enable it, encounter a case that read a file then the file
    // get deleted.
    builder.atomic_write_dir(&temp_dir);
    let obj = opendal::Operator::new(builder)
        .context(OpenDalSnafu)?
        .finish();
    Ok(obj)
}

fn cache_file_path(cache_file_dir: &str, slice_id: SliceID) -> String {
    join_path(cache_file_dir, &format!("{}", slice_id))
}

fn join_path(parent: &str, child: &str) -> String {
    let output = format!("{}/{}", parent, child);
    opendal::raw::normalize_path(&output)
}

#[derive(Debug, Clone)]
struct IndexValue {
    block_size: usize,
    access_time: u64,
    // the path of the block on the local store.
    path: String,
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
    async fn cache(&self, slice_id: u64, block: Arc<Vec<u8>>) -> bool {
        // we expose weak cache for low latency,
        // so we reject the request if the cache is in cleanup.
        self.0.cache_weak(slice_id, block).await
    }

    async fn get(&self, slice_id: SliceID) -> Option<Reader> {
        self.0.get(slice_id).await
    }

    async fn wait_on_all_flush_finish(&self) {
        self.0.wait_on_all_flush_finish().await
    }

    async fn close(&self) {
        self.0.close().await
    }

    async fn remove(&self, slice_id: SliceID) {
        self.0.remove(slice_id).await
    }

    async fn stage(&self, slice_id: SliceID, data: Arc<Vec<u8>>, keep_cache: bool) {
        self.0.stage(slice_id, data, keep_cache).await
    }
}

/// The JuiceFS file cache implementation.
pub(crate) struct JuiceFileCacheInner {
    cache_root_dir: String,
    add_checksum: bool,
    free_ratio: f32,
    capacity: isize,
    cache_eviction: CacheEviction,
    cache_expire: Duration,

    local_store: ObjectStorage,

    // for test and benchmark, tell the caller that we have finished the flush.
    flush_finished_notify: Arc<Notify>,
    used: Arc<AtomicIsize>,
    stage_full: Arc<AtomicBool>,
    raw_full: Arc<AtomicBool>, // TODO: rename me
    memory_index: DashMap<SliceID, Arc<IndexValue>>,
    cancel_token: CancellationToken,
    // if the slice is in the pending set, we won't cache it again.
    // when the background task is processing the block, we remove
    // the key from the pending set.
    pending_set: DashSet<SliceID>,
    // check how many memory use while we caching the block.
    memory_usage: Arc<AtomicUsize>,

    disk_item_tx: mpsc::Sender<DiskItem>,
    task_tracker: TaskTracker,
    // we may call the cleanup manually, so maintain the state to reject the new requests.
    in_cleanup_full: Arc<AtomicBool>,
    cleanup_full_finished_notify: Arc<Notify>,
    cleanup_full_tx: watch::Sender<SliceID>,
}

impl JuiceFileCacheInner {
    /// Load a block from cache.
    pub(crate) async fn get(self: &Arc<Self>, slice_id: SliceID) -> Option<Reader> {
        if !self.memory_index.contains_key(&slice_id) {
            return None;
        }

        let file_path = self.cache_path(slice_id);
        match self.get_reader(&file_path).await {
            Ok(Some(reader)) => {
                return Some(reader);
            }
            Err(e) => {
                if !matches!(&e, VFSError::OpenDal { error, .. } if error.kind() == opendal::ErrorKind::NotFound)
                {
                    warn!("{:?}, Failed to get file for key {:?}", e, file_path);
                };
            }
            Ok(None) => {}
        }

        // We remove the file from the index.
        self.memory_index.remove(&slice_id);
        None
    }

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

    pub(crate) async fn cache(self: &Arc<Self>, slice_id: SliceID, block: Arc<Vec<u8>>) -> bool {
        self.do_cache(slice_id, block, false).await
    }

    pub(crate) async fn cache_weak(
        self: &Arc<Self>,
        slice_id: SliceID,
        block: Arc<Vec<u8>>,
    ) -> bool {
        self.do_cache(slice_id, block, true).await
    }

    async fn do_cache(
        self: &Arc<Self>,
        slice_id: SliceID,
        block: Arc<Vec<u8>>,
        weak: bool, // weak means we can reject the request if the cache is in cleanup.
    ) -> bool {
        // check the memory index
        if self.memory_index.contains_key(&slice_id) {
            return false;
        }
        // check the memory buffer
        if self.pending_set.contains(&slice_id) {
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
        // update the total blocks.
        self.memory_usage.fetch_add(block_len, Ordering::AcqRel);
        // cache the memory buf.
        self.pending_set.insert(slice_id);
        if let Err(e) = self.disk_item_tx.send(DiskItem { slice_id, block }).await {
            // failed to send the block to the flush task, discard it
            debug!(
                "failed to cache block to disk: {e}, discard it {}",
                slice_id
            );
            self.pending_set.remove(&slice_id);
            self.memory_usage.fetch_sub(block_len, Ordering::AcqRel);
            return false;
        };
        true
    }

    fn cache_path(self: &Arc<Self>, slice_id: SliceID) -> String {
        join_path(
            &join_path(&self.cache_root_dir, RAW_CACHE_DIR),
            &format!("{}", slice_id),
        )
    }

    async fn update_memory_index(
        self: &Arc<Self>,
        slice_id: SliceID,
        block_len: usize,
        mut access_time: u64,
    ) {
        if let Some(old) = self.memory_index.get(&slice_id) {
            self.used.fetch_sub(
                old.block_size as isize + CACHE_SIZE_PADDING,
                Ordering::AcqRel,
            );
            if access_time == 0 {
                access_time = old.access_time
            };
        }
        self.memory_index.insert(
            slice_id,
            Arc::new(IndexValue {
                block_size: block_len,
                access_time,
                path: self.cache_path(slice_id),
            }),
        );
        if block_len > 0 {
            self.used
                .fetch_add(block_len as isize + CACHE_SIZE_PADDING, Ordering::AcqRel);
        }
        if self.used.load(Ordering::Acquire) > self.capacity
            && !matches!(self.cache_eviction, CacheEviction::Disable)
        {
            self.notify_to_cleanup_full(slice_id).await;
        }
    }

    pub(crate) async fn cleanup_full(self: &Arc<Self>) {
        self.notify_to_cleanup_full(random_slice_id()).await;
    }

    async fn notify_to_cleanup_full(self: &Arc<Self>, changed: u64) {
        if self.in_cleanup_full.load(Ordering::Acquire) {
            return;
        }

        if let Err(e) = self.cleanup_full_tx.send(changed) {
            warn!("failed to notify the cleanup full: {:?}", e);
        }
    }

    // we should not call this method directly, it's for the background task.
    async fn do_clean_up_full(self: &Arc<Self>) {
        defer!({
            self.in_cleanup_full.store(false, Ordering::Release);
            self.cleanup_full_finished_notify.notify_waiters();
        });
        let mut goal = self.capacity * 95 / 100;
        // Returns an approximate number of entries in this cache.
        let mut num = self.memory_index.len() as isize * 99 / 100;
        // make sure we have enough free space after cleanup
        let (bfr, ffr) = get_free_ratio(&self.cache_root_dir);
        let mut du_cache = None;
        let mut used_cache = None;
        if bfr < self.free_ratio {
            let du = get_disk_usage(&self.cache_root_dir).unwrap_or_default();
            let to_free = ((self.free_ratio - bfr) * du.total as f32) as isize;
            du_cache = Some(du);
            let used = self.used.load(Ordering::Acquire);
            used_cache = Some(used);
            if to_free > used {
                goal = 0;
            } else if used - to_free < goal {
                goal = used - to_free;
            }
        }
        if ffr < self.free_ratio {
            let du = du_cache
                .unwrap_or_else(|| get_disk_usage(&self.cache_root_dir).unwrap_or_default());
            let to_free = ((self.free_ratio - ffr) * du.files as f32) as isize;
            let cnt = self.memory_index.len() as isize;
            if to_free > cnt {
                num = 0;
            } else {
                num = cnt - to_free
            };
        }

        let mut to_del = vec![];
        let mut cnt: usize = 0;
        let mut freed: isize = 0;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now - self.cache_expire.as_secs();
        let mut last_key = 0;
        let mut last_value = Arc::new(IndexValue {
            block_size: 0,
            access_time: 0,
            path: "".to_string(),
        });
        let mut new_used = used_cache.unwrap_or_else(|| self.used.load(Ordering::Acquire));
        // for each two random keys, then compare the access time, evict the older one
        for e in self.memory_index.iter() {
            let k = e.key();
            let v = e.value();
            if v.block_size < 0 {
                continue; // staging
            }
            if !matches!(self.cache_eviction, CacheEviction::Disable) && v.access_time < cutoff {
                last_key = *k;
                last_value = v.clone();
                cnt += 1;
            } else if cnt == 0 || last_value.access_time > v.access_time {
                last_key = *k;
                last_value = v.clone();
            }
            cnt += 1;
            if cnt > 1 {
                let size = last_value.block_size as isize + CACHE_SIZE_PADDING;
                freed += size;
                new_used -= size;
                to_del.push((last_key, v.clone()));
                cnt = 0;
                if (self.memory_index.len() as isize) < num && new_used < goal {
                    self.used.store(new_used, Ordering::Release);
                    break;
                };
            }
        }
        if to_del.len() > 0 {
            debug!(
                "cleanup cache {}, {} blocks {} MB, freed {} blocks {} MB",
                self.cache_root_dir,
                self.memory_index.len(),
                self.used.load(Ordering::Acquire) >> 20,
                to_del.len(),
                freed >> 20,
            );
        }
        let handles = to_del
            .iter()
            .map(|(sid, iv)| {
                self.memory_index.remove(sid);
                debug!("delete cache file: {}", iv.path);
                self.local_store.delete(&iv.path)
            })
            .collect::<Vec<_>>();
        // wait for all the delete operations finished
        for r in futures::future::join_all(handles).await {
            if let Err(e) = r {
                warn!("failed to delete cache file: {:?}", e);
            }
        }
        // re calculate the free ratio
        let (br, fr) = self.cur_free_ratio();
        let full = br < self.free_ratio || fr < self.free_ratio;
        self.raw_full.store(full, Ordering::Release);
    }

    pub(crate) fn cur_free_ratio(self: &Arc<Self>) -> (f32, f32) {
        get_free_ratio(&self.cache_root_dir)
    }

    pub(crate) async fn stage(
        self: &Arc<Self>,
        slice_id: SliceID,
        data: Arc<Vec<u8>>,
        keep_cache: bool,
    ) {
        // TODO: implement the stage logic.
    }

    pub(crate) async fn upload_staging(self: &Arc<Self>) {}

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

    pub(crate) async fn memory_usage(self: &Arc<Self>) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }

    pub(crate) async fn remove(self: &Arc<Self>, slice_id: SliceID) {
        if self.pending_set.remove(&slice_id).is_some() {
            // the block is in the pending set, we can return now
            return;
        };

        // the slice has been cached, clean it.
        if let Some((_, iv)) = self.memory_index.remove(&slice_id) {
            if iv.block_size > 0 {
                self.used.fetch_sub(
                    iv.block_size as isize + CACHE_SIZE_PADDING,
                    Ordering::AcqRel,
                );
            }
            if let Err(e) = self.local_store.delete(&iv.path).await {
                warn!("failed to delete cache file: {:?}", e);
            }
            // TODO: remove stage.
        }
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
        .context(CacheIOSnafu)?;

    if add_checksum {
        let mut hasher = Hasher::new();
        hasher.update(block.as_slice());
        writer
            .write_u32(hasher.finalize())
            .await
            .context(CacheIOSnafu)?;
    }
    writer.close().await.context(OpenDalSnafu)?;

    Ok(())
}

// DiskItem represents a block on disk.
struct DiskItem {
    slice_id: SliceID,
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
                        debug!("flush task received a flush block req: {}", disk_item.slice_id);
                        let sto = fc.local_store.clone();
                        let path = fc.cache_path(disk_item.slice_id);
                        let block_len = disk_item.block.len();
                        let slice_id = disk_item.slice_id;
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
                            fc.update_memory_index(slice_id, block_len, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).await;
                        }
                        // remove the block from memory buffer
                        if fc.pending_set.remove(&disk_item.slice_id).is_none() {
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
    cleanup_full_rx: watch::Receiver<SliceID>,
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
                    self.fc.do_clean_up_full().await;
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
                freed += e.block_size as isize + CACHE_SIZE_PADDING;
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
                _ = self.cancel_token.cancelled() => {
                    debug!("free space checker is cancelled");
                    return;
                }
                _ = self.fc.do_clean_up_full() => {
                    debug!("cleanup full finished");
                }
            }
            // we still raw_full, we won't accept new requests.
            if self.fc.raw_full.load(Ordering::Acquire) {
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
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{common::install_fmt_log, meta::types::random_slice_id};
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
        assert!(cache.get(random_slice_id()).await.is_none());
        struct CacheReq {
            slice_id: SliceID,
            block: Vec<u8>,
        }

        let cache_reqs = (0..1024)
            .map(|_| {
                let slice_id = random_slice_id();
                let block = (0..1024).map(|_| rand::random::<u8>()).collect();
                CacheReq { slice_id, block }
            })
            .collect::<Vec<_>>();

        for req in cache_reqs.iter() {
            cache.cache(req.slice_id, Arc::new(req.block.clone())).await;
        }

        cache.wait_on_all_flush_finish().await;

        for req in cache_reqs.iter() {
            let mut reader = cache.get(req.slice_id).await;
            if let Some(r) = reader.as_mut() {
                let mut buf = vec![];
                r.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, req.block);
            }
        }

        assert_eq!(cache.0.pending_set.len(), 0);
    }
}
