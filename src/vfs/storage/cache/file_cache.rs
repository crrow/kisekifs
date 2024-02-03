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

use std::sync::atomic::AtomicIsize;
use std::{
    error::Error,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crc32fast::Hasher;
use dashmap::DashMap;
use moka::future::Cache;
use opendal::{Operator as ObjectStorage, Reader};

use snafu::ResultExt;
use tokio::{io::AsyncWriteExt, select, sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::{
    common::readable_size::ReadableSize,
    meta::types::SliceID,
    vfs::{
        err::{CacheIOSnafu, OpenDalSnafu, Result},
        storage::engine::CacheEviction,
        VFSError,
    },
};

const RAW_CACHE_DIR: &'static str = "raw";

fn cache_file_path(cache_file_dir: &str, slice_id: SliceID) -> String {
    join_path(cache_file_dir, &format!("{}", slice_id))
}

fn join_path(parent: &str, child: &str) -> String {
    let output = format!("{}/{}", parent, child);
    opendal::raw::normalize_path(&output)
}

pub(crate) struct FileCacheBuilder {
    pub(crate) cache_dir: PathBuf,
    pub(crate) capacity: ReadableSize,
    pub(crate) free_ratio: f32,
    pub(crate) add_checksum: bool,
}

impl FileCacheBuilder {
    pub(crate) fn build(self) -> FileCache {
        todo!()
    }
}

#[derive(Debug, Clone)]
struct IndexValue {
    block_size: usize,
    access_time: u64,
    // the path of the block on the local store.
    path: String,
}

/// A file cache manages files on local store and evict files based
/// on size.
pub(crate) struct FileCache {
    local_store: ObjectStorage,
    cache_root_dir: String,
    add_checksum: bool,
    free_ratio: f32,
    capacity: isize,
    cache_eviction: CacheEviction,
    cache_expire: Duration,

    used: Arc<AtomicIsize>,
    stage_full: Arc<AtomicBool>,
    raw_full: Arc<AtomicBool>, // TODO: rename me
    total_buffered_block_cnt: Arc<AtomicUsize>,
    memory_buffer: DashMap<SliceID, Arc<Vec<u8>>>,
    memory_index: Cache<SliceID, Arc<IndexValue>>,
    full: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    disk_item_tx: mpsc::Sender<DiskItem>,
}

impl FileCache {
    /// Load a block from cache.
    pub(crate) async fn load(self: &Arc<Self>, slice_id: SliceID) -> Option<Reader> {
        // We must use `get()` to update the estimator of the cache.
        // See https://docs.rs/moka/latest/moka/future/struct.Cache.html#method.contains_key
        if self.memory_index.get(&slice_id).await.is_none() {
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
        self.memory_index.remove(&slice_id).await;
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

    pub(crate) async fn cache(self: &Arc<Self>, slice_id: SliceID, block: Arc<Vec<u8>>) {
        if self.full.load(Ordering::Acquire) {
            return;
        }
        // check the memory index
        if self.memory_index.contains_key(&slice_id) {
            return;
        }
        // check the memory buffer
        if self.memory_buffer.contains_key(&slice_id) {
            return;
        }
        // update the total blocks.
        self.total_buffered_block_cnt.fetch_add(1, Ordering::AcqRel);
        // cache the memory buf.
        self.memory_buffer.insert(slice_id, block.clone());
        if let Err(e) = self.disk_item_tx.send(DiskItem { slice_id, block }).await {
            // failed to send the block to the flush task, discard it
            debug!(
                "failed to cache block to disk: {e}, discard it {}",
                slice_id
            );
            self.memory_buffer.remove(&slice_id);
            self.total_buffered_block_cnt.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn cache_path(self: &Arc<Self>, slice_id: SliceID) -> String {
        join_path(
            &join_path(&self.cache_root_dir, RAW_CACHE_DIR),
            &format!("{}", slice_id),
        )
    }

    async fn update_memory_index(self: &Arc<Self>, slice_id: SliceID, iv: Arc<IndexValue>) {
        self.memory_index.insert(slice_id, iv).await;
    }

    async fn clean_up_full(self: Arc<Self>) {
        let mut goal = self.capacity * 95 / 100;
        // Returns an approximate number of entries in this cache.
        let mut num = self.memory_index.entry_count() as isize * 99 / 100;
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
            let cnt = self.memory_index.entry_count() as isize;
            if to_free > cnt {
                num = 0;
            } else {
                num = cnt - to_free
            };
        }

        let mut to_del = vec![];
        let mut cnt: usize = 0;
        let mut freed: isize = 0;
        let now = Instant::now().elapsed().as_secs();
        let cutoff = now - self.cache_expire.as_secs();
        let mut last_key = Arc::new(0);
        let mut last_value = Arc::new(IndexValue {
            block_size: 0,
            access_time: 0,
            path: "".to_string(),
        });
        let mut new_used = used_cache.unwrap_or_else(|| self.used.load(Ordering::Acquire));
        // for each two random keys, then compare the access time, evict the older one
        for (k, v) in self.memory_index.iter() {
            if v.block_size < 0 {
                continue; // staging
            }
            if !matches!(self.cache_eviction, CacheEviction::Disable) && v.access_time < cutoff {
                last_key = k;
                last_value = v.clone();
                cnt += 1;
            } else if cnt == 0 || last_value.access_time > v.access_time {
                last_key = k;
                last_value = v.clone();
            }
            cnt += 1;
            if cnt > 1 {
                // invalid the index cache first
                self.memory_index.remove(&last_key).await;
                let size = last_value.block_size as isize + CACHE_SIZE_PADDING;
                freed += size;
                new_used -= size;
                to_del.push(v.clone());
                cnt = 0;
                if (self.memory_index.entry_count() as isize) < num && new_used < goal {
                    self.used.store(new_used, Ordering::Release);
                    break;
                };
            }
        }
        if to_del.len() > 0 {
            debug!(
                "cleanup cache {}, {} blocks {} MB, freed {} blocks {} MB",
                self.cache_root_dir,
                self.memory_index.entry_count(),
                self.used.load(Ordering::Acquire) >> 20,
                to_del.len(),
                freed >> 20,
            );
        }
        let handles = to_del
            .iter()
            .map(|iv| self.local_store.delete(&iv.path))
            .collect::<Vec<_>>();
        for r in futures::future::join_all(handles).await {
            if let Err(e) = r {
                warn!("failed to delete cache file: {:?}", e);
            }
        }
    }
}

const CACHE_SIZE_PADDING: isize = 4096;

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
    fc: Arc<FileCache>,
    rx: mpsc::Receiver<DiskItem>,
    cancel_token: CancellationToken,
}

impl FlushTask {
    async fn run(mut self) {
        let fc = self.fc;
        loop {
            select! {
                _ = self.cancel_token.cancelled() => {
                    debug!("flush task is cancelled");
                    return;
                }
                disk_item = self.rx.recv() => {
                    if let Some(disk_item) = disk_item {
                        let sto = fc.local_store.clone();
                        let path = fc.cache_path(disk_item.slice_id);
                        let block_len = disk_item.block.len();
                        let slice_id = disk_item.slice_id;
                        // do the real work
                        if persistent_block(sto.clone(), &path, disk_item.block, fc.add_checksum)
                            .await.is_ok() {
                            fc.update_memory_index(slice_id, Arc::new(IndexValue {
                                block_size: block_len,
                                access_time: Instant::now().elapsed().as_secs(),
                                path: path.clone(),
                            })).await;
                        }
                        // remove the block from memory buffer
                        if fc.memory_buffer.remove(&disk_item.slice_id).is_none() {
                            // the block is already removed by other thread.
                            // we should remove the block we just added to the disk.
                            tokio::join!(sto.delete(&path),fc.memory_index.remove(&slice_id));
                        };
                        fc.total_buffered_block_cnt.fetch_sub(1, Ordering::AcqRel);
                    }
                }
            }
        }
    }
}

/// FreeSpaceChecker is used for checking the free space of the disk.
struct FreeSpaceChecker {
    eviction: CacheEviction,
    cache_root_dir: String,
    min_free_ratio: f32,
    stage_full: Arc<AtomicBool>,
    raw_full: Arc<AtomicBool>, // TODO: rename me
    cancel_token: CancellationToken,
}

impl FreeSpaceChecker {
    async fn run(self) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            select! {
                _ = check_interval.tick() => {}
                _ = self.cancel_token.cancelled() => {
                    debug!("free space checker is cancelled");
                    return;
                }
            }
            let (bfr, ffr) = get_free_ratio(&self.cache_root_dir);
            let stage_full = bfr < self.min_free_ratio / 2.0 || ffr < self.min_free_ratio / 2.0;
            let raw_full = bfr < self.min_free_ratio || ffr < self.min_free_ratio;
            // update the state back, make we can reject the new requests.
            self.stage_full.store(stage_full, Ordering::Release);
            self.raw_full.store(raw_full, Ordering::Release);

            if raw_full && !matches!(self.eviction, CacheEviction::Disable) {
                trace!(
                    "Cleanup cache when checking free space {}: free ratio ({:.2}%), space usage ({:.2}%), inodes usage ({:.2}%)",
                    &self.cache_root_dir,
                    self.min_free_ratio * 100.0,
                    bfr * 100.0,
                    ffr * 100.0
                );
            }
        }
    }
}
