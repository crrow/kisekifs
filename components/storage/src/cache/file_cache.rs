// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A write-back cache for immutable Object Blocks, which will be used both for
//! the reading and flushing. It puts data in the local file system, and also
//! maintains a memory index.  
//!
//! ## Where it will be used?
//! 1. When slice buffer is going to flush, we take over the flush process,
//! stage the data to the local file system first, then flush the data to the
//! remote storage in the background.
//! 2. When we read the slice, we first check the cache.
//!
//! ## Key Components
//! 2. StageCache: when the block data is large, we treat the data as stage data
//! put them in the stage cache dir, each block represents a file.
//! 3. Disk Eviction: When the cache is full, we need to evict the data from the
//! disk to the remote storage.
//! 4. Flusher: a background task periodically flush the expired data to the
//!    remote
//! storage.
//! 5. Recover: when the system restarts, we will clean all cache and flush old
//!    staged data to the remote storage.

use std::{cmp::min, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use kiseki_common::{BlockIndex, PAGE_SIZE};
use kiseki_types::slice::{SliceID, SliceKey};
use kiseki_utils::{
    object_storage::{LocalStorage, ObjectReader, ObjectStorage},
    readable_size::ReadableSize,
};
use snafu::ResultExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::io::StreamReader;
use tracing::{debug, error, warn};

use crate::{
    err::{Error::CacheError, ObjectStorageSnafu, Result, UnknownIOSnafu},
    pool::Page,
};

pub const DEFAULT_STAGE_CACHE_SIZE: u64 = 10 << 30;
// 10GiB
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(24 * 3600); // 24 hours

pub struct Config {
    /// The directory to store the stage cache.
    pub stage_cache_dir: PathBuf,
    /// The max size of the stage cache. 10GiB by default.
    pub max_stage_size:  ReadableSize,
    /// How long can an object live after it has been staged.
    /// Zero means it will never expire, default by 24 hour.
    pub cache_ttl:       Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stage_cache_dir: PathBuf::from(kiseki_common::KISEKI_DEBUG_STAGE_CACHE),
            max_stage_size:  ReadableSize(DEFAULT_STAGE_CACHE_SIZE),
            cache_ttl:       DEFAULT_CACHE_TTL,
        }
    }
}

pub type FileCacheRef = Arc<FileCache>;

pub struct FileCache {
    stage_dir:      PathBuf,
    index:          moka::future::Cache<SliceKey, CacheIndex>,
    remote_storage: ObjectStorage,
    local_storage:  LocalStorage,
}

impl FileCache {
    pub fn new(config: Config, remote_storage: ObjectStorage) -> Result<Self> {
        // TODO: flush all the staged data to the remote storage at the beginning.

        let local_storage =
            kiseki_utils::object_storage::new_local_object_store(&config.stage_cache_dir)
                .context(ObjectStorageSnafu)?;

        let local_storage_clone = local_storage.clone();
        let remote_storage_clone = remote_storage.clone();
        let eviction_listener =
            move |k: Arc<SliceKey>, v: CacheIndex, cause| -> moka::notification::ListenerFuture {
                debug!(
                    "evicting block from the stage cache: {:?}, reason: {:?}",
                    k, cause
                );
                let local_storage = local_storage_clone.clone();
                let remote_storage = remote_storage_clone.clone();
                // Create a Future that removes the block from the local storage and
                // flushes it to the remote storage.
                //
                // Convert the regular Future into ListenerFuture. This method is
                // provided by moka::future::FutureExt trait.
                moka::future::FutureExt::boxed(async move {
                    if let Err(e) = migrate_from_local_to_remote(
                        local_storage,
                        remote_storage,
                        v.slice_key.block_size,
                        &k,
                    )
                    .await
                    {
                        error!("Failed to flush the block to the remote storage: {:?}", e);
                    }
                })
            };

        // The index-cache will try to evict entries that have not been used recently or
        // very often.
        let index = moka::future::Cache::builder()
            // the index-cache will be bounded by the total weighted size of entries.
            .weigher(|_, index: &CacheIndex| -> u32 { index.slice_key.block_size as u32 })
            // the cache should not grow beyond a certain size,
            // use the max_capacity method of the CacheBuilder
            // to set the upper bound.
            .max_capacity(config.max_stage_size.as_bytes())
            // A cached entry will be expired after the specified duration past from insert.
            // No matter how often the entry is accessed, it will be expired after the duration.
            .time_to_live(config.cache_ttl)
            .async_eviction_listener(eviction_listener)
            .build();

        Ok(Self {
            stage_dir: config.stage_cache_dir,
            index,
            remote_storage,
            local_storage,
        })
    }

    pub async fn get(self: &Arc<Self>, slice_key: &SliceKey) -> Result<Option<ObjectReader>> {
        match self.index.get(slice_key).await {
            None => Ok(None),
            Some(_) => {
                let path = slice_key.make_object_storage_path();
                let reader = self
                    .local_storage
                    .get(&path)
                    .await
                    .context(ObjectStorageSnafu)?;
                Ok(Some(reader))
            }
        }
    }

    pub async fn get_range(
        self: &Arc<Self>,
        slice_key: &SliceKey,
        offset: usize,
        length: usize,
    ) -> Result<Option<Bytes>> {
        match self.index.get(slice_key).await {
            None => {
                warn!("block not found in the stage cache: {:?}", slice_key);
                Ok(None)
            }
            Some(_) => {
                let path = slice_key.make_object_storage_path();
                debug!(
                    "find block in the stage cache: {:?}, try to use path: {:?} to load",
                    slice_key, &path
                );
                let bytes = self
                    .local_storage
                    .get_range(&path, offset..offset + length)
                    .await
                    .context(ObjectStorageSnafu)?;
                Ok(Some(bytes))
            }
        }
    }

    pub async fn stage(
        self: &Arc<Self>,
        sid: SliceID,
        block_index: BlockIndex,
        block_length: usize,
        pages: Box<[Option<Page>]>,
    ) -> Result<(usize, usize)> {
        let key = SliceKey::new(sid, block_index, block_length);
        debug!("staging block: {:?}", key);
        let mut total_flush_len = 0;
        let mut total_release_page_cnt = 0;
        let _ = self
            .index
            .try_get_with(key, async {
                let (_, mut writer) = self
                    .local_storage
                    .put_multipart(&key.make_object_storage_path())
                    .await
                    .context(ObjectStorageSnafu)?;
                let (tfl, trc) =
                    copy_from_buffer_to_local(block_length, pages, &mut writer).await?;
                total_flush_len = tfl;
                total_release_page_cnt = trc;
                let idx = CacheIndex { slice_key: key };
                Ok(idx) as Result<CacheIndex>
            })
            .await
            .map_err(|e| CacheError {
                error: e.to_string(),
            })?;

        Ok((total_flush_len, total_release_page_cnt))
    }
}

async fn copy_from_buffer_to_local(
    block_length: usize,
    pages: Box<[Option<Page>]>,
    writer: &mut Box<dyn AsyncWrite + Unpin + Send>,
) -> Result<(usize, usize)> {
    let mut total_released_page_cnt = 0;
    let mut current_flush_length = 0;

    while current_flush_length < block_length {
        let page_idx = current_flush_length / PAGE_SIZE;
        let page_offset = current_flush_length % PAGE_SIZE;
        let to_flush_len = min(PAGE_SIZE - page_offset, block_length - current_flush_length);
        match &pages[page_idx] {
            None => {
                let buf = vec![0u8; to_flush_len];
                writer.write_all(&buf).await.context(UnknownIOSnafu)?;
                // for _ in 0..to_flush_len {
                //     writer.write_u8(0).await.context(UnknownIOSnafu)?;
                // }
            }
            Some(page) => {
                total_released_page_cnt += 1;
                page.copy_to_writer(page_offset, to_flush_len, writer)
                    .await?;
            }
        }
        current_flush_length += to_flush_len;
    }
    if let Err(e) = writer.flush().await {
        panic!(
            "close writer failed: {:?}, expect flush len: {}",
            e,
            ReadableSize(block_length as u64).to_string()
        );
    }
    writer.shutdown().await.context(UnknownIOSnafu)?;
    Ok((block_length, total_released_page_cnt))
}

async fn migrate_from_local_to_remote(
    local_storage: LocalStorage,
    remote_storage: ObjectStorage,
    expect_copy_len: usize,
    key: &SliceKey,
) -> Result<()> {
    let path = key.make_object_storage_path();
    let reader = local_storage.get(&path).await.context(ObjectStorageSnafu)?;
    let reader_stream = reader.into_stream();
    let mut reader = StreamReader::new(reader_stream);
    // let mut reader = reader_stream.into_async_read();
    let (_id, mut writer) = remote_storage
        .put_multipart(&path)
        .await
        .context(ObjectStorageSnafu)?;
    let copy_len = tokio::io::copy(&mut reader, &mut writer)
        .await
        .context(UnknownIOSnafu)?;
    writer.flush().await.context(UnknownIOSnafu)?;
    writer.shutdown().await.context(UnknownIOSnafu)?;
    assert_eq!(copy_len, expect_copy_len as u64);

    local_storage
        .delete(&path)
        .await
        .context(ObjectStorageSnafu)?;

    Ok(())
}

#[derive(Clone, Debug)]
struct CacheIndex {
    slice_key: SliceKey,
}

#[cfg(test)]
mod tests {
    use kiseki_common::BLOCK_SIZE;

    use super::*;
    use crate::pool;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        kiseki_utils::logger::install_fmt_log();

        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            stage_cache_dir: tempdir.path().to_path_buf(),
            max_stage_size:  ReadableSize(30 << 20), // 30M
            cache_ttl:       Duration::from_secs(1),
        };
        let remote_storage = kiseki_utils::object_storage::new_memory_object_store();
        let cache = Arc::new(FileCache::new(config, remote_storage).unwrap());

        let pool_size = 20 << 20; // 20M
        let memory_pool = pool::memory_pool::MemoryPagePool::new(PAGE_SIZE, pool_size);

        let mut pages: Box<[Option<Page>]> = (0..(BLOCK_SIZE / PAGE_SIZE)).map(|_| None).collect();
        let mem_page = memory_pool.acquire_page().await;
        let content = b"hello world".to_vec();
        let mut reader = std::io::Cursor::new(&content);
        mem_page
            .copy_from_reader(0, content.len(), &mut reader)
            .await
            .unwrap();
        pages[0] = Some(Page::Memory(mem_page));

        let slice_key = SliceKey::new(1, 1, content.len());

        // the local storage should be empty at beginning.
        let get_result = cache
            .local_storage
            .get(&slice_key.make_object_storage_path())
            .await;
        assert!(get_result.is_err());

        let (total_flush_len, total_release_page) = cache
            .stage(
                slice_key.slice_id,
                slice_key.block_idx,
                slice_key.block_size,
                pages,
            )
            .await
            .unwrap();
        assert_eq!(total_flush_len, slice_key.block_size);
        assert_eq!(total_release_page, 1);

        let cache_index = cache.index.get(&slice_key).await.unwrap();
        println!("{:?}", cache_index);
        // we should be able to find the block from local storage.
        let bytes = cache
            .local_storage
            .get(&slice_key.make_object_storage_path())
            .await
            .unwrap();
        let buf = bytes.bytes().await.unwrap();
        assert_eq!(buf.len(), slice_key.block_size);
        assert_eq!(buf.as_ref(), content.as_slice());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let cached = cache.index.get(&slice_key).await;
        assert!(cached.is_none());

        // the local storage should be empty after the cache expired.
        let bytes = cache
            .local_storage
            .get(&slice_key.make_object_storage_path())
            .await;
        assert!(bytes.is_err());

        // now, we could find the block from the remote storage.
        let bytes = cache
            .remote_storage
            .get(&slice_key.make_object_storage_path())
            .await
            .unwrap();
        let buf = bytes.bytes().await.unwrap();
        assert_eq!(buf.len(), slice_key.block_size);
        assert_eq!(buf.as_ref(), content.as_slice());
    }
}
