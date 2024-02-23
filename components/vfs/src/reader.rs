use std::{
    cell::UnsafeCell,
    cmp::{max, min},
    collections::HashMap,
    fmt::Debug,
    io::Cursor,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use dashmap::DashMap;
use kiseki_common::{BlockIndex, BlockSize, ChunkIndex, BLOCK_SIZE, CHUNK_SIZE, FH};
use kiseki_meta::MetaEngineRef;
use kiseki_storage::{
    cache::{file_cache::FileCacheRef, mem_cache::MemCacheRef},
    err::{JoinErrSnafu, ObjectStorageSnafu, UnknownIOSnafu},
};
use kiseki_types::{
    ino::Ino,
    slice::{make_slice_object_key, OverlookedSlicesRef, Slice, SliceID, SliceKey},
};
use kiseki_utils::{object_storage::ObjectStorage, readable_size::ReadableSize};
use rangemap::RangeMap;
use snafu::ResultExt;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{debug, error, instrument, warn};

use crate::{
    data_manager::{DataManager, DataManagerRef},
    err::{ObjectBlockNotFoundSnafu, Result, StorageSnafu},
    handle::HandleTable,
    KisekiVFS,
};

impl DataManager {
    /// [open_file_reader] will create [FileReader] for the given [Ino] and [FH]
    /// if not exists.
    pub(crate) async fn open_file_reader(
        self: &Arc<Self>,
        inode: Ino,
        fh: FH,
        length: usize,
    ) -> Arc<FileReader> {
        let mut outer_read_guard = self.file_readers.read().await;
        if !outer_read_guard.contains_key(&inode) {
            drop(outer_read_guard);

            let mut out_write_guard = self.file_readers.write().await;
            // check again
            if !out_write_guard.contains_key(&inode) {
                out_write_guard.insert(inode, Default::default());
            }
            drop(out_write_guard);

            // acquire the read lock again.
            outer_read_guard = self.file_readers.read().await;
        }

        let inner_map = outer_read_guard.get(&inode).unwrap().clone();
        // Release the outer lock before acquiring the inner lock to avoid deadlock
        drop(outer_read_guard);

        // check if exists the file reader for the specified FH.
        let inner_read_guard = inner_map.read().await;
        if let Some(fr) = inner_read_guard.get(&fh) {
            return fr.clone();
        }
        // not found, we have to create one.
        drop(inner_read_guard);

        // use write lock
        let mut inner_write_guard = inner_map.write().await;
        // check again in case of someone has created the file reader already.
        if let Some(fr) = inner_write_guard.get(&fh) {
            return fr.clone();
        }
        // no one create the file reader for real, we have to create it.
        let fr = Arc::new(FileReader {
            data_engine: Arc::downgrade(&self),
            ino: inode,
            fh,
            length: AtomicUsize::new(length),
            file_cache: self.file_cache.clone(),
            mem_cache: self.mem_cache.clone(),
            chunks: Default::default(),
            closing: Default::default(),
        });
        inner_write_guard.insert(fh, fr.clone());
        return fr;
    }

    /// [truncate_reader] is called when we modify the file length.
    /// Invoke this method to truncate the file reader's length.
    pub(crate) async fn truncate_reader(self: &Arc<Self>, inode: Ino, length: u64) {
        /// It's possible that the [FileReader] doesn't exist, even if we create
        /// the FileReader in the [HandleTable::new_file_handle].
        /// Since there are some other operation that can modify the file
        /// length, such as [set_attr], etc. Especially when we haven't
        /// open the file handle yet.
        let out_read_guard = self.file_readers.read().await;
        if let Some(inner_map) = out_read_guard.get(&inode) {
            let inner_map = inner_map.clone();
            // drop the outer lock before we try to acquire the inner lock.
            drop(out_read_guard);

            // acquire the inner lock.
            let mut inner_read_guard = inner_map.read().await;
            for (_, fr) in inner_read_guard.iter() {
                // TODO: review me, should we use compare and swap?
                fr.length.store(length as usize, Ordering::Release);
            }
        } else {
            warn!("not found any file reader under: {:?}", inode);
        }
    }
}

/// A [FileReader] is used for read content from a file.
/// Each [FileReader] is held by a FileHandle: Ino+Fh.
pub(crate) struct FileReader {
    data_engine: Weak<DataManager>,
    // The file inode.
    ino:         Ino,
    fh:          FH,
    // The max file read length, it was set when we crate the file handle.
    length:      AtomicUsize,
    // the write back cache.
    file_cache:  FileCacheRef,
    // the read-only cache.
    mem_cache:   MemCacheRef,
    // A file be divided into multiple chunks,
    // each chunk is composed by multiple slices.
    // This map is used to store latest slices that compose the chunk.
    // TODO: get rid of DashMap
    chunks:      DashMap<ChunkIndex, OverlookedSlicesRef>,
    // The file is closing or not.
    closing:     AtomicBool,
}

impl FileReader {
    /// [read] will read the content of file from the specified [offset].
    pub(crate) async fn read(self: &Arc<Self>, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let expected_read_len = dst.len();
        let length = self.length.load(Ordering::Acquire);
        // read offset should not exceed the file length.
        if offset >= length || expected_read_len == 0 {
            return Ok(0);
        }

        // cal the real read length.
        let expected_read_len = if offset + expected_read_len > length {
            length - offset
        } else {
            expected_read_len
        };

        debug!(
            "{:?}, actual can read length: {}",
            self.ino,
            ReadableSize(expected_read_len as u64)
        );

        // get the slice inside the chunk.
        let engine = self
            .data_engine
            .upgrade()
            .expect("engine should not be dropped");
        let meta_engine = engine.meta_engine.clone();
        let chunk_size = engine.chunk_size;
        let start_chunk_idx = offset / chunk_size;
        let end_chunk_idx = (offset + expected_read_len - 1) / chunk_size;

        let mut total_read_len = 0;
        let mut left_to_read = expected_read_len;
        for chunk_idx in start_chunk_idx..=end_chunk_idx {
            // offset to current chunk.
            let mut chunk_pos = (offset + total_read_len) % chunk_size;
            // max can read in current chunk.
            let max_can_read = min(chunk_size - chunk_pos, left_to_read);
            // then we get the range to read in current chunk.
            let current_read_range = chunk_pos..chunk_pos + max_can_read;
            // according to current chunk idx, we can get the slices.
            let raw_slices = match meta_engine.read_slice(self.ino, chunk_idx).await {
                Ok(v) => v,
                Err(e) => {
                    debug!("read slice error: {:?}", e);
                    panic!("read slice error: {:?}", e);
                }
            };
            let raw_slices = match raw_slices {
                None => {
                    debug!("no slice in chunk: {:?}", chunk_idx);
                    return Ok(0);
                }
                Some(v) => v,
            };

            for x in raw_slices.0.iter() {
                debug!("find raw-slice in chunk: {:?}, slice: {:?}", chunk_idx, x);
            }

            // make a virtual slice map to record the slice and hole.
            let mut virtual_slice_map = RangeMap::new();
            {
                // let overlap = raw_slices.overlook();
                // let overlap_slices = overlap.iter().collect_vec();
                // println!("overlap_slices: {:?}", overlap_slices);
            }

            let range_map = raw_slices.overlook();
            for x in range_map.gaps(&current_read_range) {
                debug!("current range: {:?}, find gap: {:?}", current_read_range, x);
                virtual_slice_map.insert(x, VirtualSlice::Hole);
            }
            for (r, s) in range_map.overlapping(&current_read_range) {
                let new_r =
                    (max(r.start, current_read_range.start)..min(r.end, current_read_range.end));
                debug!(
                    "find overlapping slice in chunk: {:?}, range: {:?}, new_range: {:?}, slice: \
                     {:?}",
                    chunk_idx, r, new_r, s
                );
                virtual_slice_map.insert(new_r, VirtualSlice::Slice(s.clone()));
            }
            drop(range_map);

            for (r, vs) in virtual_slice_map {
                let start = total_read_len + r.start - chunk_pos;
                let end = total_read_len + r.end - chunk_pos;
                let len = end - start;

                // assert!(start <= end, "VS: {:?}, start: {}, end: {}, expected_read_len: {}",
                // vs, ReadableSize(start as u64), ReadableSize(end as u64),
                // ReadableSize(expected_read_len as u64)); assert!(start <
                // expected_read_len, "VS: {:?}, start: {}, end: {}, expected_read_len: {}", vs,
                // ReadableSize(start as u64), ReadableSize(end as u64),
                // ReadableSize(expected_read_len as u64)); assert!(end <=
                // expected_read_len, "VS: {:?}, start: {}, end: {}, expected_read_len: {}", vs,
                // ReadableSize(start as u64), ReadableSize(end as u64),
                // ReadableSize(expected_read_len as u64));

                match vs {
                    VirtualSlice::Hole => {
                        debug!(
                            "chunk_size{}, find hole in chunk: {:?}, range: {:?}",
                            chunk_size, chunk_idx, r,
                        );
                        // we may even don't have to write the 0.
                        for i in r {
                            dst[i - chunk_pos] = 0;
                        }
                    }
                    VirtualSlice::Slice(s) => {
                        debug!(
                            "find slice in chunk: {:?}, range: {:?}, slice: {:?}, write buf \
                             [{start}, {end}]",
                            chunk_idx, r, s
                        );
                        let sid = s.get_id();

                        let read_len = read_slice_from_cache(
                            sid,
                            engine.file_cache.clone(),
                            engine.mem_cache.clone(),
                            s.get_underlying_size(),
                            0,
                            &mut dst[start..end],
                        )
                        .await?;

                        // let rb = engine.new_read_buffer(s.get_id(),
                        // s.get_underlying_size());
                        // rb.read_at(0, &mut
                        // dst[start..end]).context(StorageSnafu)?;
                    }
                }
                total_read_len += len;
                left_to_read -= len;
                chunk_pos += len;
            }
        }

        Ok(total_read_len)
    }

    /// When release file handle, we will try to close the associated
    /// FileReader.
    pub(crate) async fn close(self: &Arc<Self>) {
        if self
            .closing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            debug!("someone else win the close contention: {:?}", self.ino);
            return;
        }

        let engine = self
            .data_engine
            .upgrade()
            .expect("engine should not be dropped");

        let mut outer_guard = engine.file_readers.read().await;
        if let Some(inner_map) = outer_guard.get(&self.ino) {
            let inner_map = inner_map.clone();
            drop(outer_guard);

            let inner_read_guard = inner_map.read().await;
            if inner_read_guard.contains_key(&self.fh) {
                drop(inner_read_guard);

                let mut inner_write_guard = inner_map.write().await;
                inner_write_guard.remove(&self.fh);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum VirtualSlice {
    Hole,
    Slice(Slice),
}

#[instrument(skip_all, fields(length, offset))]
async fn read_slice_from_cache(
    slice_id: SliceID,
    file_cache: FileCacheRef,
    mem_cache: MemCacheRef,
    length: usize, // length of the slice.
    offset: usize, // read offset
    dst: &mut [u8],
) -> Result<usize> {
    let expected_read_len = dst.len();
    if expected_read_len == 0 {
        return Ok(0);
    }

    debug_assert!(
        offset + expected_read_len <= CHUNK_SIZE,
        "offset {} + expect read len {} will exceed the chunk size",
        offset,
        expected_read_len
    );

    let expected_read_len = min(length - offset, expected_read_len);
    let mut total_read_len = 0;
    let mut hanldes = vec![];
    let dst_ptr = dst.as_mut_ptr();
    let dst_len = dst.len();

    while total_read_len < expected_read_len {
        let new_pos = total_read_len + offset;
        let block_idx = new_pos / BLOCK_SIZE;
        let block_offset = new_pos % BLOCK_SIZE;
        let obj_block_size = cal_object_block_size(length, block_idx, BLOCK_SIZE);
        let current_block_to_read_len = min(
            expected_read_len - total_read_len,
            obj_block_size - block_offset, // don't exceed the block boundary.
        );

        // FIXME: use chunks_exact_mut to build slice.
        let dst = unsafe { std::slice::from_raw_parts_mut(dst_ptr, dst_len) };
        let dst_slice = &mut dst[total_read_len..(total_read_len + current_block_to_read_len)];
        let mut writer = Cursor::new(dst_slice);

        total_read_len += current_block_to_read_len;

        let file_cache_clone = file_cache.clone();
        let mem_cache_clone = mem_cache.clone();
        let handle: JoinHandle<Result<usize>> = tokio::spawn(async move {
            let key = SliceKey::new(slice_id, block_idx, obj_block_size);
            // 1. first of all, try to get block from file-cache
            if let Some(mut block_reader) = file_cache_clone
                .get_range(&key, block_offset, current_block_to_read_len)
                .await?
            {
                let mut slice = block_reader.as_ref();
                // copy to the writer
                let copy_len = tokio::io::copy(&mut slice, &mut writer)
                    .await
                    .context(UnknownIOSnafu)?;
                return Ok(copy_len as usize);
            }

            // 2. try to get block from mem-cache
            if let Some(mut block) = mem_cache_clone.get(&key).await? {
                // copy to the writer
                let reader = block.slice(block_offset..block_offset + current_block_to_read_len);
                let mut slice = reader.as_ref();
                let copy_len = tokio::io::copy(&mut slice, &mut writer)
                    .await
                    .context(UnknownIOSnafu)?;
                return Ok(copy_len as usize);
            }

            // 3. report not found error
            return ObjectBlockNotFoundSnafu { key }.fail()?;
        });
        hanldes.push(handle);
    }

    let mut actual_read_cnt = 0;
    for x in futures::future::try_join_all(hanldes)
        .await
        .context(JoinErrSnafu)?
        .into_iter()
    {
        actual_read_cnt += x?;
    }
    assert_eq!(actual_read_cnt, total_read_len);

    Ok(total_read_len)
}

fn cal_object_block_size(length: usize, block_idx: BlockIndex, block_size: BlockSize) -> usize {
    // min(1025 - 0 * 1024, 1024) = min(1024) = 1024
    // min(1023 - 0 * 1024, 1024) = min(1023, 1024) = 1023
    // min(2049 - 2 * 1024, 1024) = min(1, 1024) = 1
    min(length - block_idx * block_size, block_size)
}

#[cfg(test)]
mod tests {
    use kiseki_meta::{context::FuseContext, MetaConfig};
    use kiseki_types::{ino::ROOT_INO, setting::Format};
    use kiseki_utils::{logger::install_fmt_log, object_storage::new_memory_object_store};

    use super::*;

    #[test]
    fn get_from_range_map() {
        let mut range_map = rangemap::RangeMap::new();
        range_map.insert(0..10, 1);
        range_map.insert(10..12, 2);
        range_map.iter().for_each(|(r, v)| {
            println!("{:?} -> {:?}", r, v);
        });
    }

    #[test]
    fn make_virtual_map() {
        let chunk_size = 1024usize;
        let mut rm = rangemap::RangeMap::new();
        rm.insert(0..3, Slice::new_owned(0, 0, 3));
        rm.insert(12..15, Slice::new_owned(12, 1, 3));
        rm.insert(
            chunk_size - 3..chunk_size,
            Slice::new_owned(chunk_size - 3, 2, 3),
        );
        rm.insert(
            chunk_size..chunk_size + 8,
            Slice::new_owned(chunk_size, 3, 8),
        );

        let mut virtual_slice_map = RangeMap::new();
        let read_range = chunk_size - 4..chunk_size + 8;
        for x in rm.gaps(&read_range) {
            if x.end < read_range.start {
                continue;
            }
            virtual_slice_map.insert(x, VirtualSlice::Hole);
        }
        for (r, s) in rm.overlapping(&read_range) {
            virtual_slice_map.insert(r.clone(), VirtualSlice::Slice(s.clone()));
        }
        for (r, vs) in virtual_slice_map {
            match vs {
                VirtualSlice::Hole => {
                    println!("find hole in range: {:?}", r);
                }
                VirtualSlice::Slice(s) => {
                    println!("find slice in range: {:?}, slice: {:?}", r, s);
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read() {
        install_fmt_log();

        let meta_config = MetaConfig::default();
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let fuse_ctx = Arc::new(FuseContext::background());
        let (inode, _attr) = meta_engine
            .create(fuse_ctx, ROOT_INO, "a", 0o650, 0, 0)
            .await
            .unwrap();

        let data_manager = Arc::new(DataManager::new(
            format.page_size,
            format.block_size,
            format.chunk_size,
            meta_engine,
            new_memory_object_store(),
        ));

        data_manager.open_file_writer(inode, 0);
        let data = b"hello world" as &[u8];

        let write_len = data_manager
            .write(inode, 0, data)
            .await
            .map_err(|e| println!("{}", e))
            .unwrap();

        let fw = data_manager.find_file_writer(inode).unwrap();
        fw.finish().await.unwrap();

        let file_reader = data_manager.open_file_reader(inode, 0, write_len).await;

        let mut read_data = vec![0u8; 11];
        let read_len = file_reader.read(0, read_data.as_mut_slice()).await;
        assert!(read_len.is_ok());
        let read_len = read_len.unwrap();
        assert_eq!(read_len, 11);
        assert!(read_data.starts_with(b"hello world"));
        println!("{}", String::from_utf8_lossy(&read_data));

        let chunk_size = data_manager.chunk_size;
        let write_len = data_manager
            .write(inode, chunk_size - 3, data)
            .await
            .map_err(|e| println!("{}", e))
            .unwrap();
        assert_eq!(write_len, data.len());

        fw.finish().await.unwrap();
        let file_len = fw.get_length();
        assert_eq!(file_len, chunk_size + 8);

        let mut read_data = vec![0u8; 11];

        file_reader.close().await;

        let file_reader = data_manager.open_file_reader(inode, 0, file_len).await;
        let read_len = file_reader
            .read(chunk_size - 3, read_data.as_mut_slice())
            .await
            .unwrap();
        assert_eq!(read_len, 11);
        println!("{}", String::from_utf8_lossy(&read_data));
        assert!(read_data.starts_with(b"hello world"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_write_1_g() {
        install_fmt_log();

        let mut meta_config = MetaConfig::default();
        let tempdir = tempfile::tempdir().unwrap();
        let format = Format::default();
        let temppath = tempdir.path().to_str().unwrap();
        meta_config.dsn = format!("rocksdb://:{}", temppath);
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let fuse_ctx = Arc::new(FuseContext::background());
        let (inode, _attr) = meta_engine
            .create(fuse_ctx, ROOT_INO, "a", 0o650, 0, 0)
            .await
            .unwrap();

        let data_manager = Arc::new(DataManager::new(
            format.page_size,
            format.block_size,
            format.chunk_size,
            meta_engine,
            new_memory_object_store(),
        ));

        data_manager.open_file_writer(inode, 0);
        let step_size: usize = 4 << 20;
        let total_step: usize = 1 << 30 / step_size;
        let data = vec![1u8; step_size];

        let fw = data_manager.find_file_writer(inode).unwrap();
        for i in 0..total_step {
            let write_len = fw.write(i * step_size, &data).await.unwrap();
            assert_eq!(write_len, step_size);
        }
        fw.finish().await.unwrap();

        let file_reader = data_manager
            .open_file_reader(inode, 0, total_step * step_size)
            .await;
        let page_size: usize = 128 << 10;
        let total_read_step = 1 << 30 / page_size;
        let expect_read_content = vec![1u8; page_size];
        for i in 0..total_read_step {
            let mut read_data = vec![0u8; page_size];
            let read_len = file_reader
                .read(i * page_size, &mut read_data)
                .await
                .unwrap();
            assert_eq!(read_len, page_size);
            assert_eq!(read_data, expect_read_content);
        }
    }
}
