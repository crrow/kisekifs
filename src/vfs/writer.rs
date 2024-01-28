use std::{
    cmp::{max, min},
    io::Write,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::SystemTime,
};

use bytes::BufMut;
use dashmap::{DashMap, DashSet};
use datafusion_common::{arrow::array::Array, DataFusionError};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use libc::{EINTR, EIO};
use rand::{Rng, SeedableRng};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::{
    select,
    sync::{Mutex, Notify, RwLock},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::vfs::storage::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_PAGE_SIZE};
use crate::{
    meta,
    meta::{engine::MetaEngine, types::Ino},
    vfs::{
        err::{Result, VFSError},
        storage::{cal_chunk_idx, cal_chunk_offset, BufferManager, WriteBuffer},
    },
};

#[derive(Debug, Copy, Clone)]
pub(crate) struct DataManagerConfig {
    // for writer
    pub total_buffer_cap: usize,
    // the size of chunk.
    pub chunk_size: usize,
    // the size of block which will be uploaded to object storage.
    pub block_size: usize,
    // the smallest alloc size of the write buffer.
    pub page_size: usize,
}

impl Default for DataManagerConfig {
    fn default() -> Self {
        DataManagerConfig {
            total_buffer_cap: 300 << 20,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

/// The manager of all writer buffers.
#[derive(Debug)]
pub(crate) struct DataManager {
    config: DataManagerConfig,
    meta_engine: Arc<MetaEngine>,
    buffer_manager: Arc<BufferManager>,
    ino_chunk_map: DashMap<Ino, Arc<ChunkManager>>,
}

impl DataManager {
    pub(crate) fn new(
        config: DataManagerConfig,
        meta_engine: Arc<MetaEngine>,
        buffer_manager: Arc<BufferManager>,
    ) -> DataManager {
        DataManager {
            config,
            ino_chunk_map: Default::default(),
            buffer_manager,
            meta_engine,
        }
    }

    pub(crate) fn new_write_op(self: &Arc<Self>, offset: usize, data: &[u8]) -> WriteOp {
        WriteOp::new(
            self.new_chunk_manager(Ino(1)),
            offset,
            data,
            self.meta_engine.clone(),
            self.buffer_manager.clone(),
        )
    }

    pub(crate) fn new_chunk_manager(self: &Arc<Self>, ino: Ino) -> Weak<ChunkManager> {
        let fm = Arc::new(ChunkManager::new(self));
        self.ino_chunk_map.insert(ino, fm.clone());
        Arc::downgrade(&fm)
    }

    pub(crate) fn get_length(&self, ino: Ino) -> u64 {
        // TODO: don't know what this function do
        0
    }

    pub(crate) fn update_mtime(&self, ino: Ino, mtime: SystemTime) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct ChunkWriteLocation {
    // write to which chunk
    chunk_idx: usize,
    // the start offset of this write
    chunk_offset: usize,
    // the length of this write
    need_write_len: usize,
    // the start offset of the input buf
    buf_start_at: usize,
}

/// Write operation:
/// 1. locate the chunk
/// 2. locate slice in that chunk
/// 3. wait if someone is flushing this chunk
/// 4. wait if someone is writing this slice
/// 5. write to the slice
/// 6. if the slice is full, flush it in the background

#[derive(Debug)]
pub struct WriteOp {
    // The number of bytes written by this operation.
    offset: usize,
    // The data written by this operation.
    data: *const u8,
    // The expected length of the write operation.
    expect_write_len: usize,
    // Current file's manager.
    chunk_manager: Weak<ChunkManager>, // can we drop the chunk manager?
    meta_engine: Arc<MetaEngine>,
    buffer_manager: Arc<BufferManager>,
}

impl WriteOp {
    fn new(
        chunk_manager: Weak<ChunkManager>, // TODO: use weak instead.
        offset: usize,
        data: &[u8],
        meta_engine: Arc<MetaEngine>,
        buffer_manager: Arc<BufferManager>,
    ) -> Self {
        assert!(data.len() > 0);
        WriteOp {
            offset,
            data: data.as_ptr(),
            expect_write_len: data.len(),
            chunk_manager,
            meta_engine,
            buffer_manager,
        }
    }

    // Run the write operation.
    pub async fn run(&self) -> Result<usize> {
        let chunk_manager = self
            .chunk_manager
            .upgrade()
            // TODO: use better error
            .ok_or_else(|| VFSError::ErrLIBC { kind: EINTR })?;

        // 1. find the chunk
        let write_locations = self.find_write_location();

        let handles = write_locations
            .into_iter()
            .map(|location| {
                let data = &unsafe { std::slice::from_raw_parts(self.data, self.expect_write_len) }
                    [location.buf_start_at..location.buf_start_at + location.need_write_len];
                let chunk_manager = chunk_manager.clone();
                let handle = tokio::spawn(async move {
                    let chunk = chunk_manager.get_chunk(location.chunk_idx);

                    // wait until we can write.
                    if !chunk.wait_write_buffer(location.clone()).await {
                        // TODO: use better error
                        return Err(VFSError::ErrLIBC { kind: EIO });
                    }

                    // mark here comes a writing
                    chunk.inc_waiting_count();
                    // wait if someone is flushing.
                    if !chunk.wait_flush().await {
                        // TODO: use better error
                        return Err(VFSError::ErrLIBC { kind: EIO });
                    }
                    chunk.dec_waiting_count();

                    // start writing.
                    let write_len = chunk.write(location.chunk_offset, data).await?;
                    Ok(write_len)
                });
                handle
            })
            .collect::<Vec<_>>();

        let mut write_len = 0;
        for r in futures::future::join_all(handles).await {
            match r {
                Ok(r) => match r {
                    Ok(wl) => {
                        debug!("write {} bytes", wl);
                        write_len += wl
                    }
                    Err(e) => {
                        // TODO: use better error
                        return Err(VFSError::ErrLIBC { kind: EIO });
                    }
                },
                Err(e) => {
                    // TODO: use better error
                    return Err(VFSError::ErrLIBC { kind: EIO });
                }
            }
        }

        Ok(write_len)
    }

    fn find_write_location(&self) -> Vec<ChunkWriteLocation> {
        let offset = self.offset;
        let expected_write_len = self.expect_write_len;
        let chunk_size = self.buffer_manager.chunk_size();

        let start_chunk_idx = cal_chunk_idx(offset, chunk_size);
        let end_chunk_idx = cal_chunk_idx(offset + expected_write_len - 1, chunk_size);

        let mut chunk_pos = cal_chunk_offset(offset, chunk_size);
        let mut buf_start_at = 0;
        let mut left = expected_write_len;

        (start_chunk_idx..=end_chunk_idx)
            .into_iter()
            .map(move |idx| {
                let max_can_write = min(chunk_size - chunk_pos, left);
                // debug!(
                //     "chunk-size: {}, chunk: {} chunk_pos: {}, left: {}, buf start at: {}, max
                // can write: {}",     self.chunk_size, idx, chunk_pos, left,
                // buf_start_at, max_can_write, );

                let ctx = ChunkWriteLocation {
                    chunk_idx: idx,
                    chunk_offset: chunk_pos,
                    need_write_len: max_can_write,
                    buf_start_at,
                };
                chunk_pos = cal_chunk_offset(chunk_pos + max_can_write, chunk_size);
                buf_start_at = buf_start_at + max_can_write;
                left = left - max_can_write;
                ctx
            })
            .collect::<Vec<_>>()
    }
}

/// The ChunkManager manages all chunks in one file.
/// It has a background task to flush all slices in the chunk.
#[derive(Debug)]
pub(crate) struct ChunkManager {
    config: DataManagerConfig,
    data_manager: Weak<DataManager>,
    // Key: chunk_idx
    chunks: DashMap<usize, Arc<Chunk>>,
    // Make someone can wait until all slices are flushed,
    // or just cancel the background task.
    cancel_token: CancellationToken,
    // mark if the background task is still running.
    background_task_running: Arc<AtomicBool>,
    // how many slices are flushing right now.
    flushing_cnt: Arc<AtomicUsize>,
    // notify the waiting write operation to continue.
    flushing_finished_notify: Arc<Notify>,
    flush_ch: (Sender<Arc<SliceController>>, Receiver<Arc<SliceController>>),
    release_ch: (Sender<Arc<SliceController>>, Receiver<Arc<SliceController>>),
}

impl ChunkManager {
    fn new(data_manager: &Arc<DataManager>) -> ChunkManager {
        Self {
            config: data_manager.config.clone(),
            data_manager: Arc::downgrade(data_manager),
            chunks: Default::default(),
            cancel_token: CancellationToken::new(),
            background_task_running: Arc::new(Default::default()),
            // only 5 slices can be flushed at the same time.
            flush_ch: tokio::sync::mpsc::channel(5),
            // only 5 slices can be released at the same time.
            release_ch: tokio::sync::mpsc::channel(5),
        }
    }

    fn get_chunk(self: &Arc<Self>, chunk_idx: usize) -> Arc<Chunk> {
        let c = self
            .chunks
            .entry(chunk_idx)
            .or_insert_with(|| Arc::new(Chunk::new(chunk_idx, self)));
        c.value().clone()
    }

    // Start the background task.
    async fn start_background_task(self: &Arc<Self>) {
        if self.background_task_running.load(Ordering::SeqCst) {
            return;
        }
        tokio::spawn(async move {
            self.commit_task().await;
            self.background_task_running.store(false, Ordering::SeqCst);
        });
    }

    // commit_task will exit once it founds no more slice need to flush.
    async fn commit_task(&self) {
        loop {
            select! {
                _ = self.cancel_token.cancelled() => {
                    debug!("commit_task cancelled");
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
struct Chunk {
    chunk_manager: Weak<ChunkManager>,
    config: DataManagerConfig,
    // the chunk_idx of this chunk.
    chunk_idx: usize,
    // current length of the chunk, which should be smaller
    // than CHUNK_SIZE.
    length: usize,
    // one chunk can have multiple slices.
    slice_controllers: Arc<Mutex<Vec<Arc<SliceController>>>>,
    // how many write operations are flushing this chunk right now.
    flush_count: Arc<AtomicUsize>,
    // this chunk is flushed. notify the waiting write operation to
    // continue.
    flush_finished: Arc<Notify>,
    // how many write operations are waiting for flushing end.
    write_waiting_count: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
}

impl Chunk {
    fn new(idx: usize, chunk_manager: &Arc<ChunkManager>) -> Chunk {
        let cancel_token = chunk_manager.cancel_token.clone();
        let config = chunk_manager.config.clone();
        Chunk {
            chunk_manager: Arc::downgrade(chunk_manager),
            config,
            chunk_idx: idx,
            length: 0,
            slice_controllers: Arc::new(Mutex::new(vec![])),
            flush_count: Arc::new(Default::default()),
            flush_finished: Arc::new(Default::default()),
            write_waiting_count: Arc::new(Default::default()),
            cancel_token,
        }
    }

    async fn wait_flush(&self) -> bool {
        let cancel_token = self.cancel_token.clone();
        let notify = self.flush_finished.clone();
        let flush_counter = self.flush_count.clone();
        let handle = tokio::spawn(async move {
            loop {
                if flush_counter.load(Ordering::SeqCst) == 0 {
                    break;
                }
                select! {
                    _ = notify.notified() => {
                        break;
                    }
                    _ = cancel_token.cancelled() => {
                        debug!("wait_flush cancelled");
                        break;
                    }
                }
            }
        });
        handle.await.expect("wait_flush failed");
        !self.cancel_token.is_cancelled()
    }

    fn inc_waiting_count(&self) {
        self.write_waiting_count.fetch_add(1, Ordering::SeqCst);
    }

    fn dec_waiting_count(&self) {
        self.write_waiting_count.fetch_sub(1, Ordering::SeqCst);
    }

    async fn wait_write_buffer(&self, location: ChunkWriteLocation) -> bool {
        // let mut interval =
        // tokio::time::interval(tokio::time::Duration::from_millis(10));
        // let cancel_token = self.cancel_token.clone();
        // let mr = self.buffer_usage.clone();
        // let size = self.cal_alloc_bytes(&location);
        // if size == 0 {
        //     return true;
        // }
        // let pool = self.total_buffer.clone();
        // let handle = tokio::task::spawn(async move {
        //     let mut guard = mr.lock().await;
        //     loop {
        //         if cancel_token.is_cancelled() {
        //             break;
        //         }
        //         tokio::select! {
        //             res1 = interval.tick() => {
        //                 match guard.try_grow(size) {
        //                     Ok(_) => {
        //                         debug!("chunk: {} increase memory usage {}",
        // location.chunk_idx, size);                         break;
        //                     }
        //                     Err(_) => {
        //                         warn!(
        //                             "this write operation is blocked since high
        // memory usage: \                             idx: {},
        //                             want: {}, current usage: {}, memory pool usage:
        // {}",                             location.chunk_idx,
        //                             size,
        //                             guard.size(),
        //                             pool.reserved(),
        //                         );
        //                     }
        //                 }
        //             },
        //             res2 = cancel_token.cancelled() => {
        //                 break;
        //             },
        //         }
        //     }
        // });
        // handle.await.expect("wait_until_can_write failed");
        // !self.cancel_token.is_cancelled()
        return true;
    }

    // calculate how many bytes we need to allocate.
    fn cal_alloc_bytes(&self, location: &ChunkWriteLocation) -> usize {
        // TODO: we may not need to allocate memory if the location is overlapped.
        location.need_write_len
    }

    // Write data to the specified slice, according to the buffer length,
    // we need to flush and release memory.
    async fn write(self: &Arc<Self>, chunk_pos: usize, data: &[u8]) -> Result<usize> {
        let mut slice = self.find_writable_slice(chunk_pos).await;
        let (write_len, total_write_len, flushed_len) = slice
            .write(chunk_pos - slice.chunk_start_offset, data)
            .await?;

        if total_write_len == self.config.chunk_size {
            self.flush_and_release(slice).await;
        } else {
            self.flush(slice).await;
        }

        Ok(write_len)
    }

    // We try to find slice from the back to the front,
    // if we find the slice hasn't been frozen, we try to check
    // if the buffer still in memory, since once the slice upload
    // to the background, it will try to release the buffer.
    // Then we should choose the buffer alive one.
    //
    // If we can't find a slice to write for multiple times,
    // we should try to flush the slice to the background, and
    // release the memory.
    async fn find_writable_slice(&self, chunk_offset: usize) -> Arc<SliceController> {
        let mut guard = self.slice_controllers.lock().await;
        let slice_len = guard.len();

        for i in 0..slice_len {
            let sc = &guard[slice_len - i - 1];
            if !sc.frozen().await {
                let (flushed, length) = sc.get_flushed_length_and_write_length().await;
                if chunk_offset >= sc.chunk_start_offset + flushed
                    && chunk_offset <= sc.chunk_start_offset + length
                {
                    // we can write to this slice.
                    return sc.clone();
                }
            }
        }

        let cm = self
            .chunk_manager
            .upgrade()
            .expect("chunk manager may be dropped");

        let sw = Arc::new(SliceController::new(
            self.config.chunk_size,
            chunk_offset,
            cm.data_manager
                .upgrade()
                .expect("the data manager should not be dropped")
                .buffer_manager
                .new_write_buffer(),
        ));
        guard.push(sw.clone());
        sw
    }

    async fn flush(self: &Arc<Self>, sc: Arc<SliceController>) {}

    async fn flush_and_release(self: &Arc<Self>, sc: Arc<SliceController>) {}
}

/// SliceController is used to control the write to the write buffer.
#[derive(Debug)]
struct SliceController {
    // the slice id may be None if this slice is not flushed to the cloud.
    slice_id: Option<usize>,

    // the chunk size
    chunk_size: usize,

    // The chunk offset.
    chunk_start_offset: usize,

    // This slice is flushing right now.
    frozen: AtomicBool,

    // the underlying write buffer.
    write_buffer: RwLock<WriteBuffer>,

    last_modified: RwLock<Option<Instant>>,
}

impl SliceController {
    fn new(chunk_size: usize, offset: usize, write_buffer: WriteBuffer) -> SliceController {
        SliceController {
            slice_id: None,
            chunk_size,
            chunk_start_offset: offset,
            frozen: Default::default(),
            write_buffer: RwLock::new(write_buffer),
            last_modified: RwLock::new(None),
        }
    }

    // Write will return the current write len to this buffer,
    // the buffer total length, and the flushed length.
    async fn write(
        self: &Arc<Self>,
        slice_offset: usize,
        data: &[u8],
    ) -> Result<(usize, usize, usize)> {
        let mut guard = self.write_buffer.write().await;
        let write_len = guard
            .write_at(slice_offset, data)
            .expect("write data failed");
        self.last_modified.write().await.replace(Instant::now());
        Ok((write_len, guard.length(), guard.flushed_length()))
    }

    async fn get_flushed_length_and_write_length(&self) -> (usize, usize) {
        let guard = self.write_buffer.read().await;
        let flushed_len = guard.flushed_length();
        let write_len = guard.length();
        (flushed_len, write_len)
    }

    async fn get_flushed_length(&self) -> usize {
        let guard = self.write_buffer.read().await;
        guard.length()
    }

    async fn get_write_length(&self) -> usize {
        let guard = self.write_buffer.read().await;
        guard.length()
    }

    async fn frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use std::collections::HashMap;

    use super::*;
    use crate::common::install_fmt_log;
    use crate::{
        meta::MetaConfig,
        vfs::storage::{new_debug_sto, BufferManagerConfig},
    };

    #[test]
    fn memory_pool_basic() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let mut reservation = MemoryConsumer::new("test").register(&memory_pool);
        println!("{}", reservation.size());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn find_chunk_to_write() {
        install_fmt_log();
        let buffer_manager_config = BufferManagerConfig::default();
        let buffer_manager = Arc::new(BufferManager::new(buffer_manager_config, new_debug_sto()));
        let data_manager = Arc::new(DataManager::new(
            DataManagerConfig::default(),
            Arc::new(MetaConfig::default().open().unwrap()),
            buffer_manager.clone(),
        ));

        let chunk_size = buffer_manager.chunk_size();

        struct TestCase {
            offset: usize,
            data: Vec<u8>,
            want: Vec<ChunkWriteLocation>,
        }

        impl TestCase {
            async fn run(&self, data_manager: Arc<DataManager>) {
                let write_op = data_manager.new_write_op(self.offset, &self.data);
                let locations = write_op.find_write_location();
                assert_eq!(locations, self.want);
                let write_len = write_op.run().await;
                assert!(write_len.is_ok());
                assert_eq!(write_len.unwrap(), self.data.len());
            }
        }

        for i in vec![
            TestCase {
                offset: chunk_size,
                data: vec![1, 2, 3],
                want: vec![ChunkWriteLocation {
                    chunk_idx: 1,
                    chunk_offset: 0,
                    need_write_len: 3,
                    buf_start_at: 0,
                }],
            },
            TestCase {
                offset: chunk_size - 3,
                data: vec![1, 2, 3],
                want: vec![ChunkWriteLocation {
                    chunk_idx: 0,
                    chunk_offset: chunk_size - 3,
                    need_write_len: 3,
                    buf_start_at: 0,
                }],
            },
            TestCase {
                offset: chunk_size - 3,
                data: vec![1, 2, 3, 4],
                want: vec![
                    ChunkWriteLocation {
                        chunk_idx: 0,
                        chunk_offset: chunk_size - 3,
                        need_write_len: 3,
                        buf_start_at: 0,
                    },
                    ChunkWriteLocation {
                        chunk_idx: 1,
                        chunk_offset: 0,
                        need_write_len: 1,
                        buf_start_at: 3,
                    },
                ],
            },
            TestCase {
                offset: 0,
                data: vec![1, 2, 3, 4],
                want: vec![ChunkWriteLocation {
                    chunk_idx: 0,
                    chunk_offset: 0,
                    need_write_len: 4,
                    buf_start_at: 0,
                }],
            },
            TestCase {
                // FIXME: at present we won't free the memory, then we will wait forever.
                offset: 0,
                data: vec![0; 2 * chunk_size],
                want: vec![
                    ChunkWriteLocation {
                        chunk_idx: 0,
                        chunk_offset: 0,
                        need_write_len: chunk_size,
                        buf_start_at: 0,
                    },
                    ChunkWriteLocation {
                        chunk_idx: 1,
                        chunk_offset: 0,
                        need_write_len: chunk_size,
                        buf_start_at: chunk_size,
                    },
                ],
            },
        ] {
            i.run(data_manager.clone()).await;
        }
    }
}
