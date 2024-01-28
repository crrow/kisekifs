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
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    chunk::{cal_chunk_idx, cal_chunk_pos},
    meta,
    meta::{engine::MetaEngine, types::Ino},
    vfs::err::{Result, VFSError},
};

#[derive(Debug)]
pub struct WriteOp {
    chunk_size: usize,
    // The number of bytes written by this operation.
    offset: usize,
    // The data written by this operation.
    data: *const u8,
    // The expected length of the write operation.
    expect_write_len: usize,
    // Current file's manager.
    file_manager: Weak<FileManager>,
    meta_engine: Arc<MetaEngine>,
    chunk_engine: Arc<ChunkEngine>,
}

impl WriteOp {
    fn new(
        chunk_size: usize,
        chunk_manager: Weak<FileManager>, // TODO: use weak instead.
        offset: usize,
        data: &[u8],
        meta_engine: Arc<MetaEngine>,
        chunk_engine: Arc<ChunkEngine>,
    ) -> Self {
        assert!(data.len() > 0);
        WriteOp {
            chunk_size,
            offset,
            data: data.as_ptr(),
            expect_write_len: data.len(),
            file_manager: chunk_manager,
            meta_engine,
            chunk_engine,
        }
    }

    // Run the write operation.
    pub async fn run(&self) -> Result<usize> {
        let file_manager = self
            .file_manager
            .upgrade()
            // TODO: use better error
            .ok_or_else(|| VFSError::ErrLIBC { kind: EINTR })?;

        // 1. find the chunk
        let handles = file_manager
            .find_chunk_to_write(self.offset, self.expect_write_len)
            .into_iter()
            .map(|location| {
                let file_manager = file_manager.clone();
                let token = file_manager.cancel_token.clone();
                let mem_pool = file_manager.memory_pool.clone();
                let meta_engine = self.meta_engine.clone();
                let chunk_engine = self.chunk_engine.clone();
                let chunk_size = self.chunk_size;
                let data = &unsafe { std::slice::from_raw_parts(self.data, self.expect_write_len) }
                    [location.buf_start_at..location.buf_start_at + location.need_write_len];
                let handle = tokio::spawn(async move {
                    let mut c = file_manager
                        .chunks
                        .entry(location.chunk_idx)
                        .or_insert_with(|| {
                            Chunk::new(
                                chunk_size,
                                location.chunk_idx,
                                mem_pool,
                                token,
                                meta_engine,
                                chunk_engine,
                            )
                        });
                    let chunk = c.value_mut();

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

    fn get_locations(&self) -> impl Iterator<Item = ChunkWriteCtx> {
        let file_manager = self
            .file_manager
            .upgrade()
            // TODO: use better error
            .ok_or_else(|| VFSError::ErrLIBC { kind: EIO })
            .unwrap();

        // 1. find the chunk
        file_manager
            .find_chunk_to_write(self.offset, self.expect_write_len)
            .into_iter()
    }
}

use crate::chunk::{slice::WSlice, Engine as ChunkEngine};

/// The manager of all files.
#[derive(Debug)]
pub(crate) struct DataManager {
    // config
    chunk_size: usize,

    write_buffer: Arc<dyn MemoryPool>,
    cancel_token: CancellationToken,
    files: DashMap<Ino, Arc<FileManager>>,
    chunk_engine: Arc<ChunkEngine>,
    meta_engine: Arc<MetaEngine>,
}

impl DataManager {
    pub(crate) fn new(
        memory_pool: Arc<dyn MemoryPool>,
        cancel_token: CancellationToken,
        chunk_size: usize,
        meta_engine: Arc<MetaEngine>,
        chunk_engine: Arc<ChunkEngine>,
    ) -> DataManager {
        DataManager {
            write_buffer: memory_pool,
            cancel_token,
            files: Default::default(),
            chunk_size,
            chunk_engine,
            meta_engine,
        }
    }

    pub(crate) fn new_write_op(&self, offset: usize, data: &[u8]) -> WriteOp {
        WriteOp::new(
            self.chunk_size,
            self.new_file_manager(Ino(1)),
            offset,
            data,
            self.meta_engine.clone(),
            self.chunk_engine.clone(),
        )
    }

    pub(crate) fn new_file_manager(&self, ino: Ino) -> Weak<FileManager> {
        let fm = Arc::new(FileManager::new(
            self.chunk_size,
            self.cancel_token.clone(),
            self.write_buffer.clone(),
            self.chunk_engine.clone(),
        ));
        self.files.insert(ino, fm.clone());
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

/// A file is composed of multiple chunks.
#[derive(Debug)]
pub(crate) struct FileManager {
    // config
    // max size of each chunk bytes.
    chunk_size: usize,

    // Key: chunk_idx
    chunks: DashMap<usize, Chunk>,
    // When we merge slice or append slice,
    // we should update the slice counter.
    //
    // The slice counter is used for avoiding too
    // many random writes.
    slice_counter: AtomicUsize,

    // the total write buffer of this file.
    memory_pool: Arc<dyn MemoryPool>,
    // track the buffer usage of this file.
    buffer_usage: Arc<Mutex<MemoryReservation>>,
    cancel_token: CancellationToken,
    chunk_engine: Arc<ChunkEngine>,
}

impl FileManager {
    fn new(
        chunk_size: usize,
        token: CancellationToken,
        memory_pool: Arc<dyn MemoryPool>,
        chunk_engine: Arc<ChunkEngine>,
    ) -> FileManager {
        Self {
            chunk_size,
            chunks: Default::default(),
            slice_counter: Default::default(),
            buffer_usage: Arc::new(Mutex::new(
                MemoryConsumer::new("FileChunkManager").register(&memory_pool),
            )),
            memory_pool,
            cancel_token: token,
            chunk_engine,
        }
    }
    // Count the total number of slices in current file.
    fn total_slices(&self) -> usize {
        self.slice_counter.load(Ordering::SeqCst)
    }

    // This write may cross two chunks.
    fn find_chunk_to_write(&self, offset: usize, expected_write_len: usize) -> Vec<ChunkWriteCtx> {
        let start_chunk_idx = cal_chunk_idx(offset, self.chunk_size);
        let end_chunk_idx = cal_chunk_idx(offset + expected_write_len - 1, self.chunk_size);

        let mut chunk_pos = cal_chunk_pos(offset, self.chunk_size);
        let mut buf_start_at = 0;
        let mut left = expected_write_len;

        (start_chunk_idx..=end_chunk_idx)
            .into_iter()
            .map(move |idx| {
                let max_can_write = min(self.chunk_size - chunk_pos, left);
                // debug!(
                //     "chunk-size: {}, chunk: {} chunk_pos: {}, left: {}, buf start at: {}, max
                // can write: {}",     self.chunk_size, idx, chunk_pos, left,
                // buf_start_at, max_can_write, );

                let ctx = ChunkWriteCtx {
                    chunk_idx: idx,
                    chunk_offset: chunk_pos,
                    need_write_len: max_can_write,
                    buf_start_at,
                };
                chunk_pos = cal_chunk_pos(chunk_pos + max_can_write, self.chunk_size);
                buf_start_at = buf_start_at + max_can_write;
                left = left - max_can_write;
                ctx
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct ChunkWriteCtx {
    // write to which chunk
    chunk_idx: usize,
    // the start offset of this write
    chunk_offset: usize,
    // the length of this write
    need_write_len: usize,
    // the start offset of the input buf
    buf_start_at: usize,
}

#[derive(Debug)]
struct Chunk {
    chunk_size: usize,
    // the chunk_idx of this chunk.
    chunk_idx: usize,
    // current length of the chunk, which should be smaller
    // than CHUNK_SIZE.
    length: usize,
    // one chunk can have multiple slices.
    slices: Arc<Mutex<Vec<Arc<SliceWriter>>>>,
    // how many write operations are flushing this chunk right now.
    flush_count: Arc<AtomicUsize>,
    // this chunk is flushed. notify the waiting write operation to
    // continue.
    flush_finished: Arc<Notify>,
    // how many write operations are waiting for flushing end.
    write_waiting_count: Arc<AtomicUsize>,
    // is there a background task running.
    background_task_running: Arc<AtomicBool>,
    // the total write buffer.
    total_buffer: Arc<dyn MemoryPool>,
    // track the buffer usage of this chunk.
    buffer_usage: Arc<Mutex<MemoryReservation>>,
    cancel_token: CancellationToken,
    // the meta engine.
    meta_engine: Arc<MetaEngine>,
    chunk_engine: Arc<ChunkEngine>,
}

impl Chunk {
    fn new(
        idx: usize,
        chunk_size: usize,
        memory_pool: Arc<dyn MemoryPool>,
        cancellation_token: CancellationToken,
        meta_engine: Arc<MetaEngine>,
        chunk_engine: Arc<ChunkEngine>,
    ) -> Chunk {
        Chunk {
            chunk_size,
            chunk_idx: idx,
            length: 0,
            slices: Arc::new(Mutex::new(vec![])),
            flush_count: Arc::new(Default::default()),
            flush_finished: Arc::new(Default::default()),
            write_waiting_count: Arc::new(Default::default()),
            buffer_usage: Arc::new(Mutex::new(
                MemoryConsumer::new(format!("Chunk: {}", idx)).register(&memory_pool),
            )),
            total_buffer: memory_pool,
            cancel_token: cancellation_token,
            meta_engine,
            background_task_running: Arc::new(Default::default()),
            chunk_engine,
        }
    }

    async fn wait_flush(&self) -> bool {
        let token = self.cancel_token.clone();
        let notify = self.flush_finished.clone();
        let flush_counter = self.flush_count.clone();
        let handle = tokio::spawn(async move {
            loop {
                if flush_counter.load(Ordering::SeqCst) == 0 {
                    break;
                }
                tokio::select! {
                    _ = notify.notified() => {
                        break;
                    }
                    _ = token.cancelled() => {
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

    async fn wait_write_buffer(&self, location: ChunkWriteCtx) -> bool {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(10));
        let cancel_token = self.cancel_token.clone();
        let mr = self.buffer_usage.clone();
        let size = self.cal_alloc_bytes(&location);
        if size == 0 {
            return true;
        }
        let pool = self.total_buffer.clone();
        let handle = tokio::task::spawn(async move {
            let mut guard = mr.lock().await;
            loop {
                if cancel_token.is_cancelled() {
                    break;
                }
                tokio::select! {
                    res1 = interval.tick() => {
                        match guard.try_grow(size) {
                            Ok(_) => {
                                debug!("chunk: {} increase memory usage {}", location.chunk_idx, size);
                                break;
                            }
                            Err(_) => {
                                warn!(
                                    "this write operation is blocked since high memory usage: \
                                    idx: {},
                                    want: {}, current usage: {}, memory pool usage: {}",
                                    location.chunk_idx,
                                    size,
                                    guard.size(),
                                    pool.reserved(),
                                );
                            }
                        }
                    },
                    res2 = cancel_token.cancelled() => {
                        break;
                    },
                }
            }
        });
        handle.await.expect("wait_until_can_write failed");
        !self.cancel_token.is_cancelled()
    }

    // calculate how many bytes we need to allocate.
    fn cal_alloc_bytes(&self, location: &ChunkWriteCtx) -> usize {
        // TODO: we may not need to allocate memory if the location is overlapped.
        location.need_write_len
    }

    async fn write(&self, chunk_pos: usize, data: &[u8]) -> Result<usize> {
        let mut slice = self.find_writable_slice(chunk_pos).await;
        let write_len = slice
            .write(chunk_pos - slice.chunk_start_offset, data)
            .await?;
        self.start_background_commit_task();
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
    async fn find_writable_slice(&self, chunk_offset: usize) -> Arc<SliceWriter> {
        // let mut guard = self.slices.lock().await;
        // for (i, slice) in guard.iter().rev().enumerate() {
        //     if !slice.frozen.load(Ordering::SeqCst) {
        //         if chunk_offset >= slice.offset + slice.flushed_len
        //             && chunk_offset <= slice.offset + slice.length
        //         {
        //             // we can write to this slice.
        //             return slice.clone();
        //         }
        //     }
        //     if i >= 3 {
        //         tokio::spawn(slice.flush_and_release_buffer());
        //     }
        // }
        // let sw = Arc::new(SliceWriter::new(
        //     self.chunk_size,
        //     chunk_offset,
        //     self.chunk_engine.writer(0),
        // ));
        // guard.push(sw.clone());
        // sw
        todo!()
    }

    // Start a background task if there is no background task running,
    // and there is some slice need to be flushed.
    //
    // Once flush all slices, we should exit this task.
    fn start_background_commit_task(&self) {
        if self.background_task_running.load(Ordering::SeqCst) {
            return;
        }
        tokio::spawn(async move { loop {} });
        // TODO
    }
}
/// A SliceWriter is build by a Writing operation,
/// and it can be modified by other writing operation.
#[derive(Debug)]
struct SliceWriter {
    mu: RwLock<()>,

    // if this id equals to 0, means this slice hasn't been flushed yet.
    slice_id: AtomicUsize,

    // the chunk size
    chunk_size: usize,

    // The chunk offset.
    chunk_start_offset: usize,

    // The slice it self's offset.
    offset: usize,

    // current length of the slice, which should be smaller
    // than CHUNK_SIZE.
    length: usize,

    // the flushed data of the slice in the chunk.
    flushed_len: usize,

    // This slice is flushing right now.
    frozen: AtomicBool,

    // the underlying write buffer.
    write_buffer: WSlice,

    last_modified: Option<Instant>,
}

impl SliceWriter {
    fn new(chunk_size: usize, offset: usize, w_slice: WSlice) -> SliceWriter {
        SliceWriter {
            mu: Default::default(),
            slice_id: AtomicUsize::new(0),
            chunk_size,
            chunk_start_offset: offset,
            offset: 0,
            length: 0,
            flushed_len: 0,
            frozen: Default::default(),
            write_buffer: w_slice,
            last_modified: None,
        }
    }

    async fn write(self: &Arc<Self>, slice_offset: usize, data: &[u8]) -> Result<usize> {
        // let guard = self.mu.write().await;
        // let write_len = self
        //     .write_buffer
        //     .write_at(slice_offset, data)
        //     .expect("write data failed");
        // self.length = max(self.length, slice_offset + write_len);
        // self.last_modified.replace(Instant::now());
        // if self.length == self.chunk_size {
        //     drop(guard);
        //     tokio::spawn(self.flush_and_release_buffer());
        // } else if self.length > self.write_buffer.load_block_size() {
        // }
        //
        // Ok(write_len)
        todo!()
    }

    async fn flush_and_release_buffer(&self) {}
}

#[cfg(test)]
mod tests {
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use tracing_subscriber::{layer::SubscriberExt, Registry};

    use super::*;
    use crate::meta::MetaConfig;

    fn install_log() {
        let stdout_log = tracing_subscriber::fmt::layer().pretty();
        let subscriber = Registry::default().with(stdout_log);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set global subscriber");
    }

    #[test]
    fn memory_pool_basic() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let mut reservation = MemoryConsumer::new("test").register(&memory_pool);
        println!("{}", reservation.size());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn find_chunk_to_write() {
        install_log();
        let chunk_engine = Arc::new(ChunkEngine::default());
        let memory_pool = Arc::new(GreedyMemoryPool::new(3048));
        let chunk_size = 1024;
        let data_manager = Arc::new(DataManager::new(
            memory_pool,
            CancellationToken::new(),
            chunk_size,
            Arc::new(MetaConfig::default().open().unwrap()),
            chunk_engine.clone(),
        ));

        struct TestCase {
            offset: usize,
            data: Vec<u8>,
            want: Vec<ChunkWriteCtx>,
        }

        impl TestCase {
            async fn run(&self, data_manager: Arc<DataManager>) {
                let write_op = data_manager.new_write_op(self.offset, &self.data);
                let locations = write_op.get_locations().collect::<Vec<_>>();
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
                want: vec![ChunkWriteCtx {
                    chunk_idx: 1,
                    chunk_offset: 0,
                    need_write_len: 3,
                    buf_start_at: 0,
                }],
            },
            TestCase {
                offset: chunk_size - 3,
                data: vec![1, 2, 3],
                want: vec![ChunkWriteCtx {
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
                    ChunkWriteCtx {
                        chunk_idx: 0,
                        chunk_offset: chunk_size - 3,
                        need_write_len: 3,
                        buf_start_at: 0,
                    },
                    ChunkWriteCtx {
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
                want: vec![ChunkWriteCtx {
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
                    ChunkWriteCtx {
                        chunk_idx: 0,
                        chunk_offset: 0,
                        need_write_len: chunk_size,
                        buf_start_at: 0,
                    },
                    ChunkWriteCtx {
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
