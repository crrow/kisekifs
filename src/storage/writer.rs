use crate::chunk::chunk_pos;
use crate::meta;
use crate::meta::engine::MetaEngine;
use crate::meta::types::Ino;
use crate::storage::{cal_chunk_idx, cal_chunk_pos};
use dashmap::{DashMap, DashSet};
use datafusion_common::arrow::array::Array;
use datafusion_common::DataFusionError;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use rand::{Rng, SeedableRng};
use rangemap::{RangeMap, RangeSet};
use snafu::{ResultExt, Snafu};
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("unknown error happened: {msg}",))]
    Unknown { msg: String },
    #[snafu(display("memory usage too high: {}", source))]
    MemoryUsageTooHigh { source: DataFusionError },
    #[snafu(display("wait too long: {}", msg))]
    WaitTooLong { msg: String },
    #[snafu(display("canceled: {msg}"))]
    Canceled { msg: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteOp {
    // The number of bytes written by this operation.
    offset: usize,
    // The data written by this operation.
    data: *const u8,
    // The expected length of the write operation.
    expect_write_len: usize,
    // The chunk manager.
    chunk_manager: Weak<FileManager>,
    meta_engine: Arc<MetaEngine>,
}

impl WriteOp {
    fn new(
        chunk_manager: Weak<FileManager>, // TODO: use weak instead.
        offset: usize,
        data: &[u8],
        meta_engine: Arc<MetaEngine>,
    ) -> Self {
        assert!(data.len() > 0);
        WriteOp {
            offset,
            data: data.as_ptr(),
            expect_write_len: data.len(),
            chunk_manager,
            meta_engine,
        }
    }

    // Run the write operation.
    pub async fn run(&self) -> Result<usize> {
        let file_manager = self.chunk_manager.upgrade().ok_or_else(|| Error::Unknown {
            msg: "chunk manager is dropped".to_string(),
        })?;

        // 1. find the chunk
        let handles = file_manager
            .find_chunk_to_write(self.offset, self.expect_write_len)
            .into_iter()
            .map(|location| {
                let file_manager = file_manager.clone();
                let token = file_manager.cancel_token.clone();
                let mem_pool = file_manager.memory_pool.clone();
                let meta_engine = self.meta_engine.clone();
                let data = &unsafe { std::slice::from_raw_parts(self.data, self.expect_write_len) }
                    [location.buf_start_at..location.buf_start_at + location.need_write_len];
                let handle = tokio::spawn(async move {
                    let mut c = file_manager
                        .chunks
                        .entry(location.chunk_idx)
                        .or_insert_with(|| {
                            Chunk::new(location.chunk_idx, mem_pool, token, meta_engine)
                        });
                    let chunk = c.value_mut();

                    // wait until we can write.
                    if !chunk.wait_until_can_write(location.clone()).await {
                        return Err(Error::Canceled {
                            msg: format!("wait too long for writing chunk {}", chunk.chunk_idx),
                        });
                    }

                    // mark here comes a writing
                    chunk.inc_waiting_count();
                    // wait if someone is flushing.
                    if !chunk.wait_flush().await {
                        return Err(Error::Canceled {
                            msg: format!("wait too long for flushing chunk {}", chunk.chunk_idx),
                        });
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
                        return Err(Error::Unknown {
                            msg: format!("internal write failed: {}", e),
                        })
                    }
                },
                Err(e) => {
                    return Err(Error::Unknown {
                        msg: format!("async error: {}", e),
                    });
                }
            }
        }

        Ok(write_len)
    }

    fn get_locations(&self) -> impl Iterator<Item = ChunkWriteCtx> {
        let file_manager = self
            .chunk_manager
            .upgrade()
            .ok_or_else(|| Error::Unknown {
                msg: "chunk manager is dropped".to_string(),
            })
            .unwrap();

        // 1. find the chunk
        file_manager
            .find_chunk_to_write(self.offset, self.expect_write_len)
            .into_iter()
    }
}

/// The manager of all files.
struct DataManager {
    // config
    chunk_size: usize,

    write_buffer: Arc<dyn MemoryPool>,
    cancel_token: CancellationToken,
    files: DashMap<Ino, Arc<FileManager>>,
}

impl DataManager {
    fn new(
        memory_pool: Arc<dyn MemoryPool>,
        cancel_token: CancellationToken,
        chunk_size: usize,
    ) -> DataManager {
        DataManager {
            write_buffer: memory_pool,
            cancel_token,
            files: Default::default(),
            chunk_size,
        }
    }

    fn new_file_manager(&self, ino: Ino) -> Weak<FileManager> {
        let fm = Arc::new(FileManager::new(
            self.chunk_size,
            self.cancel_token.clone(),
            self.write_buffer.clone(),
        ));
        self.files.insert(ino, fm.clone());
        Arc::downgrade(&fm)
    }
}

/// A file is composed of multiple chunks.
#[derive(Debug)]
struct FileManager {
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
}

impl FileManager {
    fn new(
        chunk_size: usize,
        token: CancellationToken,
        memory_pool: Arc<dyn MemoryPool>,
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
                //     "chunk-size: {}, chunk: {} chunk_pos: {}, left: {}, buf start at: {}, max can write: {}",
                //     self.chunk_size, idx, chunk_pos, left, buf_start_at, max_can_write,
                // );

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
    // the chunk_idx of this chunk.
    chunk_idx: usize,
    // current length of the chunk, which should be smaller
    // than CHUNK_SIZE.
    length: usize,
    // one chunk can have multiple slices.
    slice_manager: Arc<SliceManager>, // Make user can clone it.
    // how many write operations are flushing this chunk right now.
    flush_count: Arc<AtomicUsize>,
    // this chunk is flushed. notify the waiting write operation to
    // continue.
    flush_finished: Arc<Notify>,
    // how many write operations are waiting for flushing end.
    write_waiting_count: Arc<AtomicUsize>,

    // the total write buffer.
    total_buffer: Arc<dyn MemoryPool>,
    // track the buffer usage of this chunk.
    buffer_usage: Arc<Mutex<MemoryReservation>>,
    cancel_token: CancellationToken,
    // the meta engine.
    meta_engine: Arc<MetaEngine>,
}

impl Chunk {
    fn new(
        idx: usize,
        memory_pool: Arc<dyn MemoryPool>,
        cancellation_token: CancellationToken,
        meta_engine: Arc<MetaEngine>,
    ) -> Chunk {
        Chunk {
            chunk_idx: idx,
            length: 0,
            slice_manager: Arc::new(SliceManager::new(meta_engine.clone())),
            flush_count: Arc::new(Default::default()),
            flush_finished: Arc::new(Default::default()),
            write_waiting_count: Arc::new(Default::default()),
            buffer_usage: Arc::new(Mutex::new(
                MemoryConsumer::new(format!("Chunk: {}", idx)).register(&memory_pool),
            )),
            total_buffer: memory_pool,
            cancel_token: cancellation_token,
            meta_engine,
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

    async fn wait_until_can_write(&self, location: ChunkWriteCtx) -> bool {
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
        let mut slice = self.slice_manager.find_writable_slice(chunk_pos).await;
        // we should put this slice back after we finish writing.

        slice.write(chunk_pos - slice.chunk_start_offset, data);

        self.slice_manager.append_slice(slice).await;
        self.slice_manager.start_background_commit_task();
        Ok(data.len())
    }
}

/// SliceManager is used for managing slices under a chunk.
#[derive(Debug)]
struct SliceManager {
    slices: Arc<Mutex<Vec<Slice>>>,
    background_task_running: Arc<AtomicBool>,
    meta_engine: Arc<meta::engine::MetaEngine>,
}

impl SliceManager {
    fn new(meta_engine: Arc<meta::engine::MetaEngine>) -> SliceManager {
        SliceManager {
            slices: Default::default(),
            background_task_running: Default::default(),
            meta_engine,
        }
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
    async fn find_writable_slice(&self, chunk_offset: usize) -> Slice {
        let mut guard = self.slices.lock().await;
        let mut idx = None;
        for (i, slice) in guard.iter().rev().enumerate() {
            if !slice.frozen.load(Ordering::SeqCst) {
                if chunk_offset >= slice.offset + slice.flushed_len
                    && chunk_offset <= slice.offset + slice.length
                {
                    // we can write to this slice.
                    idx = Some(1);
                    break;
                }
            }
            if i >= 3 {
                slice.flush();
            }
        }
        if let Some(idx) = idx {
            guard.remove(idx)
        } else {
            Slice::new(chunk_offset)
        }
    }

    async fn append_slice(&self, slice: Slice) {
        let mut guard = self.slices.lock().await;
        guard.push(slice);
    }

    // Start a background task if there is no background task running,
    // and there is some slice need to be flushed.
    fn start_background_commit_task(&self) {
        if self.background_task_running.load(Ordering::SeqCst) {
            return;
        }
        tokio::spawn(async move { loop {} });
        // TODO
    }

    // Before we flush data to the backend storage,
    // we should generate a slice id.
    async fn acquire_slice_id(&self) -> Result<usize> {
        todo!()
    }
}
/// A slice is build by a Writing operation,
/// and it can be modified by other writing operation.
#[derive(Debug)]
struct Slice {
    // if this id equals to 0, means this slice hasn't been flushed yet.
    slice_id: AtomicUsize,

    // The chunk offset.
    chunk_start_offset: usize,

    // The slice it self's offset.
    offset: usize,

    // The blocks of the slice.
    blocks: Vec<Block>,

    // current length of the slice, which should be smaller
    // than CHUNK_SIZE.
    length: usize,

    // the flushed data of the slice in the chunk.
    flushed_len: usize,

    // This slice is flushing right now.
    frozen: AtomicBool,
}

impl Slice {
    fn new(offset: usize) -> Slice {
        Slice {
            slice_id: AtomicUsize::new(0),
            chunk_start_offset: offset,
            offset: 0,
            blocks: vec![],
            length: 0,
            flushed_len: 0,
            frozen: Default::default(),
        }
    }

    fn write(&mut self, slice_offset: usize, data: &[u8]) {}

    fn flush(&self) {}
}

#[derive(Debug)]
enum Block {
    Full(Vec<u8>),         // a full block.
    Partial(PartialBlock), // only a part of the block has been written.
}

#[derive(Debug)]
struct PartialBlock {
    // the written start offset inside the block.
    start: usize,
    // the written end offset inside the block.
    end: usize,
    // The data of the block.
    data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::meta::MetaConfig;
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    #[test]
    fn memory_pool_basic() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let mut reservation = MemoryConsumer::new("test").register(&memory_pool);
        println!("{}", reservation.size());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        let stdout_log = tracing_subscriber::fmt::layer().pretty();
        let subscriber = Registry::default().with(stdout_log);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set global subscriber");

        let memory_pool = Arc::new(GreedyMemoryPool::new(3048));
        let chunk_size = 1024;
        let data_manager = DataManager::new(memory_pool, CancellationToken::new(), chunk_size);
        let file_manager = data_manager.new_file_manager(Ino(1));
        let meta_engine = Arc::new(MetaConfig::default().open().unwrap());

        struct TestCase {
            offset: usize,
            data: Vec<u8>,
            want: Vec<ChunkWriteCtx>,
        }

        impl TestCase {
            async fn run(&self, file_manager: Weak<FileManager>, meta_engine: Arc<MetaEngine>) {
                let write_op =
                    WriteOp::new(file_manager.clone(), self.offset, &self.data, meta_engine);
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
            i.run(file_manager.clone(), meta_engine.clone()).await;
        }
    }
}
