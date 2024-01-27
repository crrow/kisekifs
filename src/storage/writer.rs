use crate::meta::types::Ino;
use crate::storage::{cal_chunk_idx, cal_chunk_pos};
use dashmap::{DashMap, DashSet};
use datafusion_common::DataFusionError;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use snafu::{ResultExt, Snafu};
use std::cmp::min;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use tokio::sync::{Mutex, Notify};
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
}

impl WriteOp {
    fn new(
        chunk_manager: Weak<FileManager>, // TODO: use weak instead.
        offset: usize,
        data: &[u8],
    ) -> Self {
        assert!(data.len() > 0);
        Self {
            offset,
            data: data.as_ptr(),
            expect_write_len: data.len(),
            chunk_manager,
        }
    }

    // Run the write operation.1
    pub async fn run(&self) -> Result<usize> {
        let file_manager = self.chunk_manager.upgrade().ok_or_else(|| Error::Unknown {
            msg: "chunk manager is dropped".to_string(),
        })?;

        // FIXME
        // if file_manager
        //     .wait_until_can_write(self.expect_write_len)
        //     .await
        // {
        //     return Err(Error::WaitTooLong {
        //         msg: format!("wait too long for writing {} bytes", self.expect_write_len),
        //     });
        // }

        // 1. find the chunk
        let handles = file_manager
            .find_chunk_to_write(self.offset, self.expect_write_len)
            .into_iter()
            .map(|location| {
                let file_manager = file_manager.clone();
                let token = file_manager.cancel_token.clone();
                let mem_pool = file_manager.memory_pool.clone();
                let data = &unsafe { std::slice::from_raw_parts(self.data, self.expect_write_len) }
                    [location.buf_start_at..location.buf_start_at + location.need_write_len];
                let handle = tokio::spawn(async move {
                    let mut c = file_manager
                        .chunks
                        .entry(location.chunk_idx)
                        .or_insert_with(|| Chunk::new(location.chunk_idx, mem_pool, token));
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

    memory_pool: Arc<dyn MemoryPool>,
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
            memory_pool,
            cancel_token,
            files: Default::default(),
            chunk_size,
        }
    }

    fn new_file_manager(&self, ino: Ino) -> Weak<FileManager> {
        let fm = Arc::new(FileManager::new(
            self.chunk_size,
            self.cancel_token.clone(),
            self.memory_pool.clone(),
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

    // The total memory usage of all files.
    memory_pool: Arc<dyn MemoryPool>,
    // Current file's memory usage.
    mem_reservation: Arc<Mutex<MemoryReservation>>,
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
            mem_reservation: Arc::new(Mutex::new(
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
    memory_pool: Arc<dyn MemoryPool>,
    memory_reservation: Arc<Mutex<MemoryReservation>>,
    cancel_token: CancellationToken,
}

impl Chunk {
    fn new(
        idx: usize,
        memory_pool: Arc<dyn MemoryPool>,
        cancellation_token: CancellationToken,
    ) -> Chunk {
        Chunk {
            chunk_idx: idx,
            length: 0,
            slice_manager: Arc::new(SliceManager::new()),
            flush_count: Arc::new(Default::default()),
            flush_finished: Arc::new(Default::default()),
            write_waiting_count: Arc::new(Default::default()),
            memory_reservation: Arc::new(Mutex::new(
                MemoryConsumer::new(format!("Chunk: {}", idx)).register(&memory_pool),
            )),
            memory_pool,
            cancel_token: cancellation_token,
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
                debug!("chunk {}: wait_flush", self.chunk_idx);
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
        let mr = self.memory_reservation.clone();
        let size = self.cal_alloc_bytes(&location);
        if size == 0 {
            return true;
        }
        let pool = self.memory_pool.clone();
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
        location.need_write_len
    }

    async fn write(&self, chunk_pos: usize, data: &[u8]) -> Result<usize> {
        Ok(data.len())
    }
}

/// SliceManager is used for managing slices under a chunk.
#[derive(Debug)]
struct SliceManager {
    mu: Mutex<()>,
    // K: (file_offset, slice_length).
    // We can lock the slice base on the range.
    ranges: Vec<(usize, usize)>,
    slices: HashMap<(usize, usize), RwLock<Slice>>,
}

impl SliceManager {
    fn new() -> SliceManager {
        SliceManager {
            mu: Default::default(),
            ranges: vec![],
            slices: HashMap::new(),
        }
    }
}

/// A slice is build by a Writing operation,
/// and it can be modified by other writing operation.
///
/// A slice can be in two states:
/// 1. Hole: the slice is not written yet.
/// 2. Normal: the slice has been written.
#[derive(Debug)]
enum Slice {
    Hole,
    Normal(NormalSlice),
}

/// A normal slice.
#[derive(Debug)]
struct NormalSlice {
    // The offset of the slice in the file.
    start_offset: usize,

    // The blocks of the slice.
    blocks: Vec<Block>,

    // current length of the slice, which should be smaller
    // than CHUNK_SIZE.
    length: usize,
}

impl Slice {
    fn write(&mut self) {}
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

        struct TestCase {
            offset: usize,
            data: Vec<u8>,
            want: Vec<ChunkWriteCtx>,
        }

        impl TestCase {
            async fn run(&self, file_manager: Weak<FileManager>) {
                let write_op = WriteOp::new(file_manager.clone(), self.offset, &self.data);
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
            i.run(file_manager.clone()).await;
        }
    }
}
