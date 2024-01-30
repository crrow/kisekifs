use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use crate::common;
use dashmap::DashMap;
use lazy_static::lazy::Lazy;
use libc::EIO;
use scopeguard::defer;
use snafu::OptionExt;
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::Instant,
};
use tracing::debug;
use unique_id::sequence::SequenceGenerator;
use unique_id::Generator;

use crate::vfs::storage::worker::FlushAndReleaseSliceReason;
use crate::vfs::{
    err::Result,
    storage::{
        cal_chunk_idx, cal_chunk_offset, err::InvalidFHSnafu, worker::WorkerRequest, Engine,
        EngineConfig, WriteBuffer,
    },
    VFSError, FH,
};

impl Engine {
    /// Creates a new [FileWriter] for the file handle.
    pub(crate) fn new_file_writer(self: &Arc<Self>, fh: FH) {
        let fw = FileWriter {
            fh,
            engine: Arc::downgrade(self),
            config: self.config.clone(),
            chunk_writers: Arc::new(Default::default()),
            total_slice_cnt: Arc::new(Default::default()),
            write_cnt: Arc::new(Default::default()),
            flushing_cnt: Arc::new(Default::default()),
            notify_flush: Arc::new(Default::default()),
            notify_write: Arc::new(Default::default()),
            id_generator: self.id_generator.clone(),
        };

        self.file_writers.insert(fh, Arc::new(fw));
    }

    /// Checks whether the file handle is writable.
    pub(crate) fn can_write(&self, fh: FH) -> bool {
        self.file_writers.contains_key(&fh)
    }

    /// Use the [FileWriter] to write data to the file.
    pub(crate) async fn write(&self, fh: FH, offset: usize, data: &[u8]) -> Result<usize> {
        let fw = self.file_writers.get(&fh).context(InvalidFHSnafu { fh })?;
        let write_len = fw.write(offset, data).await?;
        Ok(write_len)
    }

    /// Use the [FileWriter] to flush data to the backend object storage.
    pub(crate) async fn flush(&self, fh: FH) -> crate::vfs::storage::err::Result<usize> {
        todo!()
    }
}

/// [FileWriter] is responsible for writing data to the file,
/// and flushing the data to the backend object storage.
///
/// We can call [FileWriter::do_flush] to flush the data to the backend object
/// storage manually. When we call it, we should make all writer threads
/// blocking writing data to the file.
///
/// But the [FileWriter] will also flush data to the backend object storage
/// under the hood automatically, and without blocking the writer threads.
///
/// For [FileWriter::write], we can have multiple write on it at the same time,
/// as long as they write to different slices.
pub(crate) struct FileWriter {
    fh: FH,
    engine: Weak<Engine>,
    config: Arc<EngineConfig>,
    // chunks that are being written.
    chunk_writers: Arc<DashMap<usize, Arc<ChunkWriter>>>,
    // total slice count is used to count the number of slices.
    total_slice_cnt: Arc<AtomicUsize>,
    // the writing count is used to count the number of write wait threads.
    write_cnt: Arc<AtomicUsize>,
    // the flushing count is used to count the number of flush wait threads.
    flushing_cnt: Arc<AtomicUsize>,
    // notify the flush thread to flush the data to the backend object storage.
    notify_flush: Arc<Notify>,
    // notify the write thread to write the data to the file.
    notify_write: Arc<Notify>,
    id_generator: sonyflake::Sonyflake,
}

impl FileWriter {
    pub(crate) async fn write(&self, offset: usize, data: &[u8]) -> Result<usize> {
        let write_location = find_write_location(self.config.chunk_size, offset, data.len());

        self.write_cnt.fetch_add(1, Ordering::AcqRel);
        defer!(self.notify_write.notify_one(););
        defer!(self.write_cnt.fetch_sub(1, Ordering::AcqRel););
        defer!(self.notify_flush.notify_one(););

        while self.flushing_cnt.load(Ordering::Acquire) > 0 {
            // wait for the flushing threads to finish and notify the write thread.
            self.notify_write.notified().await;
        }

        let data_len = data.len();
        let data_ptr = data.as_ptr();
        let handlers = write_location
            .into_iter()
            .map(|location| {
                let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
                let data =
                    &data[location.buf_start_at..location.buf_start_at + location.need_write_len];
                let chunk_writer = self.get_chunk_writer(location.chunk_idx);
                common::runtime::spawn(async move {
                    let write_len = chunk_writer.write(location.chunk_offset, data).await;
                    write_len
                })
            })
            .collect::<Vec<_>>();

        let mut write_len = 0;
        for r in futures::future::join_all(handlers).await {
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

    pub(crate) async fn do_flush(&self) -> Result<()> {
        defer!(self.notify_write.notify_one();); // then we notify the write thread.
        defer!(self.notify_flush.notify_one();); // then we notify the flush thread.
        defer!(self.flushing_cnt.fetch_sub(1, Ordering::AcqRel);); // we first sub,

        // wait until the last manually flush finished.
        while self.flushing_cnt.fetch_add(1, Ordering::AcqRel) != 0 {
            // wait for the last flush job to notify me.
            self.notify_flush.notified().await;
        }

        // means some write has acquire the lock before this flush.
        // wait for them to finish.
        while self.write_cnt.load(Ordering::Acquire) != 0 {
            self.notify_flush.notified().await;
        }

        let chunk_writers_ref = self.chunk_writers.clone();
        while !self.chunk_writers.is_empty() {
            let handles = self
                .chunk_writers
                .iter()
                .map(|r| {
                    let idx = r.chunk_idx;
                    let cw = r.value().clone();
                    let chunk_writers_ref = self.chunk_writers.clone();
                    common::runtime::spawn(async move {
                        let cw1 = cw.clone();
                        if let Err(e) = cw1.do_flush().await {
                            debug!("flush chunk {} failed: {}", idx, e);
                            return;
                        }
                        let guard = cw.slices.lock().await;
                        if guard.is_empty() {
                            // all slices have been flushed.
                            // we can release this chunk writer.
                            chunk_writers_ref.remove(&idx);
                        };
                    })
                })
                .collect::<Vec<_>>();

            futures::future::join_all(handles).await;
        }

        Ok(())
    }

    fn get_chunk_writer(&self, idx: usize) -> Arc<ChunkWriter> {
        let c = self.chunk_writers.entry(idx).or_insert_with(|| {
            Arc::new(ChunkWriter::new(
                self.fh,
                self.engine.clone(),
                self.config.clone(),
                idx,
                self.total_slice_cnt.clone(),
                self.id_generator.clone(),
            ))
        });
        c.value().clone()
    }

    pub(crate) fn find_chunk_writer(&self, idx: usize) -> Option<Arc<ChunkWriter>> {
        self.chunk_writers.get(&idx).map(|r| r.value().clone())
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

fn find_write_location(
    chunk_size: usize,
    offset: usize,
    expect_write_len: usize,
) -> Vec<ChunkWriteCtx> {
    let offset = offset;
    let expected_write_len = expect_write_len;

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

            let ctx = ChunkWriteCtx {
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

pub(crate) struct ChunkWriter {
    engine: Weak<Engine>,
    engine_config: Arc<EngineConfig>,
    // which fh ?
    fh: FH,
    // the chunk_idx of this chunk.
    chunk_idx: usize,
    // current length of the chunk, which should be smaller
    // than CHUNK_SIZE.
    length: usize, // TODO
    slices: Mutex<BTreeMap<u64, Arc<SliceWriter>>>,
    total_slice_counter: Arc<AtomicUsize>,
    // have we start the background commit thread ?
    background_commit_started: Arc<AtomicBool>,
    // how many writes on this chunk.
    // once we found all slices have been flushed and no one is writing,
    // we can free this ChunkWriter.
    write_cnt: Arc<AtomicUsize>,
    seq_generator: sonyflake::Sonyflake,
}

impl ChunkWriter {
    fn new(
        fh: FH,
        engine: Weak<Engine>,
        engine_config: Arc<EngineConfig>,
        chunk_idx: usize,
        slice_counter: Arc<AtomicUsize>,
        seq_generator: sonyflake::Sonyflake,
    ) -> ChunkWriter {
        ChunkWriter {
            engine,
            engine_config,
            fh,
            chunk_idx,
            length: 0,
            slices: Mutex::new(BTreeMap::new()),
            total_slice_counter: slice_counter,
            background_commit_started: Arc::new(AtomicBool::new(false)),
            write_cnt: Arc::new(AtomicUsize::new(0)),
            seq_generator,
        }
    }

    async fn write(self: &Arc<Self>, chunk_pos: usize, data: &[u8]) -> Result<usize> {
        self.write_cnt.fetch_add(1, Ordering::AcqRel);
        defer!(self.write_cnt.fetch_sub(1, Ordering::AcqRel););

        let slice = self.find_writable_slice(chunk_pos).await;
        let (write_len, total_write_len, flushed_len) = slice
            .write(chunk_pos - slice.chunk_start_offset, data)
            .await?;

        if total_write_len == self.engine_config.chunk_size {
            self.submit_flush_and_release_slice_req(&slice, FlushAndReleaseSliceReason::Full)
                .await;
        } else {
            self.submit_flush_block_req(&slice, total_write_len).await;
        }

        if !self.background_commit_started.load(Ordering::Acquire) {
            let engine = self.engine.upgrade().expect("engine should not be dropped");
            engine
                .submit_request(WorkerRequest::new_commit_chunk_request(
                    self.fh,
                    self.chunk_idx,
                ))
                .await;
            self.background_commit_started
                .store(true, Ordering::Release);
        }

        Ok(write_len)
    }

    // We try to find slice from the back to the front,
    //
    // If we can't find a slice to write for multiple times,
    // we should try to flush the slice to the background, and
    // release the memory.
    async fn find_writable_slice(self: &Arc<Self>, chunk_offset: usize) -> Arc<SliceWriter> {
        let mut guard = self.slices.lock().await;
        let mut iter_cnt = 0;
        for (seq, sw) in guard.iter().rev() {
            if !sw.frozen() {
                let (flushed, length) = sw.get_flushed_length_and_write_length().await;
                if chunk_offset >= sw.chunk_start_offset + flushed
                    && chunk_offset <= sw.chunk_start_offset + length
                {
                    // we can write to this slice.
                    return sw.clone();
                }
                if iter_cnt > 3 {
                    self.submit_flush_and_release_slice_req(
                        sw,
                        FlushAndReleaseSliceReason::RandomWrite,
                    )
                    .await;
                }
            }
            iter_cnt += 1;
        }
        let engine = self.engine.upgrade().expect("engine should not be dropped");
        self.total_slice_counter.fetch_add(1, Ordering::AcqRel);
        let seq = self.seq_generator.next_id().expect("generate seq failed");
        let sw = Arc::new(SliceWriter {
            internal_seq: seq,
            slice_id: RwLock::new(None),
            chunk_start_offset: chunk_offset,
            write_buffer: RwLock::new(engine.new_write_buffer()),
            frozen: Arc::new(AtomicBool::new(false)),
            done: Arc::new(AtomicBool::new(false)),
            last_modified: RwLock::new(None),
            total_slice_counter: self.total_slice_counter.clone(),
        });
        guard.insert(seq, sw.clone());
        sw
    }

    async fn submit_flush_block_req(self: &Arc<Self>, sw: &Arc<SliceWriter>, flush_to: usize) {
        let e = self.engine.upgrade().expect("engine should not be dropped");
        e.submit_request(WorkerRequest::new_flush_block_request(
            self.fh,
            self.chunk_idx,
            sw.internal_seq,
            flush_to,
        ))
        .await;
    }

    async fn submit_flush_and_release_slice_req(
        self: &Arc<Self>,
        sw: &Arc<SliceWriter>,
        reason: FlushAndReleaseSliceReason,
    ) {
        let e = self.engine.upgrade().expect("engine should not be dropped");
        sw.freeze(); // freeze this slice, make others cannot write to it.
        e.submit_request(WorkerRequest::new_flush_and_release_slice_request(
            self.fh,
            self.chunk_idx,
            sw.internal_seq,
            reason,
        ))
        .await;
    }

    async fn do_flush(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.slices.lock().await;
        let mut need_remove = vec![];
        for (_, sw) in guard.iter() {
            if !sw.frozen() {
                sw.freeze();
                sw.do_flush_and_release().await?;
            }
            if sw.done.load(Ordering::Acquire) {
                // we can release this slice.
                need_remove.push(sw.internal_seq);
            }
        }

        need_remove.iter().for_each(|seq| {
            guard.remove(seq);
        });

        Ok(())
    }

    pub(crate) async fn find_slice_writer(
        self: &Arc<Self>,
        slice_seq: u64,
    ) -> Option<Arc<SliceWriter>> {
        let guard = self.slices.lock().await;
        guard.get(&slice_seq).map(|r| r.clone())
    }
}

/// At any time, one slice can only be written by one person.
pub(crate) struct SliceWriter {
    // the internal seq id.
    internal_seq: u64,
    // the slice id.
    slice_id: RwLock<Option<usize>>,
    // The chunk offset.
    chunk_start_offset: usize,
    // the underlying write buffer.
    write_buffer: RwLock<WriteBuffer>,
    // before we flush the slice, we need to freeze it.
    frozen: Arc<AtomicBool>,
    // as long as we aren't done, we should try to flush this
    // slice.
    // since we may fail at the flush process.
    done: Arc<AtomicBool>,
    last_modified: RwLock<Option<Instant>>,
    total_slice_counter: Arc<AtomicUsize>,
}

impl SliceWriter {
    // freeze this slice writer, make it cannot be written for the flushing.
    fn freeze(self: &Arc<Self>) {
        self.frozen.store(true, Ordering::Release)
    }
    // check if this slice writer can be written.
    fn frozen(self: &Arc<Self>) -> bool {
        self.frozen.load(Ordering::Acquire)
    }
    // get the underlying write buffer's released length and total write length.
    async fn get_flushed_length_and_write_length(self: &Arc<Self>) -> (usize, usize) {
        // TODO: use std mutex
        let guard = self.write_buffer.read().await;
        let flushed_len = guard.flushed_length();
        let write_len = guard.length();
        (flushed_len, write_len)
    }
    // return the current write len to this buffer,
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

    async fn do_flush_and_release(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.write_buffer.write().await;
        guard.finish()?;
        self.done.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) async fn do_flush_to(self: &Arc<Self>, offset: usize) -> Result<()> {
        let mut guard = self.write_buffer.write().await;
        guard.flush_to(offset)?;
        Ok(())
    }
}

impl Drop for SliceWriter {
    fn drop(&mut self) {
        self.total_slice_counter.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::install_fmt_log, meta::MetaConfig, vfs::storage::new_debug_sto};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn find_chunk_to_write() {
        install_fmt_log();

        let meta_engine = MetaConfig::default().open().unwrap();
        let sto_engine = new_debug_sto();
        let engine = Arc::new(Engine::new(
            Arc::new(EngineConfig::default()),
            sto_engine,
            Arc::new(meta_engine),
        ));
        let chunk_size = engine.config.chunk_size;

        struct TestCase {
            offset: usize,
            data: Vec<u8>,
            want: Vec<ChunkWriteCtx>,
        }

        impl TestCase {
            async fn run(&self, fh: FH, data_engine: Arc<Engine>) {
                data_engine.new_file_writer(fh);
                let locations = find_write_location(
                    data_engine.config.chunk_size,
                    self.offset,
                    self.data.len(),
                );
                assert_eq!(locations, self.want);
                let write_len = data_engine.write(fh, self.offset, &self.data).await;
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
            i.run(1, engine.clone()).await;
        }
    }
}
