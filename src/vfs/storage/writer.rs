use std::{
    cmp::min,
    collections::BTreeMap,
    default::Default,
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use dashmap::DashMap;
use kiseki_storage::slice_buffer::SliceBufferWrapper;
use kiseki_types::{
    cal_chunk_idx, cal_chunk_offset,
    ino::Ino,
    slice::{Slice, EMPTY_SLICE_ID},
};
use kiseki_utils::readable_size::ReadableSize;
use libc::EIO;
use scopeguard::defer;
use snafu::{OptionExt, ResultExt};
use tokio::{
    select,
    sync::{Notify, RwLock},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, Instrument};

use crate::{
    common, meta,
    meta::engine::MetaEngine,
    vfs::{
        err::{ErrLIBCSnafu, InvalidInoSnafu, Result},
        storage::{Engine, EngineConfig, WriteBuffer},
    },
};

impl Engine {
    /// Creates a new [FileWriter] for the file handle.
    pub(crate) fn new_file_writer(self: &Arc<Self>, ino: Ino, len: u64) {
        let fw = FileWriter {
            ino,
            engine: Arc::downgrade(self),
            config: self.config.clone(),
            length: Arc::new(AtomicUsize::new(len as usize)),
            chunk_writers: Arc::new(Default::default()),
            chunk_writer_commit_tasks: Arc::new(Default::default()),
            total_slice_cnt: Arc::new(Default::default()),
            write_cnt: Arc::new(Default::default()),
            flushing_cnt: Arc::new(Default::default()),
            notify_flush: Arc::new(Default::default()),
            notify_write: Arc::new(Default::default()),
            id_generator: self.id_generator.clone(),
            // background_task_handle: (),
        };

        self.file_writers.insert(ino, Arc::new(fw));
    }

    /// Checks whether the file handle is writable.
    pub(crate) fn check_file_writer(&self, ino: Ino) -> bool {
        self.file_writers.contains_key(&ino)
    }

    pub(crate) fn find_file_writer(&self, ino: Ino) -> Option<Arc<FileWriter>> {
        self.file_writers.get(&ino).map(|r| r.value().clone())
    }

    /// Use the [FileWriter] to write data to the file.
    pub(crate) async fn write(
        self: &Arc<Self>,
        ino: Ino,
        offset: usize,
        data: &[u8],
    ) -> Result<usize> {
        debug!("write {} bytes to ino {}", data.len(), ino);
        let fw = self
            .file_writers
            .get(&ino)
            .context(InvalidInoSnafu { ino })?;
        debug!("get file write success");
        let write_len = fw.write(offset, data).await?;
        self.truncate_reader(ino, fw.get_length() as u64);

        Ok(write_len)
    }

    pub(crate) async fn flush_if_exists(&self, ino: Ino) -> Result<()> {
        if let Some(fw) = self.file_writers.get(&ino) {
            fw.do_flush().await?;
        }
        Ok(())
    }
}

pub(crate) type FileWritersRef = Arc<DashMap<Ino, Arc<FileWriter>>>;

/// [FileWriter] is responsible for writing data to the file,
/// and flushing the data to the backend object storage.
///
/// One [FileWriter] can be held by multiple handles, We should try to
/// free the [FileWriter] when all handles are dropped.
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
    ino: Ino,
    engine: Weak<Engine>,
    config: Arc<EngineConfig>,
    length: Arc<AtomicUsize>,
    // chunks that are being written.
    chunk_writers: Arc<DashMap<usize, Arc<ChunkWriter>>>,
    chunk_writer_commit_tasks: Arc<DashMap<usize, JoinHandle<()>>>,
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
    // background_task_handle: JoinHandle<()>,
}

impl FileWriter {
    pub(crate) async fn write(self: &Arc<Self>, offset: usize, data: &[u8]) -> Result<usize> {
        let write_location = find_write_location(self.config.chunk_size, offset, data.len());
        debug!("write location: {:?}", write_location);

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
                tokio::spawn(async move { chunk_writer.write(location.chunk_offset, data).await })
            })
            .collect::<Vec<_>>();

        let mut write_len = 0;
        for r in futures::future::join_all(handlers).await {
            match r {
                Ok(r) => match r {
                    Ok(wl) => {
                        debug!("write {}", ReadableSize(wl as u64));
                        write_len += wl
                    }
                    Err(_e) => {
                        // TODO: use better error
                        return ErrLIBCSnafu { kind: EIO }.fail()?;
                    }
                },
                Err(_e) => {
                    // TODO: use better error
                    return ErrLIBCSnafu { kind: EIO }.fail()?;
                }
            }
        }

        let len = self.length.load(Ordering::Acquire);
        if offset + write_len > len {
            self.length.store(offset + write_len, Ordering::Release);
        }
        Ok(write_len)
    }

    /// do_flush will try to flush all data that in buffer to the backend object
    /// storage.
    pub(crate) async fn do_flush(&self) -> Result<()> {
        debug!(
            "do flush on {}, length: {}",
            self.ino,
            self.length.load(Ordering::Acquire)
        );
        defer!(self.notify_write.notify_one();); // then we notify the write thread.
        defer!(self.notify_flush.notify_one();); // then we notify the flush thread.
        defer!(self.flushing_cnt.fetch_sub(1, Ordering::AcqRel);); // we first sub,

        // wait until the last manually flush finished.
        // TODO: we may should cancel the unfinished flush.
        while self.flushing_cnt.fetch_add(1, Ordering::AcqRel) != 0 {
            // wait for the last flush job to notify me.
            self.notify_flush.notified().await;
        }

        // means some write has acquire the lock before this flush.
        // wait for them to finish.
        while self.write_cnt.load(Ordering::Acquire) != 0 {
            self.notify_flush.notified().await;
        }

        while !self.chunk_writers.is_empty() {
            let handles = self
                .chunk_writers
                .iter()
                .map(|r| {
                    let idx = r.chunk_idx;
                    let cw = r.value().clone();
                    let _chunk_writers_ref = self.chunk_writers.clone();
                    tokio::spawn(async move {
                        if let Err(e) = cw.do_finish().await {
                            debug!("flush chunk {} failed: {}", idx, e);
                        }
                        // let guard = cw.slices.write().await;
                        // if guard.is_empty() {
                        //     // all slices have been flushed.
                        //     // we can release this chunk writer.
                        //     chunk_writers_ref.remove(&idx);
                        // };
                    })
                })
                .collect::<Vec<_>>();

            futures::future::join_all(handles).await;
        }

        Ok(())
    }

    fn get_chunk_writer(self: &Arc<Self>, idx: usize) -> Arc<ChunkWriter> {
        let fw = self.clone();
        let c = self.chunk_writers.entry(idx).or_insert_with(|| {
            let cw = ChunkWriter::new(
                fw,
                self.ino,
                self.engine.clone(),
                self.config.clone(),
                idx,
                self.total_slice_cnt.clone(),
                self.id_generator.clone(),
            );

            let engine = self.engine.upgrade().expect("engine should not be dropped");
            let meta_engine = engine.meta_engine.clone();

            let cw_task = ChunkWriterBackgroundTask {
                ino: self.ino,
                meta_engine,
                chunk_idx: idx,
                cw: cw.clone(),
                parent: self.clone(),
                flush_duration: Duration::from_secs(10),
                has_written: cw.has_written.clone(),
                has_written_notify: cw.has_written_notify.clone(),
            };

            self.chunk_writer_commit_tasks.insert(
                idx,
                tokio::spawn(async move {
                    cw_task.run().await;
                }),
            );

            cw
        });
        c.value().clone()
    }

    pub(crate) fn find_chunk_writer(&self, idx: usize) -> Option<Arc<ChunkWriter>> {
        self.chunk_writers.get(&idx).map(|r| r.value().clone())
    }

    pub(crate) fn get_length(&self) -> usize {
        self.length.load(Ordering::Acquire)
    }
}

// Each file writer will spawn a background task for
// doing some background work.
struct FileWriterTask {
    flush_block_in_slice_rx: tokio::sync::mpsc::Receiver<(Arc<SliceWriter>, usize)>,
    finish_slice_rx: tokio::sync::mpsc::Receiver<Arc<SliceWriter>>,
    cancel_token: CancellationToken,
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
    let expected_write_len = expect_write_len;

    let start_chunk_idx = cal_chunk_idx(offset, chunk_size);
    let end_chunk_idx = cal_chunk_idx(offset + expected_write_len - 1, chunk_size);

    let mut chunk_pos = cal_chunk_offset(offset, chunk_size);
    let mut buf_start_at = 0;
    let mut left = expected_write_len;

    (start_chunk_idx..=end_chunk_idx)
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
            buf_start_at += max_can_write;
            left -= max_can_write;
            ctx
        })
        .collect::<Vec<_>>()
}

/// Each ChunkWriter have a background task for cleaning up the memory,
/// and flushing some small slices to the background periodically.
struct ChunkWriterBackgroundTask {
    ino: Ino,
    meta_engine: Arc<MetaEngine>,
    chunk_idx: usize,
    cw: Arc<ChunkWriter>,
    parent: Arc<FileWriter>,
    flush_duration: Duration,
    // we only start the clean up loop if we have written some data.
    has_written: Arc<AtomicBool>,
    has_written_notify: Arc<Notify>,
}

impl ChunkWriterBackgroundTask {
    async fn run(self) {
        debug!(
            "start background task on ino {} chunk {}",
            self.ino, self.chunk_idx
        );
        while !self.has_written.load(Ordering::Acquire) {
            debug!(
                "wait for write on ino {} chunk {}",
                self.ino, self.chunk_idx
            );
            // wait the write operation.
            self.has_written_notify.notified().await;
            debug!("has written on ino {} chunk {}", self.ino, self.chunk_idx);
            // break the waiting loop, start the real loop.
            self.has_written.store(true, Ordering::Release);
        }
        debug!(
            "ChunkWriterBackgroundTask start on inode: {} chunk {}",
            self.ino, self.chunk_idx
        );

        loop {
            // use read lock to check if the slice is empty.
            let read_guard = self.cw.slices.read().await;
            if read_guard.is_empty() {
                drop(read_guard);

                // wait for all writers to finish.
                let write_guard = self.cw.slices.write().await;
                // double check
                if write_guard.is_empty() {
                    // free this chunk writer.
                    if self.cw.write_cnt.load(Ordering::Acquire) == 0 {
                        // no one is writing, we can free this chunk writer.
                        debug!(
                            "no SliceWriter alive and no writer, exit ChunkWriterBackgroundTask: ino: {} chunk: {}",
                            self.ino, self.chunk_idx,
                        );
                        return;
                    }
                }
            } else {
                // we try to flush slices in order.
                let slice = read_guard.first_key_value();
                let sw = match slice {
                    None => continue,            // no slice, we can continue.
                    Some((_, sw)) => sw.clone(), // we release the read guard here.
                };
                drop(read_guard);

                let mut wait_possible_writer = tokio::time::interval(Duration::from_millis(100));
                while !sw.done.load(Ordering::Acquire) {
                    select! {
                        _ = wait_possible_writer.tick() => {
                            // no one was writing
                            if !sw.frozen() && sw.start_at.elapsed() > self.flush_duration * 2 {
                                // we should flush this slice to the backend.
                                if let Err(e) = sw.finish().await {
                                    debug!("{} flush slice {} in background failed: {}",self.chunk_idx, sw.internal_seq, e);
                                }
                            }
                        }
                        _ = sw.done_notify.notified() => {
                            // this sw has done, we can go back check it again.
                        }
                    }
                }
                // then the sw is done.
                // write the meta info of this slice.
                let meta_slice = sw.to_meta_slice().await;
                let mtime = sw
                    .last_modified
                    .read()
                    .await
                    .expect("last modified should exist");
                if let Err(e) = self
                    .meta_engine
                    .write_slice(
                        self.parent.ino,
                        self.chunk_idx,
                        sw.chunk_start_offset,
                        meta_slice,
                        mtime,
                    )
                    .in_current_span()
                    .await
                {
                    debug!(
                        "{} write slice {} in background failed: {}",
                        self.chunk_idx, sw.internal_seq, e
                    );
                }
                // remove it sw from the map.
                let mut write_guard = self.cw.slices.write().await;
                write_guard.remove(&sw.internal_seq);
            }
        }
    }
}

impl Drop for ChunkWriterBackgroundTask {
    fn drop(&mut self) {
        debug!(
            "cleanup chunk writer on ino: {} for chunk: {}",
            self.ino, self.chunk_idx
        );
        self.parent.chunk_writers.remove(&self.chunk_idx);
        self.parent
            .chunk_writer_commit_tasks
            .remove(&self.chunk_idx);
    }
}

pub(crate) struct ChunkWriter {
    engine: Weak<Engine>,
    engine_config: Arc<EngineConfig>,
    // which inode?
    ino: Ino,
    // the chunk_idx of this chunk.
    chunk_idx: usize,
    // current length of the chunk, which should be smaller
    // than CHUNK_SIZE.
    length: usize, // TODO
    pub(crate) slices: RwLock<BTreeMap<u64, Arc<SliceWriter>>>,
    total_slice_counter: Arc<AtomicUsize>,
    // have we actually write data to this chunk ?
    has_written: Arc<AtomicBool>,
    has_written_notify: Arc<Notify>,
    // how many writes on this chunk.
    // once we found all slices have been flushed and no one is writing,
    // we can free this ChunkWriter.
    write_cnt: Arc<AtomicUsize>,
    seq_generator: sonyflake::Sonyflake,
}

impl ChunkWriter {
    fn new(
        _parent: Arc<FileWriter>,
        ino: Ino,
        engine: Weak<Engine>,
        engine_config: Arc<EngineConfig>,
        chunk_idx: usize,
        slice_counter: Arc<AtomicUsize>,
        seq_generator: sonyflake::Sonyflake,
    ) -> Arc<ChunkWriter> {
        let has_written = Arc::new(AtomicBool::new(false));
        let has_written_notify = Arc::new(Notify::new());

        Arc::new(ChunkWriter {
            ino,
            engine,
            engine_config,
            chunk_idx,
            length: 0,
            slices: RwLock::new(BTreeMap::new()),
            total_slice_counter: slice_counter,
            has_written: has_written.clone(),
            has_written_notify: has_written_notify.clone(),
            write_cnt: Arc::new(AtomicUsize::new(0)),
            seq_generator,
        })
    }

    async fn write(self: &Arc<Self>, chunk_pos: usize, data: &[u8]) -> Result<usize> {
        self.write_cnt.fetch_add(1, Ordering::AcqRel);
        defer!(self.write_cnt.fetch_sub(1, Ordering::AcqRel););

        debug!(
            "try to write {} to chunk {}",
            ReadableSize(data.len() as u64),
            self.chunk_idx
        );
        let slice = self.find_writable_slice(chunk_pos).await;
        debug!("write to slice {}", slice.internal_seq);
        let (write_len, total_write_len, _flushed_len) = slice
            .write(chunk_pos - slice.chunk_start_offset, data)
            .await?;

        if total_write_len == self.engine_config.chunk_size {
            let me = self.engine.upgrade().unwrap().meta_engine.clone();
            let slice = slice.clone();
            let handle = tokio::spawn(async move {
                slice.prepare_slice_id(me).await?;
                slice.finish().await
            })
            .await;
            if let Err(e) = handle {
                debug!("finish failed: {}", e);
            }
        } else {
            let me = self.engine.upgrade().unwrap().meta_engine.clone();
            if let Err(e) = tokio::spawn(async move {
                slice.prepare_slice_id(me).await?;
                slice.do_flush_to(total_write_len).await
            })
            .await
            {
                debug!("flush slice failed: {}", e);
            }
        }

        if !self.has_written.load(Ordering::Acquire) {
            self.has_written.store(true, Ordering::Release);
            self.has_written_notify.notify_one();
        }

        Ok(write_len)
    }

    // We try to find slice from the back to the front,
    //
    // If we can't find a slice to write for multiple times,
    // we should try to flush the slice to the background, and
    // release the memory.
    async fn find_writable_slice(self: &Arc<Self>, chunk_offset: usize) -> Arc<SliceWriter> {
        debug!("try to find writable slice for chunk {}", self.chunk_idx);
        let read_guard = self.slices.read().await;
        debug!("get slices lock succeed");
        let mut handles = vec![];
        for (iter_cnt, (_seq, sw)) in read_guard.iter().rev().enumerate() {
            let sw = sw.clone();
            if !sw.frozen() {
                let (flushed, length) = sw.get_flushed_length_and_total_write_length().await;
                if chunk_offset >= sw.chunk_start_offset + flushed
                    && chunk_offset <= sw.chunk_start_offset + length
                {
                    // we can write to this slice.
                    return sw.clone();
                }
                if iter_cnt > 3 {
                    handles.push(tokio::spawn(async move { sw.finish().await }))
                }
            }
        }
        futures::future::join_all(handles).await;
        drop(read_guard);

        let engine = self.engine.upgrade().expect("engine should not be dropped");
        self.total_slice_counter.fetch_add(1, Ordering::AcqRel);
        let seq = self.seq_generator.next_id().expect("generate seq failed");
        let sw = Arc::new(SliceWriter {
            chunk_idx: self.chunk_idx,
            internal_seq: seq,
            slice_id_prepared: AtomicBool::new(false),
            chunk_start_offset: chunk_offset,
            // write_buffer: RwLock::new(engine.new_write_buffer()),
            write_buffer: RwLock::new(engine.new_slice_buffer_wrapper()),
            frozen: Arc::new(AtomicBool::new(false)),
            done: Arc::new(AtomicBool::new(false)),
            done_notify: Arc::new(Default::default()),
            last_modified: RwLock::new(None),
            total_slice_counter: self.total_slice_counter.clone(),
            start_at: Instant::now(),
        });
        let mut write_guard = self.slices.write().await;
        write_guard.insert(seq, sw.clone());
        sw
    }

    /// Try to flush all active slices to the backend.
    async fn do_finish(self: &Arc<Self>) -> Result<()> {
        let read_guard = self.slices.read().await;
        let engine = self.engine.upgrade().expect("engine should not be dropped");
        let x = read_guard
            .iter()
            .map(|sw| {
                let sw = sw.1.clone();
                let me = engine.meta_engine.clone();
                tokio::spawn(async move {
                    if !sw.frozen() {
                        sw.freeze();
                        if sw.prepare_slice_id(me).await.is_err() {
                            return;
                        };
                        if sw.finish().await.is_err() {
                            return;
                        };
                    }
                })
            })
            .collect::<Vec<_>>();
        futures::future::join_all(x).await;
        Ok(())
    }

    pub(crate) async fn find_slice_writer(
        self: &Arc<Self>,
        slice_seq: u64,
    ) -> Option<Arc<SliceWriter>> {
        let guard = self.slices.read().await;
        guard.get(&slice_seq).cloned()
    }
}

/// At any time, one slice can only be written by one person.
pub(crate) struct SliceWriter {
    chunk_idx: usize,
    // the internal seq id.
    internal_seq: u64,

    // did we prepare the slice id?
    slice_id_prepared: AtomicBool,

    // The chunk offset.
    chunk_start_offset: usize,
    // the underlying write buffer.
    // write_buffer: RwLock<WriteBuffer>,
    write_buffer: RwLock<SliceBufferWrapper>,
    // before we flush the slice, we need to freeze it.
    frozen: Arc<AtomicBool>,
    // as long as we aren't done, we should try to flush this
    // slice.
    // since we may fail at the flush process.
    done: Arc<AtomicBool>,
    // notify the background clean task that im done.
    done_notify: Arc<Notify>,
    last_modified: RwLock<Option<Instant>>,
    total_slice_counter: Arc<AtomicUsize>,
    start_at: Instant,
}

impl SliceWriter {
    // freeze this slice writer, make it cannot be written for the flushing.
    pub(crate) fn freeze(self: &Arc<Self>) {
        self.frozen.store(true, Ordering::Release)
    }
    // check if this slice writer can be written.
    pub(crate) fn frozen(self: &Arc<Self>) -> bool {
        self.frozen.load(Ordering::Acquire)
    }
    // get the underlying write buffer's released length and total write length.
    async fn get_flushed_length_and_total_write_length(self: &Arc<Self>) -> (usize, usize) {
        let guard = self.write_buffer.read().await;
        let flushed_len = guard.flushed_length();
        let write_len = guard.length();
        (flushed_len, write_len)
    }

    async fn get_length(self: &Arc<Self>) -> usize {
        let guard = self.write_buffer.read().await;
        guard.length()
    }

    // return the current write len to this buffer,
    // the buffer total length, and the flushed length.
    #[instrument(skip_all, name = "SliceWriter::write", fields(offset, len = ReadableSize(data.len() as u64).to_string()))]
    async fn write(
        self: &Arc<Self>,
        slice_offset: usize,
        data: &[u8],
    ) -> Result<(usize, usize, usize)> {
        let mut guard = self.write_buffer.write().await;
        let write_len = guard
            .write_at(slice_offset, data)
            .in_current_span()
            .await
            .expect("write data failed");
        self.last_modified.write().await.replace(Instant::now());
        Ok((write_len, guard.length(), guard.flushed_length()))
    }

    /// [SliceWriter::finish] will try to flush all data to the backend storage,
    /// and free the undelrying buffer.
    pub(crate) async fn finish(self: &Arc<Self>) -> Result<()> {
        debug!("do flush and release on {}", self.internal_seq);
        let mut guard = self.write_buffer.write().await;
        if guard.length() == 0 {
            self.done.store(true, Ordering::Release);
            return Ok(());
        }

        guard.finish().await?;
        self.done.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) async fn do_flush_to(self: &Arc<Self>, offset: usize) -> Result<()> {
        let mut guard = self.write_buffer.write().await;
        guard.flush_to(offset).await?;
        Ok(())
    }

    async fn prepare_slice_id(self: &Arc<Self>, meta_engine: Arc<MetaEngine>) -> Result<()> {
        if !self.slice_id_prepared.load(Ordering::Acquire) {
            let guard = self.write_buffer.read().await;
            if guard.get_slice_id() == EMPTY_SLICE_ID {
                drop(guard);
                if self.slice_id_prepared.load(Ordering::Acquire) {
                    // load again
                    return Ok(());
                }
                let mut write_guard = self.write_buffer.write().await;
                if write_guard.get_slice_id() == EMPTY_SLICE_ID {
                    let sid = meta_engine.next_slice_id().await?;
                    debug!("assign slice id {} to slice {}", sid, self.internal_seq);
                    write_guard.set_slice_id(sid);
                }
                self.slice_id_prepared.store(true, Ordering::Release);
            }
        }
        Ok(())
    }

    async fn to_meta_slice(self: &Arc<Self>) -> Slice {
        let guard = self.write_buffer.read().await;
        Slice::new_owned(
            self.chunk_start_offset,
            guard.get_slice_id(),
            guard.length(),
        )
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
    use crate::{
        common::{install_fmt_log, new_memory_sto},
        meta::MetaConfig,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn find_chunk_to_write() {
        install_fmt_log();

        let meta_engine = MetaConfig::default().open().unwrap();
        let sto_engine = new_memory_sto();
        let engine = Arc::new(
            Engine::new(
                Arc::new(EngineConfig::default()),
                sto_engine,
                Arc::new(meta_engine),
            )
            .unwrap(),
        );
        let chunk_size = engine.config.chunk_size;

        struct TestCase {
            offset: usize,
            data: Vec<u8>,
            want: Vec<ChunkWriteCtx>,
        }

        impl TestCase {
            async fn run(&self, ino: Ino, data_engine: Arc<Engine>) {
                data_engine.new_file_writer(ino, 0);
                let locations = find_write_location(
                    data_engine.config.chunk_size,
                    self.offset,
                    self.data.len(),
                );
                assert_eq!(locations, self.want);
                let write_len = data_engine.write(ino, self.offset, &self.data).await;
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
            i.run(Ino(1), engine.clone()).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_write() {
        install_fmt_log();

        let meta_engine = MetaConfig::default().open().unwrap();
        let sto_engine = new_memory_sto();
        let engine = Arc::new(
            Engine::new(
                Arc::new(EngineConfig::default()),
                sto_engine,
                Arc::new(meta_engine),
            )
            .unwrap(),
        );

        engine.new_file_writer(Ino(1), 0);
        let data = vec![0u8; 65 << 20];
        let write_len = engine.write(Ino(1), 0, &data).await;
        assert!(write_len.is_ok());
    }
}
