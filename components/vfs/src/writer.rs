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

//! A FileWriter can have multiple ChunkWriters, and a chunk writer can have
//! multiple slice writers. ChunkWriters control how(flush full or flush a part)
//! and when to flush the data (1. have a long time no write, 2. we are running
//! out of space, 3. initiative). to the remote storage. When call flush on
//! FileWriter we should wait for all the chunk writers to finish. So each chunk
//! writer has a background committer to flush the SliceWriters to the remote
//! storage by order.

use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc, Weak,
        atomic::{AtomicIsize, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, ChunkIndex, cal_chunk_idx, cal_chunk_offset};
use kiseki_meta::MetaEngineRef;
use kiseki_storage::slice_buffer::SliceBuffer;
use kiseki_types::{
    ino::Ino,
    slice::{EMPTY_SLICE_ID, SliceID},
};
use kiseki_utils::readable_size::ReadableSize;
use libc::EBADF;
use scopeguard::defer;
use snafu::{OptionExt, ResultExt};
use tokio::{
    sync::{Notify, RwLock, mpsc},
    task::yield_now,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, instrument};

use crate::{
    data_manager::DataManager,
    err::{LibcSnafu, Result},
};

impl DataManager {
    /// Creates a new [FileWriter] for the file.
    /// All file handle share the single one [FileWriter].
    pub(crate) fn open_file_writer(self: &Arc<Self>, ino: Ino, len: u64) -> Arc<FileWriter> {
        let fw = self
            .file_writers
            .entry(ino)
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(200);
                let fw = FileWriter {
                    inode:                  ino,
                    length:                 AtomicUsize::new(len as usize),
                    chunk_writers:          Default::default(),
                    slice_flush_queue:      tx,
                    total_slice_writer_cnt: Arc::new(Default::default()),
                    cancel_token:           CancellationToken::new(),
                    seq_generate:           self.id_generator.clone(),
                    ref_count:              Arc::new(Default::default()),
                    flush_waiting:          Arc::new(Default::default()),
                    flush_done_notify:      Arc::new(Default::default()),
                    write_waiting:          Arc::new(Default::default()),
                    write_notify:           Arc::new(Default::default()),
                    pattern:                Default::default(),
                    early_flush_threshold:  0.3,
                    data_manager:           Arc::downgrade(self),
                };

                let fw = Arc::new(fw);
                let fw_cloned = fw.clone();
                tokio::spawn(async move {
                    let flusher = FileWriterFlusher {
                        fw:       fw_cloned,
                        flush_rx: rx,
                    };
                    flusher.run().await
                });
                fw
            })
            .clone();
        fw.ref_count.fetch_add(1, Ordering::AcqRel);
        fw
    }

    /// Use the [FileWriter] to write data to the file.
    /// Return the number of bytes written and current file length.
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
            .context(LibcSnafu { errno: EBADF })?;
        debug!("Ino({}) get file write success", ino);
        let write_len = fw.write(offset, data).await?;
        let current_len = fw.get_length();
        debug!(
            "Ino({}) write len: {}, current_len: {}",
            ino,
            ReadableSize(write_len as u64),
            ReadableSize(current_len as u64),
        );
        self.truncate_reader(ino, current_len as u64).await;
        Ok(write_len)
    }

    /// [direct_flush] flush the content of the specified ino to the remote
    /// storage. TODO: review me.
    /// Problem: This function call the underlying flush logic without acquiring
    /// lock of [FileHandle].
    #[instrument(skip(self), fields(ino))]
    pub(crate) async fn direct_flush(&self, ino: Ino) -> Result<()> {
        if let Some(fw) = self.file_writers.get(&ino) {
            fw.finish().in_current_span().await?;
            debug!(
                "{} file writer exists and flush success, current_len: {}",
                ino,
                ReadableSize(fw.get_length() as u64)
            );
        } else {
            debug!("{} file writer not exists, don't need to flush", ino);
        }
        Ok(())
    }
}

type InternalSliceSeq = u64;

#[derive(Debug, Default)]
struct WriterPattern {
    score:                  AtomicIsize,
    last_write_stop_offset: AtomicUsize,
}

impl WriterPattern {
    const MAX_RANDOM_SCORE: isize = -5;
    const MAX_SEQ_SCORE: isize = 20;

    fn monitor_write_at(&self, offset: usize, size: usize) {
        let last_offset = self
            .last_write_stop_offset
            .swap(offset + size, Ordering::AcqRel);
        let score = self.score.load(Ordering::Acquire);
        if last_offset == offset {
            // seq
            if score < Self::MAX_SEQ_SCORE {
                // add weight on the seq side.
                self.score.fetch_add(1, Ordering::AcqRel);
            }
        } else if score > Self::MAX_RANDOM_SCORE {
            // add weight on the random side.
            self.score.fetch_add(-1, Ordering::AcqRel);
        }
    }

    fn is_seq(&self) -> bool { self.score.load(Ordering::Acquire) >= 0 }
}

pub(crate) type FileWritersRef = Arc<DashMap<Ino, Arc<FileWriter>>>;

/// FileWriter is the entry point for writing data to a file.
pub struct FileWriter {
    pub inode: Ino,

    // runtime
    // the length of current file.
    length:            AtomicUsize,
    // buffer writers.
    chunk_writers:     RwLock<HashMap<ChunkIndex, Arc<ChunkWriter>>>,
    // we may need to wait on the flush queue to flush the data to the remote storage.
    slice_flush_queue: mpsc::Sender<FlushReq>,

    cancel_token:           CancellationToken,
    seq_generate:           Arc<sonyflake::Sonyflake>,
    // tracking how many operations is using the file writer.
    // 1. when we open, we increase the counter.
    // 2. when we close the file, decrease the counter.
    ref_count:              Arc<AtomicUsize>,
    flush_waiting:          Arc<AtomicUsize>,
    flush_done_notify:      Arc<Notify>,
    write_waiting:          Arc<AtomicUsize>,
    write_notify:           Arc<Notify>,
    total_slice_writer_cnt: Arc<AtomicUsize>,

    // random write early flush
    // when we reach the threshold, we should flush some flush to the remote storage.
    pattern:               Arc<WriterPattern>,
    // when should we flush the buffer in advance, according to current buffer pool free ratio.
    early_flush_threshold: f64,

    // dependencies
    // the underlying object storage.
    data_manager: Weak<DataManager>,
}

impl FileWriter {
    pub fn get_length(self: &Arc<Self>) -> usize { self.length.load(Ordering::Acquire) }

    /// Write data to the file.
    ///
    /// 1. calculate the location
    /// 2. write to the location
    /// 3. get the written length
    /// 4. check if we buffered enough data to flush:
    ///    1. full; we remove the ref from map;
    ///    2. a whole block has been buffered, we keep the ref in the map.
    ///
    /// Summary:
    ///     We can write while flushing
    ///     1. for flushing block, we can reuse that SliceWriter.
    ///     2. for flushing whole chunk and manually flushing, we will use a new
    ///        SliceWriter. But we lock the map when we make flushReq.
    #[instrument(skip_all, fields(self.inode, offset, write_len = ReadableSize( data.len() as u64).to_string()))]
    pub async fn write(self: &Arc<Self>, offset: usize, data: &[u8]) -> Result<usize> {
        let expected_write_len = data.len();
        if expected_write_len == 0 {
            return Ok(0);
        }

        self.pattern.monitor_write_at(offset, expected_write_len);
        while let cnt = self.total_slice_writer_cnt.load(Ordering::Acquire) {
            if cnt < 1000 {
                break;
            }
            // we have too many slice writers, we should wait for some to finish.
            yield_now().await;
        }

        // wait for the flush to finish.
        self.write_waiting.fetch_add(1, Ordering::AcqRel);
        while self.flush_waiting.load(Ordering::Acquire) > 0 {
            self.flush_done_notify.notified().await;
        }
        self.write_waiting.fetch_sub(1, Ordering::AcqRel);

        // 1. find write location.
        let start = Instant::now();
        debug!(
            "Ino({}) try to find slice writer {:?}",
            self.inode,
            start.elapsed()
        );
        let slice_writers = self
            .find_writable_slice_writer(offset, expected_write_len)
            .in_current_span()
            .await;
        debug!(
            "Ino({}) find slice writer success {:?}",
            self.inode,
            start.elapsed()
        );

        let mut write_len = 0;
        for (sw, state, l) in slice_writers.iter() {
            let data = &data[l.buf_start_at..l.buf_start_at + l.need_write_len];
            let n = sw
                .write_at(*state, l.chunk_offset - sw.offset_of_chunk, data)
                .await?;
            write_len += n;
            let sw = sw.clone();

            if let Some(req) = sw.make_flush_req(self.pattern.is_seq(), false).await
                && let Err(e) = self.slice_flush_queue.send(req).await
            {
                panic!("failed to send flush request {e}");
            }
        }

        // 4. update the new file length
        let mut old_len = self.length.load(Ordering::Acquire);
        // do compare and exchange
        let may_new_len = offset + write_len;
        if may_new_len > old_len {
            // give up if someone's length is larger.
            loop {
                debug!(
                    "Ino({}) try to update file length from {} to {}",
                    self.inode,
                    ReadableSize(old_len as u64),
                    ReadableSize(may_new_len as u64)
                );
                match self.length.compare_exchange(
                    old_len,
                    may_new_len,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(new_old_len) => {
                        if new_old_len >= may_new_len {
                            break;
                        }
                        // use the new old len to try CAS.
                        old_len = new_old_len;
                    }
                }
            }
        }

        Ok(write_len)
    }

    #[instrument(skip_all, fields(self.inode))]
    async fn find_writable_slice_writer(
        self: &Arc<Self>,
        offset: usize,
        expected_write_len: usize,
    ) -> Vec<(Arc<SliceWriter>, SliceWriterState, ChunkWriteCtx)> {
        let mut sws = Vec::with_capacity(2);
        for l in locate_chunk(CHUNK_SIZE, offset, expected_write_len) {
            let mut write_guard = self.chunk_writers.write().await;
            let cw = write_guard
                .entry(l.chunk_idx)
                .or_insert(Arc::new(ChunkWriter {
                    inode:             self.inode,
                    chunk_index:       l.chunk_idx,
                    slice_writers:     Default::default(),
                    fw:                Arc::downgrade(self),
                    total_slice_count: self.total_slice_writer_cnt.clone(),
                    slice_done_notify: Default::default(),
                }))
                .clone();
            drop(write_guard);

            let (sw, state) = cw.find_writable_slice_writer(&l).await;
            sws.push((sw, state, l));
        }
        sws
    }

    /// Flush the data to the remote storage.
    #[instrument(skip(self), fields(self.inode))]
    pub async fn finish(self: &Arc<Self>) -> Result<()> {
        self.flush_waiting.fetch_add(1, Ordering::AcqRel);
        defer!({
            self.flush_waiting.fetch_sub(1, Ordering::AcqRel);
            self.flush_done_notify.notify_waiters();
        });

        let read_guard = self.chunk_writers.read().await;
        debug!(
            "Ino({}) flush is waiting for all chunk writer finish, cnt: {}",
            self.inode,
            read_guard.len()
        );
        if read_guard.is_empty() {
            return Ok(());
        }
        let handles = read_guard
            .values()
            .map(|cw| {
                let cw = cw.clone();
                tokio::spawn(async move {
                    cw.finish().await;
                })
            })
            .collect::<Vec<_>>();
        drop(read_guard);

        if let Err(e) = futures::future::try_join_all(handles).await {
            panic!("failed to flush chunk writers {e}");
        }
        let mut write_guard = self.chunk_writers.write().await;
        write_guard.clear();

        Ok(())
    }

    /// Close won't really close the file writer, it tries to decrease the ref
    /// count first.
    #[instrument(skip(self), fields(self.inode))]
    pub(crate) async fn close(self: &Arc<Self>) {
        if let Err(e) = self.finish().await {
            error!("failed to flush in close {e}");
        }
        if self.ref_count.fetch_sub(1, Ordering::AcqRel) <= 1 {
            // we are the last one, we can close the file writer.
            self.cancel_token.cancel();
            let dm = self.data_manager.upgrade().unwrap();
            dm.file_writers.remove(&self.inode);
        };
    }

    pub(crate) fn get_reference_count(self: &Arc<Self>) -> usize {
        self.ref_count.load(Ordering::Acquire)
    }
}

enum FlushReq {
    FlushBulk {
        sw:     Arc<SliceWriter>,
        offset: usize,
    },
    FlushFull(Arc<SliceWriter>),
    CommitIdle(Arc<SliceWriter>),
}

/// FileWriterFlusher flushes the data to the remote storage periodically.
struct FileWriterFlusher {
    fw:       Arc<FileWriter>,
    flush_rx: mpsc::Receiver<FlushReq>,
}

impl FileWriterFlusher {
    #[instrument(skip(self), fields(self.fw.inode))]
    async fn run(mut self) {
        debug!("Ino({}) flush task started", self.fw.inode);
        let cancel_token = self.fw.cancel_token.clone();
        let cloned_cancel_token = cancel_token.clone();
        let ino = self.fw.inode;
        let meta_engine = self
            .fw
            .data_manager
            .upgrade()
            .expect("data manager is dropped")
            .meta_engine
            .clone();

        loop {
            tokio::select! {
                _ = cloned_cancel_token.cancelled() => {
                    debug!("flusher of [{ino}] is cancelled");
                    return;
                }
                req = self.flush_rx.recv() => {
                    if let Some(req) = req {
                        match req {
                            FlushReq::FlushBulk {sw, offset} => {
                                debug!("{ino} flush bulk to {offset}");
                                let _fw = self.fw.clone();
                                let cancel_token = cancel_token.clone();
                                let _me = meta_engine.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        r = sw.do_flush_bulk(offset) => {
                                            if let Err(e) = r {
                                                error!("{ino} failed to flush bulk {e}");
                                            }
                                        }
                                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                                            // we should not wait for the flush to finish.
                                            // we should just commit the slice writer.
                                            panic!("flush full is timeout");
                                        }
                                        _ = cancel_token.cancelled() => {;
                                        }
                                    }
                                });
                            },
                            FlushReq::FlushFull(sw) => {
                                let _fw = self.fw.clone();
                                let cancel_token = cancel_token.clone();
                                let me = meta_engine.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        r = sw.flush_and_commit(ino, me) => {
                                            if let Err(e) = r {
                                                panic!("{ino} failed to flush full {e}");
                                            }
                                        }
                                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                                            // we should not wait for the flush to finish.
                                            // we should just commit the slice writer.
                                            panic!("flush full is timeout");
                                        }
                                        _ = cancel_token.cancelled() => {
                                            debug!("{ino} flush full is cancelled");;
                                        }
                                    }
                                });
                            },
                            FlushReq::CommitIdle(sw) => {
                                let _fw = self.fw.clone();
                                let me = meta_engine.clone();
                                // just commit the idle slice writer.
                                tokio::spawn(async move {
                                    sw.commit_partial(ino, me).await;
                                });
                            },
                        }
                    }
                }
            }
        }
    }
}

struct ChunkWriter {
    inode:             Ino,
    chunk_index:       ChunkIndex,
    slice_writers:     RwLock<BTreeMap<InternalSliceSeq, Arc<SliceWriter>>>,
    fw:                Weak<FileWriter>,
    total_slice_count: Arc<AtomicUsize>,
    slice_done_notify: Arc<Notify>,
}

impl ChunkWriter {
    #[instrument(skip(self), fields(chunk_index = self.chunk_index))]
    async fn find_writable_slice_writer(
        self: &Arc<Self>,
        l: &ChunkWriteCtx,
    ) -> (Arc<SliceWriter>, SliceWriterState) {
        let fw = self.fw.upgrade().unwrap();
        {
            let read_guard = self.slice_writers.read().await;
            for (idx, (_, sw)) in read_guard.iter().rev().enumerate() {
                let (flushed, length) = sw.get_flushed_length_and_total_write_length().await;
                let old_state = SliceWriterState::from(sw.state.load(Ordering::Acquire));
                if !matches!(old_state, SliceWriterState::Idle | SliceWriterState::Dirty) {
                    // we skip the writing state as well since it means we may in random writing.
                    // skip unwritable slice writer.
                    continue;
                }

                if sw.offset_of_chunk + flushed <= l.chunk_offset
                    && l.chunk_offset <= sw.offset_of_chunk + length
                {
                    if sw
                        .state
                        .compare_exchange(
                            old_state as u8,
                            SliceWriterState::Writing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone else has taken the slice writer.
                        continue;
                    }
                    // we win the contention. start to write.
                    return (sw.clone(), old_state);
                } else if idx >= 3 {
                    // try to flush in advance
                    let sw = sw.clone();
                    let req = sw.make_flush_req(fw.pattern.is_seq(), true).await;
                    if let Some(req) = req
                        && let Err(e) = fw.slice_flush_queue.send(req).await
                    {
                        panic!("failed to send flush request {e}");
                    }
                    continue;
                }
            }
        }

        // in random mode or we can't find a suitable slice writer.
        // we need to create a new one.

        let sw = Arc::new(SliceWriter::new(
            l.chunk_idx,
            self,
            SliceWriterState::Writing,
            fw.seq_generate
                .next_id()
                .expect("should not fail when generate internal seq"),
            l.chunk_offset,
            fw.data_manager.clone(),
        ));
        self.total_slice_count.fetch_add(1, Ordering::AcqRel);
        let mut write_guard = self.slice_writers.write().await;
        write_guard.insert(sw._internal_seq, sw.clone());
        (sw, SliceWriterState::Idle)
    }

    async fn finish(self: &Arc<Self>) {
        let fw = self.fw.upgrade().expect("file writer is dropped");
        loop {
            let read_guard = self.slice_writers.read().await;
            let len = read_guard.len();
            if len == 0 {
                return;
            }

            let mut done = vec![];
            let sws = read_guard.values().cloned().collect::<Vec<_>>();
            drop(read_guard);
            let _current_len = sws.len();
            for sw in sws {
                let sw = sw.clone();
                let id = sw._internal_seq;
                let state = SliceWriterState::from(sw.state.load(Ordering::Acquire));
                if matches!(
                    state,
                    SliceWriterState::Writing | SliceWriterState::Flushing
                ) {
                    continue;
                }
                if state == SliceWriterState::Done {
                    done.push(id);
                    continue;
                }

                // actually we don't care if we are in seq or random mode when the flush is
                // called.
                let req = sw.make_flush_req(false, true).await;
                if let Some(req) = req {
                    fw.slice_flush_queue
                        .send(req)
                        .await
                        .expect("failed to send flush request");
                }
            }

            let done_len = done.len();
            if done_len != 0 {
                let mut write_guard = self.slice_writers.write().await;
                for sid in done {
                    write_guard.remove(&sid);
                }
                if write_guard.is_empty() {
                    return;
                }
            }
            // wait for the slice writer to finish.
            self.slice_done_notify.notified().await;
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[repr(u8)]
enum SliceWriterState {
    // An idle SliceWriter, its ready for write.
    // Two case:
    // 1. just initialized.
    // 2. just flushed bulk, but we can still write data to the slice writer.
    //
    // When come to finish, we can just drop the Idle SliceWriter,
    // since either it has not been written, or it has been flushed.
    Idle,
    // Prepare for writing or in writing process.
    // When we write finished, it goes to dirty state.
    Writing,
    // a dirty slice, have some data need to be flushed.
    Dirty,
    // in flushing process.
    Flushing,
    // in committing process.
    Committing,
    // commit finished, we need to remove the slice writer from memory.
    Done,
}

impl From<u8> for SliceWriterState {
    fn from(value: u8) -> Self {
        match value {
            0 => SliceWriterState::Idle,
            1 => SliceWriterState::Writing,
            2 => SliceWriterState::Dirty,
            3 => SliceWriterState::Flushing,
            4 => SliceWriterState::Committing,
            5 => SliceWriterState::Done,
            _ => panic!("invalid state"),
        }
    }
}

/// SliceWriter is the entry point for writing data to a slice,
/// it depends on a SliceBuffer to buffer the write request.
/// SliceWriter can only grow from left to right, we can update it,
/// but cannot change the start offset.
struct SliceWriter {
    // dependencies
    // the underlying object storage.
    data_manager: Weak<DataManager>,
    chunk_index:  ChunkIndex,
    chunk_writer: Weak<ChunkWriter>,

    state: Arc<AtomicU8>,

    // the internal seq of the slice writer,
    // used to identify the slice writer,
    // we have it since we delay the slice-id assignment until we
    // really do the flush.
    _internal_seq:   u64,
    // the slice id of the slice, assigned by meta engine.
    slice_id:        AtomicU64,
    // where the slice start at of the chunk
    offset_of_chunk: usize,
    // the buffer to serve the write request.
    slice_buffer:    RwLock<SliceBuffer>,
    last_modified:   AtomicCell<Instant>,
}

impl Display for SliceWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SliceWriter[{}]", self._internal_seq)
    }
}

impl SliceWriter {
    fn new(
        chunk_index: ChunkIndex,
        cw: &Arc<ChunkWriter>,
        state: SliceWriterState,
        seq: u64,
        offset_of_chunk: usize,
        data_manager: Weak<DataManager>,
    ) -> SliceWriter {
        Self {
            chunk_index,
            chunk_writer: Arc::downgrade(cw),
            state: Arc::new(AtomicU8::new(state as u8)),
            _internal_seq: seq,
            slice_id: AtomicU64::new(EMPTY_SLICE_ID),
            offset_of_chunk,
            slice_buffer: RwLock::new(SliceBuffer::new()),
            last_modified: AtomicCell::new(Instant::now()),
            data_manager,
        }
    }

    async fn write_at(
        self: &Arc<Self>,
        // we may come from IDLE or DIRTY.
        before_state: SliceWriterState,
        offset: usize,
        data: &[u8],
    ) -> Result<usize> {
        let mut write_guard = self.slice_buffer.write().await;
        let written = write_guard.write_at(offset, data).await?;
        self.last_modified.store(Instant::now());
        let new_state = if written == 0 {
            before_state
        } else {
            SliceWriterState::Dirty
        };

        // we must be in the writing state.
        // the find writableSliceWriter will set the in_writing to true.
        if let Err(e) = self.state.compare_exchange(
            SliceWriterState::Writing as u8,
            new_state as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            panic!(
                "failed to change state from {:?} to dirty, new_current_state: {:?}",
                SliceWriterState::Writing,
                SliceWriterState::from(e)
            );
        }
        Ok(written)
    }

    async fn do_flush_bulk(self: &Arc<Self>, offset: usize) -> Result<()> {
        defer!({
            if self
                .state
                .compare_exchange(
                    SliceWriterState::Flushing as u8,
                    SliceWriterState::Idle as u8,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                panic!("failed to change state from flushing to idle after flush bulk");
            }
        });
        let slice_id = self.prepare_slice_id().await?;
        let mut write_guard = self.slice_buffer.write().await;
        write_guard
            .stage(
                slice_id,
                offset,
                self.data_manager.upgrade().unwrap().file_cache.clone(),
            )
            .await
            .expect("flush bulk to object storage failed");

        Ok(())
    }

    #[instrument(skip_all, fields(_internal_seq = self._internal_seq))]
    async fn flush_and_commit(
        self: &Arc<Self>,
        inode: Ino,
        meta_engine_ref: MetaEngineRef,
    ) -> Result<()> {
        let slice_id = self.prepare_slice_id().in_current_span().await?;
        let mut write_guard = self.slice_buffer.write().await;
        if let Err(e) = write_guard
            .flush_v2(
                slice_id,
                self.data_manager.upgrade().unwrap().file_cache.clone(),
            )
            .await
        {
            panic!("flush to object storage blocked, {:?}", e);
        }

        let len = write_guard.length();
        drop(write_guard);

        // then the sw is done.
        // write the meta info of this slice.
        meta_engine_ref
            .write_slice(
                inode,
                self.chunk_index,
                self.offset_of_chunk,
                kiseki_types::slice::Slice::Owned {
                    chunk_pos: self.offset_of_chunk as u32,
                    id:        slice_id,
                    size:      len as u32,
                    _padding:  0,
                },
                self.last_modified.load(),
            )
            .await?;

        let cw = self.chunk_writer.upgrade().unwrap();
        cw.total_slice_count.fetch_sub(1, Ordering::AcqRel);
        if let Err(e) = self.state.compare_exchange(
            SliceWriterState::Flushing as u8,
            SliceWriterState::Done as u8,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            panic!(
                "failed to change state from flushing to idle after flush, new_current_state: {:?}",
                SliceWriterState::from(e)
            );
        };
        cw.slice_done_notify.notify_waiters(); // notify the chunk writer to check if we are done.
        cw.slice_done_notify.notify_one();

        Ok(())
    }

    async fn prepare_slice_id(self: &Arc<Self>) -> Result<SliceID> {
        let old = self.slice_id.load(Ordering::Acquire);
        if old != EMPTY_SLICE_ID {
            return Ok(old);
        }

        let data_manager = self
            .data_manager
            .upgrade()
            .expect("data manager should not be dropped");
        let slice_id = data_manager
            .meta_engine
            .next_slice_id()
            .in_current_span()
            .await?;
        // don't care about the result, since we only care about the first time.
        let _ = self
            .slice_id
            .compare_exchange(0, slice_id, Ordering::AcqRel, Ordering::Relaxed);
        Ok(slice_id)
    }

    // make_flush_req try to make flush req if we write enough data.
    async fn make_flush_req(self: Arc<Self>, seq: bool, finish: bool) -> Option<FlushReq> {
        let state = self.state.load(Ordering::Acquire);
        match SliceWriterState::from(state) {
            SliceWriterState::Idle => {
                if finish {
                    // we should try to commit this slice writer.
                    if self
                        .state
                        .compare_exchange(
                            SliceWriterState::Idle as u8,
                            SliceWriterState::Committing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone win the contention. we give up
                        return None;
                    }
                    Some(FlushReq::CommitIdle(self.clone()))
                } else {
                    None
                }
            }
            SliceWriterState::Writing
            | SliceWriterState::Flushing
            | SliceWriterState::Committing
            | SliceWriterState::Done => {
                // Idle: nothing to flush
                // Writing: someone take the slice writer again, let him cook
                // Flushing: already in flushing, let it go.
                // Committing: we are committing, let it go.
                None
            }
            SliceWriterState::Dirty => {
                if finish {
                    if let Err(e) = self.state.compare_exchange(
                        SliceWriterState::Dirty as u8,
                        SliceWriterState::Flushing as u8,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        // someone win the contention. we give up
                        panic!(
                            "someone change the state {:?}, we expect {:?}",
                            SliceWriterState::from(e),
                            SliceWriterState::Dirty
                        );
                    }
                    return Some(FlushReq::FlushFull(self.clone()));
                }

                let read_guard = self.slice_buffer.read().await;
                let length = read_guard.length();
                let flushed_len = read_guard.flushed_length();
                drop(read_guard);

                if length == CHUNK_SIZE {
                    if self
                        .state
                        .compare_exchange(
                            SliceWriterState::Dirty as u8,
                            SliceWriterState::Flushing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone win the contention.
                        return None;
                    }
                    Some(FlushReq::FlushFull(self.clone()))
                } else if length - flushed_len >= BLOCK_SIZE && !seq {
                    if self
                        .state
                        .compare_exchange(
                            SliceWriterState::Dirty as u8,
                            SliceWriterState::Flushing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone win the contention. we give up
                        return None;
                    }
                    Some(FlushReq::FlushBulk {
                        sw:     self.clone(),
                        offset: length,
                    })
                } else {
                    None
                }
            }
        }
    }

    // get the underlying write buffer's released length and total write length.
    async fn get_flushed_length_and_total_write_length(self: &Arc<Self>) -> (usize, usize) {
        let guard = self.slice_buffer.read().await;
        let flushed_len = guard.flushed_length();
        let write_len = guard.length();
        (flushed_len, write_len)
    }

    async fn commit_partial(
        self: &Arc<Self>,
        inode: Ino,
        meta_engine_ref: MetaEngineRef,
    ) -> Result<()> {
        let cw = self.chunk_writer.upgrade().unwrap();
        defer!({
            cw.total_slice_count.fetch_sub(1, Ordering::AcqRel);
            if let Err(e) = self.state.compare_exchange(
                SliceWriterState::Committing as u8,
                SliceWriterState::Done as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                panic!(
                    "failed to change state from committing to done, origin: {:?}",
                    SliceWriterState::from(e)
                );
            }
            cw.slice_done_notify.notify_waiters();
        });

        // write slice meta info to meta engine.
        let slice_id = self.slice_id.load(Ordering::Acquire);
        let len = self.slice_buffer.read().await.length();
        if len == 0 {
            return Ok(());
        }

        // then the sw is done.
        // write the meta info of this slice.
        if let Err(e) = meta_engine_ref
            .write_slice(
                inode,
                self.chunk_index,
                self.offset_of_chunk,
                kiseki_types::slice::Slice::Owned {
                    chunk_pos: self.offset_of_chunk as u32,
                    id:        slice_id,
                    size:      len as u32,
                    _padding:  0,
                },
                self.last_modified.load(),
            )
            .await
        {
            panic!("failed to write slice meta info {e}");
        };

        Ok(())
    }
}

impl Eq for SliceWriter {}

impl PartialEq for SliceWriter {
    fn eq(&self, other: &Self) -> bool { self._internal_seq == other._internal_seq }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct ChunkWriteCtx {
    file_offset:    usize,
    // write to which chunk
    chunk_idx:      usize,
    // the start offset of this write
    chunk_offset:   usize,
    // the length of this write
    need_write_len: usize,
    // the start offset of the input buf
    buf_start_at:   usize,
}

fn locate_chunk(chunk_size: usize, offset: usize, expect_write_len: usize) -> Vec<ChunkWriteCtx> {
    let expected_write_len = expect_write_len;

    let start_chunk_idx = cal_chunk_idx(offset, chunk_size);
    let end_chunk_idx = cal_chunk_idx(offset + expected_write_len - 1, chunk_size);

    let mut chunk_pos = cal_chunk_offset(offset, chunk_size);
    let mut buf_start_at = 0;
    let mut left = expected_write_len;

    (start_chunk_idx..=end_chunk_idx)
        .map(move |idx| {
            let max_can_write = min(chunk_size - chunk_pos, left);
            debug!(
                "offset: {}, chunk-size: {}, chunk: {} chunk_pos: {}, left: {}, buf start at: {}, \
                 max can write: {}",
                ReadableSize(offset as u64),
                ReadableSize(chunk_size as u64),
                idx,
                chunk_pos,
                ReadableSize(left as u64),
                ReadableSize(buf_start_at as u64),
                ReadableSize(max_can_write as u64)
            );

            let ctx = ChunkWriteCtx {
                file_offset: offset + buf_start_at,
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
