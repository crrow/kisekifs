use crossbeam::atomic::AtomicCell;
use std::fmt::Debug;
use std::time::Duration;
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fmt::{Display, Formatter},
    io::Cursor,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use kiseki_common::{
    cal_chunk_idx, cal_chunk_offset, ChunkIndex, FileOffset, BLOCK_SIZE, CHUNK_SIZE, FH,
};
use kiseki_meta::MetaEngineRef;
use kiseki_storage::{cache::CacheRef, slice_buffer::SliceBuffer};
use kiseki_types::{
    ino::Ino,
    slice::{make_slice_object_key, SliceID, EMPTY_SLICE_ID},
};
use kiseki_utils::{object_storage::ObjectStorage, readable_size::ReadableSize};
use libc::EBADF;
use rangemap::RangeMap;
use scopeguard::defer;
use snafu::{OptionExt, ResultExt};
use tokio::task::yield_now;
use tokio::time::error::Elapsed;
use tokio::{
    sync::{mpsc, oneshot, Mutex, Notify, OnceCell, RwLock},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn, Instrument};

use crate::{
    data_manager::DataManager,
    err::{JoinErrSnafu, LibcSnafu, Result},
    reader::FileReader,
    KisekiVFS,
};

impl DataManager {
    /// Creates a new [FileWriter] for the file.
    /// All file handle share the single one [FileWriter].
    pub(crate) fn open_file_writer(self: &Arc<Self>, ino: Ino, len: u64) -> Arc<FileWriter> {
        let fw = self
            .file_writers
            .entry(ino)
            .or_insert_with(|| {
                let (tx, mut rx) = mpsc::channel(100);
                let fw = FileWriter {
                    inode: ino,
                    length: AtomicUsize::new(len as usize),
                    chunk_writers: Default::default(),
                    slice_flush_queue: tx,
                    total_slice_cnt: Arc::new(Default::default()),
                    cancel_token: CancellationToken::new(),
                    seq_generate: self.id_generator.clone(),
                    ref_count: Arc::new(Default::default()),
                    flush_waiting: Arc::new(Default::default()),
                    flush_notify: Arc::new(Default::default()),
                    write_waiting: Arc::new(Default::default()),
                    pattern: Default::default(),
                    early_flush_threshold: 0.3,
                    data_manager: Arc::downgrade(self),
                };

                let fw = Arc::new(fw);
                let fw_cloned = fw.clone();
                tokio::spawn(async move {
                    let flusher = FileWriterFlusher {
                        fw: fw_cloned,
                        flush_rx: rx,
                        flush_count: Arc::new(Default::default()),
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
        debug!("{} get file write success", ino);
        let write_len = fw.write(offset, data).await?;
        let current_len = fw.get_length();
        debug!(
            "{} write len: {}, current_len: {}",
            ino,
            ReadableSize(write_len as u64),
            ReadableSize(current_len as u64),
        );
        self.truncate_reader(ino, current_len as u64);
        Ok(write_len)
    }

    #[instrument(skip(self), fields(ino))]
    pub(crate) async fn direct_flush(&self, ino: Ino) -> Result<()> {
        if let Some(fw) = self.file_writers.get(&ino) {
            fw.flush().in_current_span().await?;
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
    is_seq_count: AtomicIsize,
    last_write_stop_offset: AtomicUsize,
}

impl WriterPattern {
    const LIMIT: isize = 5;

    fn monitor_write_at(&self, offset: usize, size: usize) {
        let last_offset = self
            .last_write_stop_offset
            .swap(offset + size, Ordering::AcqRel);
        let count = self.is_seq_count.load(Ordering::Acquire);
        if last_offset == offset {
            // seq
            if count < Self::LIMIT {
                // add weight on the seq side.
                self.is_seq_count.fetch_add(1, Ordering::SeqCst);
            }
        } else {
            if count > -Self::LIMIT {
                // add weight on the random side.
                self.is_seq_count.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
    fn is_seq(&self) -> bool {
        self.is_seq_count.load(Ordering::Acquire) >= 0
    }
}

pub(crate) type FileWritersRef = Arc<DashMap<Ino, Arc<FileWriter>>>;

/// FileWriter is the entry point for writing data to a file.
pub struct FileWriter {
    pub inode: Ino,

    // runtime
    // the length of current file.
    length: AtomicUsize,
    // buffer writers.
    chunk_writers: RwLock<HashMap<ChunkIndex, Arc<ChunkWriter>>>,
    // we may need to wait on the flush queue to flush the data to the remote storage.
    slice_flush_queue: mpsc::Sender<FlushReq>,
    total_slice_cnt: Arc<AtomicUsize>,

    cancel_token: CancellationToken,
    seq_generate: Arc<sonyflake::Sonyflake>,
    // tracking how many operations is using the file writer.
    // 1. when we open, we increase the counter.
    // 2. when we close the file, decrease the counter.
    ref_count: Arc<AtomicUsize>,
    flush_waiting: Arc<AtomicUsize>,
    flush_notify: Arc<Notify>,
    write_waiting: Arc<AtomicUsize>,

    // random write early flush
    // when we reach the threshold, we should flush some flush to the remote storage.
    pattern: WriterPattern,
    // when should we flush the buffer in advance, according to current buffer pool free ratio.
    early_flush_threshold: f64,

    // dependencies
    // the underlying object storage.
    data_manager: Weak<DataManager>,
}

impl FileWriter {
    pub fn get_length(self: &Arc<Self>) -> usize {
        self.length.load(Ordering::Acquire)
    }
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
        while let cnt = self.total_slice_cnt.load(Ordering::Acquire) {
            if cnt < 1000 {
                break;
            }
            yield_now().await;
        }

        // 1. find write location.
        let start = Instant::now();
        debug!("try to find slice writer {:?}", start.elapsed());
        let slice_writers = self
            .find_writable_slice_writer(offset, expected_write_len)
            .in_current_span()
            .await;
        debug!("find slice writer success {:?}", start.elapsed());

        let mut write_len = 0;
        for (sw, l) in slice_writers.iter() {
            let data = &data[l.buf_start_at..l.buf_start_at + l.need_write_len];
            let n = sw
                .write_at(l.chunk_offset - sw.offset_of_chunk, data)
                .await?;
            write_len += n;
            let sw = sw.clone();
            if let Some(req) = sw.make_background_flush_req().await {
                if let Err(e) =
                    tokio::time::timeout(Duration::from_secs(1), self.slice_flush_queue.send(req))
                        .await
                        .expect("send flush req to queue blocked")
                {
                    panic!("failed to send flush request {e}");
                }
            }
        }

        // 4. update the new file length
        let mut old_len = self.length.load(Ordering::Acquire);
        // do compare and exchange
        let may_new_len = offset + write_len;
        if may_new_len > old_len {
            // give up if someone's length is larger.
            loop {
                debug!("try to update file length");
                match self.length.compare_exchange(
                    old_len,
                    may_new_len,
                    Ordering::Release,
                    Ordering::Acquire,
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
    ) -> Vec<(Arc<SliceWriter>, ChunkWriteCtx)> {
        let mut sws = Vec::with_capacity(2);
        for l in locate_chunk(CHUNK_SIZE, offset, expected_write_len) {
            let mut write_guard = self.chunk_writers.write().await;
            let cw = write_guard
                .entry(l.chunk_idx)
                .or_insert(Arc::new(ChunkWriter {
                    inode: self.inode,
                    chunk_index: l.chunk_idx,
                    slice_writers: Default::default(),
                    fw: Arc::downgrade(self),
                    first_write_notify: Arc::new(Default::default()),
                    debug_committer_alive: Arc::new(Default::default()),
                }))
                .clone();
            drop(write_guard);

            let sw = cw
                .find_writable_slice_writer(&l, self.total_slice_cnt.clone())
                .await;
            sws.push((sw, l));
        }
        sws
    }

    #[instrument(skip(self), fields(self.inode))]
    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        self.flush_waiting.fetch_add(1, Ordering::AcqRel);
        defer!(self.flush_waiting.fetch_sub(1, Ordering::AcqRel););

        loop {
            let read_guard = self.chunk_writers.read().await;
            debug!(
                "flush is waiting for chunk writer finish, alive_chunk_writers: {}",
                read_guard.len()
            );
            if read_guard.is_empty() {
                return Ok(());
            }
            for (_, cw) in read_guard.iter() {
                warn!(
                    "wait flush...{} cw.debug_committer_alive: {}",
                    cw.chunk_index,
                    cw.debug_committer_alive.load(Ordering::Acquire)
                );
                cw.flush().await?;
            }

            // wait for the flush finished.
            warn!(
                "waiting for chunk writer send the notify, current flush waiter count: {}",
                self.flush_waiting.load(Ordering::Acquire)
            );
            // self.flush_notify.notified().await;
        }
    }

    /// Close won't really close the file writer, it tries to decrease the ref
    /// count first.
    #[instrument(skip(self), fields(self.inode))]
    pub(crate) async fn close(self: &Arc<Self>) {
        if let Err(e) = self.flush().await {
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

    #[instrument(skip(self), fields(self.inode))]
    async fn free_chunk(self: &Arc<Self>, chunk_index: ChunkIndex) {
        debug!("free chunk_index: {}", chunk_index);
        let mut write_guard = self.chunk_writers.write().await;
        if let Some(cw) = write_guard.remove(&chunk_index) {
            let slice_writers = cw.slice_writers.read().await;
            assert!(slice_writers.is_empty(), "slice writers should be empty");
        }
        if write_guard.is_empty() && self.flush_waiting.load(Ordering::Acquire) > 0 {
            self.flush_notify.notify_waiters();
        }
    }
}

enum FlushReq {
    FlushBulk { sw: Arc<SliceWriter>, offset: usize },
    FlushFull(Arc<SliceWriter>),
}

/// FileWriterFlusher flushes the data to the remote storage periodically.
struct FileWriterFlusher {
    fw: Arc<FileWriter>,
    flush_rx: mpsc::Receiver<FlushReq>,
    flush_count: Arc<AtomicUsize>,
}

impl FileWriterFlusher {
    #[instrument(skip(self), fields(self.fw.inode))]
    async fn run(mut self) {
        debug!("Ino({}) flush task started", self.fw.inode);
        let cancel_token = self.fw.cancel_token.clone();
        let cloned_cancel_token = cancel_token.clone();
        let ino = self.fw.inode.clone();

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
                                sw.debug_flush_queue_has_received.store(true, Ordering::Release);
                                debug!("{ino} flush bulk to {offset}");
                                let fw = self.fw.clone();
                                let cancel_token = cancel_token.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        r = sw.flush_bulk(offset) => {
                                            if let Err(e) = r {
                                                error!("{ino} failed to flush bulk {e}");
                                            }
                                            if let (fl, l) = sw.get_flushed_length_and_total_write_length().await {
                                                debug!("{ino} flush bulk success, flushed_length: {}, length: {}", ReadableSize(fl as u64), ReadableSize(l as u64));
                                                if fl == l && l == CHUNK_SIZE {
                                                    sw.mark_done();
                                                }
                                            }
                                        }
                                        _ = cancel_token.cancelled() => {
                                            return;
                                        }
                                    }
                                });
                            },
                            FlushReq::FlushFull(sw) => {
                                sw.debug_flush_queue_has_received.store(true, Ordering::Release);
                                let (fl, wl ) = sw.get_flushed_length_and_total_write_length().await;
                                debug!("Ino({ino}) flush full [{}] flushed_length {}, length: {}",
                                    sw.slice_id.load(Ordering::Acquire),
                                    ReadableSize(fl as u64),
                                    ReadableSize(wl as u64),
                                );
                                if wl == 0 {
                                    sw.mark_done();
                                    continue;
                                }
                                let fw = self.fw.clone();
                                let cancel_token = cancel_token.clone();
                                let cnt = self.flush_count.clone();
                                cnt.fetch_add(1, Ordering::AcqRel);
                                // FIXME: didn't get scheduled
                                tokio::spawn(async move {
                                    tokio::select! {
                                        r = sw.flush() => {
                                            if let Err(e) = r {
                                                error!("{ino} failed to flush full {e}");
                                            }
                                            sw.mark_done();
                                            cnt.fetch_sub(1, Ordering::AcqRel);
                                        }
                                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                                            panic!("{ino} flush full timeout");
                                        }
                                        _ = cancel_token.cancelled() => {
                                            debug!("{ino} flush full is cancelled");
                                            return;
                                        }
                                    }
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
    inode: Ino,
    chunk_index: ChunkIndex,
    slice_writers: RwLock<BTreeMap<InternalSliceSeq, Arc<SliceWriter>>>,
    fw: Weak<FileWriter>,
    first_write_notify: Arc<Notify>,
    debug_committer_alive: Arc<AtomicBool>,
}

impl ChunkWriter {
    #[instrument(skip(self), fields(chunk_index = self.chunk_index))]
    async fn flush(self: &Arc<Self>) -> Result<()> {
        let fw = self.fw.upgrade().unwrap();
        let read_guard = self.slice_writers.read().await;
        for (_, sw) in read_guard.iter() {
            let req = sw.make_full_flush_in_advance();
            if let Some(req) = req {
                if let Err(e) = fw.slice_flush_queue.send(req).await {
                    panic!("failed to send flush request {e}");
                }
            } else {
                warn!("{:?} no need to make flush req", sw);
            }
        }
        Ok(())
    }
    #[instrument(skip(self), fields(chunk_index = self.chunk_index))]
    async fn find_writable_slice_writer(
        self: &Arc<Self>,
        l: &ChunkWriteCtx,
        total_slice_cnt: Arc<AtomicUsize>,
    ) -> Arc<SliceWriter> {
        let fw = self.fw.upgrade().unwrap();
        {
            let read_guard = self.slice_writers.read().await;
            for (idx, (_, sw)) in read_guard.iter().rev().enumerate() {
                let (flushed, length) = sw.get_flushed_length_and_total_write_length().await;
                debug!(
                    "chunk_index: {} found {} flushed: {}, length: {}",
                    l.chunk_idx,
                    sw,
                    ReadableSize(flushed as u64),
                    ReadableSize(length as u64),
                );
                if !sw.has_frozen() {
                    if sw.offset_of_chunk + flushed <= l.chunk_offset
                        && l.chunk_offset <= sw.offset_of_chunk + length
                    {
                        // we can write to this slice.
                        return sw.clone();
                    }
                }
                // flush some for random write
                // if idx >= 3 && kiseki_storage::get_pool_free_ratio() < fw.early_flush_threshold {
                //     debug!("chunk_index: {} flush some for random write", l.chunk_idx);
                //     if let Some(req) = sw.make_full_flush_in_advance() {
                //         if let Err(e) = fw.slice_flush_queue.send(req) {
                //             panic!("failed to send flush request {e}");
                //         }
                //     }
                // }
            }
        }

        debug!("not found suitable slice writer, create a new one");

        // in random mode or we can't find a suitable slice writer.
        // we need to create a new one.

        let sw = Arc::new(SliceWriter::new(
            l.chunk_idx,
            self,
            fw.seq_generate
                .next_id()
                .expect("should not fail when generate internal seq"),
            l.chunk_offset,
            total_slice_cnt,
            fw.data_manager.clone(),
        ));
        let mut write_guard = self.slice_writers.write().await;
        write_guard.insert(sw._internal_seq, sw.clone());
        if write_guard.len() == 1 && !self.debug_committer_alive.load(Ordering::Acquire) {
            let cw_clone = self.clone();
            tokio::spawn(async move { cw_clone.background_committer().await });
        }
        sw
    }

    #[instrument(skip(self), fields(chunk_index = self.chunk_index))]
    async fn background_committer(self: &Arc<Self>) {
        let fw = self.fw.upgrade().unwrap();
        let dm = fw.data_manager.upgrade().unwrap();
        let meta_engine = dm.meta_engine.clone();
        fw.ref_count.fetch_add(1, Ordering::AcqRel); // add reference
        defer!(fw.ref_count.fetch_sub(1, Ordering::AcqRel);); // remove reference
        self.debug_committer_alive.store(true, Ordering::Release);
        defer!(self.debug_committer_alive.store(false, Ordering::Release));

        // wait for the first write.
        self.first_write_notify.notified().await;
        loop {
            let sw = {
                let read_guard = self.slice_writers.read().await;
                info!(
                    "chunk_index: {} slice writer count: {}",
                    self.chunk_index,
                    read_guard.len()
                );
                match read_guard.first_key_value() {
                    None => {
                        debug!(
                            "chunk_index: {} has no slice writer, exit",
                            self.chunk_index
                        );
                        break;
                    }
                    Some((_, v)) => v.clone(),
                }
            };

            // wait on the slice writer has done.
            let start = Instant::now();
            while !sw.has_done() {
                let (fl, l) = sw.get_flushed_length_and_total_write_length().await;
                warn!(
                    "wait on {:?} done, flushed_len({}), total_len({}), cost: {:?}",
                    sw,
                    fl,
                    l,
                    start.elapsed()
                );
                let notify = sw.done_notify.clone();
                if let Err(e) =
                    tokio::time::timeout(std::time::Duration::from_millis(300), notify.notified())
                        .await
                {
                    if let Some(req) = sw.make_full_flush_in_advance() {
                        if let Err(e) = fw.slice_flush_queue.send(req).await {
                            panic!("failed to send flush request {e}");
                        }
                    }
                }
            }

            // remove the slice writer.
            let mut write_guard = self.slice_writers.write().await;
            if let Some(removed_sw) = write_guard.remove(&sw._internal_seq) {
                debug!(
                    "chunk_index: {} remove slice writer: {}",
                    self.chunk_index, removed_sw
                );
                if !removed_sw.has_done() {
                    panic!("{} has not done, should not happen!", sw);
                }
                removed_sw.total_slice_cnt.fetch_sub(1, Ordering::AcqRel);
            } else {
                panic!("{} has been removed, should not happen!", sw);
            }
            drop(write_guard);

            // write slice meta info to meta engine.
            let slice_id = sw.slice_id.load(Ordering::Acquire);
            let len = sw.slice_buffer.read().await.length();
            // then the sw is done.
            // write the meta info of this slice.
            if let Err(e) = meta_engine
                .write_slice(
                    self.inode,
                    self.chunk_index,
                    sw.offset_of_chunk,
                    kiseki_types::slice::Slice::Owned {
                        chunk_pos: sw.offset_of_chunk as u32,
                        id: slice_id,
                        size: len as u32,
                        _padding: 0,
                    },
                    sw.last_modified.load(),
                )
                .await
            {
                panic!("failed to write slice meta info {e}");
            }
        }

        // remove the chunk writer.
        fw.free_chunk(self.chunk_index).await;
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        debug!(
            "{} chunk_index: {} is dropped",
            self.inode, self.chunk_index
        );
        if let Some(fw) = self.fw.upgrade() {
            // fw.ref_count.fetch_sub(1, Ordering::AcqRel);
            if fw.flush_waiting.load(Ordering::Acquire) > 0 {
                fw.flush_notify.notify_waiters();
            }
        }
    }
}

/// SliceWriter is the entry point for writing data to a slice,
/// it depends on a SliceBuffer to buffer the write request.
/// SliceWriter can only grow from left to right, we can update it,
/// but cannot change the start offset.
struct SliceWriter {
    chunk_index: ChunkIndex,
    chunk_writer: Weak<ChunkWriter>,
    // the internal seq of the slice writer,
    // used to identify the slice writer,
    // we have it since we delay the slice-id assignment until we
    // really do the flush.
    _internal_seq: u64,
    // the slice id of the slice, assigned by meta engine.
    slice_id: AtomicU64,
    // where the slice start at of the chunk
    offset_of_chunk: usize,
    // the buffer to serve the write request.
    slice_buffer: RwLock<SliceBuffer>,
    // 1. FlushFull will set this to true
    // 2. Manual flush will set this to true
    frozen: AtomicBool,
    // 1. FlushFull will set this to true.
    // 2. Manual flush will set this to true.
    done: AtomicBool,
    done_notify: Arc<Notify>,
    last_modified: AtomicCell<Instant>,
    debug_in_flushing: Arc<AtomicBool>,
    debug_flush_queue_has_received: Arc<AtomicBool>,
    debug_has_become_req: Arc<AtomicBool>,
    total_slice_cnt: Arc<AtomicUsize>,
    in_writing: Arc<AtomicBool>,

    // dependencies
    // the underlying object storage.
    data_manager: Weak<DataManager>,
}

impl Debug for SliceWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SliceWriter[{}],\
             slice_id({}), \
             chunk_offset({}), \
             done({}), \
            in_flush({}),
            flush_queue_has_received({}),\
            has_become_req({}),\
            frozen({})",
            self._internal_seq,
            self.slice_id.load(Ordering::Acquire),
            self.offset_of_chunk,
            self.done.load(Ordering::Acquire),
            self.debug_in_flushing.load(Ordering::Acquire),
            self.debug_flush_queue_has_received.load(Ordering::Acquire),
            self.debug_has_become_req.load(Ordering::Acquire),
            self.frozen.load(Ordering::Acquire),
        )
    }
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
        seq: u64,
        offset_of_chunk: usize,
        total_slice_cnt: Arc<AtomicUsize>,
        data_manager: Weak<DataManager>,
    ) -> SliceWriter {
        total_slice_cnt.fetch_add(1, Ordering::AcqRel);
        Self {
            chunk_index,
            chunk_writer: Arc::downgrade(cw),
            _internal_seq: seq,
            slice_id: AtomicU64::new(EMPTY_SLICE_ID),
            offset_of_chunk,
            slice_buffer: RwLock::new(SliceBuffer::new()),
            frozen: AtomicBool::new(false),
            done: AtomicBool::new(false),
            done_notify: Arc::new(Default::default()),
            last_modified: AtomicCell::new(Instant::now()),
            debug_in_flushing: Arc::new(Default::default()),
            debug_flush_queue_has_received: Arc::new(Default::default()),
            debug_has_become_req: Arc::new(Default::default()),
            total_slice_cnt: total_slice_cnt.clone(),
            in_writing: Arc::new(Default::default()),
            data_manager,
        }
    }

    async fn write_at(self: &Arc<Self>, offset: usize, data: &[u8]) -> Result<usize> {
        self.in_writing.store(true, Ordering::Release);
        defer!(self.in_writing.store(false, Ordering::Release));

        let mut write_guard = self.slice_buffer.write().await;
        let written = write_guard.write_at(offset, data).await?;
        let cw = self
            .chunk_writer
            .upgrade()
            .expect("chunk writer should not be dropped");
        cw.first_write_notify.notify_one();
        self.last_modified.store(Instant::now());
        Ok(written)
    }

    async fn flush_bulk(self: &Arc<Self>, offset: usize) -> Result<()> {
        self.debug_in_flushing.store(true, Ordering::Release);

        self.prepare_slice_id().await?;

        let mut write_guard = tokio::time::timeout(
            Duration::from_secs(1),
            self.slice_buffer.write(),
        )
        .await
        .expect(
            "failed to get write guard of slice buffer, since we have timeout, we should not block",
        );

        tokio::time::timeout(
            Duration::from_secs(1),
            write_guard.flush_bulk_to(
                offset,
                |bi, bs| -> String {
                    make_slice_object_key(self.slice_id.load(Ordering::Acquire), bi, bs)
                },
                self.data_manager.upgrade().unwrap().object_storage.clone(),
            ),
        )
        .await
        .expect("flush bulk to object storage blocked")?;

        // .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(_internal_seq = self._internal_seq))]
    async fn flush(self: &Arc<Self>) -> Result<()> {
        self.debug_in_flushing.store(true, Ordering::Release);

        debug!("try to flush slice writer {}", self._internal_seq);
        self.prepare_slice_id().await?;
        debug!(
            "prepare slice id success {}",
            self.slice_id.load(Ordering::Acquire)
        );
        let mut write_guard = self.slice_buffer.write().await;
        debug!("start to flush slice buffer {}", self._internal_seq);
        write_guard
            .flush(
                |bi, bs| -> String {
                    make_slice_object_key(self.slice_id.load(Ordering::Acquire), bi, bs)
                },
                self.data_manager.upgrade().unwrap().object_storage.clone(),
            )
            .in_current_span()
            .await?;
        debug!(
            "SliceWriter flush slice buffer success slice_id: {}",
            self.slice_id.load(Ordering::Acquire),
        );
        Ok(())
    }

    async fn prepare_slice_id(self: &Arc<Self>) -> Result<()> {
        let old = self.slice_id.load(Ordering::Acquire);
        if old != EMPTY_SLICE_ID {
            return Ok(());
        }

        let data_manager = self
            .data_manager
            .upgrade()
            .expect("data manager should not be dropped");
        let slice_id = data_manager.meta_engine.next_slice_id().await?;
        // don't care about the result, since we only care about the first time.
        let _ = self
            .slice_id
            .compare_exchange(0, slice_id, Ordering::AcqRel, Ordering::Acquire);
        Ok(())
    }

    // try_make_background_flush_req try to make flush req if we write enough data.
    async fn make_background_flush_req(self: Arc<Self>) -> Option<FlushReq> {
        if self.in_writing.load(Ordering::Acquire) {
            return None;
        }
        if self.has_frozen() {
            return None;
        }
        let read_guard = self.slice_buffer.read().await;
        let length = read_guard.length();
        if length == CHUNK_SIZE {
            if !self.freeze() {
                // someone else has frozen it.
                return None;
            }
            self.debug_has_become_req.store(true, Ordering::Release);
            return Some(FlushReq::FlushFull(self.clone()));
        } else if length - read_guard.flushed_length() > BLOCK_SIZE {
            self.debug_has_become_req.store(true, Ordering::Release);
            return Some(FlushReq::FlushBulk {
                sw: self.clone(),
                offset: length,
            });
        }
        None
    }

    fn make_full_flush_in_advance(self: &Arc<Self>) -> Option<FlushReq> {
        if self.in_writing.load(Ordering::Acquire) {
            return None;
        }
        if self.frozen.load(Ordering::Acquire) {
            return None;
        }
        if !self.freeze() {
            return None;
        }
        self.debug_has_become_req.store(true, Ordering::Release);
        Some(FlushReq::FlushFull(self.clone()))
    }

    // Freeze the slice writer, so the waiting write operations will be dropped,
    // and new write operations will create new slice writer.
    fn freeze(self: &Arc<Self>) -> bool {
        self.frozen
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    fn has_frozen(self: &Arc<Self>) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    // mark self as done, then we will remove the ref from the map.
    fn mark_done(self: &Arc<Self>) {
        self.done.store(true, Ordering::Release);
        self.done_notify.notify_waiters();
    }

    fn has_done(self: &Arc<Self>) -> bool {
        self.done.load(Ordering::Acquire)
    }

    // get the underlying write buffer's released length and total write length.
    async fn get_flushed_length_and_total_write_length(self: &Arc<Self>) -> (usize, usize) {
        let guard = self.slice_buffer.read().await;
        let flushed_len = guard.flushed_length();
        let write_len = guard.length();
        (flushed_len, write_len)
    }
}

impl Eq for SliceWriter {}

impl PartialEq for SliceWriter {
    fn eq(&self, other: &Self) -> bool {
        self._internal_seq == other._internal_seq
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct ChunkWriteCtx {
    file_offset: usize,
    // write to which chunk
    chunk_idx: usize,
    // the start offset of this write
    chunk_offset: usize,
    // the length of this write
    need_write_len: usize,
    // the start offset of the input buf
    buf_start_at: usize,
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
            // debug!(
            //     "chunk-size: {}, chunk: {} chunk_pos: {}, left: {}, buf start at: {}, max
            // can write: {}",     self.chunk_size, idx, chunk_pos, left,
            // buf_start_at, max_can_write, );

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
