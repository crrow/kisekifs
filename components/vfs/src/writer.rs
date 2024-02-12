use std::{
    cmp::min,
    collections::HashMap,
    fmt::{Display, Formatter},
    io::Cursor,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::{mapref::one::Ref, DashMap};
use kiseki_storage::slice_buffer::SliceBuffer;
use kiseki_types::slice::make_slice_object_key;
use kiseki_types::{
    cal_chunk_idx, cal_chunk_offset,
    ino::Ino,
    slice::{SliceID, EMPTY_SLICE_ID},
    ChunkIndex, FileOffset, ObjectStorage, BLOCK_SIZE, CHUNK_SIZE,
};
use kiseki_utils::readable_size::ReadableSize;
use rangemap::RangeMap;
use snafu::ResultExt;
use tokio::{
    sync::{mpsc, oneshot, Mutex, Notify, OnceCell, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::err::{JoinErrSnafu, Result, StorageErrSnafu};

/// FileWriter is the entry point for writing data to a file.
pub struct FileWriter {
    pub inode: Ino,

    /*runtime*/
    // the length of current file.
    length: AtomicUsize,
    // buffer writers.
    // chunk_idx -> RangeMap
    //              [unflushed_offset(to file), length)-> SliceWriter
    //
    // We rely on the RangeMap to find the right SliceWriter for the write request.
    // and the RangeMap will help us drop some overlapped SliceWriter automatically.
    //
    // TODO: Optimization:
    // the new slice will overlap the old one, so when we flush,
    // we can only flush the necessary part.
    slice_writers: DashMap<ChunkIndex, RangeMap<FileOffset, Arc<SliceWriter>>>,
    // we may need to wait on the flush queue to flush the data to the remote storage.
    slice_flush_queue: mpsc::Sender<FlushReq>,
    // to tell the background don't send the buffer back to the map.
    manually_flush: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    seq_generate: sonyflake::Sonyflake,

    /* dependencies */
    // the underlying object storage.
    object_storage: ObjectStorage,
}

impl FileWriter {
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
    #[instrument(skip_all, fields(self.inode, offset, write_len = data.len()))]
    pub async fn write(self: &Arc<Self>, offset: usize, data: &[u8]) -> Result<usize> {
        let expected_write_len = data.len();
        if expected_write_len == 0 {
            return Ok(0);
        }

        let data_len = data.len();
        let data_ptr = data.as_ptr();

        // 1. find write location.
        let slice_writers = self.find_slice_writer(offset, expected_write_len).await;
        let handles = slice_writers
            .iter()
            .map(|(sw, l)| {
                let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
                let data = &data[l.buf_start_at..l.buf_start_at + l.need_write_len];
                let sw = sw.clone();
                let l = l.clone();
                // 2. write
                let handle = tokio::spawn(async move {
                    // if we are in writing, but then someone call flush,
                    // then this write will be frozen, we should cancel this write.
                    tokio::select! {
                        _ = sw.freeze_notify.notified() => {
                            warn!("{} write is frozen", sw);
                            return Ok(0);
                        }
                        r = sw.write_at(l.chunk_offset - sw.offset_of_chunk, data) => r,
                    }
                });
                handle
            })
            .collect::<Vec<_>>();

        // 2. wait on write finished and calculate write len.
        let mut write_len = 0;
        for r in futures::future::join_all(handles).await {
            let wl = r.context(JoinErrSnafu)??;
            debug!("{} write {}", self.inode, ReadableSize(wl as u64));
            write_len += wl;
        }
        // update the total buffered length.
        // self.buffered_length.fetch_add(write_len, Ordering::AcqRel);

        // 3. make flush request if we can
        for (sw, l) in slice_writers.into_iter() {
            if let Some(req) = sw.make_background_flush_req().await {
                if let Err(e) = self.slice_flush_queue.send(req).await {
                    error!("failed to send flush request {e}");
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

    async fn find_slice_writer(
        self: &Arc<Self>,
        offset: usize,
        expected_write_len: usize,
    ) -> Vec<(Arc<SliceWriter>, ChunkWriteCtx)> {
        let mut sws = Vec::with_capacity(2);
        for l in locate_chunk(CHUNK_SIZE, offset, expected_write_len) {
            let range = l.file_offset..l.file_offset + l.need_write_len;
            let sw = match self.slice_writers.get_mut(&l.chunk_idx) {
                None => {
                    let mut cw = RangeMap::new();
                    let sw = Arc::new(SliceWriter::new(
                        self.seq_generate
                            .next_id()
                            .expect("should not fail when generate internal seq"),
                        l.chunk_offset,
                        self.object_storage.clone(),
                    ));
                    cw.insert(range, sw.clone());
                    self.slice_writers.insert(l.chunk_idx, cw);
                    sw
                }
                Some(mut chunk_writer) => {
                    let off = if l.chunk_offset == 0 {
                        0
                    } else {
                        l.chunk_offset - 1
                    };
                    // Attempt to retrieve an existing writer, handling both cases:
                    match chunk_writer.get(&off) {
                        Some(sw) if !sw.frozen.load(Ordering::Acquire) => sw.clone(), /* Use existing writer if not frozen */
                        _ => {
                            // Otherwise, create a new writer and insert it:
                            let seq = self
                                .seq_generate
                                .next_id()
                                .expect("should not fail when generate internal seq");
                            let sw = Arc::new(SliceWriter::new(
                                seq,
                                l.chunk_offset,
                                self.object_storage.clone(),
                            ));
                            chunk_writer.insert(range, sw.clone());
                            sw
                        }
                    }
                }
            };
            sws.push((sw, l));
        }
        sws
    }

    fn remove_done_slice_writer(self: &Arc<Self>) {
        let mut to_remove = Vec::new();
        for mut cw in self.slice_writers.iter_mut() {
            let chunk_idx = cw.key().clone();
            // show mut to make sure we get the right range as key.
            let mut cw = cw.value_mut();
            let keys = cw
                .iter()
                .filter_map(|(range, sw)| {
                    if sw.has_done() {
                        Some(range.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            for k in keys {
                cw.remove(k);
            }
            if cw.is_empty() {
                to_remove.push(chunk_idx);
            }
        }
        for chunk_idx in to_remove {
            self.slice_writers.remove(&chunk_idx);
        }
    }

    /// flush all buffered data.
    /// 1. mark manually flush as true, to tell the background don't send the
    ///    buffer back to the map.
    /// 2. move all slice writers to flush queue
    /// 3. wait for all slice writers to be flushed
    /// 4. mark manually flush as false
    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        // check if someone is flushing, or if we actually need to flush?
        if self.manually_flush.load(Ordering::Acquire) || self.slice_writers.is_empty() {
            return Ok(()); // nothing to flush
        }

        if self
            .manually_flush
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // someone else wins the flush contention.
            return Ok(());
        }

        // win this contention, we are the one who flush.
        // mut the map, and move all slice writers to flush queue.
        let chunk_writers = self
            .slice_writers
            .iter_mut()
            .map(|mut e| {
                // now no one can use the original chunk_idx every again.
                // its ok we take the value out, since the background flusher won't put buffer
                // back in we are in manually flushing.
                let cw = e.value_mut();
                std::mem::replace(cw, RangeMap::default())
            })
            .collect::<Vec<_>>();

        // now new write requests will create new slice writer.

        let mut sws = Vec::with_capacity(chunk_writers.len());
        for cw in chunk_writers {
            // TODO: optimize, we can even do truncate on the slice buffer.
            for (_, sw) in cw {
                if sw.can_flush() {
                    sws.push(sw);
                }
            }
        }
        if !sws.is_empty() {
            let (tx, rx) = oneshot::channel();
            let req = FlushReq::ManualFlush { sws, finished: tx };
            if let Err(e) = self.slice_flush_queue.send(req).await {
                panic!(
                    "failed to send manual flush request {e}, and since we take the buffer out, we can't put it back"
                );
            }
            match rx.await {
                Ok(_) => info!("{} manual flush finished", self.inode),
                Err(e) => println!("the sender dropped {}", e),
            }
        }
        // release the manually flush lock.
        self.manually_flush.store(false, Ordering::Release);
        Ok(())
    }
}

enum FlushReq {
    FlushBulk {
        sw: Arc<SliceWriter>,
        offset: usize,
    },
    FlushFull(Arc<SliceWriter>),
    ManualFlush {
        sws: Vec<Arc<SliceWriter>>,
        finished: oneshot::Sender<()>,
    },
}

/// FileWriterFlusher flushes the data to the remote storage periodically.
struct FileWriterFlusher {
    fw: Arc<FileWriter>,
    rx: mpsc::Receiver<FlushReq>,
}

impl FileWriterFlusher {
    async fn run(mut self) {
        let cancel_token = self.fw.cancel_token.clone();
        let ino = self.fw.inode.clone();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("{ino} flush task is cancelled");
                    return;
                }
                req = self.rx.recv() => {
                    if let Some(req) = req {
                        match req {
                            FlushReq::FlushBulk {sw, offset} => {
                                tokio::spawn(async move {
                                    let r = sw.flush_bulk(offset).await;
                                    if let Err(e) = r {
                                        error!("{ino} failed to flush bulk {e}");
                                    }
                                });
                            },
                            FlushReq::FlushFull(sw) => {
                                tokio::spawn(async move {
                                    let r = sw.flush().await;
                                    if let Err(e) = r {
                                        error!("{ino} failed to flush full {e}");
                                    }
                                    sw.mark_done();
                                });
                            },
                            FlushReq::ManualFlush{sws, finished} => {
                                let handles  = sws.into_iter().map(|sw | {
                                    tokio::spawn(async move {
                                        let r = sw.flush().await;
                                        if let Err(e) = r {
                                            error!("{ino} failed to flush manually {e}");
                                        }
                                        // FIXME: we don't have to mark done, since all the slice writers
                                        // has been moved out from the map.
                                        sw.mark_done();
                                    })
                                }).collect::<Vec<_>>();
                                tokio::select! {
                                    // wait on all flush finished
                                    _ = futures::future::join_all(handles) => {
                                        let _ = finished.send(());
                                    },
                                    // or we are cancelled.
                                    _ = cancel_token.cancelled() => {
                                        debug!("{ino} flush task is cancelled");
                                        let _ = finished.send(());
                                        return;
                                    }
                                }
                            },
                        }
                        // clean the map
                        self.fw.remove_done_slice_writer();
                    }
                }
            }
        }
    }
}

/// SliceWriter is the entry point for writing data to a slice,
/// it depends on a SliceBuffer to buffer the write request.
/// SliceWriter can only grow from left to right, we can update it,
/// but cannot change the start offset.
struct SliceWriter {
    // the internal seq of the slice writer,
    // used to identify the slice writer,
    // we have it since we delay the slice-id assignment until we
    // really do the flush.
    _internal_seq: u64,
    // the slice id of the slice, assigned by meta engine.
    slice_id: OnceCell<SliceID>,
    // where the slice start at of the chunk
    offset_of_chunk: usize,
    // the buffer to serve the write request.
    slice_buffer: RwLock<SliceBuffer>,
    // 1. FlushFull will set this to true
    // 2. Manual flush will set this to true
    frozen: AtomicBool,
    // notify the write that someone has frozen it.
    // we drop the operation.
    freeze_notify: Notify,
    // 1. FlushFull will set this to true.
    // 2. Manual flush will set this to true.
    done: AtomicBool,

    /* dependencies */
    // the underlying object storage.
    object_storage: ObjectStorage,
}

impl Display for SliceWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SliceWriter[{}]", self._internal_seq)
    }
}

impl SliceWriter {
    fn new(seq: u64, offset_of_chunk: usize, object_storage: ObjectStorage) -> SliceWriter {
        Self {
            _internal_seq: seq,
            slice_id: OnceCell::const_new_with(EMPTY_SLICE_ID),
            offset_of_chunk,
            slice_buffer: RwLock::new(SliceBuffer::new()),
            frozen: AtomicBool::new(false),
            freeze_notify: Default::default(),
            done: AtomicBool::new(false),
            object_storage,
        }
    }

    async fn write_at(self: &Arc<Self>, offset: usize, data: &[u8]) -> Result<usize> {
        let mut write_guard = self.slice_buffer.write().await;
        let written = write_guard
            .write_at(offset, data)
            .await
            .context(StorageErrSnafu)?;
        Ok(written)
    }

    async fn flush_bulk(self: &Arc<Self>, offset: usize) -> Result<()> {
        let mut write_guard = self.slice_buffer.write().await;
        // TODO: implement it
        // write_guard.flush_bulk_to(offset, | bi, bs | -> String {
        //     make_slice_object_key(self.slice_id.get_or_init(), bi, bs)
        // }, self.object_storage.clone()).await?;
        Ok(())
    }

    async fn flush(self: &Arc<Self>) -> Result<()> {
        todo!()
    }

    // try_make_background_flush_req try to make flush req if we write enough data.
    async fn make_background_flush_req(self: Arc<Self>) -> Option<FlushReq> {
        if self.has_frozen() {
            return None;
        }
        if self.has_done() {
            return None;
        }
        let read_guard = self.slice_buffer.read().await;
        let length = read_guard.len();
        if length == CHUNK_SIZE {
            if !self.freeze() {
                // someone else has frozen it.
                return None;
            }
            return Some(FlushReq::FlushBulk {
                sw: self.clone(),
                offset: length,
            });
        } else if length - read_guard.flushed_len() > BLOCK_SIZE {
            return Some(FlushReq::FlushFull(self.clone()));
        }
        None
    }

    // check if we can flush the sliceWriter, it is used in manual flush.
    fn can_flush(self: &Arc<Self>) -> bool {
        if self.has_frozen() || self.has_done() {
            // this two state may already be set by background full flush.
            // the background job will handle it.
            return false;
        }

        // try to freeze it, if we fail means someone else has frozen it,
        self.freeze()
    }

    // Freeze the slice writer, so the waiting write operations will be dropped,
    // and new write operations will create new slice writer.
    fn freeze(self: &Arc<Self>) -> bool {
        let r = self
            .frozen
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok();
        if r {
            // notify the waiters that we have frozen it,
            // so they can drop the operation.
            self.freeze_notify.notify_waiters();
        }
        r
    }

    fn has_frozen(self: &Arc<Self>) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    // mark self as done, then we will remove the ref from the map.
    fn mark_done(self: &Arc<Self>) {
        self.done.store(true, Ordering::Release)
    }

    fn has_done(self: &Arc<Self>) -> bool {
        self.done.load(Ordering::Acquire)
    }

    fn can_write(self: &Arc<Self>) -> bool {
        !self.frozen.load(Ordering::Acquire) && !self.done.load(Ordering::Acquire)
    }
}

impl Eq for SliceWriter {}

impl PartialEq for SliceWriter {
    fn eq(&self, other: &Self) -> bool {
        self._internal_seq == other._internal_seq
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
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
