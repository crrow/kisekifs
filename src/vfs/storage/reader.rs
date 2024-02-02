use std::collections::{BTreeMap, HashMap};
use std::{
    cmp::min,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use crate::common::runtime;
use dashmap::DashMap;
use rangemap::{RangeMap, RangeSet};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, Subscriber};
use vfs::err::{Result, ThisFileReaderIsClosingSnafu};

use crate::common::err::ToErrno;
use crate::meta::engine::MetaEngine;
use crate::meta::types::Slice;
use crate::vfs::storage::buffer;
use crate::vfs::storage::buffer::ReadBuffer;
use crate::{
    meta::types::Ino,
    vfs,
    vfs::{
        storage::{Engine, EngineConfig},
        FH,
    },
};

impl Engine {
    /// Get the file reader for the given inode and file handle.
    pub(crate) fn open_file_reader(
        self: &Arc<Self>,
        inode: Ino,
        fh: FH,
        length: usize,
    ) -> Arc<FileReader> {
        self.file_readers
            .entry((inode, fh))
            .or_insert_with(|| {
                let fr = FileReader::new(self.clone(), inode, fh, length);
                Arc::new(fr)
            })
            .value()
            .clone()
    }

    pub(crate) fn find_file_reader(
        self: &Arc<Self>,
        inode: Ino,
        fh: FH,
    ) -> Option<Arc<FileReader>> {
        self.file_readers
            .get(&(inode, fh))
            .and_then(|m| Some(m.value().clone()))
    }

    pub(crate) fn truncate_reader(self: &Arc<Self>, inode: Ino, length: usize) {
        debug!("DO NOTHING: truncate inode {} to {}", inode, length);
    }
}

/// Each handle to the file reader.
pub(crate) type FileReadersRef = Arc<DashMap<(Ino, FH), Arc<FileReader>>>;

/// [FileReader] is responsible for reading the file content.
/// Each inode and handle will have a unique file reader, even though
/// we may have concurrent read requests for the same [FileReader].
///
/// This [FileReader] will be closed when the Handle notify the data engine.
pub(crate) struct FileReader {
    inode: Ino,
    fh: FH,
    engine: Weak<Engine>,
    config: Arc<EngineConfig>,
    length: AtomicUsize,
    slice_readers: SliceReadersRef,
    slice_readers_background_tasks: SliceReaderBackgroundTasksRef,
    // when we clean this file reader, we will set this flag to true.
    closing: Arc<AtomicBool>,
    // tracking how many concurrent read requests are there.
    read_count: Arc<AtomicUsize>,
    // when we clean this file reader, we should wait for the read_count to be zero.
    read_count_notify: Arc<Notify>,
    read_buffer_usage: Arc<AtomicUsize>,
    seq_generator: sonyflake::Sonyflake,
}

impl FileReader {
    pub(crate) fn new(engine: Arc<Engine>, ino: Ino, fh: FH, length: usize) -> Self {
        Self {
            inode: ino,
            fh,
            engine: Arc::downgrade(&engine),
            config: engine.config.clone(),
            length: AtomicUsize::new(length),
            slice_readers: Arc::new(RwLock::new(RangeMap::new())),
            slice_readers_background_tasks: Arc::new(Default::default()),
            closing: Arc::new(AtomicBool::new(false)),
            read_count: Arc::new(AtomicUsize::new(0)),
            read_count_notify: Arc::new(Notify::new()),
            read_buffer_usage: Arc::new(AtomicUsize::new(0)),
            seq_generator: sonyflake::Sonyflake::new().unwrap(),
        }
    }
    pub(crate) async fn read(self: &Arc<Self>, offset: usize, dst: &mut [u8]) -> Result<usize> {
        ensure!(
            self.closing.load(Ordering::Acquire),
            ThisFileReaderIsClosingSnafu {
                ino: self.inode,
                fh: self.fh,
            }
        );

        let expected_read_len = dst.len();
        // according to the current self length to do the following job.
        let flen = self.length.load(Ordering::Acquire);
        if offset >= flen || expected_read_len == 0 {
            return Ok(0);
        }

        let expected_range_by_filesize = cal_real_read_range(offset, expected_read_len, flen);
        // maximum read ahead size.
        let max_read_ahead_size = (32 << 10) as usize;
        // check if we need to read head ?
        if expected_range_by_filesize.start + max_read_ahead_size > flen {
            // currently read range exceeds the range of current file reader.
            let read_ahead_range = if flen < max_read_ahead_size {
                // just read the whole file.
                0..flen
            } else {
                // read the last block in the file.
                flen - max_read_ahead_size..flen
            };
            // we have some read ahead to do.
            self.read_ahead(read_ahead_range);
        }
        let real_read_size = expected_range_by_filesize.end - expected_range_by_filesize.start;
        let mut dst = &mut dst[..real_read_size];

        let expected_range_by_chunksize = (expected_range_by_filesize.start % self.config.chunk_size
            ..expected_range_by_filesize.end % self.config.chunk_size);
        let engine = self.engine.upgrade().expect("engine should not be dropped");
        let meta_engine = engine.meta_engine.clone();

        // get all slices in the chunk.
        let slices_in_chunk = meta_engine
            .read_slice(self.inode, offset / engine.config.chunk_size)
            .await?;

        // let mut rmap = rangemap::RangeMap::new();
        // for sr in slices_in_chunk.iter() {
        //     rmap.insert(sr.off..sr.off + sr.len, sr.clone());
        // }

        // let mut slice_map = HashMap::new();
        // for sv in slices_in_chunk.iter() {
        //     let rb = slice_map
        //         .entry(sv.id)
        //         .or_insert_with(|| engine.new_read_buffer(sv.id as usize, sv.size as usize));
        //
        //     rb.read_at(offset + sv.off, &mut dst[0..sv.size as usize])?;
        //
        //     // rb.read_at(offset + sv.off, &mut dst[0..sv.size as usize])?;
        // }

        Ok(0)
    }

    pub(crate) fn read_ahead(self: &Arc<Self>, read_range: Range<usize>) {
        debug!("implement the read ahead");
    }

    async fn make_requests(self: &Arc<Self>, flen: usize, read_range: Range<usize>) -> Vec<Req> {
        let mut reqs = vec![];
        let mut srs = self.slice_readers.write().await;
        let mut rs = rangemap::RangeSet::new();
        srs.gaps(&read_range).for_each(|r| {
            // current read buffer doesn't have the data we expected, build request.
            rs.insert(r);
        });
        srs.overlapping(&read_range).for_each(|(valid_range, sr)| {
            // cut the unnecessary part
            let real = if read_range.start < valid_range.start {
                // current:     [----]
                // slice-reader:  [----]
                valid_range.start..min(read_range.end, valid_range.end) // only get the overlapping part.
            } else {
                // current:         [----]
                // slice-reader:  [----]
                read_range.start..min(read_range.end, valid_range.end)
            };
            rs.insert(real.clone());
            println!(
                "want: {:?}, overlapping but invalid range {:?}, cut result: {:?}",
                read_range.clone(),
                valid_range,
                real.clone(),
            );
        });
        // rs.into_iter().for_each(|r| {
        //     let sr = self.new_slice_reader(flen, &mut );
        //     (&mut srs).insert(sr.range.clone(), sr.clone());
        //     reqs.push(Req {
        //         read_range: r,
        //         slice_reader: self.new_slice_reader(),
        //     });
        // });

        reqs
    }

    fn new_slice_reader(self: &Arc<Self>, flen: usize, r: &mut Range<usize>) -> Arc<SliceReader> {
        // random read
        let mut block = r.clone();
        block.end = min(
            block.end,
            min(
                flen,
                (r.start / self.config.block_size + 1) * self.config.block_size,
            ),
        );
        let block_len = block.end - block.start;
        r.start = block.end;
        r.end = r.end - block_len;
        let sr = Arc::new(SliceReader {
            internal_seq: self.seq_generator.next_id().unwrap(),
            chunk_idx: r.start / self.config.chunk_size,
            range: block,
            state: Arc::new(AtomicU8::new(SliceReaderState::IDLE as u8)),
            last_access: AtomicUsize::new(std::time::Instant::now().elapsed().as_secs() as usize),
            read_buf: RwLock::new(vec![0u8; block_len]), // FIXME: we allocate memory here.
            closing: self.closing.clone(),
        });
        self.read_buffer_usage
            .fetch_add(block_len, Ordering::AcqRel);

        let mut srbk = SliceReaderBackgroundTask {
            file_reader: self.clone(),
            slice_reader: sr.clone(),
            state: sr.state.clone(),
            try_cnt: 0,
            max_retry: 60, // FIXME: configuration it
            do_retry: false,
            ready_notify: Arc::new(Default::default()),
        };
        let handle = runtime::spawn(async move {
            srbk.run().await;
        });
        self.slice_readers_background_tasks
            .insert(sr.internal_seq, handle);
        sr
    }

    async fn wait_io(self: &Arc<Self>, reqs: Vec<Req>, dst: &mut [u8]) -> Result<usize> {
        todo!()
    }

    fn delete_slice_reader(self: &Arc<Self>, sr: Arc<SliceReader>) {
        todo!()
    }
}

fn read_slice(
    engine: &Arc<Engine>,
    slice_view: &Slice,
    dst: &mut [u8],
    offset: usize,
) -> Result<usize> {
    // let expected_read_len = 0;
    // let mut read = 0;
    //
    // let read_buffer = engine.new_read_buffer(slice_view.id as usize, slice_view.size as usize);
    // while read < expected_read_len {
    //     let mut p = &mut dst[read..expected_read_len - read];
    //     let n = read_buffer.read_at(read + offset + slice_view.off, &mut p)?;
    //     read += n;
    // }

    // Ok(read)
    todo!()
}

fn cal_real_read_range(offset: usize, expected_read_len: usize, len: usize) -> Range<usize> {
    let right = offset + expected_read_len;
    if right > len {
        (offset..len)
    } else {
        (offset..right)
    }
}

struct Req {
    // real read range
    read_range: Range<usize>,
    slice_reader: Arc<SliceReader>,
}

/// SliceReadersRef maintains the mapping between the read range to the slice reader.
///
/// Example:
/// 1. we read (0, 1024) in the file, and now we cache the SliceReader for this range.
/// 2. next read (512, 1024), we can know that
///
type SliceReadersRef = Arc<RwLock<RangeMap<usize, Arc<SliceReader>>>>;
type SliceReaderBackgroundTasksRef = Arc<DashMap<u64, JoinHandle<()>>>;
type SliceReadersMutMap<'a> = &'a mut RangeMap<usize, Arc<SliceReader>>;

// state of sliceReader
//
//    <-- REFRESH
//   |      |
//  NEW -> BUSY  -> READY
//          |         |
//        BREAK ---> INVALID
#[repr(C)]
enum SliceReaderState {
    /// The basic state of a slice reader, means the FSM does nothing.
    /// It can transferred to [SliceReaderState::BUSY], or [SliceReaderState::REFRESH].
    IDLE,
    /// [SliceReaderState::BUSY] represents we are in reading data process.
    BUSY,
    /// [SliceReaderState::REFRESH] only works when we need to interrupt the read operation,
    /// to avoid old data to be read.
    REFRESH,
    /// [SliceReaderState::BREAK] used for drop the process of FSM.
    /// Each state should check if someone changed it, when we in the break, we should go to the invalid state.
    BREAK,
    /// [SliceReaderState::READY] means we actually have the data to read, and we need to notify the waiters.
    /// When no waiters, we will mark this state to [SliceReaderState::INVALID] to clean up memory.
    /// When we have waiters, we will mark this state to [SliceReaderState::IDLE] to wait for the next retry.
    READY,
    /// We need to clean up the associated resources when there is no waiters.
    INVALID,
}
impl SliceReaderState {
    fn valid(&self) -> bool {
        !matches!(self, SliceReaderState::BREAK | SliceReaderState::INVALID)
    }
}

impl From<u8> for SliceReaderState {
    fn from(v: u8) -> Self {
        match v {
            0 => SliceReaderState::IDLE,
            1 => SliceReaderState::BUSY,
            2 => SliceReaderState::REFRESH,
            3 => SliceReaderState::BREAK,
            4 => SliceReaderState::READY,
            5 => SliceReaderState::INVALID,
            _ => SliceReaderState::INVALID,
        }
    }
}

/// SliceReader is used for managing the reading status,
/// each SliceReader is bind to a real Slice.
struct SliceReader {
    internal_seq: u64,
    chunk_idx: usize,
    range: Range<usize>,
    state: Arc<AtomicU8>,
    last_access: AtomicUsize,
    read_buf: RwLock<Vec<u8>>,
    closing: Arc<AtomicBool>,
}

impl PartialEq<Self> for SliceReader {
    fn eq(&self, other: &Self) -> bool {
        self.internal_seq == other.internal_seq && self.range == other.range
    }
}

impl Eq for SliceReader {}

impl SliceReader {
    fn valid(&self) -> bool {
        SliceReaderState::from(self.state.load(Ordering::Acquire)).valid()
    }

    fn include(&self, rhs: &Range<usize>) -> bool {
        self.range.start <= rhs.start && self.range.end >= rhs.end
    }
}

/// Each SliceReader will have a background task to refresh the read buffer.
struct SliceReaderBackgroundTask {
    file_reader: Arc<FileReader>,
    slice_reader: Arc<SliceReader>,
    state: Arc<AtomicU8>,
    try_cnt: u64,
    max_retry: u64,
    do_retry: bool, // runtime status, check if we need to do retry.
    // notify the read operation that the data is ready to read.
    ready_notify: Arc<Notify>,
}

impl SliceReaderBackgroundTask {
    async fn run(&mut self) {
        while self.do_retry {
            if self.file_reader.closing.load(Ordering::Acquire) {
                self.exit_task().await;
                return;
            }

            let state = SliceReaderState::from(self.state.load(Ordering::Acquire));
            match state {
                SliceReaderState::IDLE => {
                    self.handle_read().await;
                }
                /// only [SliceReaderState::handle_read] will transfer to [SliceReaderState::BUSY].
                SliceReaderState::BUSY => {}
                SliceReaderState::REFRESH => {
                    // REFRESH represents the interrupt of the read operation.
                }
                SliceReaderState::BREAK => {}
                SliceReaderState::READY => {
                    // notify all the waiters.
                    self.ready_notify.notify_waiters();
                }
                SliceReaderState::INVALID => {}
            }
        }
    }

    async fn exit_task(&mut self) {}

    async fn handle_read(&mut self) {
        self.state
            .store(SliceReaderState::BUSY as u8, Ordering::Release);

        let engine = self
            .file_reader
            .engine
            .upgrade()
            .expect("engine should not be dropped");
        let meta_engine = engine.meta_engine.clone();
        let slice_view = meta_engine
            .read_slice(self.file_reader.inode, self.slice_reader.chunk_idx)
            .await;

        // check if someone change the state.
        if !matches!(
            SliceReaderState::from(self.state.load(Ordering::Acquire)),
            SliceReaderState::BUSY
        ) || self.file_reader.closing.load(Ordering::Acquire)
        {
            // someone change the state, go back to the main FSM
            return;
        }

        let slice_view = match slice_view {
            Ok(slice_view) => slice_view,
            Err(e) => {
                debug!(
                    "read slice {}:{}:{} failed: {}",
                    self.file_reader.inode,
                    self.slice_reader.chunk_idx,
                    self.slice_reader.internal_seq,
                    e
                );

                // not found, should not retry.
                if e.to_errno() == libc::ENOENT {
                    // go the invalid state to clean up resources.
                    self.state
                        .store(SliceReaderState::INVALID as u8, Ordering::Release);
                    return;
                }

                // some other error, we need to retry.
                self.try_cnt += 1;
                if self.try_cnt >= self.max_retry {
                    self.do_retry = false;
                }
                // wait some time before retry.
                Self::cal_retry_delay_interval(self.try_cnt).tick().await;
                return;
            }
        };
        let flen = self.file_reader.length.load(Ordering::Acquire);
        if self.slice_reader.range.start > flen {
            // the read range exceeds the file length,
            // we have nothing to read.
            // update state to ready
            self.state
                .store(SliceReaderState::READY as u8, Ordering::Release);
            return;
        }

        // calculate the real necessary size.
        let real_read_len = if self.slice_reader.range.end > flen {
            flen - self.slice_reader.range.start
        } else {
            self.slice_reader.range.end - self.slice_reader.range.start
        };

        let mut buf_guard = self.slice_reader.read_buf.write().await;
        // TODO: read data from background storage.

        return;
    }

    fn cal_retry_delay_interval(cnt: u64) -> tokio::time::Interval {
        let duration = if cnt < 30 {
            std::time::Duration::from_millis(cnt * 300)
        } else {
            std::time::Duration::from_secs(10)
        };
        tokio::time::interval(duration)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::{common::install_fmt_log, meta::MetaConfig, vfs::storage::new_debug_sto};

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct S {
        original: Range<usize>,
    }

    impl Drop for S {
        fn drop(&mut self) {
            println!("drop {:?}", self.original);
        }
    }
    #[test]
    fn range_map() {
        fn new_s(r: Range<usize>) -> Arc<S> {
            Arc::new(S { original: r })
        }
        let mut rs = rangemap::RangeMap::new();
        rs.insert(0..512, new_s(0..512));
        rs.insert(512..1024, new_s(512..1024)); // cut the first one.
        rs.insert(512..520, new_s(512..520)); // cut self
        rs.insert(512..520, new_s(512..520)); // cur self

        // the rs map should cover 0..1024

        let ranges = rs.iter().collect_vec();
        for r in ranges.iter() {
            println!("{:?}", r);
        }
        // rs.gaps(&(512..2048)).for_each(|r| {
        //     println!("gaps: {:?}", r);
        // });
        //
        // // what if I want to read from 512 to 1024 ?
        // rs.overlapping(&(512..1024)).for_each(|r| {
        //     println!("overlapping: {:?}", r);
        // });
    }

    #[tokio::test]
    async fn split_ranges_basic() {
        install_fmt_log();

        let meta_engine = MetaConfig::default().open().unwrap();
        let sto_engine = new_debug_sto();
        let engine = Arc::new(Engine::new(
            Arc::new(EngineConfig::default()),
            sto_engine,
            Arc::new(meta_engine),
        ));

        struct Case {
            fr_len: usize,
            read_req: (usize, usize),
            want: Vec<Range<usize>>,
        }

        for c in vec![
            Case {
                fr_len: 1024,
                read_req: (0, 512),
                want: vec![0..512],
            },
            Case {
                fr_len: 4096,
                read_req: (2048, 2048 + 4096),
                want: vec![2048..4096],
            },
        ] {
            // let fr = Arc::new(FileReader::new(engine.clone(), Ino(1), 1, c.fr_len));
            // let slice_readers = fr.slice_readers.clone();
            // let mut srs = slice_readers.write().await;
            // let rs = split_ranges(
            //     &mut srs,
            //     cal_real_read_range(c.read_req.0, c.read_req.1, c.fr_len),
            // );
            // assert_eq!(rs.into_iter().collect_vec(), c.want);
        }
    }
}
