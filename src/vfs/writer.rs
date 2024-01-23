use dashmap::DashMap;
use std::collections::HashMap;
use std::io::{Error, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::time::Instant;

use tracing::debug;

use crate::chunk::{ChunkEngine, ChunkError, ChunkIndex, ChunkOffset, SliceID, WSlice};
use crate::vfs::handle::HandleWriteGuard;
use crate::{meta::types::Ino, vfs::err::Result};

#[derive(Debug, Default)]
pub(crate) struct DataWriter {}

impl DataWriter {
    pub(crate) fn open(&self, inode: Ino, length: u64) -> FileWriter {
        debug!("data writer open do nothing: inode: {inode}, length: {length}");
        FileWriter::default()
    }
    pub(crate) fn get_length(&self, ino: Ino) -> u64 {
        debug!("writer get_length do nothing, inode: {ino}");
        return 0;
    }

    pub(crate) fn update_mtime(&self, ino: Ino, mtime: SystemTime) -> Result<()> {
        // TODO: implement me
        return Ok(());
    }
}

/// Each file is composed of one or more chunks.
/// Each chunk has a maximum size of 64 MB.
///
/// Chunks exist to optimize lookup and positioning,
/// while the actual file writing is performed on
/// slices.
///
/// Each slice represents a single continuous write,
/// belongs to a specific chunk, and cannot overlap
/// between adjacent chunks. This ensures that the
/// slice length never exceeds 64 MB.
///
/// For example, if a file is generated through a
/// continuous sequential write, each chunk contains
/// only one slice.
///
/// File writing generates slices, and invoking flush
/// persists these slices.
///
/// When persisting to the object storage, slices are
/// further split into individual blocks (default
/// maximum size of 4 MB) to enable multi-threaded
/// concurrent writes, thereby enhancing write
/// performance.
///
/// The previously mentioned chunks and slices are
/// logical data structures, while blocks represent
/// the final physical storage form and serve as the
/// smallest storage unit for the object storage and
/// disk cache.
///
#[derive(Debug)]
pub(crate) struct FileWriter {
    chunk_engine: ChunkEngine,
    offset: usize, // set by user seek

    reference: AtomicU64,
    chunks: HashMap<ChunkIndex, ChunkWriter>,
}

impl Default for FileWriter {
    fn default() -> Self {
        todo!()
    }
}

impl tokio::io::AsyncWrite for FileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        todo!()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        todo!()
    }
}

impl FileWriter {
    fn write_chunk(
        &mut self,
        id: ChunkIndex,
        pos: ChunkOffset,
        data: &[u8],
    ) -> std::io::Result<()> {
        let cw = self.chunks.entry(id).or_insert(ChunkWriter {
            idx: id,
            slices: Vec::new(),
        });
        let mut slice_writer = match cw.find_writable_slice(pos, data.len()) {
            None => {
                let slice_writer = SliceWriter::new(pos, self.chunk_engine.new_writer(0));
                cw.slices.push(slice_writer);
                if cw.slices.len() == 1 {
                    self.reference.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(cw.commit_in_background());
                }
                let sw = cw.slices.last_mut().unwrap();
                sw
            }
            Some(s) => s,
        };
        todo!()
    }
}

#[derive(Debug)]
struct ChunkWriter {
    idx: ChunkIndex,
    slices: Vec<SliceWriter>,
}

impl ChunkWriter {
    fn find_writable_slice(&mut self, pos: ChunkOffset, size: usize) -> Option<&mut SliceWriter> {
        todo!()
    }

    async fn commit_in_background(&mut self) {}
}

#[derive(Debug)]
struct SliceWriter {
    writer: WSlice,
    offset: usize,
    started: Instant,
    notify: Notify,
}

impl SliceWriter {
    fn new(offset: usize, wslice: WSlice) -> Self {
        Self {
            offset,
            writer: wslice,
            started: Instant::now(),
            notify: Default::default(),
        }
    }
}
