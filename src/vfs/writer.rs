use std::sync::{Arc, Weak};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::SystemTime,
};

use dashmap::DashMap;
use tokio::{sync::Notify, time::Instant};
use tracing::debug;

use crate::chunk::{chunk_id, ChunkID};
use crate::{
    chunk::{ChunkError, ChunkOffset, Engine, SliceID},
    meta::types::Ino,
    vfs::{err::Result, handle::HandleWriteGuard},
};

#[derive(Debug, Default)]
pub(crate) struct DataWriter {
    engine: Engine,
    file_writers: DashMap<Ino, Arc<FileWriter>>,
}

impl DataWriter {
    pub(crate) fn open(&self, inode: Ino, length: u64) -> Weak<FileWriter> {
        debug!("data writer open do nothing: inode: {inode}, length: {length}");
        let engine = self.engine.clone();
        let x = self.file_writers.entry(inode).or_insert_with(|| {
            Arc::new(FileWriter {
                chunk_engine: engine,
                offset: 0,
                reference: AtomicU64::new(0),
                chunk_writers: Default::default(),
            })
        });
        Arc::downgrade(x.value())
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

/// There may be multiple writers for a file.
#[derive(Debug)]
pub(crate) struct FileWriter {
    chunk_engine: Engine,
    offset: usize, // set by user seek

    reference: AtomicU64,
    chunk_writers: DashMap<ChunkID, ChunkWriter>,
}

impl FileWriter {
    // We can only write data when we acquire the write lock.
    //
    // But there may some background tasks, so we use shared
    // reference here.
    fn write(&self, guard: &HandleWriteGuard, offset: usize, data: &[u8]) -> Result<usize> {
        let chunk_id = chunk_id(offset);
        let mut entry = self
            .chunk_writers
            .entry(chunk_id)
            .or_insert(ChunkWriter::new(chunk_id));
        let mut chunk_writer = entry.value_mut();

        Ok(0)
    }
}

/// ChunkWriter is used for writing a chunk.
#[derive(Debug)]
struct ChunkWriter {
    cid: ChunkID,
    slices: Vec<SliceWriter>,
}

impl ChunkWriter {
    fn new(cid: ChunkID) -> Self {
        Self {
            cid,
            slices: vec![],
        }
    }
}

/// SliceWriter is a wrapper for writing data to a slice.
#[derive(Debug)]
struct SliceWriter {}
