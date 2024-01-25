use std::{
    collections::HashMap,
    io::{Error, Write},
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::SystemTime,
};

use dashmap::DashMap;
use tokio::{sync::Notify, time::Instant};
use tracing::debug;

use crate::{
    chunk::{ChunkError, ChunkOffset, Engine, SliceID},
    meta::types::Ino,
    vfs::{err::Result, handle::HandleWriteGuard},
};

#[derive(Debug, Default)]
pub(crate) struct DataWriter {}

impl DataWriter {
    pub(crate) fn open(&self, inode: Ino, length: u64) -> FileWriter {
        debug!("data writer open do nothing: inode: {inode}, length: {length}");
        todo!()
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

#[derive(Debug)]
pub(crate) struct FileWriter {
    chunk_engine: Engine,
    offset: usize, // set by user seek

    reference: AtomicU64,
}
