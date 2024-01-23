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

use crate::chunk::{ChunkError, ChunkOffset, Engine, SliceID};
use crate::vfs::handle::HandleWriteGuard;
use crate::{meta::types::Ino, vfs::err::Result};

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
