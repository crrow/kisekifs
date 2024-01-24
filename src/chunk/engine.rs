use std::{
    cmp::{max, min},
    future::Future,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use byteorder::WriteBytesExt;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::MapOk;
use opendal::Operator;
use scopeguard::defer;
use snafu::ResultExt;
use tracing::{debug, instrument, Instrument};

use crate::chunk::{
    config::Config,
    err::{Result, StoErrSnafu},
    slice, BlockIdx, ChunkError, PageIdx, SliceID, DEFAULT_BLOCK_SIZE, MAX_CHUNK_SIZE,
    MIN_BLOCK_SIZE,
};

#[derive(Debug, Default)]
pub struct Builder {
    config: Config,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

    pub fn build(self) -> Result<Engine> {
        todo!()
    }
}

/// Engine will use disk as cache, and use object storage as backend.
#[derive(Debug)]
pub struct Engine(Inner);

impl Clone for Engine {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Engine {
    pub fn slice_config(&self) -> slice::Config {
        self.0.slice_config()
    }
    pub fn slice_reader(&self, sid: SliceID, length: usize) -> slice::RSlice {
        slice::RSlice::new(sid, length, self.clone())
    }
    pub fn slice_writer(&self, sid: SliceID) -> slice::WSlice {
        slice::WSlice::new(sid, self.clone())
    }
    pub fn remove_slice(&self, sid: SliceID) -> Result<()> {
        todo!()
    }
    pub(crate) fn block_load<K: AsRef<str>>(
        &mut self,
        key: K,
        cache: bool,
        force_cache: bool,
    ) -> Result<Vec<u8>> {
        let v = self
            .0
            .storage
            .blocking()
            .read(key.as_ref())
            .context(StoErrSnafu)?;
        Ok(v)
    }

    pub(crate) async fn put<K: AsRef<str>>(&self, key: K, data: Vec<u8>) -> Result<()> {
        let op = self.0.storage.clone();
        let key = key.as_ref();
        op.write(key, data).await.context(StoErrSnafu)?;
        Ok(())
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self(Inner::new())
    }
}

/// Inner is the real implementation of Engine.
#[derive(Debug, Clone)]
struct Inner {
    storage: Arc<Operator>,
    config: Config,
}

impl Inner {
    fn new() -> Self {
        let mut builder = opendal::services::Memory::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.root(tempdir_path);

        let op = Arc::new(Operator::new(builder).unwrap().finish());
        let config = Config::default();
        Self {
            storage: op,
            config,
        }
    }

    fn slice_config(&self) -> slice::Config {
        slice::Config::default()
    }
}
