// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    slice, BlockIdx, ChunkError, PageIdx, SliceID, BLOCK_SIZE, DEFAULT_CHUNK_SIZE, PAGE_SIZE,
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
    pub fn new_sled() -> Engine {
        let mut builder = opendal::services::Sled::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.datadir(tempdir_path);

        let op = Arc::new(Operator::new(builder).unwrap().finish());
        let config = Config::default();
        let inner = Inner {
            storage: op,
            config,
        };
        Self(inner)
    }
    pub fn slice_config(&self) -> slice::Config {
        self.0.slice_config()
    }
    pub fn reader(&self, sid: SliceID, length: usize) -> slice::RSlice {
        slice::RSlice::new(sid, length, self.clone())
    }
    pub fn writer(&self, sid: SliceID) -> slice::WSlice {
        slice::WSlice::new(sid, self.clone())
    }
    pub async fn remove(&self, sid: SliceID, length: usize) -> Result<()> {
        let mut x = self.reader(sid, length);
        x.remove().await?;
        Ok(())
    }

    pub fn used_memory(&self) -> usize {
        // TODO
        return 0;
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

    pub(crate) async fn delete<K: AsRef<str>>(&self, key: K) -> Result<()> {
        let op = self.0.storage.clone();
        let key = key.as_ref();
        op.delete(key).await.context(StoErrSnafu)?;
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

#[cfg(test)]
mod tests {
    use tracing_subscriber::{prelude::*, Registry};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        let stdout_log = tracing_subscriber::fmt::layer().pretty();
        let subscriber = Registry::default().with(stdout_log);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set global subscriber");

        let engine = Engine::default();
        let mut ws = engine.writer(1);
        let data = b"hello world" as &[u8];
        let n = ws.write_at(0, data).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(ws.length(), data.len());

        let offset = ws.load_block_size_from_config() - 3;
        let n = ws.write_at(offset, data).unwrap();
        assert_eq!(n, data.len());
        let size = offset + data.len();
        assert_eq!(ws.length(), size);

        ws.flush_to(BLOCK_SIZE + 3).await.unwrap();
        ws.finish(size).await.unwrap();

        let mut rs = engine.reader(1, size);
        let page = &mut [0; 5];
        let n = rs.read_at(6, page).unwrap();
        assert_eq!(n, 5);
        assert_eq!(page, b"world");

        let page = &mut [0; 20];
        let n = rs.read_at(offset, page).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&page[..n], data);

        engine.remove(1, size).await.unwrap();
    }
}
