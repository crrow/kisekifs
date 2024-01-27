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
    io::{Cursor, ErrorKind, Seek, SeekFrom, Write},
};

use bytes::{BufMut, Bytes, BytesMut};
use opendal::raw::oio::StreamExt;
use slice::Config;
use tracing::{debug, instrument, Instrument};

// #[derive(Debug)]
// pub(crate) struct Config {
//     pub(crate) block_size: usize,
//     pub(crate) disk_cache_enabled: bool,
//     pub(crate) has_prefix: bool,
//     // weather the data on the object storage is seekable,
//     // which is decided by if we compress the data or not.
//     pub(crate) seekable: bool,
//     // write to local cache first then write back to object storage.
//     pub(crate) write_back: bool,
// }
//
// impl Default for Config {
//     fn default() -> Self {
//         Self {
//             block_size: DEFAULT_BLOCK_SIZE,
//             disk_cache_enabled: false,
//             has_prefix: false,
//             seekable: false,
//             write_back: false,
//         }
//     }
// }
use crate::chunk::page::{UnsafePage, UnsafePageView, UnsafePages};
use crate::chunk::{
    err::Result, page, slice, BlockIdx, ChunkError, Engine, SliceID, BLOCK_SIZE,
    DEFAULT_CHUNK_SIZE, PAGE_SIZE,
};

#[derive(Debug)]
struct SliceInner {
    config: Config,
    /// the unique id of this slice.
    sid: SliceID,
    /// the length of this slice, which is less equal
    /// than `MAX_CHUNK_SIZE`.
    length: usize,
    /// the length of bytes that has been uploaded to
    /// the object storage.
    engine: Engine,
}

impl SliceInner {
    fn new(sid: SliceID, config: Config, length: usize, engine: Engine) -> Self {
        Self {
            config,
            sid,
            length,
            engine,
        }
    }
    // Read only work after writer finish...
    // TODO: figure out how to make it async.
    #[instrument(skip_all, fields(sid=self.sid, offset=offset, page_len=page.len()))]
    fn read_at(&mut self, offset: usize, page: &mut [u8]) -> Result<usize> {
        let page_len = page.len();
        if page_len == 0 {
            return Ok(0);
        }
        assert!(offset <= self.length, "offset exceeds slice length");

        let mut offset = offset; // the current offset within the slice where data is being read.
        let block_idx = offset / self.config.block_size; // write at which block.
        let block_pos = offset % self.config.block_size; // start write at which position of the block.
        let read_block_size = self.possible_read_block_size(block_idx);
        if block_pos + page_len > read_block_size {
            // Handles Reads Spanning Multiple Pages
            debug!(
                "handle read spanning multiple pages, block_pos:{block_pos} +page_len:{page_len} > read_block_size: {read_block_size}"
            );
            let mut got = 0; // The total number of bytes already read in the loop.
            while got < page_len {
                debug!("got: {got}, page_len: {page_len}");
                // determines how much data to read at most in the current iteration of the
                // loop. It considers both the page boundary and the block
                // boundary to avoid reading beyond either limit.
                let l = min(
                    page_len - got, // calculates the remaining bytes in the current page
                    self.possible_read_block_size(offset / self.config.block_size)
                        - offset % self.config.block_size, /* calculates the offset within the
                                     * current block. */
                );
                debug!("expect read len: {l}");
                let pp = &mut page[got..got + l];
                let n = self.read_at(offset, pp)?;
                if n == 0 {
                    return Ok(got);
                }
                got += n;
                offset += n;
            }
            return Ok(got);
        }

        if self.config.disk_cache_enabled {
            debug!("try to read from disk cache")
            // TODO: read disk cache
        }

        // TODO: record cache miss

        // This ensures that the amount of data to be read (len(p)) is relatively small,
        // less than or equal to a quarter of the block size (blockSize).
        // Seeking might be less efficient for larger reads.
        if self.config.seekable && block_pos > 0 && page_len <= read_block_size / 4 {
            debug!("try to PartRead from object storage")
            // TODO: do partial read from object storage
        }

        // read from object storage, TODO: cache it

        let key = self.generate_slice_key(block_idx);
        debug!("block_idx: {block_idx}, try to read [{key}] from object storage");
        let mut buf = self.engine.block_load(&key, true, true)?;

        let mut cursor = Cursor::new(page);
        // TODO: try to use copy.
        let n = cursor
            .write(&buf[block_pos..block_pos + page_len])
            .expect("in memory write should not fail");
        Ok(n)
    }

    // Return the possible read block size, it should always be MAX_BLOCK_SIZE,
    // or a small one when it reach to the end.
    fn possible_read_block_size(&self, block_idx: BlockIdx) -> usize {
        possible_read_block_size(self.config.block_size, self.length, block_idx)
    }

    fn generate_slice_key(&self, block_idx: BlockIdx) -> String {
        generate_slice_key(
            self.sid,
            self.config.block_size,
            self.length,
            self.config.has_prefix,
            block_idx,
        )
    }
}

fn generate_slice_key(
    sid: SliceID,
    block_size: usize,
    length: usize,
    has_prefix: bool,
    block_idx: BlockIdx,
) -> String {
    if has_prefix {
        format!(
            "chunks/{:02X}/{:08X}/{:08X}_{:08X}_{:08X}",
            sid % 256,
            sid / 1000 / 1000,
            sid,
            block_idx,
            possible_read_block_size(block_size, length, block_idx)
        )
    } else {
        format!(
            "chunks/{}/{}/{:08X}_{:08X}_{:08X}",
            sid / 1000 / 1000,
            sid / 1000,
            sid,
            block_idx,
            possible_read_block_size(block_size, length, block_idx)
        )
    }
}

// Return the possible read block size, it should always be MAX_BLOCK_SIZE,
// or a small one when it reach to the end.
fn possible_read_block_size(block_size: usize, length: usize, block_idx: BlockIdx) -> usize {
    min(length - block_idx * block_size, block_size)
}

pub struct RSlice(SliceInner);

impl RSlice {
    pub fn new(sid: SliceID, length: usize, engine: Engine) -> Self {
        Self(SliceInner {
            config: engine.slice_config(),
            sid,
            length,
            engine,
        })
    }

    pub fn read_at(&mut self, offset: usize, page: &mut [u8]) -> Result<usize> {
        self.0.read_at(offset, page)
    }

    // TODO: control the remove concurrent number.
    pub async fn remove(&mut self) -> Result<()> {
        if self.length() == 0 {
            // no block, nothing to do
            return Ok(());
        };

        let last_index = (self.length() - 1) / self.0.config.block_size;
        let handles = (0..last_index)
            .into_iter()
            .map(|idx| {
                let key = self.0.generate_slice_key(idx);
                debug!("remove slice: {} at key: {}", self.0.sid, key);
                // self.0.engine.delete(&key).await?;
                SliceDeleter {
                    sid: self.0.sid,
                    key,
                    engine: self.0.engine.clone(),
                }
            })
            .map(|mut deleter| tokio::spawn(async move { deleter.delete().await }))
            .collect::<Vec<_>>();

        // TODO: handle the error
        futures::future::join_all(handles).await;

        Ok(())
    }

    pub fn length(&self) -> usize {
        self.0.length
    }
}

struct SliceDeleter {
    sid: SliceID,
    key: String,
    engine: Engine,
}

impl SliceDeleter {
    // there could be multiple clients try to remove the same chunk in the same
    // time, any of them should succeed if any blocks is removed
    async fn delete(mut self) -> Result<()> {
        return match self.engine.delete(&self.key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                if let ChunkError::StoErr { source } = e {
                    if source.kind() == opendal::ErrorKind::NotFound {
                        return Ok(());
                    }
                    Err(ChunkError::StoErr { source })
                } else {
                    Err(e)
                }
            }
        };
    }
}

#[derive(Debug)]
pub struct WSlice {
    inner: SliceInner,
    /// the length of bytes that has been uploaded to
    /// the object storage.
    uploaded: usize,
    pages: Vec<Vec<page::UnsafePage>>, // BLOCK_IDX -> PAGE_IDX -> PAGE
}

impl WSlice {
    pub fn new(sid: SliceID, engine: Engine) -> Self {
        let config = engine.slice_config();
        let block_cnt = DEFAULT_CHUNK_SIZE / config.block_size;
        Self {
            inner: SliceInner::new(sid, config, 0, engine),
            uploaded: 0,
            pages: vec![vec![]; block_cnt],
        }
    }

    pub fn write_at(&mut self, offset: usize, buf: &[u8]) -> Result<usize> {
        let expected_write_len = buf.len();
        if expected_write_len <= 0 {
            return Ok(0);
        }
        debug!(
            "writing slice: {} at offset: {}, expect write len: {}",
            self.inner.sid, offset, expected_write_len
        );
        assert!(
            offset + buf.len() <= DEFAULT_CHUNK_SIZE,
            "slice size {} + write len {} will exceed maximum chunk size",
            buf.len(),
            expected_write_len
        );
        assert!(
            offset >= self.uploaded,
            "Cannot overwrite uploaded block, uploaded: {}",
            self.uploaded
        );

        let mut total_written_len = 0;
        if self.inner.length < offset {
            // padding 0 to the offset.
            // TODO: maybe there exists a better way to do this.
            let padding = vec![0; offset - self.inner.length];
            debug!(
                "padding 0 from {} to {}",
                self.inner.length,
                offset - self.inner.length
            );
            let n = self.write_at(self.inner.length, &padding)?;
            debug_assert_eq!(n, padding.len());
            // don't count the padding
        }

        while total_written_len < expected_write_len {
            let new_pos = total_written_len + offset;

            let block_idx = new_pos / self.inner.config.block_size; // write at which block.
            let block_pos = new_pos % self.inner.config.block_size; // start write at which position of the block.
            let mut page_size = PAGE_SIZE;
            if block_idx > 0 || page_size > self.inner.config.block_size {
                // decide the page size of this block.
                // means we write to the sec block.
                page_size = self.inner.config.block_size;
            }

            // divide the block into pages, and write data to the page.
            let page_idx = block_pos / page_size;
            let page_pos = block_pos % page_size;

            // TODO: introduce a page table to manage these pages.
            let page = if page_idx < self.pages[block_idx].len() {
                let p = &mut self.pages[block_idx][page_idx];
                p
            } else {
                let p = UnsafePage::allocate(page_size);
                self.pages[block_idx].push(p);
                &mut self.pages[block_idx][page_idx]
            };

            let left = expected_write_len - total_written_len;
            let current_need_write = if page_pos + left > page_size {
                // we should make sure the page has enough space to write.
                let v = page_size - page_pos;
                if page.len() < page_size {
                    page.resize(page_size);
                }
                v
            } else if page.len() < page_pos + left {
                page.resize(page_pos + left);
                left
            } else {
                left
            };

            let mut page_writer = page.writer(page_pos, current_need_write);
            let n = page_writer
                .write(&buf[total_written_len..total_written_len + current_need_write])
                .expect("in memory write should not failed");
            debug_assert_eq!(n, current_need_write);
            total_written_len += current_need_write;
        }

        self.inner.length = max(self.inner.length, total_written_len + offset);
        Ok(total_written_len)
    }

    #[instrument(skip_all, fields(sid=%self.inner.sid, offset=offset))]
    pub async fn flush_to(&mut self, offset: usize) -> Result<()> {
        // just flush a part of data[..self.offset],
        // which means we should call seek before flush.
        assert!(offset >= self.uploaded);

        let handles = self
            .pages
            .iter_mut()
            .enumerate()
            .filter(|(block_idx, block_pages)| {
                let block_idx = *block_idx;
                let start = block_idx * self.inner.config.block_size;
                let end = start + self.inner.config.block_size;
                if start >= self.uploaded && end <= offset {
                    self.uploaded = end;
                    if !block_pages.is_empty() {
                        return true;
                    }
                }
                return false;
            })
            .map(|(block_idx, block_pages)| {
                let pages = std::mem::replace(block_pages, vec![]);
                let block_size = possible_read_block_size(
                    self.inner.config.block_size,
                    self.inner.length,
                    block_idx,
                );
                let key = generate_slice_key(
                    self.inner.sid,
                    self.inner.config.block_size,
                    self.inner.length,
                    self.inner.config.has_prefix,
                    block_idx,
                );

                SliceUploader {
                    slice_id: self.inner.sid,
                    block_idx,
                    block_size,
                    key,
                    block_data: UnsafePages(pages).into(),
                    write_back: self.inner.config.write_back,
                }
            })
            .map(|mut uploader| {
                let engine = self.inner.engine.clone();
                tokio::spawn(async move {
                    let block_idx = uploader.block_idx;
                    debug!("spawn a new task to upload block {}", block_idx);
                    uploader.upload(engine).await.in_current_span()
                })
                .in_current_span()
            })
            .collect::<Vec<_>>();

        futures::future::join_all(handles).await;

        Ok(())
    }

    pub async fn finish(&mut self, length: usize) -> Result<()> {
        assert_eq!(length, self.inner.length);
        let n = (self.inner.length - 1) / self.inner.config.block_size + 1;
        self.flush_to(n * self.inner.config.block_size).await?;
        // TODO: wait all upload finish
        Ok(())
    }

    pub fn length(&self) -> usize {
        self.inner.length
    }

    pub fn load_block_size_from_config(&self) -> usize {
        self.inner.config.block_size
    }
}

struct SliceUploader {
    slice_id: SliceID,
    block_idx: BlockIdx,
    block_size: usize,
    key: String,
    block_data: Vec<u8>,
    write_back: bool,
}

impl SliceUploader {
    async fn upload(mut self, engine: Engine) -> Result<()> {
        // TODO: we should try to upload to cache.

        debug!(
            "SliceUploader: {} uploading block {} to {}, len: {}",
            self.slice_id,
            self.block_idx,
            self.key,
            self.block_data.len(),
        );

        // let buf = self.buf.to_vec();
        engine.put(&self.key, self.block_data).await?;
        Ok(())
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
        let mut ws = WSlice::new(1, engine.clone());
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

        let mut rs = RSlice::new(1, size, engine.clone());
        let page = &mut [0; 5];
        let n = rs.read_at(6, page).unwrap();
        assert_eq!(n, 5);
        assert_eq!(page, b"world");

        let page = &mut [0; 20];
        let n = rs.read_at(offset, page).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&page[..n], data);

        // engine.remove(1, size).await.unwrap();
    }
}
