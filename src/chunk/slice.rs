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
use tracing::{debug, instrument, Instrument};

use crate::chunk::{
    err::Result, BlockIdx, ChunkError, Engine, SliceID, DEFAULT_BLOCK_SIZE, MAX_CHUNK_SIZE,
    MIN_BLOCK_SIZE,
};

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) block_size: usize,
    pub(crate) disk_cache_enabled: bool,
    pub(crate) has_prefix: bool,
    // weather the data on the object storage is seekable,
    // which is decided by if we compress the data or not.
    pub(crate) seekable: bool,
    // write to local cache first then write back to object storage.
    pub(crate) write_back: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            disk_cache_enabled: false,
            has_prefix: false,
            seekable: false,
            write_back: false,
        }
    }
}

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
    pub(crate) fn new(sid: SliceID, length: usize, engine: Engine) -> Self {
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
    pages: Vec<Vec<Page>>, // BLOCK_IDX -> PAGE_IDX -> PAGE
}

impl WSlice {
    pub(crate) fn new(sid: SliceID, engine: Engine) -> Self {
        let config = engine.slice_config();
        let block_cnt = MAX_CHUNK_SIZE / config.block_size;
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
            offset + buf.len() <= MAX_CHUNK_SIZE,
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
            let mut page_size = MIN_BLOCK_SIZE;
            if block_idx > 0 || page_size > self.inner.config.block_size {
                // decide the page size of this block.
                // means we write to the sec block.
                page_size = self.inner.config.block_size;
            }

            // divide the block into pages, and write data to the page.
            let page_idx = block_pos / page_size;
            let page_pos = block_pos % page_size;

            let page = if page_idx < self.pages[block_idx].len() {
                let p = &mut self.pages[block_idx][page_idx];
                p
            } else {
                let p = Page::allocate(page_size);
                self.pages[block_idx].push(p);
                &mut self.pages[block_idx][page_idx]
            };

            let left = expected_write_len - total_written_len;
            let current_need_write = if page_pos + left > page_size {
                // we should make sure the page has enough space to write.
                let v = page_size - page_pos;
                if page.len() < page_size {
                    page.data.resize(page_size, 0);
                }
                v
            } else if page.data.len() < page_pos + left {
                // we should make sure the page has enough space to write.
                page.data.resize(page_pos + left, 0);
                left
            } else {
                left
            };

            let mut cursor = Cursor::new(&mut page.data);
            cursor
                .seek(SeekFrom::Start(page_pos as u64))
                .expect("seek on memory should not failed");
            let n = cursor
                .write(&buf[total_written_len..total_written_len + current_need_write])
                .expect("in memory write should not failed");
            debug_assert_eq!(n, current_need_write);

            // let writer = &mut page.data.as_mut_slice()[page_pos..current_need_write];
            // writer.copy_from_slice(&buf[total_written_len..current_need_write]);

            // // let current_need_write = min(expected_write_len - total_written_len,
            // page_size); let current_write_len = page.write(
            //     page_pos,
            //     &buf[total_written_len..total_written_len + current_need_write],
            // )?;
            // debug_assert_eq!(current_write_len, current_need_write);
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

                let mut buffer;
                let mut offset = 0;
                if pages.len() == 1 {
                    offset = pages[0].len();
                    buffer = BytesMut::from(pages[0].data.as_slice());
                } else {
                    buffer = BytesMut::with_capacity(block_size);
                    pages.into_iter().for_each(|page| {
                        let data = page.data.as_slice();
                        offset += data.len();
                        buffer.put_slice(data);
                    });
                }
                assert_eq!(offset, buffer.len(), "block length does not match offset");
                SliceUploader {
                    slice_id: self.inner.sid,
                    block_idx,
                    block_size,
                    key,
                    buf: buffer.freeze(),
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
    buf: Bytes,
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
            self.buf.len(),
        );
        let buf = self.buf.to_vec();
        engine.put(&self.key, buf).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Page {
    data: Vec<u8>,
}

impl Page {
    fn allocate(size: usize) -> Self {
        Self {
            data: Vec::with_capacity(size),
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> std::io::Result<usize> {
        let mut cursor = Cursor::new(&mut self.data);
        cursor.seek(SeekFrom::Start(offset as u64))?;
        cursor.write(data)
    }

    fn clear(&mut self) {
        self.data.clear()
    }

    fn slice(&mut self, offset: usize, size: usize) -> &mut [u8] {
        assert!(offset + size <= self.data.len());
        &mut self.data[offset..offset + size]
    }
}
