use crate::chunk::config::Config;
use crate::chunk::err::{Result, StoErrSnafu};
use crate::chunk::{
    BlockIdx, ChunkError, PageIdx, SliceID, DEFAULT_BLOCK_SIZE, MAX_CHUNK_SIZE, MIN_BLOCK_SIZE,
};
use byteorder::WriteBytesExt;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::MapOk;
use opendal::Operator;
use scopeguard::defer;
use snafu::ResultExt;
use std::cmp::{max, min};
use std::future::Future;
use std::io::{copy, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{debug, instrument, Instrument};

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
    pub fn slice_reader(&self, sid: SliceID) {
        todo!()
    }
    pub fn slice_writer(&self, sid: SliceID) {
        todo!()
    }
    pub fn remove_slice(&self, sid: SliceID) -> Result<()> {
        todo!()
    }
    fn load<K: AsRef<str>>(&mut self, key: K, cache: bool, force_cache: bool) -> Result<Vec<u8>> {
        todo!()
    }

    async fn put<K: AsRef<str>>(&self, key: K, data: Vec<u8>) -> Result<()> {
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
}

#[derive(Debug)]
struct SliceConfig {
    block_size: usize,
    disk_cache_enabled: bool,
    has_prefix: bool,
    // weather the data on the object storage is seekable,
    // which is decided by if we compress the data or not.
    seekable: bool,
    // write to local cache first then write back to object storage.
    write_back: bool,
}
impl Default for SliceConfig {
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
pub struct Slice {
    engine: Engine,
    config: SliceConfig,
    /// the unique id of this slice.
    sid: SliceID,
    /// the length of this slice, which is less equal
    /// than `MAX_CHUNK_SIZE`.
    length: usize,
    /// the length of bytes that has been uploaded to
    /// the object storage.
    uploaded: usize,
    pages: Vec<Vec<Page>>, // BLOCK_IDX -> PAGE_IDX -> PAGE
}

impl Slice {
    pub fn new(engine: Engine, config: SliceConfig, sid: SliceID) -> Self {
        let block_cnt = MAX_CHUNK_SIZE / config.block_size;
        Self {
            engine,
            config,
            sid,
            length: 0,
            uploaded: 0,
            pages: vec![vec![]; block_cnt],
        }
    }
    pub fn write_at(&mut self, offset: usize, buf: &[u8]) -> std::io::Result<usize> {
        let expected_write_len = buf.len();
        if expected_write_len <= 0 {
            return Ok(0);
        }
        debug!(
            "writing slice: {} at offset: {}, expect write len: {}",
            self.sid, offset, expected_write_len
        );

        if offset + buf.len() > MAX_CHUNK_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "slice size exceeds maximum chunk size",
            ));
        }
        assert!(
            offset >= self.uploaded,
            "Cannot overwrite uploaded block, uploaded: {}",
            self.uploaded
        );

        let mut total_written_len = 0;
        if self.length < offset {
            // padding 0 to the offset.
            // TODO: maybe there exists a better way to do this.
            let padding = vec![0; offset - self.length];
            debug!("padding 0 from {} to {}", self.length, offset - self.length);
            let n = self.write_at(self.length, &padding)?;
            debug_assert_eq!(n, padding.len());
            // don't count the padding
        }

        while total_written_len < expected_write_len {
            let new_pos = total_written_len + offset;

            let block_idx = new_pos / self.config.block_size; // write at which block.
            let block_pos = new_pos % self.config.block_size; // start write at which position of the block.
            let mut page_size = MIN_BLOCK_SIZE;
            if block_idx > 0 || page_size > self.config.block_size {
                // decide the page size of this block.
                // means we write to the sec block.
                page_size = self.config.block_size;
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
            cursor.seek(SeekFrom::Start(page_pos as u64))?;
            let n =
                cursor.write(&buf[total_written_len..total_written_len + current_need_write])?;
            debug_assert_eq!(n, current_need_write);

            // let writer = &mut page.data.as_mut_slice()[page_pos..current_need_write];
            // writer.copy_from_slice(&buf[total_written_len..current_need_write]);

            // // let current_need_write = min(expected_write_len - total_written_len, page_size);
            // let current_write_len = page.write(
            //     page_pos,
            //     &buf[total_written_len..total_written_len + current_need_write],
            // )?;
            // debug_assert_eq!(current_write_len, current_need_write);
            total_written_len += current_need_write;
        }

        self.length = max(self.length, total_written_len + offset);
        Ok(total_written_len)
    }

    #[instrument(skip_all, fields(sid=%self.sid, offset=offset))]
    pub async fn flush_to(&mut self, offset: usize) -> std::io::Result<()> {
        // just flush a part of data[..self.offset],
        // which means we should call seek before flush.
        assert!(offset >= self.uploaded);

        let handles = self
            .pages
            .iter_mut()
            .enumerate()
            .filter(|(block_idx, block_pages)| {
                let block_idx = *block_idx;
                let start = block_idx * self.config.block_size;
                let end = start + self.config.block_size;
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
                let block_size =
                    possible_read_block_size(self.config.block_size, self.length, block_idx);
                let key = generate_slice_key(
                    self.sid,
                    self.config.block_size,
                    self.length,
                    self.config.has_prefix,
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
                    slice_id: self.sid,
                    block_idx,
                    block_size,
                    key,
                    buf: buffer.freeze(),
                    write_back: self.config.write_back,
                }
            })
            .map(|mut uploader| {
                let engine = self.engine.clone();
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

    // Read only work after writer finish...
    #[instrument(skip_all, fields(sid=self.sid, offset=offset, page_len=page.len()))]
    pub fn read_at(&mut self, offset: usize, page: &mut [u8]) -> std::io::Result<usize> {
        let page_len = page.len();
        if page_len == 0 {
            return Ok(0);
        }
        if offset > self.length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "offset exceeds slice length",
            ));
        }

        let mut offset = offset; // the current offset within the slice where data is being read.
        let block_idx = offset / self.config.block_size; // write at which block.
        let block_pos = offset % self.config.block_size; // start write at which position of the block.
        let read_block_size = self.possible_read_block_size(block_idx);
        if block_pos + page_len > read_block_size {
            // Handles Reads Spanning Multiple Pages
            debug!("handle read spanning multiple pages, block_pos:{block_pos} +page_len:{page_len} > read_block_size: {read_block_size}");
            let mut got = 0; // The total number of bytes already read in the loop.
            while got < page_len {
                debug!("got: {got}, page_len: {page_len}");
                // determines how much data to read at most in the current iteration of the loop.
                // It considers both the page boundary and the block boundary to avoid reading beyond either limit.
                let l = min(
                    page_len - got, // calculates the remaining bytes in the current page
                    self.possible_read_block_size(offset / self.config.block_size)
                        - offset % self.config.block_size, // calculates the offset within the current block.
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
        let mut buf = self
            .engine
            .0
            .storage
            .blocking()
            .read(&key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut cursor = Cursor::new(page);
        // TODO: try to use copy.
        cursor.write(&buf[block_pos..block_pos + page_len])
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
    async fn finish(&mut self, length: usize) -> std::io::Result<()> {
        assert_eq!(length, self.length);
        let n = (self.length - 1) / self.config.block_size + 1;
        self.flush_to(n * self.config.block_size).await?;
        // TODO: wait all upload finish
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::{prelude::*, Registry};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slice_writer() {
        let stdout_log = tracing_subscriber::fmt::layer().pretty();
        let subscriber = Registry::default().with(stdout_log);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set global subscriber");

        let engine = Engine::default();
        let mut sw = Slice::new(engine, SliceConfig::default(), 0);
        let data = b"hello world" as &[u8];
        let n = sw.write_at(0, data).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(sw.length, data.len());

        let offset = sw.config.block_size - 3;
        let n = sw.write_at(offset, data).unwrap();
        assert_eq!(n, data.len());
        let size = offset + data.len();
        assert_eq!(sw.length, size);

        sw.flush_to(DEFAULT_BLOCK_SIZE + 3).await.unwrap();
        sw.finish(size).await.unwrap();

        let page = &mut [0; 5];
        let n = sw.read_at(6, page).unwrap();
        assert_eq!(n, 5);
        assert_eq!(page, b"world");

        let page = &mut [0; 20];
        let n = sw.read_at(offset, page).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&page[..n], data);
    }
}
