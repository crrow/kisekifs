use crate::chunk::config::Config;
use crate::chunk::err::Result;
use crate::chunk::{BlockIdx, PageIdx, SliceID, MAX_BLOCK_SIZE, MAX_CHUNK_SIZE, MIN_BLOCK_SIZE};
use byteorder::WriteBytesExt;
use bytes::{Buf, BufMut, BytesMut};
use opendal::Operator;
use std::cmp::{max, min};
use std::io::{copy, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

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
}

impl Default for Engine {
    fn default() -> Self {
        Self(Inner::new())
    }
}

/// Inner is the real implementation of Engine.
#[derive(Debug)]
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
    disk_cache_enabled: bool,
    has_prefix: bool,
    // weather the data on the object storage is seekable,
    // which is decided by if we compress the data or not.
    seekable: bool,
}
impl Default for SliceConfig {
    fn default() -> Self {
        Self {
            disk_cache_enabled: false,
            has_prefix: false,
            seekable: false,
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
    pages: [Vec<Page>; MAX_CHUNK_SIZE / MAX_BLOCK_SIZE], // BLOCK_IDX -> PAGE_IDX -> PAGE
}

impl Slice {
    pub fn new(engine: Engine, config: SliceConfig, sid: SliceID) -> Self {
        Self {
            engine,
            config,
            sid,
            length: 0,
            uploaded: 0,
            pages: Default::default(),
        }
    }
    pub fn write_at(&mut self, offset: usize, buf: &[u8]) -> std::io::Result<usize> {
        let expected_write_len = buf.len();
        if expected_write_len <= 0 {
            return Ok(0);
        }

        if offset + buf.len() > MAX_CHUNK_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "slice size exceeds maximum chunk size",
            ));
        }

        let mut total_written_len = 0;
        if self.length < offset {
            // padding 0 to the offset.
            // TODO: maybe there exists a better way to do this.
            let mut padding = vec![0; offset - self.length];
            let n = self.write_at(offset - self.length, &padding)?;
            debug_assert_eq!(n, padding.len());
            total_written_len += n;
        }

        while total_written_len < expected_write_len {
            let new_pos = total_written_len + offset;

            let block_idx = new_pos / MAX_BLOCK_SIZE; // write at which block.
            let block_pos = new_pos % MAX_BLOCK_SIZE; // start write at which position of the block.
            let page_size = if block_idx > 0 {
                // decide the page size of this block.
                // means we write to the sec block.
                MAX_BLOCK_SIZE
            } else {
                // we start write data with small block size.
                MIN_BLOCK_SIZE
            };

            // divide the block into pages, and write data to the page.
            let page_idx = block_pos / page_size;
            let page_pos = block_pos % page_size;

            let page = if page_idx < self.pages[block_idx].len() {
                let p = &mut self.pages[block_idx][page_idx];
                p.clear();
                p
            } else {
                let p = Page::allocate(page_size);
                self.pages[block_idx].push(p);
                &mut self.pages[block_idx][page_idx]
            };

            let current_need_write = min(expected_write_len - total_written_len, page_size);
            let current_write_len = page.write(
                page_pos,
                &buf[total_written_len..total_written_len + current_need_write],
            )?;
            debug_assert_eq!(current_write_len, current_need_write);
            total_written_len += current_write_len;
        }

        self.length = max(self.length, total_written_len + offset);
        Ok(total_written_len)
    }

    pub fn flush_to(&mut self, offset: usize) -> std::io::Result<()> {
        // just flush a part of data[..self.offset],
        // which means we should call seek before flush.
        assert!(offset >= self.uploaded);
        for block_idx in 0..MAX_CHUNK_SIZE / MAX_BLOCK_SIZE {
            let start = block_idx * MAX_BLOCK_SIZE;
            let end = start + MAX_BLOCK_SIZE;
            if start >= self.uploaded && end <= offset {
                if !self.pages[block_idx].is_empty() {
                    self.upload_block(block_idx)?;
                }
                // otherwise, means the block is empty, we do nothing.
                // but we mark it as uploaded.
                self.uploaded = end;
            }
        }

        Ok(())
    }

    // Read only work after writer finish...
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
        let block_idx = offset / MAX_BLOCK_SIZE; // write at which block.
        let block_pos = offset % MAX_BLOCK_SIZE; // start write at which position of the block.
        let read_block_size = self.possible_read_block_size(block_idx);
        if block_pos + page_len > read_block_size {
            // Handles Reads Spanning Multiple Pages
            let mut got = 0; // The total number of bytes already read in the loop.
            while got < page_len {
                // determines how much data to read at most in the current iteration of the loop.
                // It considers both the page boundary and the block boundary to avoid reading beyond either limit.
                let l = min(
                    page_len - got, // calculates the remaining bytes in the current page
                    self.possible_read_block_size(offset / MAX_BLOCK_SIZE)
                        - offset % MAX_BLOCK_SIZE, // calculates the offset within the current block.
                );
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
            // TODO: read disk cache
        }

        // TODO: record cache miss

        // This ensures that the amount of data to be read (len(p)) is relatively small,
        // less than or equal to a quarter of the block size (blockSize).
        // Seeking might be less efficient for larger reads.
        if self.config.seekable && block_pos > 0 && page_len <= read_block_size / 4 {
            // TODO: do partial read from object storage
        }

        // read from object storage, TODO: cache it
        let mut buf = self
            .engine
            .0
            .storage
            .blocking()
            .read(&self.generate_slice_key(block_idx))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        page.as_mut().write(&buf[block_pos..])
    }

    // Return the possible read block size, it should always be MAX_BLOCK_SIZE,
    // or a small one when it reach to the end.
    fn possible_read_block_size(&self, block_idx: BlockIdx) -> usize {
        min(self.length - block_idx * MAX_BLOCK_SIZE, MAX_BLOCK_SIZE)
    }

    fn upload_block(&mut self, block_idx: BlockIdx) -> std::io::Result<()> {
        todo!()
    }

    fn generate_slice_key(&self, block_idx: BlockIdx) -> String {
        if self.config.has_prefix {
            format!(
                "chunks/{:02X}/{:08X}/{:08X}_{:08X}_{:08X}",
                self.sid % 256,
                self.sid / 1000 / 1000,
                self.sid,
                block_idx,
                self.possible_read_block_size(block_idx)
            )
        } else {
            format!(
                "chunks/{}/{}/{:08X}_{:08X}_{:08X}",
                self.sid / 1000 / 1000,
                self.sid / 1000,
                self.sid,
                block_idx,
                self.possible_read_block_size(block_idx)
            )
        }
    }

    fn finish(&mut self) -> std::io::Result<()> {
        // TODO
        Ok(())
    }
}
#[derive(Debug)]
struct Page {
    reference: u64,
    data: Vec<u8>,
}

impl Page {
    fn allocate(size: usize) -> Self {
        Self {
            reference: 0,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_writer() {
        let engine = Engine::default();
        let mut sw = Slice::new(engine, SliceConfig::default(), 0);
        let n = sw.write_at(0, &[1, 2, 3]).unwrap();
        assert_eq!(n, 3);
        assert_eq!(sw.length, 3);
        let n = sw.write_at(1, &[4, 5, 6]).unwrap();
        assert_eq!(n, 3);
        assert_eq!(sw.length, 4);

        // let key = sw.generate_slice_key(0);
        // println!("key: {}", key);
        // sw.config.has_prefix = false;
        // println!("no prefix key: {}", key);

        let page = &mut [0; 3];
        let n = sw.read_at(0, page).unwrap();
        assert_eq!(n, 3);
        assert_eq!(page, &[1, 4, 5]);
    }
}
