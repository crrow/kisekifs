use crate::chunk::{MAX_CHUNK_SIZE, PAGE_SIZE};
use std::io::{Seek, SeekFrom, Write};

#[derive(Debug)]
struct SliceConfig {
    block_size: usize,
    hash_prefix: usize,
}

pub(crate) type SliceID = usize;

/// The basic slice structure.
#[derive(Debug)]
struct SliceInner {
    id: SliceID,
    offset: usize, // the current write offset, it can be set by user seek.
    length: usize,
    conf: SliceConfig,
}

impl SliceInner {
    fn new(id: SliceID, length: usize, slice_config: SliceConfig) -> Self {
        Self {
            id,
            offset: 0,
            length,
            conf: slice_config,
        }
    }
    fn block_index(&self) -> usize {
        self.offset / self.conf.block_size
    }
    fn block_offset(&self, written_len: usize) -> usize {
        (self.offset + written_len) % self.conf.block_size
    }
}

impl Seek for SliceInner {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                if offset > self.length as u64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "seek out of slice boundary: {} > {}",
                            offset, self.length as u64
                        ),
                    ));
                }
                self.offset = offset as usize;
            }
            SeekFrom::End(offset) => {
                if offset > 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("seek out of slice boundary: {} > 0", offset),
                    ));
                }
                self.offset = (self.length as i64 + offset) as usize;
            }
            SeekFrom::Current(offset) => {
                if offset > 0 && self.offset + offset as usize > self.length {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "seek out of slice boundary: {} > {}",
                            self.offset + offset as usize,
                            self.length
                        ),
                    ));
                }
                if offset < 0 && self.offset < offset.abs() as usize {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "seek out of slice boundary: {} < {}",
                            self.offset,
                            offset.abs()
                        ),
                    ));
                }
                self.offset = (self.offset as i64 + offset) as usize;
            }
        }
        Ok(self.offset as u64)
    }
}

/// WSlice for write only, writing is actually performed on slices.
/// Each slice represents a single continuous write,
/// belongs to a specific chunk, and cannot overlap between adjacent chunks.
/// This ensures that the slice length never exceeds 64 MB.
#[derive(Debug)]
pub(crate) struct WSlice {
    inner: SliceInner,

    uploaded_offset: usize, // the last offset that has been uploaded(flushed).
}

impl WSlice {
    pub(crate) fn new(id: SliceID, slice_config: SliceConfig) -> Self {
        todo!()
    }
}

impl Write for WSlice {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let offset = self.inner.offset;
        if offset + buf.len() > MAX_CHUNK_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "write out of chunk boundary: {} > {}",
                    offset + buf.len(),
                    MAX_CHUNK_SIZE
                ),
            ));
        }

        if self.inner.offset < self.uploaded_offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "cannot overwrite uploaded block: {} < {}",
                    offset, self.uploaded_offset
                ),
            ));
        }

        // Fill previous blocks with zeros
        if self.inner.length < offset {
            self.write(&vec![0; offset - self.inner.length])?;
        }

        let mut write_len = 0;
        while write_len < buf.len() {
            // TODO
            let block_index = self.inner.block_index();
            let block_offset = self.inner.block_offset(write_len);
            let mut block_size = PAGE_SIZE;
            if block_index > 0 || block_size > self.inner.conf.block_size {
                block_size = self.inner.conf.block_size;
            }
        }

        todo!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

impl Seek for WSlice {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

/// RSlice for read and remove.
#[derive(Debug)]
pub(crate) struct RSlice {}
