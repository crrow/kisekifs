use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use snafu::Snafu;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Snafu)]
pub(crate) enum BufferError {}

type Result<T> = std::result::Result<T, BufferError>;

const DEFAULT_BUFFER_CAPACITY: usize = 300 << 20; // 300 MiB
const DEFAULT_CHUNK_SIZE: usize = 64 << 20; // 64 MiB
const DEFAULT_BLOCK_SIZE: usize = 4 << 20; // 4 MiB
const DEFAULT_PAGE_SIZE: usize = 64 << 10; // 64 KiB

#[derive(Debug, Clone, Copy)]
pub(crate) struct Config {
    /// The total memory size for the write/read buffer.
    pub(crate) total_buffer_capacity: usize,
    /// chunk_size is the max size can one buffer
    /// hold no matter it is for reading or writing.
    pub(crate) chunk_size: usize,
    /// block_size is the max size when we upload
    /// the data to the cloud.
    ///
    /// When the data is not enough to fill the block,
    /// then the block size is equal to the data size,
    /// for example, the last block of the file.
    pub(crate) block_size: usize,
    /// The page_size can be also called as the MIN_BLOCK_SIZE,
    /// which is the min size of the block.
    ///
    /// And under the hood, the block is divided into pages.
    pub(crate) page_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            total_buffer_capacity: DEFAULT_BUFFER_CAPACITY, // 300 MiB
            chunk_size: DEFAULT_CHUNK_SIZE,                 // 64 MiB
            block_size: DEFAULT_BLOCK_SIZE,                 // 4 MiB
            page_size: DEFAULT_PAGE_SIZE,                   // 64 KiB
        }
    }
}

impl Config {
    #[inline]
    fn get_buffer_config(&self) -> BufferConfig {
        BufferConfig {
            chunk_size: self.chunk_size,
            block_size: self.block_size,
            page_size: self.page_size,
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct BufferConfig {
    chunk_size: usize,
    block_size: usize,
    page_size: usize,
}

/// The buffer manager is responsible for managing
/// the read and write buffer.
///
/// And when the buffer is full, it should wait for
/// enough space to write/read.
pub(crate) struct BufferManager {
    config: Config,
    // the memory pool is used to tracking buffer memory usage.
    // When alloc new buffer, we should make a memory reservation
    // for it.
    memory_pool: Arc<dyn MemoryPool>,
    // the next buffer id.
    next_buffer_id: Arc<AtomicUsize>,
}

impl BufferManager {
    pub(crate) fn new(config: Config) -> Self {
        todo!()
    }

    pub(crate) fn get_write_buffer(&self, slice_id: usize) -> WriteBuffer {
        let config = self.config.get_buffer_config();
        WriteBuffer::new(config, slice_id, &self.memory_pool)
    }
}

enum Block {
    // The block is empty, means we have not write
    // it yet.
    Empty,
    // This block is used for place holder.
    Hole,
    // The block is full.
    Full(Vec<u8>),
    // The block is partial.
    Partial {
        // The start offset of the block.
        start: usize,
        // The end offset of the block.
        end: usize,
        // The data of the block.
        data: Vec<u8>,
    },
    // This block has been released.
    Released,
}

/// A write buffer can only be hold by one slice.
/// And the max size of the write buffer is equal
/// to the the given chunk size.
pub(crate) struct WriteBuffer {
    config: BufferConfig,
    // the buffer id, for tracking memory usage.
    id: usize,
    // who owns this buffer, this id need to be
    // set if we need to upload the buffer to the cloud.
    slice_id: Option<usize>,
    // tracking current buffer usage.
    memory_reservation: MemoryReservation,
    // current length of this buffer.
    length: usize,
    // the released length of current buffer,
    // it should less equal than the length.
    released_length: usize,
    // the buffer is divided into blocks.
    block_slots: Vec<Block>,
}

impl WriteBuffer {
    fn new(config: BufferConfig, id: usize, memory_pool: &Arc<dyn MemoryPool>) -> WriteBuffer {
        let r = MemoryConsumer::new(format!("w-slice-id:{}", id)).register(&memory_pool);
        WriteBuffer {
            config,
            id,
            slice_id: None,
            memory_reservation: r,
            length: 0,
            released_length: 0,
            block_slots: (0..(config.chunk_size / config.block_size))
                .into_iter()
                .map(|_| Block::Empty)
                .collect(),
        }
    }
    pub(crate) fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        let expected_write_len = data.len();
        if expected_write_len <= 0 {
            return Ok(0);
        }

        debug!(
            "writing buffer: {} at offset: {}, expect write len: {}",
            self.id, offset, expected_write_len
        );
        debug_assert!(
            offset + expected_write_len <= self.config.chunk_size,
            "offset {} + expect write len {} will exceed the chunk size",
            offset,
            expected_write_len
        );
        debug_assert!(
            offset >= self.released_length,
            "cannot overwrite released block, released length: {}",
            self.released_length
        );

        if self.length < offset {
            // we need to make a hole.
            self.make_hole(self.length, offset - self.length);
        }

        let mut total_write_len = 0;
        while total_write_len < expected_write_len {
            let block_idx = offset / self.config.block_size;
            let block_offset = offset % self.config.block_size;
            let block = &mut self.block_slots[block_idx];
            match block {
                Block::Empty => {
                    let write_len = std::cmp::min(
                        expected_write_len - total_write_len,
                        self.config.block_size - block_offset,
                    );
                    let mut data = vec![0; self.config.block_size];
                    data[block_offset..(block_offset + write_len)]
                        .copy_from_slice(&data[total_write_len..(total_write_len + write_len)]);
                    *block = Block::Partial {
                        start: offset,
                        end: offset + write_len,
                        data,
                    };
                    total_write_len += write_len;
                    offset += write_len;
                }
                Block::Partial {
                    start,
                    end,
                    data: block_data,
                } => {
                    let write_len = std::cmp::min(
                        expected_write_len - total_write_len,
                        self.config.block_size - block_offset,
                    );
                    let mut data = vec![0; self.config.block_size];
                    data[block_offset..(block_offset + write_len)]
                        .copy_from_slice(&data[total_write_len..(total_write_len + write_len)]);
                    *block = Block::Partial {
                        start: *start,
                        end: *end,
                        data,
                    };
                    total_write_len += write_len;
                    offset += write_len;
                }
                Block::Full(_) => {
                    // we need to make a hole.
                    self.make_hole(offset, self.config.block_size);
                }
                Block::Hole => {
                    // we need to make a hole.
                    self.make_hole(offset, self.config.block_size);
                }
                Block::Released => {
                    // we need to make a hole.
                    self.make_hole(offset, self.config.block_size);
                }
            }
        }

        todo!()
    }

    fn make_hole(&mut self, offset: usize, size: usize) {
        let start_block_id = offset / self.config.block_size;
        let end_block_id = (offset + size) / self.config.block_size;
        debug_assert!(
            start_block_id <= end_block_id,
            "start block id {} should less than end block id {}",
            start_block_id,
            end_block_id
        );

        for block_id in start_block_id..end_block_id {
            match self.block_slots[block_id] {
                Block::Empty => {
                    self.block_slots[block_id] = Block::Hole;
                }
                _ => unreachable!("make hole should do when we do padding zero", block_id),
            }
        }

        debug!("make hole, from: {}, length: {}", self.length, size);
        self.length += size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_for() {
        for i in 1..=1 {
            println!("{}", i);
        }
    }
}
