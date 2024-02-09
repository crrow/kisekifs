use crate::buffer_pool::{get_page, Page};
use crate::error::Result;
use crate::{BLOCK_SIZE, CHUNK_SIZE, PAGE_SIZE};
use std::cmp::{max, min};
use std::sync::Arc;
use tokio::sync::Mutex;

///
/// Global write buffer manager, the entry point of write buffer.
/// It receives write requests and decides weather to flush the
/// write buffer.
///
pub struct WriteBufferManager {}

///
/// SliceBuffer is a buffer that stores the write requests.
///
/// Random write requests may hit the same slice buffer.
///
/// Once the SliceBuffer is not frozen, we should be able to
/// modify on it, for achieving better random write performance.
///
pub struct SliceBuffer(Arc<Mutex<SliceBufferInner>>);

struct SliceBufferInner {
    /// the slice length, the total write len of the slice.
    /// we may write some block we just write before.
    length: usize,
    /// the slice frozen status, once frozen, it can't be modified.
    frozen: bool,
    /// Each block is variable size when we flush, from 128KB to 4MB.
    /// So we can have max CHUNK_SIZE / PAGE_SIZE blocks,
    /// but we use CHUNK_SIZE / BLOCK_SIZE to simplify the logic.
    block_slots: Box<[Block]>,
    /// how many page do we have in the slice buffer.
    total_page_cnt: usize,
}

impl SliceBufferInner {
    fn new() -> Self {
        Self {
            length: 0,
            frozen: false,
            block_slots: (0..(CHUNK_SIZE / BLOCK_SIZE))
                .map(|_| Block::Empty)
                .collect(),
            total_page_cnt: 0,
        }
    }

    /// read_at only for debug.
    fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<usize> {
        if offset >= self.length {
            return Ok(0);
        }
        let expected_read_len = dst.len();
        if expected_read_len == 0 {
            return Ok(0);
        }
        debug_assert!(
            expected_read_len + offset <= CHUNK_SIZE,
            "offset: {}, expected_read_len: {} should not exceed CHUNK_SIZE: {}",
            offset,
            expected_read_len,
            CHUNK_SIZE
        );

        let mut total_read_len = 0;
        while total_read_len < expected_read_len {
            let new_pos = total_read_len + offset;
            let block_idx = new_pos / BLOCK_SIZE;
            let block_offset = new_pos % BLOCK_SIZE;
            let block = unsafe { self.block_slots.get_unchecked(block_idx) };

            let current_block_to_read_len = min(
                expected_read_len - total_read_len,
                BLOCK_SIZE - block_offset, // don't exceed the block boundary.
            );
            // how many bytes we can read from the block.
            let mut total_current_block_read_len = 0;
            while total_current_block_read_len < current_block_to_read_len {
                let new_block_offset = block_offset + total_current_block_read_len;
                let page_idx = new_block_offset / PAGE_SIZE;
                let page_offset = new_block_offset % PAGE_SIZE;

                let current_page_to_read_len = min(
                    current_block_to_read_len - total_current_block_read_len,
                    PAGE_SIZE - page_offset, // don't exceed the page boundary.
                );

                match block {
                    Block::Empty => {
                        // it is a hole, we should pad zero to the dst.
                        for i in total_read_len..total_read_len + current_block_to_read_len {
                            dst[i] = 0;
                        }
                    }
                    Block::Data(pages) => {
                        if let Some(page) = unsafe { pages.get_unchecked(page_idx) } {
                            let page_slice = page.as_slice();
                            dst[total_read_len..(total_read_len + current_page_to_read_len)]
                                .copy_from_slice(
                                    &page_slice
                                        [page_offset..(page_offset + current_page_to_read_len)],
                                );
                        } else {
                            // it is a hole, we should pad zero to the dst.
                            for i in total_read_len..total_read_len + current_block_to_read_len {
                                dst[i] = 0;
                            }
                        }
                    }
                };
                total_current_block_read_len += current_page_to_read_len;
                total_read_len += current_page_to_read_len;
            }
        }

        Ok(total_read_len)
    }

    // offset: the offset in the slice buffer, range: [0, CHUNK_SIZE).
    // data size: [0, PAGE_SIZE).
    //
    // usually one write_at call only need to wait on one page available.
    // the worst case is but when we cross the page boundary,
    // we may need to wait on two pages.
    async fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        debug_assert!(!self.frozen);
        let expected_write_len = data.len();
        if expected_write_len == 0 {
            return Ok(0);
        }

        debug_assert!(
            offset + expected_write_len <= CHUNK_SIZE,
            "offset: {}, expected_write len: {} should not exceed CHUNK_SIZE: {}",
            offset,
            expected_write_len,
            CHUNK_SIZE,
        );

        let mut total_write_len = 0;
        while total_write_len < expected_write_len {
            let new_offset = offset + total_write_len;
            let block_index = new_offset / BLOCK_SIZE;
            let block_offset = new_offset % BLOCK_SIZE;
            let mut block = unsafe { self.block_slots.get_unchecked_mut(block_index) };

            if matches!(block, Block::Empty) {
                *block = Block::new_data_block();
            }

            // how many bytes we can write to the block.
            let mut total_page_write_len = 0;
            let to_write_block_len = min(
                expected_write_len - total_write_len,
                BLOCK_SIZE - block_offset, // don't exceed the block boundary.
            );
            while total_page_write_len < to_write_block_len {
                let new_block_offset = block_offset + total_page_write_len;
                let page_index = new_block_offset / PAGE_SIZE;
                let page_offset = new_block_offset % PAGE_SIZE;
                let (page, new_one) = block.get_page(page_index).await;
                if new_one {
                    self.total_page_cnt += 1;
                }
                let page_slice = page.as_mut_slice();
                let to_write_page_len = min(
                    to_write_block_len - total_page_write_len,
                    PAGE_SIZE - page_offset, // don't exceed the page boundary.
                );
                page_slice[page_offset..(page_offset + to_write_page_len)]
                    .copy_from_slice(&data[total_write_len..(total_write_len + to_write_page_len)]);
                total_page_write_len += to_write_page_len;
                total_write_len += to_write_page_len;
            }
        }
        self.length = max(self.length, offset + total_write_len);
        Ok(total_write_len)
    }
}

///
/// Block represents the real data that is written to the storage.
///
/// Once a block has been flushed, then it become Empty, then we
/// can write new data to it.
///
enum Block {
    // The block is empty, doesn't hold any memory,
    // 1. we may just flush it
    // 2. just haven't written data yet.
    // 3. it is a hole.(we don't upload it, reader should fill the gap)
    Empty,
    // The actual data block we have written.
    // Block is composed by one or more pages.
    // Block size from PAGE_SIZE to BLOCK_SIZE.
    // One block can max hold BLOCK_SIZE / PAGE_SIZE pages.
    Data(Box<[Option<Page>]>),
}

impl Block {
    fn new_data_block() -> Block {
        Block::Data((0..(BLOCK_SIZE / PAGE_SIZE)).map(|_| None).collect())
    }

    async fn get_page(&mut self, page_idx: usize) -> (&mut Page, bool) {
        debug_assert!(!matches!(self, Block::Empty));
        if let Block::Data(pages) = self {
            let mut new_one = false;
            if matches!(pages[page_idx], None) {
                let page = get_page().await;
                pages[page_idx] = Some(page);
                new_one = true;
            };
            (pages[page_idx].as_mut().unwrap(), new_one)
        } else {
            panic!("Block is empty");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_write() {
        let mut slice_buffer = SliceBufferInner::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());
        println!(
            "len: {} ~= {} KB, page cnt: {}, size: {} KB",
            slice_buffer.length,
            slice_buffer.length / 1024,
            slice_buffer.total_page_cnt,
            slice_buffer.total_page_cnt * PAGE_SIZE / 1024
        );

        let write_len = slice_buffer.write_at(PAGE_SIZE - 3, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, PAGE_SIZE + data.len() - 3);
        println!(
            "len: {} ~= {} KB, page cnt: {}, size: {} KB",
            slice_buffer.length,
            slice_buffer.length / 1024,
            slice_buffer.total_page_cnt,
            slice_buffer.total_page_cnt * PAGE_SIZE / 1024
        );

        let write_len = slice_buffer
            .write_at(PAGE_SIZE - 3, vec![1u8; PAGE_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, PAGE_SIZE);
        assert_eq!(slice_buffer.length, PAGE_SIZE - 3 + PAGE_SIZE);
        println!(
            "len: {} ~= {} KB, page cnt: {}, size: {} KB",
            slice_buffer.length,
            slice_buffer.length / 1024,
            slice_buffer.total_page_cnt,
            slice_buffer.total_page_cnt * PAGE_SIZE / 1024
        );

        let write_len = slice_buffer
            .write_at(BLOCK_SIZE - 3, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE + BLOCK_SIZE - 3);
        println!(
            "len: {} ~= {} KB, page cnt: {}, size: {} KB",
            slice_buffer.length,
            slice_buffer.length / 1024,
            slice_buffer.total_page_cnt,
            slice_buffer.total_page_cnt * PAGE_SIZE / 1024
        );
    }

    #[tokio::test]
    async fn basic_read() {
        let mut slice_buffer = SliceBufferInner::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());

        let mut dst = vec![0u8; 5];
        let read_len = slice_buffer.read_at(0, dst.as_mut_slice()).unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(dst, data);

        let write_len = slice_buffer.write_at(PAGE_SIZE - 3, &data).await.unwrap();
        assert_eq!(write_len, data.len());

        let mut dst = vec![0u8; 5];
        let read_len = slice_buffer
            .read_at(PAGE_SIZE - 3, dst.as_mut_slice())
            .unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(dst, data);

        // read a hole
        let mut dst = vec![0u8; 5];
        let read_len = slice_buffer.read_at(5, dst.as_mut_slice()).unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(dst, vec![0u8; 5]);
    }
}
