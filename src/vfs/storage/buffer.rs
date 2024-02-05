use std::{
    cmp::{max, min},
    io::{Cursor, Write},
    sync::Arc,
};

use bytesize::ByteSize;
use opendal::Operator;
use snafu::{ensure, ResultExt};
use tracing::debug;

use crate::{
    meta::types::{SliceID, EMPTY_SLICE_ID},
    vfs::{
        err::{ErrLIBCSnafu, OpenDalSnafu, Result},
        storage::{make_slice_object_key, Cache, EngineConfig},
        VFSError,
    },
};

pub(crate) struct ReadBuffer {
    config: Arc<EngineConfig>,
    slice_id: SliceID,
    length: usize,
    object_storage: Operator,
}

impl ReadBuffer {
    pub(crate) fn new(
        config: Arc<EngineConfig>,
        object_storage: Operator,
        slice_id: SliceID,
        length: usize,
    ) -> ReadBuffer {
        ReadBuffer {
            config,
            slice_id,
            length,
            object_storage,
        }
    }

    pub(crate) fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let expected_read_len = dst.len();
        if expected_read_len == 0 {
            return Ok(0);
        }

        debug!(
            "reading buffer: {} at offset: {}, expect read len: {}",
            self.slice_id, offset, expected_read_len
        );
        debug_assert!(
            offset + expected_read_len <= self.config.chunk_size,
            "offset {} + expect read len {} will exceed the chunk size",
            offset,
            expected_read_len
        );

        let mut offset = offset; // the current offset within the slice where data is being read.
        let block_idx = offset / self.config.block_size; // write at which block.
        let block_pos = offset % self.config.block_size; // start write at which position of the block.
        let read_block_size = cal_object_block_size(self.length, self.config.block_size, block_idx);
        if block_pos + expected_read_len > read_block_size {
            // Handles Reads Spanning Multiple Pages
            debug!(
                "handle read spanning multiple pages, block_pos:{block_pos} +page_len:{expected_read_len} > read_block_size: {read_block_size}"
            );
            let mut got = 0; // The total number of bytes already read in the loop.
            while got < expected_read_len {
                debug!("got: {got}, page_len: {expected_read_len}");
                // determines how much data to read at most in the current iteration of the
                // loop. It considers both the page boundary and the block
                // boundary to avoid reading beyond either limit.
                let l = min(
                    expected_read_len - got, // calculates the remaining bytes in the current page
                    cal_object_block_size(
                        self.length,
                        self.config.block_size,
                        offset / self.config.block_size,
                    ) - offset % self.config.block_size, /* calculates the offset within the
                                              * current block. */
                );
                debug!("expect read len: {l}");
                let pp = &mut dst[got..got + l];
                let n = self.read_at(offset, pp)?;
                if n == 0 {
                    return Ok(got);
                }
                got += n;
                offset += n;
            }
            return Ok(got);
        }

        let key = make_slice_object_key(self.slice_id, block_idx, read_block_size);
        debug!("block_idx: {block_idx}, try to read [{key}]");
        let buf = self
            .object_storage
            .blocking()
            .read(&key)
            .context(OpenDalSnafu)?;
        debug!(
            "read block: {block_idx} from object storage succeed, read len: {} kib",
            buf.len() / 1024,
        );
        let mut cursor = Cursor::new(dst);
        // TODO: try to use copy.
        let n = cursor
            .write(&buf[block_pos..block_pos + expected_read_len])
            .expect("in memory write should not fail");
        Ok(n)
    }
}

#[derive(Debug)]
pub(crate) enum Block {
    // The block is empty, means we have not write
    // it yet.
    Empty,
    // This block has been written before.
    Occupy(Vec<u8>),
    // This block has been released, we cannot write to
    // it any more.
    Released,
}

/// A write buffer can only be hold by one slice.
/// And the max size of the write buffer is equal
/// to the the given chunk size.
#[derive(Debug)]
pub(crate) struct WriteBuffer {
    config: Arc<EngineConfig>,
    // who owns this buffer, this id need to be
    // set if we need to upload the buffer to the cloud.
    slice_id: SliceID,
    // current length of this buffer.
    length: usize,
    // the length of bytes that has been released.
    flushed_length: usize,
    // the buffer is divided into blocks.
    block_slots: Vec<Block>,
    cache: Arc<dyn Cache>,
    object_storage: Operator,
}

impl WriteBuffer {
    pub(crate) fn new(
        config: Arc<EngineConfig>,
        cache: Arc<dyn Cache>,
        object_storage: Operator,
    ) -> WriteBuffer {
        WriteBuffer {
            block_slots: (0..(config.chunk_size / config.block_size))
                .map(|_| Block::Empty)
                .collect(),
            config,
            slice_id: EMPTY_SLICE_ID,
            length: 0,
            flushed_length: 0,
            object_storage,
            cache,
        }
    }

    pub(crate) fn set_slice_id(&mut self, sid: SliceID) {
        self.slice_id = sid;
    }

    pub(crate) fn get_slice_id(&self) -> SliceID {
        self.slice_id
    }

    pub(crate) fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        let expected_write_len = data.len();
        if expected_write_len == 0 {
            return Ok(0);
        }

        debug!(
            "writing buffer, at offset: {}, expect write len: {}",
            offset, expected_write_len
        );
        debug_assert!(
            offset + expected_write_len <= self.config.chunk_size,
            "offset {} + expect write len {} will exceed the chunk size",
            offset,
            expected_write_len
        );
        debug_assert!(
            offset >= self.flushed_length,
            "cannot overwrite released block, released length: {}",
            self.flushed_length
        );

        if self.length < offset {
            // OPTIMIZATION: make a hole to fill the gap.
            // self.make_hole(self.length, offset - self.length);
        }

        let mut total_write_len = 0;
        while total_write_len < expected_write_len {
            let new_pos = offset + total_write_len;
            let block_idx = new_pos / self.config.block_size;
            let block_offset = new_pos % self.config.block_size;
            let block = &mut self.block_slots[block_idx];

            let write_len = std::cmp::min(
                expected_write_len - total_write_len,
                self.config.block_size - block_offset,
            ); // we cannot write more than the block size.

            match block {
                Block::Empty => {
                    // alloc at least one page size.
                    let alloc_size = if block_idx > 0 {
                        self.config.block_size
                    } else {
                        max(
                            self.config.page_size,
                            round_to(block_offset + write_len, self.config.page_size),
                        )
                    };

                    let mut buf = vec![0; alloc_size];
                    buf[block_offset..(block_offset + write_len)]
                        .copy_from_slice(&data[total_write_len..(total_write_len + write_len)]);
                    *block = Block::Occupy(buf);
                    total_write_len += write_len;
                }
                Block::Occupy(buf) => {
                    if buf.len() < block_offset + write_len {
                        // we need to alloc more memory.
                        let alloc_size = round_to(block_offset + write_len, self.config.page_size);
                        buf.resize(alloc_size, 0);
                        debug_assert!(buf.len() <= self.config.block_size);
                    }
                    buf[block_offset..(block_offset + write_len)]
                        .copy_from_slice(&data[total_write_len..(total_write_len + write_len)]);
                    total_write_len += write_len;
                }
                Block::Released => unreachable!("cannot write to released block"),
            };
        }

        self.length = max(self.length, offset + total_write_len);
        Ok(total_write_len)
    }

    /// Read data from the write buffer, which actually we should not do it.
    fn read_at(&mut self, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let expected_read_len = dst.len();
        if expected_read_len == 0 {
            return Ok(0);
        }

        ensure!(offset >= self.length, ErrLIBCSnafu { kind: libc::EOF });

        debug!(
            "reading buffer, at offset: {}, expect read len: {}",
            offset,
            ByteSize::kib(expected_read_len as u64),
        );
        debug_assert!(
            offset + expected_read_len <= self.config.chunk_size,
            "offset {} + expect read len {} will exceed the chunk size",
            offset,
            ByteSize::kib(expected_read_len as u64)
        );
        debug_assert!(
            offset >= self.flushed_length,
            "cannot read released block, released length: {}",
            self.flushed_length
        );

        let mut total_read_len = 0;
        while total_read_len < expected_read_len {
            let new_pos = offset + total_read_len;
            let block_idx = new_pos / self.config.block_size;
            let block_offset = new_pos % self.config.block_size;
            let block = &mut self.block_slots[block_idx];

            let read_len = std::cmp::min(
                expected_read_len - total_read_len,
                self.config.block_size - block_offset,
            ); // we cannot read more than the block size.

            match block {
                Block::Empty => {
                    return Err(VFSError::ReadEmptyBlock)?;
                }
                Block::Occupy(buf) => {
                    dst[total_read_len..(total_read_len + read_len)]
                        .copy_from_slice(&buf[block_offset..(block_offset + read_len)]);
                    total_read_len += read_len;
                }
                Block::Released => unreachable!("cannot read to released block"),
            };
        }

        Ok(total_read_len)
    }

    /// Try to flush the buffer to the given offset.
    pub(crate) async fn flush_to(&mut self, offset: usize) -> Result<()> {
        debug_assert!(self.flushed_length <= offset);

        let datas: Vec<(String, Vec<u8>)> = self
            .block_slots
            .iter_mut()
            .enumerate()
            .filter(|(idx, b)| match b {
                Block::Empty | Block::Released => false,
                Block::Occupy(..) => {
                    let block_idx = *idx;
                    let end = (block_idx + 1) * self.config.block_size;
                    end <= offset
                }
            })
            .map(|(idx, b)| match b {
                Block::Occupy(data) => {
                    let data = std::mem::take(data);
                    *b = Block::Released;
                    let l = self.length;
                    let block_size = self.config.block_size;
                    let block_size = cal_object_block_size(l, block_size, idx);
                    self.flushed_length += block_size;
                    assert_ne!(self.slice_id, EMPTY_SLICE_ID, "slice id should be set");
                    let key = make_slice_object_key(self.slice_id, idx, block_size);
                    (key, data)
                }
                _ => unreachable!("we have filtered out the empty and released block"),
            })
            .collect::<Vec<_>>();
        let sto = self.object_storage.clone();
        let futures = datas
            .into_iter()
            .map(|(k, v)| {
                let sto = sto.clone(); // Clone sto within the closure
                async move {
                    debug!("flushing block: [{}], block_len: {} KiB", k, v.len() / 1024,);
                    let _ = sto.write(&k, v).await;
                }
            })
            .collect::<Vec<_>>();

        futures::future::join_all(futures).await;

        Ok(())
    }

    /// Flush the buffer to the cloud.
    pub(crate) async fn finish(&mut self) -> Result<()> {
        let n = self.length / self.config.block_size + 1;
        self.flush_to(n * self.config.block_size).await?;
        Ok(())
    }

    pub(crate) fn length(&self) -> usize {
        self.length
    }

    pub(crate) fn flushed_length(&self) -> usize {
        self.flushed_length
    }

    pub(crate) fn block_size(&self) -> usize {
        self.config.block_size
    }

    pub(crate) fn chunk_size(&self) -> usize {
        self.config.chunk_size
    }
}

fn round_to(size: usize, round: usize) -> usize {
    (size + round - 1) / round * round
}

fn cal_object_block_size(length: usize, block_size: usize, block_idx: usize) -> usize {
    // min(1025 - 0 * 1024, 1024) = min(1024) = 1024
    // min(1023 - 0 * 1024, 1024) = min(1023, 1024) = 1023
    // min(2049 - 2 * 1024, 1024) = min(1, 1024) = 1
    min(length - block_idx * block_size, block_size)
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use super::*;
    use crate::{common::new_memory_sto, vfs::storage::new_juice_builder};

    #[test]
    fn buffer_write() {
        let config = Arc::new(EngineConfig::default());
        let sto = new_memory_sto();
        let cache = new_juice_builder().build().unwrap();
        let mut wb = WriteBuffer::new(config.clone(), cache, sto);

        let write_data = b"hello" as &[u8];
        let expected_write_len = write_data.len();

        let write_len = wb.write_at(0, write_data).unwrap();
        assert_eq!(write_len, expected_write_len);

        let expect_read_len = expected_write_len;
        let mut buf = vec![0; expect_read_len];
        let read_len = wb.read_at(0, &mut buf).unwrap();
        assert_eq!(read_len, expect_read_len);
        assert_eq!(buf, write_data);

        assert!(wb.read_at(6, &mut [0; 5]).is_err());

        let write_len = wb.write_at(config.block_size - 3, write_data).unwrap();
        assert_eq!(write_len, 5);
        let read_len = wb.read_at(config.block_size - 3, &mut buf).unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(buf, write_data);

        let mut write_data = vec![0; config.block_size];
        rand::thread_rng().fill_bytes(&mut write_data);
        let write_len = wb.write_at(0, &mut write_data).unwrap();
        assert_eq!(write_len, write_data.len());

        let mut read_data = vec![0; config.block_size];
        let read_len = wb.read_at(0, &mut read_data).unwrap();
        assert_eq!(read_len, read_data.len());
        assert_eq!(read_data, write_data);
    }

    #[tokio::test]
    async fn read_and_write() {
        use crate::common::install_fmt_log;
        install_fmt_log();

        let config = Arc::new(EngineConfig::default());
        let sto = new_memory_sto();
        let cache = new_juice_builder().build().unwrap();
        let mut wb = WriteBuffer::new(config.clone(), cache, sto.clone());
        wb.set_slice_id(1);

        let data = b"hello world" as &[u8];
        let n = wb.write_at(0, data).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(wb.length(), data.len());

        let offset = wb.block_size() - 3;
        let n = wb.write_at(offset, data).unwrap();
        assert_eq!(n, data.len());
        let size = offset + data.len();
        assert_eq!(wb.length(), size);

        wb.flush_to(config.block_size + 3).await.unwrap();
        wb.finish().await.unwrap();

        let rb = ReadBuffer::new(config.clone(), sto.clone(), 1, size);
        let dst = &mut [0; 5];
        let n = rb.read_at(6, dst).unwrap();
        assert_eq!(n, 5);
        assert_eq!(dst, b"world");

        let page = &mut [0; 20];
        let n = rb.read_at(offset, page).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&page[..n], data);
    }

    #[tokio::test]
    async fn write_smallest() {
        use crate::common::install_fmt_log;
        install_fmt_log();

        let config = Arc::new(EngineConfig::default());
        let sto = new_memory_sto();
        let cache = new_juice_builder().build().unwrap();
        let mut wb = WriteBuffer::new(config.clone(), cache, sto.clone());
        wb.set_slice_id(1);

        let data = vec![0u8; 65 << 10];
        let mut offset = 0;
        let n = wb.write_at(offset, &data).unwrap();
        assert_eq!(n, data.len());
        offset += n;
        let n = wb.write_at(offset, &data).unwrap();
        assert_eq!(n, data.len());

        let size = wb.length;
        wb.finish().await.unwrap();

        let rb = ReadBuffer::new(config.clone(), sto.clone(), 1, size);
        let dst = &mut [0; 1];
        let n = rb.read_at(0, dst).unwrap();
        assert_eq!(n, 1);
    }
}
