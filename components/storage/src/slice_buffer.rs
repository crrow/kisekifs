use std::{
    cmp::{max, min},
    io::Cursor,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use kiseki_utils::readable_size::ReadableSize;
use snafu::ResultExt;
use tokio::time::Instant;
use tokio::{io::AsyncWriteExt, task::JoinHandle};
use tracing::{debug, instrument, Instrument};

use kiseki_types::slice::{make_slice_object_key, SliceID, EMPTY_SLICE_ID};
use kiseki_types::{BlockIndex, BlockSize, ObjectStorage, BLOCK_SIZE, CHUNK_SIZE, PAGE_SIZE};

use crate::error::{
    InvalidSliceBufferWriteOffsetSnafu, JoinErrSnafu, OpenDalSnafu, Result, UnknownIOSnafu,
};
use crate::pool::{Page, GLOBAL_PAGE_POOL};

// read_slice_from_object_storage will allocate memory in place and then drop
// it.
#[instrument(skip_all, fields(length, offset))]
pub async fn read_slice_from_object_storage<F: Fn(BlockIndex, BlockSize) -> String>(
    gen_key: F,
    object_storage: ObjectStorage,
    length: usize, // length of the slice.
    offset: usize, // read offset
    dst: &mut [u8],
) -> Result<usize> {
    let expected_read_len = dst.len();
    if expected_read_len == 0 {
        return Ok(0);
    }

    debug_assert!(
        offset + expected_read_len <= CHUNK_SIZE,
        "offset {} + expect read len {} will exceed the chunk size",
        offset,
        expected_read_len
    );

    let expected_read_len = min(length - offset, expected_read_len);
    let mut total_read_len = 0;
    let mut hanldes = vec![];
    let dst_ptr = dst.as_mut_ptr();
    let dst_len = dst.len();

    while total_read_len < expected_read_len {
        let new_pos = total_read_len + offset;
        let block_idx = new_pos / BLOCK_SIZE;
        let block_offset = new_pos % BLOCK_SIZE;
        let obj_block_size = cal_object_block_size(length, block_idx, BLOCK_SIZE);
        let current_block_to_read_len = min(
            expected_read_len - total_read_len,
            obj_block_size - block_offset, // don't exceed the block boundary.
        );

        let key = gen_key(block_idx, obj_block_size);
        let sto = object_storage.clone();
        let dst = unsafe { std::slice::from_raw_parts_mut(dst_ptr, dst_len) };
        let dst_slice = &mut dst[total_read_len..(total_read_len + current_block_to_read_len)];

        total_read_len += current_block_to_read_len;

        let handle: JoinHandle<Result<usize>> = tokio::spawn(async move {
            let block_buf = sto.read(&key).await.context(OpenDalSnafu)?;
            let mut cursor = Cursor::new(dst_slice);
            let n = cursor
                .write(&block_buf[block_offset..block_offset + current_block_to_read_len])
                .await
                .context(UnknownIOSnafu)?;
            Ok(n)
        });
        hanldes.push(handle);
    }

    let mut actual_read_cnt = 0;
    for x in futures::future::try_join_all(hanldes)
        .await
        .context(JoinErrSnafu)?
        .into_iter()
    {
        actual_read_cnt += x?;
    }
    assert_eq!(actual_read_cnt, total_read_len);

    Ok(total_read_len)
}

fn cal_object_block_size(length: usize, block_idx: BlockIndex, block_size: BlockSize) -> usize {
    // min(1025 - 0 * 1024, 1024) = min(1024) = 1024
    // min(1023 - 0 * 1024, 1024) = min(1023, 1024) = 1023
    // min(2049 - 2 * 1024, 1024) = min(1, 1024) = 1
    min(length - block_idx * block_size, block_size)
}

pub struct SliceBufferWrapper {
    slice_id: SliceID,
    inner: SliceBuffer,
    object_storage: ObjectStorage,
}

impl SliceBufferWrapper {
    pub fn new(object_storage: ObjectStorage) -> Self {
        Self {
            slice_id: EMPTY_SLICE_ID,
            inner: SliceBuffer::new(),
            object_storage,
        }
    }
    pub fn set_slice_id(&mut self, sid: SliceID) {
        self.slice_id = sid;
    }

    pub fn get_slice_id(&self) -> SliceID {
        self.slice_id
    }

    pub async fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        self.inner.write_at(offset, data).in_current_span().await
    }

    pub async fn flush_to(&mut self, offset: usize) -> Result<()> {
        let sid = self.slice_id;

        let key_gen = move |idx, size| -> String { make_slice_object_key(sid, idx, size) };
        let _ = self
            .inner
            .flush_bulk_to(offset, key_gen, self.object_storage.clone())
            .await?;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        let sid = self.slice_id;
        let key_gen = move |idx, size| -> String { make_slice_object_key(sid, idx, size) };
        let _ = self
            .inner
            .flush(key_gen, self.object_storage.clone())
            .await?;
        Ok(())
    }

    pub fn length(&self) -> usize {
        self.inner.length
    }

    pub fn flushed_length(&self) -> usize {
        self.inner.flushed_length
    }
}

/// SliceAppendOnlyBuffer is a buffer that handle the write requests.
///
/// Random write requests may hit the same slice buffer.
///
/// As long as the SliceBuffer is not frozen, we should be able to
/// modify on it, for achieving better random write performance.
pub struct SliceBuffer {
    /// the slice length, the total write len of the slice.
    /// we may write some block we just write before.
    length: usize,
    // the last flushed block index.
    flushed_length: usize,
    /// Each block is variable size when we flush, from 128KB to 4MB.
    /// So we can have max CHUNK_SIZE / PAGE_SIZE blocks,
    /// but we use CHUNK_SIZE / BLOCK_SIZE to simplify the logic.
    block_slots: Box<[Block]>,
    /// how many page do we have in the slice buffer.
    total_page_cnt: usize,
}

impl SliceBuffer {
    fn new() -> Self {
        Self {
            length: 0,
            flushed_length: 0,
            block_slots: (0..(CHUNK_SIZE / BLOCK_SIZE))
                .map(|_| Block::Empty)
                .collect(),
            total_page_cnt: 0,
        }
    }

    /// read_at only for debug.
    pub fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<usize> {
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
                    Block::Data(db) => {
                        if let Some(page) = unsafe { db.pages.get_unchecked(page_idx) } {
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
    pub async fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
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

        if offset < self.flushed_length {
            return InvalidSliceBufferWriteOffsetSnafu.fail()?;
        }

        // debug_assert!(
        //     offset >= self.flushed_length,
        //     "offset: {} should be greater than flushed length: {}",
        //     offset,
        //     self.flushed_length
        // );

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
                block.update_len(max(block.get_len(), block_offset + total_page_write_len));
            }
        }
        self.length = max(self.length, offset + total_write_len);
        Ok(total_write_len)
    }

    /// should_flush check if we can flush the slice buffer.
    pub fn should_flush(&self) -> bool {
        self.length == CHUNK_SIZE
            || self.length >= BLOCK_SIZE
            || self.total_page_cnt * PAGE_SIZE > BLOCK_SIZE
    }

    fn full_block_cnt(&self) -> usize {
        self.block_slots
            .iter()
            .filter(|block| block.is_full())
            .count()
    }

    fn partial_block_cnt(&self) -> usize {
        self.block_slots
            .iter()
            .filter(|block| !block.is_full())
            .count()
    }

    pub fn flushed_len(&self) -> usize {
        self.flushed_length
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn status(&self) -> SliceBufferStatus {
        let full_cnt = self.full_block_cnt();
        SliceBufferStatus {
            length: self.length,
            logic_size: ReadableSize(self.length as u64),
            page_cnt: self.total_page_cnt,
            real_size: ReadableSize((self.total_page_cnt * PAGE_SIZE) as u64),
            full_block_cnt: full_cnt,
            partial_block_cnt: self.block_slots.len() - full_cnt,
        }
    }

    /// flush_bulk flush as much as possible blocks to the storage.
    #[instrument(skip(self, key_gen, object_storage))]
    pub async fn flush_bulk<F: Fn(BlockIndex, BlockSize) -> String>(
        &mut self,
        key_gen: F,
        object_storage: ObjectStorage,
    ) -> Result<usize> {
        self.flush_bulk_to(self.length, key_gen, object_storage)
            .await
    }

    /// flush all written data to the storage.
    #[instrument(skip(self, key_gen, object_storage))]
    pub async fn flush<F: Fn(BlockIndex, BlockSize) -> String>(
        &mut self,
        key_gen: F,
        object_storage: ObjectStorage,
    ) -> Result<usize> {
        self.flush_bulk_to(
            ((self.length - 1) / BLOCK_SIZE + 1) * BLOCK_SIZE,
            key_gen,
            object_storage,
        )
        .await
    }

    /// flush_to flush the slice buffer to the storage until the offset.
    ///
    /// Ignore all empty block.
    async fn flush_bulk_to<F: Fn(BlockIndex, BlockSize) -> String>(
        &mut self,
        offset: usize,
        key_gen: F,
        object_storage: ObjectStorage,
    ) -> Result<usize> {
        assert!(
            self.flushed_length <= offset,
            "offset should be greater than flushed length {}, {}",
            self.flushed_length,
            offset
        );

        let pending_block_idxes = self
            .block_slots
            .iter()
            .enumerate()
            .filter(|(idx, block)| match block {
                Block::Empty => false,
                Block::Data(..) => {
                    let block_idx = *idx;
                    let end = (block_idx + 1) * BLOCK_SIZE;
                    end <= offset
                }
            })
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();

        let total_released_page_cnt = Arc::new(AtomicUsize::new(0));
        let handles = pending_block_idxes
            .into_iter()
            .map(|idx| {
                let data_block = std::mem::take(&mut self.block_slots[idx]);
                (idx, data_block.get_data_block())
            })
            .map(|(idx, data_block)| {
                self.flushed_length += data_block.length;
                let key = key_gen(idx, data_block.length);
                let sto = object_storage.clone();
                let page_cnt = total_released_page_cnt.clone();
                let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                    let mut writer = sto.writer(&key).await.context(OpenDalSnafu)?;
                    let total_flush_data = data_block.length;
                    let mut current_flush_data = 0;
                    while current_flush_data < total_flush_data {
                        let page_idx = current_flush_data / PAGE_SIZE;
                        let page_offset = current_flush_data % PAGE_SIZE;
                        let to_flush_len = min(
                            PAGE_SIZE - page_offset,
                            total_flush_data - current_flush_data,
                        );
                        match &data_block.pages[page_idx] {
                            None => {
                                for _ in 0..to_flush_len {
                                    writer.write_u8(0).await.context(UnknownIOSnafu)?;
                                }
                            }
                            Some(page) => {
                                let page_slice = page.as_slice();
                                writer
                                    .write_all(
                                        &page_slice[page_offset..(page_offset + to_flush_len)],
                                    )
                                    .await
                                    .context(UnknownIOSnafu)?;
                                page_cnt.fetch_add(1, Ordering::AcqRel);
                            }
                        }
                        current_flush_data += to_flush_len;
                    }
                    writer.close().await.context(OpenDalSnafu)?;
                    Ok(())
                });
                handle
            })
            .collect::<Vec<_>>();

        for r in futures::future::join_all(handles).await.into_iter() {
            r.context(JoinErrSnafu)??;
        }

        let released_page = total_released_page_cnt.load(Ordering::Relaxed);
        self.total_page_cnt -= released_page;
        Ok(released_page)
    }
}

#[derive(Debug)]
pub struct SliceBufferStatus {
    pub length: usize,
    pub logic_size: ReadableSize,
    // how many pages we use in the slice buffer.
    pub page_cnt: usize,
    pub real_size: ReadableSize,
    pub full_block_cnt: usize,
    pub partial_block_cnt: usize,
}

/// Block represents the real data that is written to the storage.
///
/// Once a block has been flushed, then it become Empty, then we
/// can write new data to it.
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
    // It is the smallest unit we can flush to the storage.
    Data(DataBlock),
}

struct DataBlock {
    // the written len of the block
    length: usize,
    pages: Box<[Option<Page>]>,
}

impl Default for Block {
    fn default() -> Self {
        Block::Empty
    }
}

impl Block {
    fn new_data_block() -> Block {
        Block::Data(DataBlock {
            length: 0,
            pages: (0..(BLOCK_SIZE / PAGE_SIZE)).map(|_| None).collect(),
        })
    }

    fn get_data_block(self) -> DataBlock {
        if let Block::Data(db) = self {
            db
        } else {
            panic!("Block is empty")
        }
    }

    async fn get_page(&mut self, page_idx: usize) -> (&mut Page, bool) {
        let start = Instant::now();
        debug_assert!(!matches!(self, Block::Empty));
        debug!("try to get a page from block.");
        if let Block::Data(db) = self {
            let mut new_one = false;
            if matches!(db.pages[page_idx], None) {
                let page = GLOBAL_PAGE_POOL.acquire_page().await;
                db.pages[page_idx] = Some(page);
                new_one = true;
            };
            debug!("get a page from block, cost: {:?}", start.elapsed());
            (db.pages[page_idx].as_mut().unwrap(), new_one)
        } else {
            panic!("Block is empty");
        }
    }

    fn update_len(&mut self, len: usize) {
        if let Block::Data(db) = self {
            db.length = len;
        }
    }

    fn get_len(&self) -> usize {
        if let Block::Data(db) = self {
            db.length
        } else {
            panic!("Block is empty")
        }
    }

    fn is_full(&self) -> bool {
        match self {
            Block::Empty => false,
            Block::Data(db) => db.length == BLOCK_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, TryStreamExt};
    use kiseki_utils::logger::install_fmt_log;
    use tracing::info;

    use super::*;
    use kiseki_types::new_mem_object_storage;

    #[tokio::test]
    async fn basic_write() {
        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());
        println!("status {:?}", slice_buffer.status());

        let write_len = slice_buffer.write_at(PAGE_SIZE - 3, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, PAGE_SIZE + data.len() - 3);
        println!("status {:?}", slice_buffer.status());

        let write_len = slice_buffer
            .write_at(PAGE_SIZE - 3, vec![1u8; PAGE_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, PAGE_SIZE);
        assert_eq!(slice_buffer.length, PAGE_SIZE - 3 + PAGE_SIZE);
        println!("status {:?}", slice_buffer.status());

        let write_len = slice_buffer
            .write_at(BLOCK_SIZE - 3, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE + BLOCK_SIZE - 3);
        println!("status {:?}", slice_buffer.status());
    }

    #[tokio::test]
    async fn basic_read() {
        let mut slice_buffer = SliceBuffer::new();
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

    #[tokio::test]
    async fn flush() {
        install_fmt_log();

        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());

        let my_id = kiseki_utils::random_id();
        let key_gen = |block_idx: BlockIndex, block_size: BlockSize| {
            let key = format!(
                "{:08X}_{:08X}_{:08X}_{:08X}_{:08X}",
                // we can overwrite a slice, so we need to avoid the conflict.
                my_id / 1000 / 1000,
                my_id / 1000,
                my_id,
                block_idx,
                block_size
            );
            info!("generate key: {}", &key);
            key
        };

        let root = &format!("{}", my_id);
        let object_sto = new_mem_object_storage(root);

        slice_buffer
            .flush_bulk(key_gen, object_sto.clone())
            .await
            .unwrap();
        assert_eq!(slice_buffer.flushed_length, 0); // we have nothing to flush

        let write_len = slice_buffer
            .write_at(0, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE);
        assert_eq!(slice_buffer.total_page_cnt, BLOCK_SIZE / PAGE_SIZE);
        let released_page_cnt = slice_buffer
            .flush_bulk(key_gen, object_sto.clone())
            .await
            .unwrap();
        assert_eq!(released_page_cnt, BLOCK_SIZE / PAGE_SIZE);
        // we cannot write at the flushed block ever again.
        assert!(slice_buffer.write_at(0, b"hello".as_slice()).await.is_err()); // we cannot write at the flushed block ever again.
                                                                               // we should be able to write the next block
        let write_len = slice_buffer
            .write_at(BLOCK_SIZE, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE * 2);
        assert_eq!(slice_buffer.total_page_cnt, BLOCK_SIZE / PAGE_SIZE);

        let released_page_cnt = slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap(); // try to flush all
        assert_eq!(released_page_cnt, BLOCK_SIZE / PAGE_SIZE);
        assert_eq!(slice_buffer.flushed_length, BLOCK_SIZE * 2);
        assert_eq!(slice_buffer.total_page_cnt, 0);

        // we can flush it again, but we have nothing to flush.
        let released_page_cnt = slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap(); // try to flush all
        assert_eq!(released_page_cnt, 0);
        assert_eq!(slice_buffer.flushed_length, BLOCK_SIZE * 2);
        assert_eq!(slice_buffer.total_page_cnt, 0);

        let mut lister = object_sto.lister("").await.unwrap();
        while let Some(entry) = lister.try_next().await.unwrap() {
            info!("entry: {:?}", entry.path());
        }
    }

    #[tokio::test]
    async fn basic_rw() {
        install_fmt_log();
        let my_id = kiseki_utils::random_id();
        let key_gen = |block_idx: BlockIndex, block_size: BlockSize| {
            let key = format!(
                "{:08}_{:08}_{:08}_{:08}_{:08}",
                // we can overwrite a slice, so we need to avoid the conflict.
                my_id / 1000 / 1000,
                my_id / 1000,
                my_id,
                block_idx,
                block_size
            );
            info!("generate key: {}", &key);
            key
        };

        let root = &format!("{}", my_id);
        let object_sto = new_mem_object_storage(root);

        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello world".as_slice();

        let write_len = slice_buffer.write_at(0, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());

        let write_len = slice_buffer.write_at(BLOCK_SIZE - 3, &data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, BLOCK_SIZE - 3 + data.len());

        slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap();

        let mut dst = vec![0u8; data.len()];
        let read_len = read_slice_from_object_storage(
            key_gen,
            object_sto.clone(),
            slice_buffer.length,
            0,
            dst.as_mut_slice(),
        )
        .await
        .unwrap();
        assert_eq!(read_len, data.len());
        assert_eq!(dst, data);

        let mut dst = vec![0u8; data.len()];
        let read_len = read_slice_from_object_storage(
            key_gen,
            object_sto.clone(),
            slice_buffer.length,
            BLOCK_SIZE - 3,
            dst.as_mut_slice(),
        )
        .await
        .unwrap();
        assert_eq!(read_len, data.len());
        assert_eq!(dst, data);
    }
}
