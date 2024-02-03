use crate::meta::engine::MetaEngine;
use crate::meta::types::{Ino, OverlookedSlicesRef};
use crate::vfs::storage::Engine;
use crate::vfs::{err::Result, VFSError, FH};
use dashmap::DashMap;
use itertools::Itertools;
use std::cmp::min;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use tracing::warn;

type ChunkIdx = usize;

pub(crate) type Handle2FileReadersRef = Arc<DashMap<(Ino, FH), Arc<FileReaderV2>>>;

/// A [FileReaderV2] is used for read content from a file.
/// Each [FileReaderV2] is held by a FileHandle: Ino+Fh.
#[derive(Debug)]
pub(crate) struct FileReaderV2 {
    engine: Weak<Engine>,
    // The file handle.
    fh: FH,
    // The file inode.
    ino: Ino,
    // The max file read length, it was set when we crate the file handle.
    length: usize,
    // A file be divided into multiple chunks,
    // each chunk is composed by multiple slices.
    // This map is used to store latest slices that compose the chunk.
    chunks: DashMap<ChunkIdx, OverlookedSlicesRef>,
    // The file is closing or not.
    closing: AtomicBool,
}

impl FileReaderV2 {
    pub(crate) async fn read(self: &Arc<Self>, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let expected_read_len = dst.len();
        // read offset should not exceed the file length.
        if offset >= self.length || expected_read_len == 0 {
            return Ok(0);
        }

        // cal the real read length.
        let expected_read_len = if offset + expected_read_len > self.length {
            self.length - offset
        } else {
            expected_read_len
        };

        // get the slice inside the chunk.
        let engine = self.engine.upgrade().expect("engine should not be dropped");
        let meta_engine = engine.meta_engine.clone();
        let chunk_size = engine.config.chunk_size;
        let start_chunk_idx = offset / chunk_size;
        let end_chunk_idx = (offset + expected_read_len - 1) / chunk_size;
        let mut chunk_pos = offset % chunk_size;

        let mut total_read_len = 0;
        let mut left_to_read = expected_read_len;

        for chunk_idx in start_chunk_idx..=end_chunk_idx {
            let max_can_read = min(chunk_size - chunk_pos, left_to_read);
            let current_read_range = chunk_pos..chunk_pos + max_can_read;

            let raw_slices = meta_engine
                .read_slice(self.ino, chunk_idx)
                .await?
                .expect("slices should not be none");
            let range_map = raw_slices.overlook();
            let gaps = range_map.gaps(&current_read_range).collect_vec();
            if gaps.len() != 0 {
                warn!("we should not found a hole in the slices once we have the length");
                return Err(VFSError::ErrLIBC { kind: libc::EIO });
            }

            let mut inner_chunk_pos = chunk_pos;
            let mut inner_left_to_read = max_can_read;
            for (r, s) in range_map.overlapping(&current_read_range) {
                // we should use underlying size here, otherwise, we will fail to fetch the slice data.
                let rb = engine.new_read_buffer(s.get_id(), s.get_underlying_size());
                // at present, don't consider the Borrowed slice.
                let offset_of_slice = if r.start < inner_chunk_pos {
                    inner_chunk_pos - r.start
                } else {
                    0
                };
                let max_read_len = min(inner_left_to_read, r.end - r.start - offset_of_slice);
                let n = rb.read_at(
                    offset_of_slice,
                    &mut dst[total_read_len..total_read_len + max_read_len],
                )?;
                assert_eq!(n, max_read_len);
                inner_chunk_pos += max_read_len;
                inner_left_to_read -= max_read_len;
                total_read_len += max_read_len;
            }

            chunk_pos = offset + max_can_read;
            left_to_read -= max_can_read;
        }

        Ok(total_read_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::install_fmt_log;
    use crate::meta::types::ROOT_INO;
    use crate::meta::{Format, MetaConfig, MetaContext};
    use crate::vfs::storage::{new_debug_sto, EngineConfig};

    #[test]
    fn get_from_range_map() {
        let mut range_map = rangemap::RangeMap::new();
        range_map.insert(0..10, 1);

        let x = range_map.get(&1);
        assert_eq!(*x.unwrap(), 1);

        let x = range_map.get(&10);
        assert!(x.is_none())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read() {
        install_fmt_log();

        let meta_engine = MetaConfig::test_config().open().unwrap();
        let meta_ctx = MetaContext::background();
        let format = Format::default();
        meta_engine.init(format, false).await.unwrap();

        let (inode, attr) = meta_engine
            .create(&meta_ctx, ROOT_INO, "a", 0o650, 0, 0)
            .await
            .unwrap();

        let sto_engine = new_debug_sto();
        let engine = Arc::new(Engine::new(
            Arc::new(EngineConfig::default()),
            sto_engine,
            Arc::new(meta_engine),
        ));

        engine.new_file_writer(inode, 0);

        let data = b"hello world" as &[u8];
        let write_len = engine
            .write(inode, 0, data)
            .await
            .map_err(|e| println!("{}", e))
            .unwrap();

        let fw = engine.get_file_writer(inode).unwrap();
        fw.do_flush().await.unwrap();

        let file_reader = Arc::new(FileReaderV2 {
            engine: Arc::downgrade(&engine),
            fh: 0,
            ino: inode,
            length: write_len,
            chunks: Default::default(),
            closing: Default::default(),
        });

        let mut read_data = vec![0u8; 11];
        let read_len = file_reader.read(0, read_data.as_mut_slice()).await.unwrap();
        assert_eq!(read_len, 11);
        assert!(read_data.starts_with(b"hello world"));
        println!("{}", String::from_utf8_lossy(&read_data));

        let chunk_size = engine.config.chunk_size;
        let write_len = engine
            .write(inode, chunk_size - 3, data)
            .await
            .map_err(|e| println!("{}", e))
            .unwrap();
        assert_eq!(write_len, data.len());
        // fw.do_flush().await.unwrap();

        let mut read_data = vec![0u8; 11];
        let read_len = file_reader
            .read(chunk_size - 3, read_data.as_mut_slice())
            .await
            .unwrap();
        assert_eq!(read_len, 11);
        assert!(read_data.starts_with(b"hello world"));
        println!("{}", String::from_utf8_lossy(&read_data));
    }
}
