use std::{
    cmp::min,
    sync::{atomic::AtomicBool, Arc, Weak},
};

use dashmap::DashMap;
use itertools::Itertools;
use rangemap::RangeMap;
use tracing::debug;

use kiseki_types::{
    ino::Ino,
    slice::{OverlookedSlicesRef, Slice},
};

use crate::vfs::{err::Result, storage::Engine, FH};

impl Engine {
    /// Get the file reader for the given inode and file handle.
    pub(crate) fn new_file_reader(
        self: &Arc<Self>,
        inode: Ino,
        fh: FH,
        length: usize,
    ) -> Arc<FileReader> {
        self.file_readers
            .entry((inode, fh))
            .or_insert_with(|| {
                let fr = FileReader::new(self.clone(), inode, fh, length);
                Arc::new(fr)
            })
            .value()
            .clone()
    }

    pub(crate) fn find_file_reader(
        self: &Arc<Self>,
        inode: Ino,
        fh: FH,
    ) -> Option<Arc<FileReader>> {
        self.file_readers
            .get(&(inode, fh))
            .map(|m| m.value().clone())
    }

    pub(crate) fn truncate_reader(self: &Arc<Self>, inode: Ino, length: u64) {
        debug!("DO NOTHING: truncate inode {} to {}", inode, length);
    }
}

type ChunkIdx = usize;

pub(crate) type FileReadersRef = Arc<DashMap<(Ino, FH), Arc<FileReader>>>;

/// A [FileReader] is used for read content from a file.
/// Each [FileReader] is held by a FileHandle: Ino+Fh.
#[derive(Debug)]
pub(crate) struct FileReader {
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

impl FileReader {
    pub(crate) fn new(engine: Arc<Engine>, ino: Ino, fh: FH, length: usize) -> Self {
        Self {
            engine: Arc::downgrade(&engine),
            fh,
            ino,
            length,
            chunks: Default::default(),
            closing: Default::default(),
        }
    }
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

        let mut total_read_len = 0;
        let mut left_to_read = expected_read_len;
        for chunk_idx in start_chunk_idx..=end_chunk_idx {
            // offset to current chunk.
            let mut chunk_pos = (offset + total_read_len) % chunk_size;
            // max can read in current chunk.
            let max_can_read = min(chunk_size - chunk_pos, left_to_read);
            // then we get the range to read in current chunk.
            let current_read_range = chunk_pos..chunk_pos + max_can_read;
            // according to current chunk idx, we can get the slices.
            let raw_slices = meta_engine
                .read_slice(self.ino, chunk_idx)
                .await?
                .expect("slices should not be none");

            for x in raw_slices.0.iter() {
                debug!("find raw-slice in chunk: {:?}, slice: {:?}", chunk_idx, x);
            }

            // make a virtual slice map to record the slice and hole.
            let mut virtual_slice_map = RangeMap::new();
            {
                let overlap = raw_slices.overlook();
                let overlap_slices = overlap.iter().collect_vec();
                println!("overlap_slices: {:?}", overlap_slices);
            }

            let range_map = raw_slices.overlook();
            for x in range_map.gaps(&current_read_range) {
                debug!("current range: {:?}, find gap: {:?}", current_read_range, x);
                virtual_slice_map.insert(x, VirtualSlice::Hole);
            }
            for (r, s) in range_map.overlapping(&current_read_range) {
                virtual_slice_map.insert(r.clone(), VirtualSlice::Slice(s.clone()));
            }
            drop(range_map);

            for (r, vs) in virtual_slice_map {
                let start = total_read_len + r.start - chunk_pos;
                let end = total_read_len + r.end - chunk_pos;
                let len = end - start;

                match vs {
                    VirtualSlice::Hole => {
                        debug!(
                            "chunk_size{}, find hole in chunk: {:?}, range: {:?}",
                            chunk_size, chunk_idx, r,
                        );
                        // we may even don't have to write the 0.
                        for i in r {
                            dst[i - chunk_pos] = 0;
                        }
                    }
                    VirtualSlice::Slice(s) => {
                        debug!(
                            "find slice in chunk: {:?}, range: {:?}, slice: {:?}, write buf [{start}, {end}]",
                            chunk_idx, r, s
                        );
                        let rb = engine.new_read_buffer(s.get_id(), s.get_underlying_size());
                        rb.read_at(0, &mut dst[start..end])?;
                    }
                }
                total_read_len += len;
                left_to_read -= len;
                chunk_pos += len;
            }
        }

        Ok(total_read_len)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum VirtualSlice {
    Hole,
    Slice(Slice),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::{install_fmt_log, new_memory_sto},
        meta::{Format, MetaConfig, MetaContext},
        vfs::storage::EngineConfig,
    };
    use kiseki_types::ino::ROOT_INO;

    #[test]
    fn get_from_range_map() {
        let mut range_map = rangemap::RangeMap::new();
        range_map.insert(0..10, 1);
        range_map.insert(10..12, 2);
        range_map.iter().for_each(|(r, v)| {
            println!("{:?} -> {:?}", r, v);
        });
    }

    #[test]
    fn make_virtual_map() {
        let chunk_size = 1024usize;
        let mut rm = rangemap::RangeMap::new();
        rm.insert(0..3, Slice::new_owned(0, 0, 3));
        rm.insert(12..15, Slice::new_owned(12, 1, 3));
        rm.insert(
            chunk_size - 3..chunk_size,
            Slice::new_owned(chunk_size - 3, 2, 3),
        );
        rm.insert(
            chunk_size..chunk_size + 8,
            Slice::new_owned(chunk_size, 3, 8),
        );

        let mut virtual_slice_map = RangeMap::new();
        let read_range = chunk_size - 4..chunk_size + 8;
        for x in rm.gaps(&read_range) {
            if x.end < read_range.start {
                continue;
            }
            virtual_slice_map.insert(x, VirtualSlice::Hole);
        }
        for (r, s) in rm.overlapping(&read_range) {
            virtual_slice_map.insert(r.clone(), VirtualSlice::Slice(s.clone()));
        }
        for (r, vs) in virtual_slice_map {
            match vs {
                VirtualSlice::Hole => {
                    println!("find hole in range: {:?}", r);
                }
                VirtualSlice::Slice(s) => {
                    println!("find slice in range: {:?}, slice: {:?}", r, s);
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read() {
        install_fmt_log();

        let meta_engine = MetaConfig::test_config().open().unwrap();
        let meta_ctx = MetaContext::background();
        let format = Format::default();
        meta_engine.init(format, false).await.unwrap();

        let (inode, _attr) = meta_engine
            .create(&meta_ctx, ROOT_INO, "a", 0o650, 0, 0)
            .await
            .unwrap();

        let sto_engine = new_memory_sto();
        let engine = Arc::new(
            Engine::new(
                Arc::new(EngineConfig::default()),
                sto_engine,
                Arc::new(meta_engine),
            )
            .unwrap(),
        );

        engine.new_file_writer(inode, 0);
        let data = b"hello world" as &[u8];

        let write_len = engine
            .write(inode, 0, data)
            .await
            .map_err(|e| println!("{}", e))
            .unwrap();

        let fw = engine.find_file_writer(inode).unwrap();
        fw.do_flush().await.unwrap();

        let file_reader = Arc::new(FileReader {
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

        fw.do_flush().await.unwrap();

        let mut read_data = vec![0u8; 11];

        let file_reader = Arc::new(FileReader {
            engine: Arc::downgrade(&engine),
            fh: 0,
            ino: inode,
            length: chunk_size + 8,
            chunks: Default::default(),
            closing: Default::default(),
        });
        let read_len = file_reader
            .read(chunk_size - 3, read_data.as_mut_slice())
            .await
            .unwrap();
        assert_eq!(read_len, 11);
        println!("{}", String::from_utf8_lossy(&read_data));
        assert!(read_data.starts_with(b"hello world"));
    }
}
