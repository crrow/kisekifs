use std::{fmt::Debug, sync::Arc, time::SystemTime};

use dashmap::DashMap;
use kiseki_storage::slice_buffer::SliceBufferWrapper;
use opendal::Operator;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use kiseki_types::{ino::Ino, slice::SliceID, BLOCK_SIZE, CHUNK_SIZE, PAGE_SIZE};

use crate::{
    meta::engine::MetaEngine,
    vfs::{
        err::Result,
        storage::{
            buffer::ReadBuffer, new_juice_builder, reader::FileReadersRef, writer::FileWritersRef,
            Cache, WriteBuffer,
        },
    },
};

const DEFAULT_BUFFER_CAPACITY: usize = 300 << 20; // 300 MiB

/// The configuration of the storage [Engine].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // ========Cache Configs ===>
    pub capacity: usize,

    // ========Buffer configs ===>
    /// The total memory size for the write/read buffer.
    pub total_buffer_capacity: usize,
    /// chunk_size is the max size can one buffer
    /// hold no matter it is for reading or writing.
    pub chunk_size: usize,
    /// block_size is the max size when we upload
    /// the data to the cloud.
    ///
    /// When the data is not enough to fill the block,
    /// then the block size is equal to the data size,
    /// for example, the last block of the file.
    pub block_size: usize,
    /// The page_size can be also called as the MIN_BLOCK_SIZE,
    /// which is the min size of the block.
    ///
    /// And under the hood, the block is divided into pages.
    pub page_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            capacity: 100 << 10,
            total_buffer_capacity: DEFAULT_BUFFER_CAPACITY, // 300MB
            chunk_size: CHUNK_SIZE,                         // 64MB
            block_size: BLOCK_SIZE,                         // 4MB
            page_size: PAGE_SIZE,                           // 64KB
        }
    }
}

/// Divide cpu num by a non-zero `divisor` and returns at least 1.
fn divide_num_cpus(divisor: usize) -> usize {
    debug_assert!(divisor > 0);
    let cores = num_cpus::get();
    debug_assert!(cores > 0);
    (cores + divisor - 1) / divisor
}

/// The core logic of the storage engine which support the vfs.
pub(crate) struct Engine {
    pub(crate) config: Arc<Config>,
    object_storage: Operator,
    pub(crate) meta_engine: Arc<MetaEngine>,
    pub(crate) cache: Arc<dyn Cache>,
    pub(crate) file_writers: FileWritersRef,
    pub(crate) file_readers: FileReadersRef,
    pub(crate) id_generator: sonyflake::Sonyflake,
}

impl Engine {
    pub(crate) fn new(
        config: Arc<Config>,
        object_storage: Operator,
        meta_engine: Arc<MetaEngine>,
    ) -> Result<Engine> {
        let file_writers = Arc::new(DashMap::new());
        let id_generator = sonyflake::Sonyflake::new().expect("failed to create id generator");
        let cache = new_juice_builder().with_root_cache_dir("/tmp/jc").build()?;

        Ok(Engine {
            config,
            object_storage,
            meta_engine,
            cache,
            file_writers,
            file_readers: Arc::new(Default::default()),
            id_generator,
        })
    }

    pub(crate) fn new_write_buffer(&self) -> WriteBuffer {
        WriteBuffer::new(
            self.get_config(),
            self.cache.clone(),
            self.object_storage.clone(),
        )
    }

    pub(crate) fn new_slice_buffer_wrapper(&self) -> SliceBufferWrapper {
        SliceBufferWrapper::new(self.object_storage.clone())
    }

    pub(crate) fn new_read_buffer(&self, sid: SliceID, length: usize) -> ReadBuffer {
        ReadBuffer::new(self.get_config(), self.object_storage.clone(), sid, length)
    }

    fn get_config(&self) -> Arc<Config> {
        self.config.clone()
    }
}

// TODO: unimplemented

impl Engine {
    pub(crate) fn get_length(self: &Arc<Self>, ino: Ino) -> u64 {
        self.file_writers
            .get(&ino)
            .map_or(0, |w| w.value().get_length() as u64)
    }

    pub(crate) fn update_mtime(self: &Arc<Self>, ino: Ino, mtime: SystemTime) -> Result<()> {
        debug!("update_mtime do nothing, {ino}: {:?}", mtime);
        Ok(())
    }
}
