use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::meta::types::Ino;
use crate::vfs::storage::scheduler::BackgroundTaskPool;
use crate::vfs::storage::worker::Worker;
use crate::vfs::storage::ObjectSto;
use crate::{
    meta::engine::MetaEngine,
    vfs::{
        storage::{
            buffer::ReadBuffer, err::Result, sto::StoEngine, worker, writer::FileWriter,
            WriteBuffer, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_PAGE_SIZE,
        },
        FH,
    },
};

const DEFAULT_BUFFER_CAPACITY: usize = 300 << 20; // 300 MiB

/// The configuration of the storage [Engine].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // Worker configs ===>
    /// Number of region workers (default: 1/2 of cpu cores).
    /// Sets to 0 to use the default value.
    pub number_of_workers: usize,
    /// Request channel size of each worker (default 128).
    pub worker_channel_size: usize,
    /// Max batch size for a worker to handle requests (default 64).
    pub worker_request_batch_size: usize,

    // Buffer configs ===>
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
            number_of_workers: divide_num_cpus(2),
            worker_channel_size: 128,
            worker_request_batch_size: 64,
            total_buffer_capacity: DEFAULT_BUFFER_CAPACITY, // 300MB
            chunk_size: DEFAULT_CHUNK_SIZE,                 // 64MB
            block_size: DEFAULT_BLOCK_SIZE,                 // 4MB
            page_size: DEFAULT_PAGE_SIZE,                   // 64KB
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

pub(crate) type FileWritersRef = Arc<DashMap<FH, Arc<FileWriter>>>;

/// The core logic of the storage engine which support the vfs.
pub(crate) struct Engine {
    pub(crate) config: Arc<Config>,
    object_sto: Arc<dyn StoEngine>,
    meta_engine: Arc<MetaEngine>,
    workers: Worker,
    pub(crate) file_writers: FileWritersRef,
    pub(crate) id_generator: sonyflake::Sonyflake,
}

impl Engine {
    pub(crate) fn new(
        config: Arc<Config>,
        object_sto: Arc<dyn StoEngine>,
        meta_engine: Arc<MetaEngine>,
    ) -> Engine {
        let file_writers = Arc::new(DashMap::new());
        let worker = worker::WorkerStarter {
            id: 0,
            config: config.clone(),
            task_pool_ref: Arc::new(BackgroundTaskPool::start(config.number_of_workers)),
            listener: worker::WorkerListener::default(),
            file_writers: file_writers.clone(),
        }
        .start();
        let id_generator = sonyflake::Sonyflake::new().expect("failed to create id generator");

        Engine {
            config,
            object_sto,
            meta_engine,
            workers: worker,
            file_writers,
            id_generator,
        }
    }

    pub(crate) fn new_write_buffer(&self) -> WriteBuffer {
        WriteBuffer::new(self.get_config(), self.get_object_sto())
    }

    pub(crate) fn new_read_buffer(&self, sid: usize, length: usize) -> ReadBuffer {
        ReadBuffer::new(self.get_config(), self.get_object_sto(), sid, length)
    }

    fn get_config(&self) -> Arc<Config> {
        self.config.clone()
    }

    fn get_object_sto(&self) -> Arc<dyn StoEngine> {
        self.object_sto.clone()
    }

    pub(crate) async fn submit_request(&self, req: worker::WorkerRequest) {
        if let Err(e) = self.workers.submit_request(req).await {
            warn!("submit request failed: {}", e);
        }
    }
}

// TODO: unimplemented

impl Engine {
    pub(crate) fn get_length(self: &Arc<Self>, ino: Ino) -> u64 {
        debug!("get_length do nothing");
        0
    }

    pub(crate) fn update_mtime(self: &Arc<Self>, ino: Ino, mtime: SystemTime) -> Result<()> {
        debug!("update_mtime do nothing, {ino}: {:?}", mtime);
        Ok(())
    }
}
