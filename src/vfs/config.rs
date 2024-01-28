use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::vfs::{
    storage,
    storage::{BufferManagerConfig, StoEngine},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VFSConfig {
    pub attr_timeout: Duration,
    pub dir_entry_timeout: Duration,
    pub entry_timeout: Duration,
    pub backup_meta_interval: Duration,
    pub prefix_internal: bool,
    pub hide_internal: bool,

    // for writer
    pub total_buffer_cap: usize,
    // the size of chunk.
    pub chunk_size: usize,
    // the size of block which will be uploaded to object storage.
    pub block_size: usize,
    // the smallest alloc size of the write buffer.
    pub page_size: usize,
}

impl VFSConfig {
    pub(crate) fn buffer_manager_config(&self) -> BufferManagerConfig {
        BufferManagerConfig {
            total_buffer_capacity: self.total_buffer_cap,
            chunk_size: self.chunk_size,
            block_size: self.block_size,
            page_size: self.page_size,
        }
    }

    pub(crate) fn debug_sto_engine(&self) -> Arc<dyn StoEngine> {
        storage::new_debug_sto()
    }
}
