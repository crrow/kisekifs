use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VFSConfig {
    pub attr_timeout: Duration,
    pub dir_entry_timeout: Duration,
    pub entry_timeout: Duration,
    pub backup_meta_interval: Duration,
    pub prefix_internal: bool,
    pub hide_internal: bool,

    // for writer
    pub write_buffer_size: usize,
    // the size of chunk.
    pub chunk_size: usize,
    // the size of block which will be uploaded to object storage.
    pub block_size: usize,
}
