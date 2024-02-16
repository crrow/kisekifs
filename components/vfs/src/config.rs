use std::time::Duration;

use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, PAGE_BUFFER_SIZE, PAGE_SIZE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub backup_meta_interval: Duration,
    pub prefix_internal: bool,
    pub hide_internal: bool,

    // attributes cache timeout in seconds
    pub attr_timeout: Duration,
    // dir entry cache timeout in seconds
    pub dir_entry_timeout: Duration,
    // file entry cache timeout in seconds
    pub file_entry_timeout: Duration,

    // ========Object Storage Configs ===>
    pub object_storage_dsn: String,

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
            backup_meta_interval: Default::default(),
            prefix_internal: false,
            hide_internal: false,
            attr_timeout: Duration::from_secs(1),
            dir_entry_timeout: Duration::from_secs(1),
            file_entry_timeout: Duration::from_secs(1),
            object_storage_dsn: "/tmp/kiseki.object_storage".to_string(),
            capacity: 100 << 10,
            total_buffer_capacity: PAGE_BUFFER_SIZE, // 300MB
            chunk_size: CHUNK_SIZE,                  // 64MB
            block_size: BLOCK_SIZE,                  // 4MB
            page_size: PAGE_SIZE,                    // 64KB
        }
    }
}

/// Divide cpu num by a non-zero `divisor` and returns at least 1.
fn divide_num_cpus(divisor: usize) -> usize {
    debug_assert!(divisor > 0);
    let cores = kiseki_utils::num_cpus();
    debug_assert!(cores > 0);
    (cores + divisor - 1) / divisor
}
