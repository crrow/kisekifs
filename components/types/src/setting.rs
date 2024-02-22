use std::fmt::Display;

use serde::{Deserialize, Serialize};

use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, KISEKI, PAGE_SIZE};

/// [Format] can be thought of as the configuration of the filesystem.
/// We can set up different filesystems with different configurations
/// on the same infrastructure, kind of like tenants. We can use
/// Rocksdb's column family to implement this feature. But tikv doesn't
/// open that feature yet. So there may some work to implement that.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Format {
    /// [name] of the filesystem
    pub name: String,

    /// [chunk_size] is the max size can one buffer
    /// hold no matter it is for reading or writing.
    pub chunk_size: usize,
    /// [block_size] is the max object size when we upload
    /// the file content data to the remote.
    ///
    /// When the data is not enough to fill the block,
    /// then the block size is equal to the data size,
    /// for example, the last block of the file.
    pub block_size: usize,
    /// [page_size] can be also called as the MIN_BLOCK_SIZE,
    /// which is the min size of the block. Since under the hood,
    /// each block is divided into fixed size pages.
    pub page_size: usize,

    /// [max_capacity] set limit on the capacity of the filesystem
    pub max_capacity: Option<usize>,
    /// [max_inodes] set limit on the number of inodes
    pub max_inodes: Option<usize>,
    /// [trash_days] days to keep trash
    pub trash_days: usize,
    /// [cache_dir_stat] cache the dir stats or not, which is necessary
    /// for fast summary and dir quota.
    pub cache_dir_stat: bool,
}

impl Default for Format {
    fn default() -> Self {
        Format {
            name: String::from(KISEKI),
            chunk_size: CHUNK_SIZE, // 64MB
            block_size: BLOCK_SIZE, // 4MB
            page_size: PAGE_SIZE,   // 64KB
            max_capacity: None,
            max_inodes: None,
            trash_days: 0,
            cache_dir_stat: false,
        }
    }
}

impl Format {
    pub fn with_name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_string();
        self
    }
}
