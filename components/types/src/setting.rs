use std::fmt::Display;

use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, KISEKI, PAGE_SIZE};
use serde::{Deserialize, Serialize};

/// FsSetting is the setting of the filesystem.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Format {
    // custom name of the filesystem
    pub name: String,

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

    // set limit on the capacity of the filesystem
    pub max_capacity: Option<usize>,
    // set limit on the number of inodes
    pub max_inodes: Option<usize>,
    // days to keep trash
    pub trash_days: usize,
    // cache dir stats?
    pub dir_stats: bool,
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
            dir_stats: false,
        }
    }
}

impl Format {
    pub fn with_name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_string();
        self
    }
}
