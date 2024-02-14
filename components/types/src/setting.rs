use kiseki_common::{BLOCK_SIZE, KISEKI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// FsSetting is the setting of the filesystem.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Format {
    // custom name of the filesystem
    pub name: String,
    // block size of the filesystem
    pub block_size: usize,
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
            block_size: BLOCK_SIZE,
            max_capacity: None,
            max_inodes: None,
            trash_days: 0,
            dir_stats: false,
        }
    }
}
