use std::fmt::{Debug, Formatter};

use serde::{Deserialize, Serialize};

use kiseki_utils::readable_size::ReadableSize;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DirStat {
    pub length: i64,
    pub space: i64,
    pub inodes: i64,
}

/// [FSStat] represents the filesystem statistics.
#[derive(Clone, Copy)]
pub struct FSStat {
    /// Represents the total available size.
    pub total_size: u64,
    /// Represents the used size.
    pub used_size: u64,
    /// Represents the total used file count.
    pub file_count: u64,
}

impl Debug for FSStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FSStat")
            .field("total_size", &ReadableSize(self.total_size.clone()))
            .field("used_size", &ReadableSize(self.used_size.clone()))
            .field("file_count", &self.file_count)
            .finish()
    }
}

impl Default for FSStat {
    fn default() -> Self {
        FSStat {
            total_size: u64::MAX,
            used_size: 0,
            file_count: 0,
        }
    }
}
