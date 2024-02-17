use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DirStat {
    pub length: i64,
    pub space: i64,
    pub inodes: i64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FSStat {
    /// Represents the total available size.
    pub total_size: u64,
    /// Represents the used size.
    pub used_size: u64,
    /// Represents the total used file count.
    pub file_count: u64,
}
