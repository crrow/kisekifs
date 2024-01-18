mod attr;
mod entry;
mod ino;

pub use attr::*;
pub use entry::*;
pub use ino::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct DirStat {
    pub(crate) length: i64,
    pub(crate) space: i64,
    pub(crate) inodes: i64,
}

impl DirStat {
    pub(crate) fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<Self, bincode::Error> {
        let ds: DirStat = bincode::deserialize(buf)?;
        Ok(ds)
    }
}

#[derive(Debug, Default)]
pub(crate) struct FSStates {
    /// Represents the total amount of storage space in bytes allocated for the
    /// file system.
    pub total_space: u64,
    /// Represents the amount of free storage space in bytes available for new
    /// data.
    pub avail_space: u64,
    /// Represents the used of inodes.
    pub used_inodes: u64,
    /// Represents the number of available inodes that can be used for new files
    /// or directories.
    pub available_inodes: u64,
}
