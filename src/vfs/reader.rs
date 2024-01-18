use tracing::debug;

use crate::meta::types::Ino;

#[derive(Debug, Default)]
pub struct DataReader {}

impl DataReader {
    pub fn truncate(&self, inode: Ino, length: u64) {
        debug!("reader truncate do nothing")
    }
}
