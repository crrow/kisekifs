use crate::meta::Ino;
use tracing::debug;

#[derive(Debug, Default)]
pub struct DataReader {}

impl DataReader {
    pub fn truncate(&self, inode: Ino, length: u64) {
        debug!("reader truncate do nothing")
    }
}
