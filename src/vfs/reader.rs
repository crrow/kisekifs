use std::sync::Weak;
use tracing::debug;

use crate::meta::types::Ino;

#[derive(Debug, Default)]
pub(crate) struct DataReader {}

impl DataReader {
    pub(crate) fn open(&self, inode: Ino, length: u64) -> Weak<FileReader> {
        debug!("data reader do nothing inode: {inode}, length: {length}");
        return Weak::new();
    }
    pub(crate) fn truncate(&self, inode: Ino, length: u64) {
        debug!("reader truncate do nothing, inode: {inode}, length: {length}");
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct FileReader {}
