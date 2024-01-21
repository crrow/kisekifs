use std::time::SystemTime;

use tracing::debug;

use crate::{meta::types::Ino, vfs::err::Result};

#[derive(Debug, Default)]
pub(crate) struct DataWriter {}

impl DataWriter {
    pub(crate) fn open(&self, inode: Ino, length: u64) -> FileWriter {
        debug!("data writer open do nothing: inode: {inode}, length: {length}");
        FileWriter::default()
    }
    pub(crate) fn get_length(&self, ino: Ino) -> u64 {
        debug!("writer get_length do nothing, inode: {ino}");
        return 0;
    }

    pub(crate) fn update_mtime(&self, ino: Ino, mtime: SystemTime) -> Result<()> {
        // TODO: implement me
        return Ok(());
    }
}

#[derive(Debug, Default)]
pub(crate) struct FileWriter {}
