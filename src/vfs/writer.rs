use std::time::SystemTime;
use tracing::debug;

use crate::meta::types::Ino;
use crate::vfs::err::Result;

#[derive(Debug, Default)]
pub(crate) struct DataWriter {}

impl DataWriter {
    pub fn get_length(&self, ino: Ino) -> u64 {
        debug!("writer get_length do nothing");
        return 0;
    }

    pub fn update_mtime(&self, ino: Ino, mtime: SystemTime) -> Result<()> {
        // TODO: implement me
        return Ok(());
    }
}
