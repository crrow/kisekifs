use std::sync::Arc;

use dashmap::DashMap;
use tracing::debug;

use crate::{meta::types::Ino, vfs::storage::Engine};

impl Engine {
    pub(crate) fn truncate_reader(self: &Arc<Self>, inode: Ino, length: usize) {
        debug!("DO NOTHING: truncate inode {} to {}", inode, length);
    }
}

pub(crate) type FileReadersRef = Arc<DashMap<Ino, Arc<FileReader>>>;

/// [FileReader] is responsible for reading the file content.
///
/// We should be able to read the same file concurrently.
pub(crate) struct FileReader {
    inode: Ino,
}
