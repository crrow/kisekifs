use tokio::time::Instant;

use crate::meta::types::{Entry, Ino};

#[derive(Debug)]
pub(crate) struct Handle {
    pub fh: u64,
    pub inode: Ino,
    pub children: Vec<Entry>,
    pub read_at: Option<Instant>,
}

impl Handle {
    pub fn new(fh: u64, inode: Ino) -> Self {
        Self {
            fh,
            inode,
            children: Vec::new(),
            read_at: None,
        }
    }
}
