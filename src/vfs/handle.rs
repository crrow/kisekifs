use crate::meta::Ino;

#[derive(Debug)]
pub(crate) struct Handle {
    pub fh: u64,
    pub inode: Ino,
}

impl Handle {
    pub fn new(fh: u64, inode: Ino) -> Self {
        Self { fh, inode }
    }
}
