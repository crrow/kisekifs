use crate::meta::Ino;

#[derive(Debug, Default)]
pub struct DataReader {}

impl DataReader {
    pub fn truncate(&self, inode: Ino, length: u64) {
        todo!()
    }
}
