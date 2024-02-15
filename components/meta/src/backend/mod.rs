use kiseki_common::ChunkIndex;
use kiseki_types::entry::DEntry;
use kiseki_types::stat::DirStat;
use kiseki_types::{attr::InodeAttr, ino::Ino, setting::Format, slice::Slices, stat, FileType};
use std::path::Path;
use std::sync::Arc;

use crate::{backend::key::Counter, err::Result};

pub mod key;
#[cfg(feature = "meta-rocksdb")]
mod rocksdb;

// TODO: optimize me
pub fn open_backend<P: AsRef<Path>>(path: P) -> Result<BackendRef> {
    let mut builder = rocksdb::Builder::default();
    builder.with_path(path.as_ref());
    builder.build()
}

pub type BackendRef = Arc<dyn Backend>;

pub trait Backend {
    fn set_format(&self, format: &Format) -> Result<()>;
    fn load_format(&self) -> Result<Format>;

    fn increase_count_by(&self, counter: Counter, step: usize) -> Result<u64>;
    fn load_count(&self, counter: Counter) -> Result<u64>;

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr>;
    fn set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()>;

    fn get_entry_info(&self, parent: Ino, name: &str) -> Result<DEntry>;
    fn set_dentry(&self, parent: Ino, name: &str, inode: Ino, typ: FileType) -> Result<()>;
    fn list_entry_info(&self, parent: Ino) -> Result<Vec<DEntry>>;

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()>;
    fn get_symlink(&self, inode: Ino) -> Result<String>;

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()>;
    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Option<Vec<u8>>>;
    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices>;

    fn set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()>;
    fn get_dir_stat(&self, inode: Ino) -> Result<DirStat>;
}
