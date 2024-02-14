use kiseki_common::ChunkIndex;
use kiseki_types::{attr::InodeAttr, entry::EntryInfo, ino::Ino, setting::Format, slice::Slices};
use std::sync::Arc;

use crate::{backend::key::Counter, err::Result};

pub mod key;
#[cfg(feature = "meta-rocksdb")]
mod rocksdb;

pub type BackendRef = Arc<dyn Backend>;

pub trait Backend {
    fn change_format(&self, format: &Format) -> Result<()>;
    fn load_format(&mut self, name: &str) -> Result<Format>;

    fn increase_count_by(&self, counter: Counter, step: usize) -> Result<u64>;
    fn load_count(&self, counter: Counter) -> Result<u64>;

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr>;
    fn set_attr(&self, inode: Ino, attr: InodeAttr) -> Result<()>;

    fn get_entry_info(&self, parent: Ino, name: &str) -> Result<EntryInfo>;
    fn set_entry_info(&self, parent: Ino, name: &str, entry_info: EntryInfo) -> Result<()>;
    fn list_entry_info(&self, parent: Ino) -> Result<Vec<EntryInfo>>;

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()>;
    fn get_symlink(&self, inode: Ino) -> Result<String>;

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()>;
    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Vec<u8>>;
    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices>;
}
