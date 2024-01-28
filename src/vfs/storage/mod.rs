mod buffer;

pub(crate) use buffer::{BufferManager, Config as BufferManagerConfig, WriteBuffer};

mod cache;
mod err;
mod sto;

pub(crate) use sto::{new_debug_sto, StoEngine};

pub(crate) const DEFAULT_CHUNK_SIZE: usize = 64 << 20; // 64 MiB
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4 << 20; // 4 MiB
pub(crate) const DEFAULT_PAGE_SIZE: usize = 64 << 10; // 64 KiB

pub(crate) const MAX_FILE_SIZE: usize = DEFAULT_CHUNK_SIZE << 31;

pub(crate) fn cal_chunk_idx(offset: usize, chunk_size: usize) -> usize {
    offset / chunk_size
}

pub(crate) fn cal_chunk_offset(offset: usize, chunk_size: usize) -> usize {
    offset % chunk_size
}