mod cache;
mod writer;

pub const DEFAULT_CHUNK_SIZE: usize = 64 << 20; // 64 MiB
pub const BLOCK_SIZE: usize = 4 << 20; // 4 MiB
pub const PAGE_SIZE: usize = 1 << 16; // 64 KiB

pub fn cal_chunk_idx(offset: usize, chunk_size: usize) -> usize {
    offset / chunk_size
}
pub fn cal_chunk_pos(offset: usize, chunk_size: usize) -> usize {
    offset % chunk_size
}

struct Sto {}
