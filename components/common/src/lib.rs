pub const KISEKI: &str = "kiseki";

pub const PAGE_BUFFER_SIZE: usize = 300 << 20; // 300MiB
                                               // pub const PAGE_SIZE: usize = 64 << 10;
pub const PAGE_SIZE: usize = 128 << 10; // 128 KiB
                                        // The max block size is 4MB.
pub const BLOCK_SIZE: usize = 4 << 20; // 4 MiB

pub const MIN_BLOCK_SIZE: usize = PAGE_SIZE; // 128 KiB

pub const MAX_BLOCK_SIZE: usize = 512 << 10; // 16 KiB

// The max size of a slice buffer can grow.
pub const CHUNK_SIZE: usize = 64 << 20; // 64 MiB

pub const MAX_FILE_SIZE: usize = CHUNK_SIZE << 31; // 2 TiB

pub fn cal_chunk_idx(offset: usize, chunk_size: usize) -> usize {
    offset / chunk_size
}

pub fn cal_chunk_offset(offset: usize, chunk_size: usize) -> usize {
    offset % chunk_size
}

pub type BlockIndex = usize;
pub type BlockSize = usize;
pub type ChunkIndex = usize;
pub type ChunkOffset = usize;
pub type FileOffset = usize;
