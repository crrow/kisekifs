pub mod attr;
pub mod ino;
pub mod slice;

pub const PAGE_BUFFER_SIZE: usize = 300 << 20; // 300MiB
                                               // pub const PAGE_SIZE: usize = 64 << 10;
pub const PAGE_SIZE: usize = 128 << 10;
// The max block size is 4MB.
pub const BLOCK_SIZE: usize = 4 << 20;
// The max size of a slice buffer can grow.
pub const CHUNK_SIZE: usize = 64 << 20;

pub const MAX_FILE_SIZE: usize = CHUNK_SIZE << 31;

pub fn cal_chunk_idx(offset: usize, chunk_size: usize) -> usize {
    offset / chunk_size
}

pub fn cal_chunk_offset(offset: usize, chunk_size: usize) -> usize {
    offset % chunk_size
}

pub type ObjectStorage = opendal::Operator;
pub type LocalStorage = opendal::Operator;

pub fn new_mem_object_storage(root: &str) -> ObjectStorage {
    let mut builder = opendal::services::Memory::default();
    builder.root(root);
    opendal::Operator::new(builder).unwrap().finish()
}

pub type BlockIndex = usize;
pub type BlockSize = usize;

pub type ChunkIndex = usize;
pub type FileOffset = usize;
