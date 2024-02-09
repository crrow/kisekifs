use opendal::Operator;

mod buffer_pool;
mod error;
mod slice_buffer;

pub const PAGE_SIZE: usize = 128 << 10;
// The max block size is 4MB.
pub const BLOCK_SIZE: usize = 4 << 20;
// The max size of a slice buffer can grow.
pub const CHUNK_SIZE: usize = 64 << 20;

pub type ObjectStorage = opendal::Operator;
pub type LocalStorage = opendal::Operator;

pub fn new_mem_object_storage(root: &str) -> ObjectStorage {
    let mut builder = opendal::services::Memory::default();
    builder.root(root);
    Operator::new(builder).unwrap().finish()
}

pub type BlockIndex = usize;
pub type BlockSize = usize;

pub struct Storage {}

impl Storage {}
