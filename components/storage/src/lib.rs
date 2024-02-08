mod buffer_pool;
mod error;
mod write_buffer;

pub const PAGE_SIZE: usize = 128 << 10;
// The max block size is 4MB.
pub const BLOCK_SIZE: usize = 4 << 20;
// The max size of a slice buffer can grow.
pub const CHUNK_SIZE: usize = 64 << 20;

pub struct Storage {}

impl Storage {}
