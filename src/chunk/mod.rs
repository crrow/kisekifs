mod config;
pub use config::Config as ChunkConfig;
mod engine;
pub use engine::Engine;
mod disk_cache;
mod err;
pub mod page;
pub mod slice;

pub use err::ChunkError;

pub mod slice2;

/// Each file is composed of one or more chunks.
/// Each chunk has a maximum size of 64 MB.
///
/// Chunks exist to optimize lookup and positioning,
/// while the actual file writing is performed on
/// slices.
///
/// Each slice represents a single continuous write,
/// belongs to a specific chunk, and cannot overlap
/// between adjacent chunks. This ensures that the
/// slice length never exceeds 64 MB.
///
/// For example, if a file is generated through a
/// continuous sequential write, each chunk contains
/// only one slice.
///
/// File writing generates slices, and invoking flush
/// persists these slices.
///
/// When persisting to the object storage, slices are
/// further split into individual blocks (default
/// maximum size of 4 MB) to enable multi-threaded
/// concurrent writes, thereby enhancing write
/// performance.
///
/// The previously mentioned chunks and slices are
/// logical data structures, while blocks represent
/// the final physical storage form and serve as the
/// smallest storage unit for the object storage and
/// disk cache.

pub fn cal_chunk_idx(offset: usize, chunk_size: usize) -> usize {
    offset / chunk_size
}
pub fn cal_chunk_pos(offset: usize, chunk_size: usize) -> usize {
    offset % chunk_size
}

pub const DEFAULT_CHUNK_SIZE: usize = 1 << 26; // 64 MB
                                               // pub(crate) const MAX_BLOCK_SIZE: usize = 1 << 22; // 4 MB TODO: we may need
                                               // to config the block size.
pub const BLOCK_SIZE: usize = 1 << 20; // 1 MB
pub const PAGE_SIZE: usize = 1 << 16; // 64 KB

pub(crate) type PageIdx = usize;
pub(crate) type BlockIdx = usize;
pub(crate) type ChunkID = usize;
pub(crate) type ChunkOffset = usize;
pub(crate) type SliceID = usize;
