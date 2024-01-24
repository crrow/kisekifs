mod config;
pub use config::Config as ChunkConfig;
mod engine;
pub use engine::Engine;
mod err;
mod slice;

pub use err::ChunkError;

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

pub(crate) const MAX_CHUNK_SIZE: usize = 1 << 26; // 64 MB
                                                  // pub(crate) const MAX_BLOCK_SIZE: usize = 1 << 22; // 4 MB TODO: we may need
                                                  // to config the block size.
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 1 << 20; // 1 MB
pub(crate) const MIN_BLOCK_SIZE: usize = 1 << 16; // 64 KB

pub(crate) fn chunk_id(offset: usize) -> usize {
    offset / MAX_CHUNK_SIZE
}
pub(crate) fn chunk_pos(offset: usize) -> usize {
    offset % MAX_CHUNK_SIZE
}

pub(crate) type PageIdx = usize;
pub(crate) type BlockIdx = usize;

/// ChunkID is calculated by the offset / MAX_CHUNK_SIZE.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash)]
pub(crate) struct ChunkID(pub(crate) usize);

impl ChunkID {
    pub(crate) fn new(file_offset: usize) -> Self {
        Self(chunk_id(file_offset))
    }
}

impl Into<usize> for ChunkID {
    fn into(self) -> usize {
        self.0
    }
}

pub(crate) type ChunkOffset = usize;

/// SliceID is a unique identifier for a slice.
pub(crate) type SliceID = usize;
