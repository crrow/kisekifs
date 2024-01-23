mod config;
mod engine;
pub use engine::ChunkEngine;
mod err;
mod slice;
pub(crate) use {slice::SliceID, slice::WSlice};

pub use err::ChunkError;

/// In JuiceFS, each file is composed of one or more chunks.
/// Each chunk has a maximum size of 64 MB.
/// Regardless of the file's size, all reads and writes are
/// located based on their offsets (the position in the file
/// where the read or write operation occurs) to the
/// corresponding chunk.
pub(crate) const MAX_CHUNK_SIZE: usize = 1 << 26; // 64 MB
pub(crate) const PAGE_SIZE: usize = 1 << 16; // 64 KB

pub(crate) fn chunk_index(offset: usize) -> usize {
    offset / MAX_CHUNK_SIZE
}
pub(crate) fn chunk_pos(offset: usize) -> usize {
    offset % MAX_CHUNK_SIZE
}

pub(crate) type ChunkIndex = usize;
pub(crate) type ChunkOffset = usize;
