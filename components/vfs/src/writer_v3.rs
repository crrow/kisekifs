//! # Writer for VFS
//!
//! ## Logic Design
//! A File is divide into chunks, and each chunk is divide into slices.
//! Each chunk has a fixed size, slice size is not fixed, but it cannot exceed
//! the chunk size. Each slice is a continuous piece of data, sequentially write
//! can extend the slice, for random write, we basically always need to create a
//! new slice. In this way, we convert the random write to sequential write.
//!
//! ## Key Point
//! 1. JuiceFS commit the Slice in order, i don't know what they are doing.
//!    Since they already have a monotonically increasing SliceID, they can just
//!    rebuild the right order of the slices, very strange. So in our
//!    implementation, we just commit the Slice randomly.
//! 2. fuse::Flush should try to commit all SliceWriter, return until all
//!    SliceWriter have been committed.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

use kiseki_common::ChunkIndex;

struct FileWriter {
    total_sw: Arc<AtomicUsize>,
    chunks: RwLock<HashMap<ChunkIndex, ChunkWriter>>,
}

struct ChunkWriter {
    // use BtreeMap to find the latest SliceWriter we can write.
    slices: BTreeMap<SliceWriterID, Arc<SliceWriter>>,
}

type SliceWriterID = u64;

struct SliceWriter {}
