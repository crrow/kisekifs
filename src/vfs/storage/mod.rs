mod buffer;

use std::{cmp::Ordering, str::FromStr};

pub(crate) use buffer::WriteBuffer;
use snafu::ResultExt;

mod cache;

pub use cache::{new_juice_builder, Cache};
mod engine;

pub use engine::Config as EngineConfig;
pub(crate) use engine::Engine;
mod reader;
mod writer;

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
