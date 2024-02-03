mod file_cache;
mod write_cache;

use std::{fmt::Debug, sync::Arc};

use moka::future::Cache;

use crate::{
    meta::types::{Slice, SliceID},
    vfs::storage::buffer::Block,
};

/// The cache manager.
pub(crate) struct CacheManager {}
