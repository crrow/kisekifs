mod juice_cache;
mod moka_cache;
mod stack_cache;
mod write_cache;

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use opendal::Reader;

use crate::{meta::types::SliceID, vfs::err::Result};

pub fn new_juice_builder() -> juice_cache::JuiceFileCacheBuilder {
    juice_cache::JuiceFileCacheBuilder::default()
}

/// The exposed cache trait.
#[async_trait]
pub trait Cache: Send + Sync + Debug + Unpin + 'static {
    /// The cache operation is called for [WriteBehind].
    async fn cache(&self, slice_id: u64, block: Arc<Vec<u8>>) -> bool;
    async fn get(&self, slice_id: SliceID) -> Option<Reader>;
    async fn wait_on_all_flush_finish(&self);
    /// close the cache and wait on all background task exit.
    async fn close(&self);
    /// Remove the slice from the cache.
    async fn remove(&self, slice_id: SliceID);
    /// Stage is used for [WriteBack], in this case, we flush data to
    /// the remote in the background.
    ///
    /// When we flush the stage data to the remote, we should remove the stage
    /// date.
    async fn stage(&self, slice_id: SliceID, data: Arc<Vec<u8>>, keep_cache: bool) -> Result<()>;
}

/// The cache manager.
pub(crate) struct CacheManager {}
