mod juice_cache;
mod moka_cache;
mod partitioned_cache;
mod write_cache;

use std::{fmt::Debug, hash::Hash, sync::Arc};

use async_trait::async_trait;
use kiseki_types::slice::SliceKey;
use opendal::Reader;

use crate::err::Result;

pub fn new_juice_builder() -> juice_cache::JuiceFileCacheBuilder {
    juice_cache::JuiceFileCacheBuilder::new("/tmp/kiseki.cache")
}

pub type CacheRef = Arc<dyn Cache>;

/// The exposed cache trait.
#[async_trait]
pub trait Cache: Send + Sync + Debug + Unpin + 'static {
    /// The cache operation is called for [WriteBehind].
    async fn cache(&self, key: SliceKey, block: Arc<Vec<u8>>) -> bool;
    async fn get(&self, key: SliceKey) -> Option<Reader>;
    async fn wait_on_all_flush_finish(&self);
    /// close the cache and wait on all background task exit.
    async fn close(&self);
    /// Remove the slice from the cache.
    async fn remove(&self, key: SliceKey);
    /// Stage is used for [WriteBack], in this case, we flush data to
    /// the remote in the background.
    ///
    /// When we flush the stage data to the remote, we should remove the stage
    /// date.
    async fn stage(&self, key: SliceKey, data: Arc<Vec<u8>>, keep_cache: bool) -> Result<()>;
}

/// Manages cached data for the engine.
pub struct CacheManager {}
