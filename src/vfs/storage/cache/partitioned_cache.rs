use std::sync::Arc;

use dashmap::DashMap;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use snafu::ResultExt;

use crate::vfs::{
    err::{OpenDalSnafu, Result},
    storage::cache::juice_cache::JuiceFileCache,
};

lazy_static! {
    pub static ref ID_GENERATOR: sonyflake::Sonyflake =
        sonyflake::Sonyflake::new().expect("failed to create id generator");
}
const CACHE_NAME_PREFIX: &str = "juice_cache_";

type FileCacheID = u64;

fn random_file_cache_id() -> FileCacheID {
    ID_GENERATOR.next_id().expect("failed to generate id")
}

pub struct PartitionedCacheBuilder {
    // the cache dir for the partitioned cache
    pub cache_dir: String,
}

impl PartitionedCacheBuilder {
    pub async fn inner_build(self) -> Result<PartitionedCache> {
        let mut builder = opendal::services::Fs::default();
        builder.root(&self.cache_dir);
        let obj = opendal::Operator::new(builder)
            .context(OpenDalSnafu)?
            .finish();
        let mut lister = obj.lister(&self.cache_dir).await.context(OpenDalSnafu)?;
        let mut all_caches = vec![];
        while let Some(entry) = lister.try_next().await.context(OpenDalSnafu)? {
            if entry.metadata().mode().is_file() {
                continue;
            }
            if !entry.name().starts_with(CACHE_NAME_PREFIX) {
                continue;
            }
            let cache_id = entry
                .name()
                .trim_start_matches(CACHE_NAME_PREFIX)
                .parse::<FileCacheID>()
                .expect("invalid cache id");
            all_caches.push((cache_id, entry.path().to_string()));
        }
        if all_caches.is_empty() {
            (0..5).for_each(|_| {
                let cache_id = random_file_cache_id();
                let cache_dir = format!("{}/{}{}", self.cache_dir, CACHE_NAME_PREFIX, cache_id);
                all_caches.push((cache_id, cache_dir));
            })
        }

        todo!()
    }

    fn make_juice_cache_builder() {}
}

/// PartitionedCache is composed with multiple normal [JuiceFileCache],
/// and it will dispatch the request to the corresponding cache according to the
/// key.
pub(crate) struct PartitionedCache {
    cache: Arc<DashMap<FileCacheID, JuiceFileCache>>,
}
