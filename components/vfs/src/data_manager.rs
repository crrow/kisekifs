use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::SystemTime,
};

use kiseki_common::FH;
use kiseki_meta::MetaEngineRef;
// use kiseki_storage::cache::CacheRef;
use kiseki_storage::cache;
use kiseki_storage::cache::file_cache::{FileCache, FileCacheRef};
use kiseki_types::ino::Ino;
use kiseki_utils::object_storage::ObjectStorage;
use snafu::OptionExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    err::Result,
    reader::FileReadersRef,
    writer::{FileWriter, FileWritersRef},
};

pub(crate) type DataManagerRef = Arc<DataManager>;

/// DataManager is responsible for managing the data of the VFS.
pub(crate) struct DataManager {
    pub(crate) page_size: usize,
    pub(crate) block_size: usize,
    pub(crate) chunk_size: usize,
    pub(crate) file_writers: FileWritersRef,
    pub(crate) file_readers: FileReadersRef,
    pub(crate) id_generator: Arc<sonyflake::Sonyflake>,
    // Dependencies
    pub(crate) meta_engine: MetaEngineRef,
    pub(crate) object_storage: ObjectStorage,
    pub(crate) file_cache: FileCacheRef,
    // pub(crate) data_cache: CacheRef,
}

impl DataManager {
    pub(crate) fn new(
        page_size: usize,
        block_size: usize,
        chunk_size: usize,
        meta_engine_ref: MetaEngineRef,
        object_storage: ObjectStorage,
        // cache_ref: CacheRef,
    ) -> Self {
        let remote_storage = object_storage.clone();
        Self {
            page_size,
            block_size,
            chunk_size,
            file_writers: Arc::new(Default::default()),
            file_readers: Arc::new(Default::default()),
            id_generator: Arc::new(sonyflake::Sonyflake::new().unwrap()),
            meta_engine: meta_engine_ref,
            object_storage,
            // data_cache: cache_ref,
            file_cache: Arc::new(
                FileCache::new(cache::file_cache::Config::default(), remote_storage).unwrap(),
            ),
        }
    }

    pub(crate) fn find_file_writer(&self, ino: Ino) -> Option<Arc<FileWriter>> {
        self.file_writers.get(&ino).map(|r| r.value().clone())
    }

    pub(crate) fn get_length(self: &Arc<Self>, ino: Ino) -> u64 {
        self.file_writers
            .get(&(ino))
            .map_or(0, |w| w.value().get_length() as u64)
    }

    pub(crate) fn update_mtime(self: &Arc<Self>, ino: Ino, mtime: SystemTime) -> Result<()> {
        debug!("update_mtime do nothing, {ino}: {:?}", mtime);
        Ok(())
    }
}
