use crate::err::InvalidInoSnafu;
use crate::err::Result;
use crate::reader::FileReadersRef;
use crate::writer::{FileWriter, FileWritersRef};
use kiseki_meta::MetaEngineRef;
use kiseki_storage::cache::CacheRef;
use kiseki_types::ino::Ino;
use kiseki_utils::object_storage::ObjectStorage;
use snafu::OptionExt;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub(crate) type DataManagerRef = Arc<DataManager>;

/// DataManager is responsible for managing the data of the VFS.
pub(crate) struct DataManager {
    pub(crate) page_size: usize,
    pub(crate) block_size: usize,
    pub(crate) chunk_size: usize,
    pub(crate) file_writers: FileWritersRef,
    pub(crate) file_readers: FileReadersRef,
    pub(crate) id_generator: Arc<sonyflake::Sonyflake>,
    /* Dependencies */
    pub(crate) meta_engine: MetaEngineRef,
    pub(crate) object_storage: ObjectStorage,
    pub(crate) data_cache: CacheRef,
}

impl DataManager {
    pub(crate) fn new(
        page_size: usize,
        block_size: usize,
        chunk_size: usize,
        meta_engine_ref: MetaEngineRef,
        object_storage: ObjectStorage,
        cache_ref: CacheRef,
    ) -> Self {
        Self {
            page_size,
            block_size,
            chunk_size,
            file_writers: Arc::new(Default::default()),
            file_readers: Arc::new(Default::default()),
            id_generator: Arc::new(sonyflake::Sonyflake::new().unwrap()),
            meta_engine: meta_engine_ref,
            object_storage,
            data_cache: cache_ref,
        }
    }

    /// Checks whether the file handle is writable.
    pub(crate) fn file_writer_exists(&self, ino: Ino) -> bool {
        self.file_writers.contains_key(&ino)
    }

    pub(crate) fn find_file_writer(&self, ino: Ino) -> Option<Arc<FileWriter>> {
        self.file_writers.get(&ino).map(|r| r.value().clone())
    }

    pub(crate) fn get_length(self: &Arc<Self>, ino: Ino) -> u64 {
        self.file_writers
            .get(&ino)
            .map_or(0, |w| w.value().get_length() as u64)
    }

    pub(crate) fn update_mtime(self: &Arc<Self>, ino: Ino, mtime: SystemTime) -> Result<()> {
        debug!("update_mtime do nothing, {ino}: {:?}", mtime);
        Ok(())
    }
}