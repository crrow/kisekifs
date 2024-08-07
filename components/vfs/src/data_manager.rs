// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use kiseki_common::FH;
use kiseki_meta::MetaEngineRef;
use kiseki_storage::{
    cache,
    cache::{
        file_cache::{FileCache, FileCacheRef},
        mem_cache::{MemCache, MemCacheRef},
    },
};
use kiseki_types::ino::Ino;
use kiseki_utils::object_storage::ObjectStorage;
use tokio::sync::RwLock;
use tracing::debug;

use crate::{
    err::Result,
    reader::FileReader,
    writer::{FileWriter, FileWritersRef},
};

pub(crate) type DataManagerRef = Arc<DataManager>;

/// DataManager is responsible for managing the data of the VFS.
pub(crate) struct DataManager {
    pub(crate) page_size:      usize,
    pub(crate) block_size:     usize,
    pub(crate) chunk_size:     usize,
    pub(crate) file_writers:   FileWritersRef,
    pub(crate) file_readers:   RwLock<HashMap<Ino, Arc<RwLock<HashMap<FH, Arc<FileReader>>>>>>,
    pub(crate) id_generator:   Arc<sonyflake::Sonyflake>,
    // Dependencies
    pub(crate) meta_engine:    MetaEngineRef,
    pub(crate) object_storage: ObjectStorage,
    pub(crate) file_cache:     FileCacheRef,
    pub(crate) mem_cache:      MemCacheRef,
    // pub(crate) data_cache: CacheRef,
}

impl DataManager {
    pub(crate) fn new(
        page_size: usize,
        block_size: usize,
        chunk_size: usize,
        meta_engine_ref: MetaEngineRef,
        object_storage: ObjectStorage,
    ) -> Self {
        let remote_storage = object_storage.clone();
        Self {
            page_size,
            block_size,
            chunk_size,
            file_writers: Arc::new(Default::default()),
            file_readers: Default::default(),
            id_generator: Arc::new(sonyflake::Sonyflake::new().unwrap()),
            meta_engine: meta_engine_ref,
            object_storage,
            file_cache: Arc::new(
                FileCache::new(cache::file_cache::Config::default(), remote_storage.clone())
                    .unwrap(),
            ),
            mem_cache: Arc::new(MemCache::new(
                cache::mem_cache::Config::default(),
                remote_storage,
            )),
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
