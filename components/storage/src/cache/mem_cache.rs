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

use std::sync::Arc;

use bytes::Bytes;
use kiseki_types::slice::SliceKey;
use kiseki_utils::readable_size::ReadableSize;
use snafu::ResultExt;

use crate::err::{Error::CacheError, ObjectStorageSnafu, Result};

#[derive(Debug)]
pub struct Config {
    pub capacity: ReadableSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            capacity: ReadableSize::gb(1),
        }
    }
}

pub type MemCacheRef = Arc<MemCache>;

/// MemCache is responsible for caching the object block in memory.
pub struct MemCache {
    inner:          moka::future::Cache<SliceKey, Bytes>,
    remote_storage: kiseki_utils::object_storage::ObjectStorage,
}

impl MemCache {
    pub fn new(
        config: Config,
        remote_storage: kiseki_utils::object_storage::ObjectStorage,
    ) -> Self {
        let inner = moka::future::Cache::builder()
            .weigher(|_, value: &Bytes| -> u32 { value.len() as u32 })
            .max_capacity(config.capacity.as_bytes())
            // only one minute for the object to be alive
            .time_to_idle(std::time::Duration::from_secs(60))
            .build();
        Self {
            inner,
            remote_storage,
        }
    }

    pub async fn get(&self, key: &SliceKey) -> Result<Option<Bytes>> {
        match self
            .inner
            .try_get_with_by_ref(key, async {
                let path = key.make_object_storage_path();
                let object = self
                    .remote_storage
                    .get(&path)
                    .await
                    .context(ObjectStorageSnafu)?;
                let v = object.bytes().await.context(ObjectStorageSnafu)?;
                Ok(v) as Result<Bytes>
            })
            .await
        {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                if e.is_not_found() {
                    Ok(None)
                } else {
                    Err(CacheError {
                        error: format!("failed to get from cache: {}", e),
                    })
                }
            }
        }
    }
}
