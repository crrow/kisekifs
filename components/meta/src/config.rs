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

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct MetaConfig {
    pub dsn: String,

    pub read_only:        bool,
    /// The duration to reuse open file without checking update (0 means disable
    /// this feature)
    pub open_cache:       Duration,
    /// max number of open files to cache (soft limit, 0 means unlimited)
    pub open_cache_limit: usize,
    /// `skip_dir_mtime` skips updating a directory attribute if the mtime
    /// difference is smaller than this value
    pub skip_dir_mtime:   Duration,
}

impl MetaConfig {
    pub fn with_dsn(&mut self, dsn: &str) -> &mut Self {
        self.dsn = dsn.to_string();
        self
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            dsn:              kiseki_common::KISEKI_DEBUG_META_ADDR.to_string(),
            read_only:        false,
            open_cache:       Duration::default(),
            open_cache_limit: 10_000,
            skip_dir_mtime:   Duration::from_millis(100),
        }
    }
}
