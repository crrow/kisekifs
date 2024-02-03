// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::meta::{engine::MetaEngine, err::Result, types::Ino, MetaContext};

impl MetaEngine {
    pub fn check_quota(
        &self,
        _ctx: &MetaContext,
        _space: i64,
        _inodes: i64,
        _parent: Ino,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn update_dir_quota(&self, _ino: Ino, _space: i64, _inodes: i64) -> Result<()> {
        // TODO
        Ok(())
    }
}
