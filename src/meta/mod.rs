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

use std::time::Instant;

use bitflags::bitflags;
use fuser::Request;

mod config;
pub use config::MetaConfig;
pub mod engine;
mod err;
pub use err::MetaError;
mod engine_quota;
mod engine_sto;
pub mod types;
mod util;

pub mod internal_nodes {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaContext {
    pub gid: u32,
    pub gid_list: Vec<u32>,
    pub uid: u32,
    pub pid: u32,
    pub check_permission: bool,
    pub start_at: Instant,
}
impl<'a> From<&'a fuser::Request<'a>> for MetaContext {
    fn from(req: &'a Request) -> Self {
        Self {
            gid: req.gid(),
            gid_list: vec![],
            uid: req.uid(),
            pid: req.pid(),
            check_permission: true,
            start_at: Instant::now(),
        }
    }
}

impl MetaContext {
    #[allow(dead_code)]
    pub(crate) fn background() -> Self {
        Self {
            gid: 1,
            gid_list: vec![],
            uid: 1,
            pid: 1,
            check_permission: false,
            start_at: Instant::now(),
        }
    }
    pub fn contains_gid(&self, gid: u32) -> bool {
        self.gid_list.contains(&gid)
    }
}

pub const MAX_NAME_LENGTH: usize = 255;
pub const DOT: &str = ".";
pub const DOT_DOT: &str = "..";

pub const MODE_MASK_R: u8 = 0b100;
pub const MODE_MASK_W: u8 = 0b010;
pub const MODE_MASK_X: u8 = 0b001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetAttrFlags(pub u32);

bitflags! {
    impl SetAttrFlags: u32 {
        const MODE = 1 << 0;
        const UID = 1 << 1;
        const GID = 1 << 2;
        const SIZE = 1 << 3;
        const ATIME = 1 << 4;
        const MTIME = 1 << 5;
        const CTIME = 1 << 6;
        const ATIME_NOW = 1 << 7;
        const MTIME_NOW = 1 << 8;
        const FLAG = 1 << 15;
    }
}
