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
pub use config::{Compression, Format, MetaConfig};
pub mod engine;
mod err;
pub use err::MetaError;
mod engine_quota;
mod engine_sto;
pub mod types;
mod util;

pub mod internal_nodes {
    use std::{collections::HashMap, time::Duration};

    use crate::meta::{
        types::{Entry, InodeAttr},
        util::UID_GID,
    };
    use kiseki_types::ino::*;

    pub const LOG_INODE_NAME: &str = ".accesslog";
    pub const CONTROL_INODE_NAME: &str = ".control";
    pub const STATS_INODE_NAME: &str = ".stats";
    pub const CONFIG_INODE_NAME: &str = ".config";
    pub const TRASH_INODE_NAME: &str = ".trash";
    #[derive(Debug)]
    pub struct PreInternalNodes {
        nodes: HashMap<&'static str, InternalNode>,
    }

    impl PreInternalNodes {
        pub fn new(entry_timeout: (Duration, Duration)) -> Self {
            let mut map = HashMap::new();
            let control_inode: InternalNode = InternalNode(Entry {
                inode: CONTROL_INODE,
                name: CONTROL_INODE_NAME.to_string(),
                attr: InodeAttr::default().set_perm(0o666).set_full().to_owned(),
                ttl: Some(entry_timeout.0),
                generation: Some(1),
            });
            let log_inode: InternalNode = InternalNode(Entry {
                inode: LOG_INODE,
                name: LOG_INODE_NAME.to_string(),
                attr: InodeAttr::default().set_perm(0o400).set_full().to_owned(),
                ttl: Some(entry_timeout.0),
                generation: Some(1),
            });
            let stats_inode: InternalNode = InternalNode(Entry {
                inode: STATS_INODE,
                name: STATS_INODE_NAME.to_string(),
                attr: InodeAttr::default().set_perm(0o400).set_full().to_owned(),
                ttl: Some(entry_timeout.0),
                generation: Some(1),
            });
            let config_inode: InternalNode = InternalNode(Entry {
                inode: CONFIG_INODE,
                name: CONFIG_INODE_NAME.to_string(),
                attr: InodeAttr::default().set_perm(0o400).set_full().to_owned(),
                ttl: Some(entry_timeout.0),
                generation: Some(1),
            });
            let trash_inode: InternalNode = InternalNode(Entry {
                inode: MAX_INTERNAL_INODE,
                name: TRASH_INODE_NAME.to_string(),
                attr: InodeAttr::default()
                    .set_perm(0o555)
                    .set_kind(fuser::FileType::Directory)
                    .set_nlink(2)
                    .set_uid(UID_GID.0)
                    .set_gid(UID_GID.1)
                    .set_full()
                    .to_owned(),
                ttl: Some(entry_timeout.1),
                generation: Some(1),
            });
            map.insert(LOG_INODE_NAME, log_inode);
            map.insert(CONTROL_INODE_NAME, control_inode);
            map.insert(STATS_INODE_NAME, stats_inode);
            map.insert(CONFIG_INODE_NAME, config_inode);
            map.insert(TRASH_INODE_NAME, trash_inode);
            Self { nodes: map }
        }
    }

    impl PreInternalNodes {
        pub fn get_internal_node_by_name(&self, name: &str) -> Option<&InternalNode> {
            self.nodes.get(name)
        }
        pub fn get_mut_internal_node_by_name(&mut self, name: &str) -> Option<&mut InternalNode> {
            self.nodes.get_mut(name)
        }
        pub fn get_internal_node(&self, ino: Ino) -> Option<&InternalNode> {
            self.nodes.values().find(|node| node.0.inode == ino)
        }
        pub fn remove_trash_node(&mut self) {
            self.nodes.remove(TRASH_INODE_NAME);
        }
        pub fn add_prefix(&mut self) {
            for n in self.nodes.values_mut() {
                n.0.name = format!(".kfs{}", n.0.name);
            }
        }
        pub fn contains_name(&self, name: &str) -> bool {
            self.nodes.contains_key(name)
        }
    }

    #[derive(Debug)]
    pub struct InternalNode(pub Entry);

    impl From<InternalNode> for Entry {
        fn from(val: InternalNode) -> Self {
            val.0
        }
    }

    impl From<&'_ InternalNode> for Entry {
        fn from(val: &'_ InternalNode) -> Self {
            val.0.clone()
        }
    }

    impl InternalNode {
        pub fn get_attr(&self) -> InodeAttr {
            self.0.attr.clone()
        }
    }
}

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
