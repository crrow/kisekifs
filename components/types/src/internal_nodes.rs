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

use std::{collections::HashMap, time::Duration};

use fuser::FileType;

use crate::{
    attr::InodeAttr,
    entry::FullEntry,
    ino::{Ino, CONFIG_INODE, CONTROL_INODE, LOG_INODE, MAX_INTERNAL_INODE, STATS_INODE, *},
};

pub const LOG_INODE_NAME: &str = ".accesslog";
pub const CONTROL_INODE_NAME: &str = ".control";
pub const STATS_INODE_NAME: &str = ".stats";
pub const CONFIG_INODE_NAME: &str = ".config";
pub const TRASH_INODE_NAME: &str = ".trash";
#[derive(Debug)]
pub struct InternalNodeTable {
    nodes: HashMap<&'static str, InternalNode>,
}

impl InternalNodeTable {
    pub fn new(entry_timeout: (Duration, Duration)) -> Self {
        let mut map = HashMap::new();
        let control_inode: InternalNode = InternalNode(FullEntry {
            inode: CONTROL_INODE,
            name:  CONTROL_INODE_NAME.to_string(),
            attr:  InodeAttr::default().set_perm(0o666).to_owned(),
        });
        let log_inode: InternalNode = InternalNode(FullEntry {
            inode: LOG_INODE,
            name:  LOG_INODE_NAME.to_string(),
            attr:  InodeAttr::default().set_perm(0o400).to_owned(),
        });
        let stats_inode: InternalNode = InternalNode(FullEntry {
            inode: STATS_INODE,
            name:  STATS_INODE_NAME.to_string(),
            attr:  InodeAttr::default().set_perm(0o400).to_owned(),
        });
        let config_inode: InternalNode = InternalNode(FullEntry {
            inode: CONFIG_INODE,
            name:  CONFIG_INODE_NAME.to_string(),
            attr:  InodeAttr::default().set_perm(0o400).to_owned(),
        });
        let trash_inode: InternalNode = InternalNode(FullEntry {
            inode: MAX_INTERNAL_INODE,
            name:  TRASH_INODE_NAME.to_string(),
            attr:  InodeAttr::default()
                .set_perm(0o555)
                .set_kind(fuser::FileType::Directory)
                .set_nlink(2)
                .set_uid(kiseki_utils::uid())
                .set_gid(kiseki_utils::gid())
                .to_owned(),
        });
        map.insert(LOG_INODE_NAME, log_inode);
        map.insert(CONTROL_INODE_NAME, control_inode);
        map.insert(STATS_INODE_NAME, stats_inode);
        map.insert(CONFIG_INODE_NAME, config_inode);
        map.insert(TRASH_INODE_NAME, trash_inode);
        Self { nodes: map }
    }
}

impl InternalNodeTable {
    pub fn get_internal_node_by_name(&self, name: &str) -> Option<&InternalNode> {
        self.nodes.get(name)
    }

    pub fn get_mut_internal_node_by_name(&mut self, name: &str) -> Option<&mut InternalNode> {
        self.nodes.get_mut(name)
    }

    pub fn get_internal_node(&self, ino: Ino) -> Option<&InternalNode> {
        self.nodes.values().find(|node| node.0.inode == ino)
    }

    pub fn remove_trash_node(&mut self) { self.nodes.remove(TRASH_INODE_NAME); }

    pub fn add_prefix(&mut self) {
        for n in self.nodes.values_mut() {
            n.0.name = format!(".kfs{}", n.0.name);
        }
    }

    pub fn contains_name(&self, name: &str) -> bool { self.nodes.contains_key(name) }
}

#[derive(Debug)]
pub struct InternalNode(pub FullEntry);

impl InternalNode {
    pub fn get_attr(&self) -> InodeAttr { self.0.attr.clone() }
}
