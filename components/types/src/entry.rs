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

use std::time::Duration;

use fuser::FileType;
use serde::{Deserialize, Serialize};

use crate::{attr::InodeAttr, ino::Ino};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EntryV2 {
    // FullEntry when we need to return the full entry.
    Full(FullEntry),
    // Only contains the inode number and attr.
    Attr(AttrEntry),
    // DEntry is the storage structure of an entry in a directory.
    DEntry(DEntry),
}

/// lookup
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FullEntry {
    pub inode: Ino,
    pub name: String,
    pub attr: InodeAttr,
    // entry timeout
    pub ttl: Duration,
    pub generation: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AttrEntry {
    pub inode: Ino,
    pub attr: InodeAttr,
}

/// DEntry represents the storage structure of an entry in a directory.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DEntry {
    pub parent: Ino,
    pub name: String,
    pub inode: Ino,
    pub typ: FileType,
}

impl EntryV2 {
    pub fn get_inode(&self) -> Ino {
        match self {
            EntryV2::Full(e) => e.inode,
            EntryV2::Attr(e) => e.inode,
            EntryV2::DEntry(e) => e.inode,
        }
    }

    pub fn is_file(&self) -> bool {
        match self {
            EntryV2::Full(e) => e.attr.kind == FileType::RegularFile,
            EntryV2::Attr(e) => e.attr.kind == FileType::RegularFile,
            EntryV2::DEntry(e) => e.typ == FileType::RegularFile,
        }
    }
}

// Entry is an entry inside a directory.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub inode: Ino,
    pub name: String,
    pub typ: FileType,
    pub attr: Option<InodeAttr>,
    // entry timeout
    pub ttl: Option<Duration>,
    pub generation: Option<u64>,
}

impl Entry {
    // pub fn new(inode: Ino, name: String, typ: FileType) -> Self {
    //     Self {
    //         inode,
    //         name,
    //         typ,
    //         attr: None,
    //         ttl: None,
    //         generation: None,
    //     }
    // }
    // pub fn new_with_attr(dentry: &DEntry, attr: InodeAttr) -> Self {
    //     Self {
    //         inode: dentry.inode,
    //         name: dentry.name.clone(),
    //         typ: dentry.typ,
    //         attr: Some(attr),
    //         ttl: None,
    //         generation: None,
    //     }
    // }
    // pub fn new_from_attr(inode: Ino, name: &str, attr: InodeAttr) -> Self {
    //     Self {
    //         inode,
    //         name: name.to_string(),
    //         typ: attr.kind,
    //         attr: Some(attr),
    //         ttl: None,
    //         generation: None,
    //     }
    // }
    // pub fn set_ttl(mut self, ttl: Duration) -> Self {
    //     self.ttl = Some(ttl);
    //     self
    // }
    // pub fn with_ttl(&mut self, ttl: Duration) -> &mut Self {
    //     self.ttl = Some(ttl);
    //     self
    // }
    // pub fn set_generation(mut self, generation: u64) -> Self {
    //     self.generation = Some(generation);
    //     self
    // }
    // pub fn with_generation(&mut self, generation: u64) -> &mut Self {
    //     self.generation = Some(generation);
    //     self
    // }
    // pub fn is_special_inode(&self) -> bool {
    //     self.inode.is_special()
    // }
    //
    // pub fn to_fuse_attr(&self) -> fuser::FileAttr {
    //     self.attr.to_fuse_attr(self.inode)
    // }
    //
    // pub fn is_file(&self) -> bool {
    //     self.typ == FileType::RegularFile
    // }
    //
    // pub fn is_dir(&self) -> bool {
    //     self.typ == FileType::Directory
    // }
}
