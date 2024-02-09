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

use std::{io::Write, time::Duration};

use fuser::FileType;
use serde::{Deserialize, Serialize};

use crate::meta::types::InodeAttr;
use kiseki_types::ino::Ino;

// Entry is an entry inside a directory.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub inode: Ino,
    pub name: String,
    pub attr: InodeAttr,
    // entry timeout
    pub ttl: Option<Duration>,
    pub generation: Option<u64>,
}

impl Entry {
    pub fn new<N: Into<String>>(inode: Ino, name: N, typ: FileType) -> Self {
        Self {
            inode,
            name: name.into(),
            attr: InodeAttr::default().set_kind(typ).to_owned(),
            ttl: None,
            generation: None,
        }
    }
    pub fn new_with_attr<N: Into<String>>(inode: Ino, name: N, attr: InodeAttr) -> Self {
        Self {
            inode,
            name: name.into(),
            attr,
            ttl: None,
            generation: None,
        }
    }
    pub fn set_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
    pub fn with_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.ttl = Some(ttl);
        self
    }
    pub fn set_generation(mut self, generation: u64) -> Self {
        self.generation = Some(generation);
        self
    }
    pub fn with_generation(&mut self, generation: u64) -> &mut Self {
        self.generation = Some(generation);
        self
    }
    pub fn is_special_inode(&self) -> bool {
        self.inode.is_special()
    }

    pub fn to_fuse_attr(&self) -> fuser::FileAttr {
        self.attr.to_fuse_attr(self.inode)
    }

    pub fn is_file(&self) -> bool {
        self.attr.kind == FileType::RegularFile
    }

    pub fn is_dir(&self) -> bool {
        self.attr.kind == FileType::Directory
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EntryInfo {
    pub inode: Ino,
    pub typ: FileType,
}

impl EntryInfo {
    pub fn new(inode: Ino, typ: FileType) -> Self {
        Self { inode, typ }
    }

    pub fn parse_from<R: AsRef<[u8]>>(r: R) -> Result<Self, bincode::Error> {
        bincode::deserialize(r.as_ref())
    }
    pub fn encode_to<W: Write>(&self, w: W) -> Result<(), bincode::Error> {
        bincode::serialize_into(w, self)
    }
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiseki_types::ino::ROOT_INO;

    #[test]
    fn encode_entry() {
        let entry = EntryInfo::new(ROOT_INO, FileType::Directory);
        let mut buf = vec![];
        entry.encode_to(&mut buf).unwrap();
        println!("{:?}", buf);

        let entry2 = EntryInfo::parse_from(&buf).unwrap();
        assert_eq!(entry, entry2)
    }
}
