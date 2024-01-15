use crate::fs::InodeError;
use fuser::{FileAttr, FileType};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::time;
use std::time::{Duration, SystemTime};

/// Atime (Access Time):
/// Every file has three timestamps:
/// atime (access time): The last time the file was read or accessed.
/// mtime (modification time): The last time the file's content was modified.
/// ctime (change time): The last time the file's metadata (e.g., permissions, owner) was changed.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum AccessTimeMode {
    /// Disables atime updates entirely.
    /// Reading a file doesn't update its atime timestamp.
    /// Improves performance, especially for frequently accessed files.
    /// Can make it difficult to determine when a file was last accessed.
    Never,
    /// Default atime mode on many Linux systems.
    /// Updates atime only if:
    /// The file's atime is older than its mtime or ctime.
    /// The file has been accessed more than a certain time threshold (usually 1 day).
    /// Balances performance and access time tracking.
    Relative,
    /// Always updates atime whenever a file is read.
    /// Accurately tracks file access times.
    /// Can impact performance, especially on storage systems with slow write speeds.
    Everytime,
}

impl Default for AccessTimeMode {
    fn default() -> Self {
        Self::Never
    }
}

pub type Ino = u64;

const MIN_INTERNAL_INODE: Ino = 0x7FFFFFFF00000000;
const MAX_INTERNAL_INODE: Ino = 0x7FFFFFFF10000000;

pub const LOG_INODE_NAME: &str = ".accesslog";
pub const CONTROL_INODE_NAME: &str = ".control";
pub const STATS_INODE_NAME: &str = ".stats";
pub const CONFIG_INODE_NAME: &str = ".config";
pub const TRASH_INODE_NAME: &str = ".trash";

#[inline]
pub fn get_current_uid_gid() -> (u32, u32) {
    (unsafe { libc::getuid() as u32 }, unsafe {
        libc::getegid() as u32
    })
}

lazy_static! {
    pub static ref UID_GID: (u32, u32) = get_current_uid_gid();
    pub static ref LOG_INODE: InternalNode = InternalNode(Entry {
        inode: MIN_INTERNAL_INODE + 1,
        name: LOG_INODE_NAME.to_string(),
        attr: Attr::default().set_perm(0o400).set_full(),
    });
    pub static ref CONTROL_INODE: InternalNode = InternalNode(Entry {
        inode: MIN_INTERNAL_INODE + 2,
        name: CONTROL_INODE_NAME.to_string(),
        attr: Attr::default().set_perm(0o666).set_full(),
    });
    pub static ref STATS_INODE: InternalNode = InternalNode(Entry {
        inode: MIN_INTERNAL_INODE + 3,
        name: STATS_INODE_NAME.to_string(),
        attr: Attr::default().set_perm(0o400).set_full(),
    });
    pub static ref CONFIG_INODE: InternalNode = InternalNode(Entry {
        inode: MIN_INTERNAL_INODE + 4,
        name: CONFIG_INODE_NAME.to_string(),
        attr: Attr::default().set_perm(0o400).set_full(),
    });
    pub static ref TRASH_INODE: InternalNode = InternalNode(Entry {
        inode: MAX_INTERNAL_INODE,
        name: TRASH_INODE_NAME.to_string(),
        attr: Attr::default()
            .set_perm(0o555)
            .set_kind(fuser::FileType::Directory)
            .set_nlink(2)
            .set_uid(UID_GID.0)
            .set_gid(UID_GID.1)
            .set_full(),
    });
}

pub fn get_internal_node_by_name(name: &str) -> Option<&'static InternalNode> {
    match name {
        LOG_INODE_NAME => Some(&LOG_INODE),
        CONTROL_INODE_NAME => Some(&CONTROL_INODE),
        STATS_INODE_NAME => Some(&STATS_INODE),
        CONFIG_INODE_NAME => Some(&CONFIG_INODE),
        TRASH_INODE_NAME => Some(&TRASH_INODE),
        _ => None,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attr {
    pub inner: FileAttr,
    pub parent: Ino,
    pub full: bool, // the attributes are completed or not
}

impl Attr {
    pub fn default() -> Self {
        let now = SystemTime::now();
        Self {
            inner: FileAttr {
                ino: 0,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: fuser::FileType::RegularFile,
                perm: 0,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            },
            parent: 0,
            full: false,
        }
    }
    pub fn set_perm(mut self, perm: u16) -> Self {
        self.inner.perm = perm;
        self
    }
    pub fn set_kind(mut self, kind: fuser::FileType) -> Self {
        self.inner.kind = kind;
        self
    }
    pub fn set_nlink(mut self, nlink: u32) -> Self {
        self.inner.nlink = nlink;
        self
    }
    pub fn set_gid(mut self, gid: u32) -> Self {
        self.inner.gid = gid;
        self
    }
    pub fn set_uid(mut self, uid: u32) -> Self {
        self.inner.uid = uid;
        self
    }
    pub fn set_full(mut self) -> Self {
        self.full = true;
        self
    }
}

// Entry is an entry inside a directory.
#[derive(Debug)]
pub struct Entry {
    pub inode: Ino,
    pub name: String,
    pub attr: Attr,
}

impl Entry {
    pub fn new(inode: Ino, name: String, attr: Attr) -> Self {
        Self { inode, name, attr }
    }
    pub fn is_filetype(&self, ft: FileType) -> bool {
        return self.attr.inner.kind == ft;
    }
}

pub struct InternalNode(Entry);

impl From<InternalNode> for Entry {
    fn from(value: InternalNode) -> Self {
        value.0
    }
}

impl Deref for InternalNode {
    type Target = Entry;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct OpenFile {
    pub attr: Attr,
    pub reference_count: usize,
    pub last_check: SystemTime,
    pub chunks: HashMap<u32, Vec<Slice>>,
}

pub struct OpenFiles {
    ttl: Duration,
    limit: usize,
    files: HashMap<Ino, OpenFile>,
    // TODO: background clean up
}

impl OpenFiles {
    pub(crate) fn new(ttl: Duration, limit: usize) -> Self {
        Self {
            ttl,
            limit,
            files: Default::default(),
        }
    }
}

// Slice is a slice of a chunk.
// Multiple slices could be combined together as a chunk.
pub struct Slice {
    id: u64,
    size: u32,
    off: u32,
    len: u32,
}
