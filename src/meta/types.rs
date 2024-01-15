use crate::fs::InodeError;
use fuser::{FileAttr, FileType, ReplyEntry};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::cell::{RefCell, UnsafeCell};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Into;
use std::ops::{Add, AddAssign, Deref, DerefMut};
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

pub const ZERO_INO: Ino = Ino(0);

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Ino(u64);

impl AddAssign for Ino {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl Add for Ino {
    type Output = Ino;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl From<u64> for Ino {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl Into<u64> for Ino {
    fn into(self) -> u64 {
        self.0
    }
}

impl Ino {
    pub fn is_special(&self) -> bool {
        *self >= MIN_INTERNAL_INODE
    }
    pub fn is_normal(&self) -> bool {
        !self.is_special()
    }
}

const MIN_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF00000000);

const LOG_INODE: Ino = Ino(0x7FFFFFFF00000001);
const CONTROL_INODE: Ino = Ino(0x7FFFFFFF00000002);
const STATS_INODE: Ino = Ino(0x7FFFFFFF00000003);
const CONFIG_INODE: Ino = Ino(0x7FFFFFFF00000004);
const MAX_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF10000000);
const TRASH_NODE: Ino = MAX_INTERNAL_INODE;

pub const LOG_INODE_NAME: &'static str = ".accesslog";
pub const CONTROL_INODE_NAME: &'static str = ".control";
pub const STATS_INODE_NAME: &'static str = ".stats";
pub const CONFIG_INODE_NAME: &'static str = ".config";
pub const TRASH_INODE_NAME: &'static str = ".trash";

lazy_static! {
    pub static ref UID_GID: (u32, u32) = get_current_uid_gid();
}

#[inline]
pub fn get_current_uid_gid() -> (u32, u32) {
    (unsafe { libc::getuid() as u32 }, unsafe {
        libc::getegid() as u32
    })
}
#[derive(Debug)]
pub struct PreInternalNodes {
    nodes: HashMap<&'static str, InternalNode>,
}

impl Default for PreInternalNodes {
    fn default() -> Self {
        let mut map = HashMap::new();
        let control_inode: InternalNode = InternalNode(Entry {
            inode: CONTROL_INODE,
            name: CONTROL_INODE_NAME.to_string(),
            attr: RefCell::new(InodeAttr::default().set_perm(0o666).set_full()),
        });
        let log_inode: InternalNode = InternalNode(Entry {
            inode: LOG_INODE,
            name: LOG_INODE_NAME.to_string(),
            attr: RefCell::new(InodeAttr::default().set_perm(0o400).set_full()),
        });
        let stats_inode: InternalNode = InternalNode(Entry {
            inode: STATS_INODE,
            name: STATS_INODE_NAME.to_string(),
            attr: RefCell::new(InodeAttr::default().set_perm(0o400).set_full()),
        });
        let config_inode: InternalNode = InternalNode(Entry {
            inode: CONFIG_INODE,
            name: CONFIG_INODE_NAME.to_string(),
            attr: RefCell::new(InodeAttr::default().set_perm(0o400).set_full()),
        });
        let trash_inode: InternalNode = InternalNode(Entry {
            inode: MAX_INTERNAL_INODE,
            name: TRASH_INODE_NAME.to_string(),
            attr: RefCell::new(
                InodeAttr::default()
                    .set_perm(0o555)
                    .set_kind(fuser::FileType::Directory)
                    .set_nlink(2)
                    .set_uid(UID_GID.0)
                    .set_gid(UID_GID.1)
                    .set_full(),
            ),
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InodeAttr {
    pub inner: FileAttr,
    pub parent: Ino,
    pub full: bool, // the attributes are completed or not
}

impl InodeAttr {
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
            parent: ZERO_INO,
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
    pub attr: RefCell<InodeAttr>,
}

impl Entry {
    pub fn new(inode: Ino, name: String, attr: InodeAttr) -> Self {
        Self {
            inode,
            name,
            attr: RefCell::new(attr),
        }
    }
    pub fn is_filetype(&self, ft: FileType) -> bool {
        return self.attr.borrow().inner.kind == ft;
    }
    pub fn is_special_inode(&self) -> bool {
        self.inode.is_special()
    }
}

#[derive(Debug)]
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
impl DerefMut for InternalNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct OpenFile {
    pub attr: InodeAttr,
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
