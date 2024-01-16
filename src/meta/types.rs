use crate::fuse::FuseError;
use byteorder::{LittleEndian, WriteBytesExt};
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use fuser::{FileAttr, FileType, ReplyEntry};
use lazy_static::lazy_static;
use libc::open;
use serde::{Deserialize, Serialize};
use std::cell::{RefCell, UnsafeCell};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Into;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::sync::{Mutex, RwLock};
use std::time;
use std::time::{Duration, Instant, SystemTime};

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
pub const ROOT_INO: Ino = Ino(1);

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Ino(u64);

const INO_SIZE: usize = std::mem::size_of::<Ino>();

impl Display for Ino {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
    pub fn is_trash(&self) -> bool {
        self.0 >= TRASH_INODE.0
    }
    pub fn is_special(&self) -> bool {
        *self >= MIN_INTERNAL_INODE
    }
    pub fn is_normal(&self) -> bool {
        !self.is_special()
    }
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
    pub fn is_root(&self) -> bool {
        self.0 == ROOT_INO.0
    }
    pub fn eq(&self, other: u64) -> bool {
        self.0 == other
    }
    // FIXME: use a better way
    // key: AiiiiiiiiI
    // key-len: 10
    pub fn generate_key(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 10];
        buf.write_u8('A' as u8).unwrap();
        buf.write_u64::<LittleEndian>(self.0).unwrap();
        buf.write_u8('I' as u8).unwrap();
        buf
    }
    pub fn generate_key_str(&self) -> String {
        self.generate_key().into_iter().map(|x| x as char).collect()
    }
}
pub const MIN_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF00000000);

pub const LOG_INODE: Ino = Ino(0x7FFFFFFF00000001);
pub const CONTROL_INODE: Ino = Ino(0x7FFFFFFF00000002);
pub const STATS_INODE: Ino = Ino(0x7FFFFFFF00000003);
pub const CONFIG_INODE: Ino = Ino(0x7FFFFFFF00000004);
pub const MAX_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF10000000);
pub const TRASH_INODE: Ino = MAX_INTERNAL_INODE;

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

impl PreInternalNodes {
    pub fn new(ttl: (Duration, Duration)) -> Self {
        let mut map = HashMap::new();
        let control_inode: InternalNode = InternalNode(Entry {
            inode: CONTROL_INODE,
            name: CONTROL_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o666).set_full(),
            ttl: Default::default(),
        });
        let log_inode: InternalNode = InternalNode(Entry {
            inode: LOG_INODE,
            name: LOG_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o400).set_full(),
            ttl: Default::default(),
        });
        let stats_inode: InternalNode = InternalNode(Entry {
            inode: STATS_INODE,
            name: STATS_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o400).set_full(),
        });
        let config_inode: InternalNode = InternalNode(Entry {
            inode: CONFIG_INODE,
            name: CONFIG_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o400).set_full(),
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
                .set_full(),
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
    pub fn get_internal_node(&self, ino: Ino) -> Option<&InternalNode> {
        self.nodes.values().find(|node| node.0.inode == ino)
    }
    pub fn get_mut_internal_node_by_name(&mut self, name: &str) -> Option<&mut InternalNode> {
        self.nodes.get_mut(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InodeAttr {
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
    /// Kind of file (directory, file, pipe, etc)
    pub kind: FileType,
    /// permission mode
    pub perm: u16,
    /// owner id
    pub uid: u32,
    /// group id of owner
    pub gid: u32,
    /// device number
    pub rdev: u32,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Number of hard links
    pub nlink: u32,
    /// length of regular file
    pub length: u64,
    /// inode of parent; 0 means tracked by parentKey (for hardlinks)
    pub parent: Ino,
    // the attributes are completed or not
    pub full: bool,
    // whether to keep the cached page or not
    pub keep_cache: bool,
}

impl InodeAttr {
    pub fn get_filetype(&self) -> FileType {
        self.kind
    }
    pub fn is_filetype(&self, typ: FileType) -> bool {
        self.kind == typ
    }
    /// Providing default values guarantees for some critical inode,
    /// makes them always available, even under slow or unreliable conditions.
    pub fn hard_code_inode_attr(is_trash: bool) -> Self {
        Self {
            flags: 0,
            kind: FileType::Directory,
            perm: if is_trash { 0o555 } else { 0o777 },
            uid: 0,
            gid: 0,
            rdev: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            nlink: 2,
            length: 4 << 10,
            parent: ROOT_INO,
            full: true,
            keep_cache: false,
        }
    }
    pub fn set_perm(mut self, perm: u16) -> Self {
        self.perm = perm;
        self
    }
    pub fn set_kind(mut self, kind: fuser::FileType) -> Self {
        self.kind = kind;
        self
    }
    pub fn set_nlink(mut self, nlink: u32) -> Self {
        self.nlink = nlink;
        self
    }
    pub fn set_gid(mut self, gid: u32) -> Self {
        self.gid = gid;
        self
    }
    pub fn set_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }
    pub fn set_full(mut self) -> Self {
        self.full = true;
        self
    }

    // Enforces different access levels for owner, group, and others.
    // Grants full access to the root user.
    // Determines access based on user and group IDs.
    pub(crate) fn access_perm(&self, uid: u32, gids: &Vec<u32>) -> u8 {
        if uid == 0 {
            // If uid is 0 (root user), returns 0x7 (full access) unconditionally.
            return 0x7;
        }
        let perm = self.perm;
        if uid == self.uid {
            // If uid matches attr.Uid (file owner),
            // extracts owner permissions by shifting mode 6 bits to the right and masking with 7,
            // returning a value like 4 (read-only),
            // 6 (read-write), or 7 (read-write-execute).
            return (perm >> 6) as u8 & 7;
        }
        // If any gid matches attr.Gid (file group),
        // extracts group permissions by shifting mode 3 bits to the right and masking with 7.
        for gid in gids {
            if *gid == self.gid {
                return (perm >> 3) as u8 & 7;
            }
        }
        // If no previous conditions match,
        // returns other permissions by masking mode with 7.
        perm as u8 & 7
    }
}

impl Default for InodeAttr {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: 0,
            nlink: 1,
            length: 0,
            parent: Default::default(),
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            full: false,
            keep_cache: false,
        }
    }
}

// Entry is an entry inside a directory.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub inode: Ino,
    pub name: String,
    pub attr: InodeAttr,
    pub ttl: Duration,
}

impl Entry {
    pub fn new(inode: Ino, name: String, attr: InodeAttr) -> Self {
        // Self { inode, name, attr,  }
        todo!()
    }
    pub fn is_special_inode(&self) -> bool {
        self.inode.is_special()
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
    // key: AiiiiiiiiD{name}
    // key-len: 10 + name.len()
    pub fn generate_entry_key(parent: Ino, name: &str) -> Vec<u8> {
        let mut buf = vec![0u8; 10 + name.len()];
        buf.write_u8('A' as u8).unwrap();
        buf.write_u64::<LittleEndian>(parent.0).unwrap();
        buf.write_u8('D' as u8).unwrap();
        buf.extend_from_slice(name.as_bytes());
        buf
    }
    pub fn generate_entry_key_str(parent: Ino, name: &str) -> String {
        Self::generate_entry_key(parent, name)
            .into_iter()
            .map(|x| x as char)
            .collect()
    }
    pub fn parse_from<R: AsRef<[u8]>>(r: R) -> Result<Self, bincode::Error> {
        bincode::deserialize(r.as_ref())
    }
    pub fn encode_to<W: Write>(&self, w: W) -> Result<(), bincode::Error> {
        bincode::serialize_into(w, self)
    }
}

#[derive(Debug)]
pub struct InternalNode(Entry);

// impl From<InternalNode> for Entry {
//     fn from(value: InternalNode) -> Self {
//         value.0
//     }
// }
// impl<'a> Into<&'a Entry> for &'a InternalNode {
//     fn into(self) -> &'a Entry {
//         &self.0
//     }
// }
//
impl Into<Entry> for InternalNode {
    fn into(self) -> Entry {
        self.0
    }
}

impl Into<Entry> for &'_ InternalNode {
    fn into(self) -> Entry {
        self.0.clone()
    }
}

// impl Deref for InternalNode {
//     type Target = Entry;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
// impl DerefMut for InternalNode {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

pub struct OpenFile {
    pub attr: InodeAttr,
    pub reference_count: usize,
    pub last_check: Instant,
    pub chunks: HashMap<u32, Vec<Slice>>,
}

pub struct OpenFiles {
    ttl: Duration,
    limit: usize,
    files: DashMap<Ino, OpenFile>,
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

    pub(crate) fn check(&self, ino: Ino) -> Option<InodeAttr> {
        self.files.get(&ino).and_then(|f| {
            if f.last_check.elapsed() < self.ttl {
                Some(f.attr.clone())
            } else {
                None
            }
        })
    }

    pub(crate) fn update(&self, ino: Ino, attr: &mut InodeAttr) -> bool {
        self.files
            .get_mut(&ino)
            .and_then(|mut open_file| {
                if attr.mtime != open_file.attr.mtime {
                    open_file.chunks = HashMap::new();
                } else {
                    attr.keep_cache = open_file.attr.keep_cache;
                }
                open_file.attr = attr.clone();
                open_file.last_check = Instant::now();
                return Some(());
            })
            .is_some()
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

pub const MAX_NAME_LENGTH: usize = 255;
pub const DOT: &'static str = ".";
pub const DOT_DOT: &'static str = "..";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ino() {
        let key = ROOT_INO.generate_key();
        println!("{:?}", key);
        let key_str = ROOT_INO.generate_key_str();
        println!("{:?}", key_str)
    }

    #[test]
    fn option_to_bool() {
        let mut x: Option<isize> = None;
        let r = x
            .and_then(|x| {
                println!("{ }", x);
                return Some(());
            })
            .is_some();
        assert!(!r)
    }

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
