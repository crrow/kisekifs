use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use dashmap::DashMap;
use fuser::{FileAttr, FileType, ReplyEntry};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use opendal::{ErrorKind, Operator};
use snafu::ResultExt;
use std::collections::HashMap;
use std::convert::Into;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};

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

pub const MAX_NAME_LENGTH: usize = 255;
pub const DOT: &'static str = ".";
pub const DOT_DOT: &'static str = "..";

pub const ZERO_INO: Ino = Ino(0);
pub const ROOT_INO: Ino = Ino(1);

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
    pub fn new(entry_timeout: (Duration, Duration)) -> Self {
        let mut map = HashMap::new();
        let control_inode: InternalNode = InternalNode(Entry {
            inode: CONTROL_INODE,
            name: CONTROL_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o666).set_full(),
            ttl: Some(entry_timeout.0),
            generation: Some(1),
        });
        let log_inode: InternalNode = InternalNode(Entry {
            inode: LOG_INODE,
            name: LOG_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o400).set_full(),
            ttl: Some(entry_timeout.0),
            generation: Some(1),
        });
        let stats_inode: InternalNode = InternalNode(Entry {
            inode: STATS_INODE,
            name: STATS_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o400).set_full(),
            ttl: Some(entry_timeout.0),
            generation: Some(1),
        });
        let config_inode: InternalNode = InternalNode(Entry {
            inode: CONFIG_INODE,
            name: CONFIG_INODE_NAME.to_string(),
            attr: InodeAttr::default().set_perm(0o400).set_full(),
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
                .set_full(),
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
    pub fn is_dir(&self) -> bool {
        self.kind == FileType::Directory
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
    pub fn to_fuse_attr<I: Into<u64>>(&self, ino: I) -> fuser::FileAttr {
        let mut fa = FileAttr {
            ino: ino.into(),
            size: 0,
            blocks: 0,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: self.crtime,
            kind: self.kind,
            // TODO juice combine the file type and file perm together.
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: 0x10000,
            flags: self.flags,
        };

        match fa.kind {
            FileType::Directory | FileType::Symlink | FileType::RegularFile => {
                fa.size = self.length;
                fa.blocks = (fa.size + 511) / 512;
            }
            FileType::BlockDevice | FileType::CharDevice => {
                fa.rdev = self.rdev;
            }
            _ => {
                // Handle other types if needed
            }
        }

        fa
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
            uid: UID_GID.0,
            gid: UID_GID.1,
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
    // entry timeout
    pub ttl: Option<Duration>,
    pub generation: Option<u64>,
}

impl Entry {
    pub fn new<N: Into<String>>(inode: Ino, name: N, typ: FileType) -> Self {
        Self {
            inode,
            name: name.into(),
            attr: InodeAttr::default().set_kind(typ),
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
    pub fn set_generation(mut self, generation: u64) -> Self {
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
    // key: AiiiiiiiiD/{name}
    // key-len: 11 + name.len()
    pub fn generate_entry_key(parent: Ino, name: &str) -> Vec<u8> {
        let mut buf = vec![0u8; 11 + name.len()];
        buf.write_u8('A' as u8).unwrap();
        buf.write_u64::<LittleEndian>(parent.0).unwrap();
        buf.write_u8('D' as u8).unwrap();
        buf.write_u8('/' as u8).unwrap();
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

#[derive(Debug, Default)]
pub struct FSStates {
    /// Represents the total amount of storage space in bytes allocated for the file system.
    pub total_space: u64,
    /// Represents the amount of free storage space in bytes available for new data.
    pub avail_space: u64,
    /// Represents the used of inodes.
    pub used_inodes: u64,
    /// Represents the number of available inodes that can be used for new files or directories.
    pub available_inodes: u64,
}

#[derive(Debug, Default)]
pub(crate) struct FSStatesInner {
    pub(crate) new_space: AtomicI64,
    pub(crate) new_inodes: AtomicI64,
    pub(crate) used_space: AtomicI64,
    pub(crate) used_inodes: AtomicI64,
}

lazy_static! {
    pub static ref COUNTER_LOCKERS: DashMap<Counter, RwLock<()>> = {
        let mut map = DashMap::new();
        for counter in COUNTER_ENUMS.iter() {
            map.insert(counter.clone(), RwLock::new(()));
        }
        map
    };
}

const COUNTER_ENUMS: [Counter; 4] = [
    Counter::UsedSpace,
    Counter::TotalInodes,
    Counter::LegacySessions,
    Counter::NextTrash,
];
const COUNTER_STRINGS: [&str; 4] = [
    "used_space",
    "total_inodes",
    "legacy_sessions",
    "next_trash",
];
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub enum Counter {
    UsedSpace,
    TotalInodes,
    LegacySessions,
    NextTrash,
}

impl Counter {
    pub fn to_str(&self) -> &'static str {
        match self {
            Counter::UsedSpace => COUNTER_STRINGS[0],
            Counter::TotalInodes => COUNTER_STRINGS[1],
            Counter::LegacySessions => COUNTER_STRINGS[2],
            Counter::NextTrash => COUNTER_STRINGS[3],
        }
    }
    // FIXME: do we actually need it ?
    async fn get_counter_with_lock(&self, operator: &Operator) -> Result<i64, opendal::Error> {
        let counter_key = self.generate_kv_key_str();
        let locker_ref = COUNTER_LOCKERS.get(self).unwrap();
        let locker_ref = locker_ref.value();
        let guard = locker_ref.read().await;
        let counter = match operator.read(&counter_key).await {
            Ok(ref buf) => buf
                .as_slice()
                .read_i64::<LittleEndian>()
                .expect("read counter error"),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    0
                } else {
                    return Err(e);
                }
            }
        };
        Ok(counter)
    }
    pub async fn load(&self, operator: &Operator) -> Result<i64, opendal::Error> {
        let counter_key = self.generate_kv_key_str();
        let counter = match operator.read(&counter_key).await {
            Ok(ref buf) => {
                let v = buf
                    .as_slice()
                    .read_i64::<LittleEndian>()
                    .expect("read counter error");
                v
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    0
                } else {
                    return Err(e);
                }
            }
        };
        Ok(counter)
    }
    pub fn generate_kv_key_str(&self) -> String {
        format!("C{}", self.to_str())
    }
    pub async fn increment(&self, operator: &Operator) -> Result<i64, opendal::Error> {
        self.increment_by(operator, 1).await
    }
    pub async fn increment_by(&self, operator: &Operator, by: i64) -> Result<i64, opendal::Error> {
        let counter_key = self.generate_kv_key_str();
        let locker_ref = COUNTER_LOCKERS.get(self).unwrap();
        let locker_ref = locker_ref.value();
        let guard = locker_ref.write().await;
        let counter = match operator.read(&counter_key).await {
            Ok(ref buf) => {
                let v: i64 = buf
                    .as_slice()
                    .read_i64::<LittleEndian>()
                    .expect("read counter error");
                v
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    0
                } else {
                    return Err(e);
                }
            }
        };
        let counter = counter + by;
        let mut buf = vec![0u8; 8];
        buf.as_mut_slice()
            .write_i64::<LittleEndian>(counter)
            .unwrap();
        operator.write(&counter_key, buf).await?;
        Ok(counter)
    }

    pub async fn load_counter(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

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

    #[tokio::test]
    async fn counter() {
        let mut builder = opendal::services::Memory::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.root(tempdir_path);

        let op: Operator = Operator::new(builder).unwrap().finish();
        let counter = Counter::UsedSpace;
        let v = counter.get_counter_with_lock(&op).await.unwrap();
        assert_eq!(v, 0);
        let v = counter.increment(&op).await.unwrap();
        assert_eq!(v, 1);

        let (first, sec) = tokio::join!(counter.increment(&op), counter.increment(&op),);
        println!("{}, {}", first.unwrap(), sec.unwrap());

        let v = counter.load(&op).await.unwrap();
        assert_eq!(v, 3);
    }
}
