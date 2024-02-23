use kiseki_types::ino::Ino;
use lazy_static::lazy_static;

pub const CURRENT_FORMAT: &str = "current_format";
pub const USED_SPACE: &str = "used_space";
pub const TOTAL_INODES: &str = "total_inodes";
pub const LEGACY_SESSIONS: &str = "legacy_sessions";
pub const NEXT_TRASH: &str = "next_trash";
pub const NEXT_INODE: &str = "next_inode";
pub const NEXT_SLICE: &str = "next_slice";

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub(crate) enum Counter {
    UsedSpace,
    TotalInodes,
    LegacySessions,
    NextTrash,
    NextInode,
    NextSlice,
}

impl Into<Vec<u8>> for Counter {
    fn into(self) -> Vec<u8> {
        match self {
            Counter::UsedSpace => USED_SPACE.as_bytes().to_vec(),
            Counter::TotalInodes => TOTAL_INODES.as_bytes().to_vec(),
            Counter::LegacySessions => LEGACY_SESSIONS.as_bytes().to_vec(),
            Counter::NextTrash => NEXT_TRASH.as_bytes().to_vec(),
            Counter::NextInode => NEXT_INODE.as_bytes().to_vec(),
            Counter::NextSlice => NEXT_SLICE.as_bytes().to_vec(),
        }
    }
}

impl AsRef<[u8]> for Counter {
    fn as_ref(&self) -> &[u8] {
        match self {
            Counter::UsedSpace => USED_SPACE.as_bytes(),
            Counter::TotalInodes => TOTAL_INODES.as_bytes(),
            Counter::LegacySessions => LEGACY_SESSIONS.as_bytes(),
            Counter::NextTrash => NEXT_TRASH.as_bytes(),
            Counter::NextInode => NEXT_INODE.as_bytes(),
            Counter::NextSlice => NEXT_SLICE.as_bytes(),
        }
    }
}

impl Counter {
    pub fn get_step(&self) -> usize {
        match self {
            Counter::NextTrash => 1,
            Counter::NextInode => 1 << 10,
            Counter::NextSlice => 4 << 10,
            _ => panic!("Counter {:?} does not have a step", self),
        }
    }
}

pub fn attr(inode: Ino) -> Vec<u8> { format!("A{:0>8}I", inode.0).into_bytes() }

pub fn dentry(parent: Ino, name: &str) -> Vec<u8> {
    format!("A{:0>8}D/{}", parent.0, name).into_bytes()
}
pub fn dentry_prefix(parent: Ino) -> Vec<u8> {
    format!("A{:0>8}D/", parent.0).into_bytes()
}

pub fn symlink(inode: Ino) -> Vec<u8> { format!("A{:0>8}S", inode.0).into_bytes() }

pub fn chunk_slices(inode: Ino, chunk_idx: kiseki_common::ChunkIndex) -> Vec<u8> {
    format!("A{:0>8}C/{}", inode.0, chunk_idx).into_bytes()
}

pub fn dir_stat(inode: Ino) -> Vec<u8> { format!("U{:0>8}I", inode.0).into_bytes() }
