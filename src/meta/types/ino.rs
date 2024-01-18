use std::{
    fmt::{Display, Formatter},
    ops::{Add, AddAssign},
};

use byteorder::{LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};

pub const ZERO_INO: Ino = Ino(0);
pub const ROOT_INO: Ino = Ino(1);

pub const MIN_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF00000000);
pub const LOG_INODE: Ino = Ino(0x7FFFFFFF00000001);
pub const CONTROL_INODE: Ino = Ino(0x7FFFFFFF00000002);
pub const STATS_INODE: Ino = Ino(0x7FFFFFFF00000003);
pub const CONFIG_INODE: Ino = Ino(0x7FFFFFFF00000004);
pub const MAX_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF10000000);
pub const TRASH_INODE: Ino = MAX_INTERNAL_INODE;

const INO_SIZE: usize = std::mem::size_of::<Ino>();

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Ino(pub u64);

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
}
