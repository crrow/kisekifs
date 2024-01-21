use bitflags::bitflags;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref UID_GID: (u32, u32) = get_current_uid_gid();
}

#[inline]
pub fn get_current_uid_gid() -> (u32, u32) {
    (unsafe { libc::getuid() as u32 }, unsafe {
        libc::getegid() as u32
    })
}

pub fn align4k(length: u64) -> i64 {
    if length == 0 {
        return 1 << 12; // 4096
    }

    // Calculate the number of 4K blocks needed to hold the data
    let blocks_needed = (length - 1) / 4096 + 1;

    // Return the aligned length (number of blocks * block size) as i64
    (blocks_needed * 4096) as i64
}

bitflags! {
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub struct Flags: u8 {
        const IMMUTABLE = 0x01;
        const APPEND = 0x02;
    }
}
