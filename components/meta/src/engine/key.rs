use kiseki_types::ino::Ino;
pub const SETTING_PREFIX: &str = "Setting/";
pub const CURRENT_SETTING: &str = "CurrentSetting";

pub fn setting_key(name: &str) -> Vec<u8> {
    format!("{}{}", SETTING_PREFIX, name).into_bytes()
}

pub fn attr(inode: Ino) -> Vec<u8> {
    format!("A{:0>8}I", inode.0).into_bytes()
}

pub fn entry_info(parent: Ino, name: &str) -> Vec<u8> {
    format!("A{:0>8}D/{}", parent.0, name).into_bytes()
}

pub fn symlink(inode: Ino) -> Vec<u8> {
    format!("A{:0>8}S", inode.0).into_bytes()
}
