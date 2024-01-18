use byteorder::{LittleEndian, WriteBytesExt};
use snafu::ResultExt;

use crate::meta::{
    engine::MetaEngine,
    err::*,
    types::{DirStat, EntryInfo, Ino, InodeAttr},
};

impl MetaEngine {
    pub(crate) async fn sto_get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        // TODO: do we need transaction ?
        let inode_key = inode.generate_key_str();
        let attr_buf =
            self.operator
                .read(&inode_key)
                .await
                .context(ErrFailedToReadFromStoSnafu {
                    key: inode_key.to_string(),
                })?;
        let attr: InodeAttr =
            bincode::deserialize(&attr_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        Ok(attr)
    }

    pub(crate) async fn sto_set_attr(&self, inode: Ino, attr: InodeAttr) -> Result<()> {
        let inode_key = inode.generate_key_str();
        let attr_buf = bincode::serialize(&attr).unwrap();
        self.operator
            .write(&inode_key, attr_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu {
                key: inode_key.to_string(),
            })?;
        Ok(())
    }

    pub(crate) async fn sto_get_entry_info(&self, parent: Ino, name: &str) -> Result<EntryInfo> {
        let entry_key = generate_sto_entry_key_str(parent, name);
        let entry_buf = self
            .operator
            .read(&entry_key)
            .await
            .context(ErrFailedToReadFromStoSnafu { key: entry_key })?;

        EntryInfo::parse_from(&entry_buf).context(ErrBincodeDeserializeFailedSnafu)
    }

    pub(crate) async fn sto_set_entry_info(
        &self,
        parent: Ino,
        name: &str,
        entry_info: EntryInfo,
    ) -> Result<()> {
        let entry_key = generate_sto_entry_key_str(parent, name);
        let entry_buf = entry_info.encode();
        self.operator
            .write(&entry_key, entry_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: entry_key })?;
        Ok(())
    }

    pub(crate) async fn sto_set_sym(&self, inode: Ino, path: String) -> Result<()> {
        let sym_key = generate_sto_sym_key_str(inode);
        let sym_buf = path.into_bytes();
        self.operator
            .write(&sym_key, sym_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: sym_key })?;
        Ok(())
    }

    pub(crate) async fn sto_set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()> {
        let dir_stat_key = generate_sto_dir_stat_key_str(inode);
        let dir_stat_buf = dir_stat.encode();
        self.operator
            .write(&dir_stat_key, dir_stat_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: dir_stat_key })?;
        Ok(())
    }
}

pub(crate) fn generate_sto_sym_key_str(inode: Ino) -> String {
    let mut buf = vec![0u8; 10];
    buf.write_u8('A' as u8).unwrap();
    buf.write_u64::<LittleEndian>(inode.0).unwrap();
    buf.write_u8('S' as u8).unwrap();
    String::from_utf8(buf).unwrap()
}
pub(crate) fn generate_sto_dir_stat_key_str(inode: Ino) -> String {
    let mut buf = vec![0u8; 10];
    buf.write_u8('U' as u8).unwrap();
    buf.write_u64::<LittleEndian>(inode.0).unwrap();
    buf.write_u8('I' as u8).unwrap();
    String::from_utf8(buf).unwrap()
}
// key: AiiiiiiiiD/{name}
// key-len: 11 + name.len()
pub(crate) fn generate_entry_key(parent: Ino, name: &str) -> Vec<u8> {
    let mut buf = vec![0u8; 11 + name.len()];
    buf.write_u8('A' as u8).unwrap();
    buf.write_u64::<LittleEndian>(parent.0).unwrap();
    buf.write_u8('D' as u8).unwrap();
    buf.write_u8('/' as u8).unwrap();
    buf.extend_from_slice(name.as_bytes());
    buf
}
pub(crate) fn generate_sto_entry_key_str(parent: Ino, name: &str) -> String {
    generate_entry_key(parent, name)
        .into_iter()
        .map(|x| x as char)
        .collect()
}
