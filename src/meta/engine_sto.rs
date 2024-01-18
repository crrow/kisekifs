use snafu::ResultExt;

use crate::meta::{
    engine::MetaEngine,
    err::*,
    types::{EntryInfo, Ino, InodeAttr},
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
        let entry_key = EntryInfo::generate_entry_key_str(parent, name);
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
        let entry_key = EntryInfo::generate_entry_key_str(parent, name);
        let entry_buf = entry_info.encode();
        self.operator
            .write(&entry_key, entry_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: entry_key })?;
        Ok(())
    }

    pub(crate) async fn sto_set_sym(&self, inode: Ino, path: String) -> Result<()> {
        todo!()
    }
    pub(crate) async fn sto_set_dir_stat(&self, inode: Ino) -> Result<()> {
        todo!()
    }
}
