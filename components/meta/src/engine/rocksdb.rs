use kiseki_types::entry::EntryInfo;
use kiseki_types::{attr::InodeAttr, ino::Ino};
use snafu::{OptionExt, ResultExt};
use std::fmt::{Debug, Formatter};

use crate::engine::key;
use crate::err::{model_err, ModelKind, Result, RocksdbSnafu};

pub(crate) struct RocksdbEngine {
    db: rocksdb::OptimisticTransactionDB,
    fs_name: String,
    cf_handle: rocksdb::ColumnFamily,
}

impl Debug for RocksdbEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbEngine");
        ds.field("path", &self.db.path());
        ds.field("fs_name", &self.fs_name);
        ds.finish()
    }
}

impl RocksdbEngine {
    pub fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        let attr_key = key::attr(inode);
        let buf = self
            .db
            .get_pinned_cf(&self.cf_handle, &attr_key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::Attr,
                key: attr_key.clone(),
            })?;

        let attr: InodeAttr = bincode::deserialize(&buf).context(model_err::CorruptionSnafu {
            kind: ModelKind::Attr,
            key: attr_key,
        })?;
        Ok(attr)
    }
    pub fn set_attr(&self, inode: Ino, attr: InodeAttr) -> Result<()> {
        let attr_key = key::attr(inode);
        let buf = bincode::serialize(&attr).context(model_err::CorruptionSnafu {
            kind: ModelKind::Attr,
            key: attr_key.clone(),
        })?;
        self.db
            .put_cf(&self.cf_handle, &attr_key, &buf)
            .context(RocksdbSnafu)?;
        Ok(())
    }

    pub fn get_entry_info(&self, parent: Ino, name: &str) -> Result<EntryInfo> {
        let entry_key = key::entry_info(parent, name);
        let entry_buf = self
            .db
            .get_pinned_cf(&self.cf_handle, &entry_key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::EntryInfo,
                key: entry_key.clone(),
            })?;

        let entry_info: EntryInfo =
            bincode::deserialize(&entry_buf).context(model_err::CorruptionSnafu {
                kind: ModelKind::EntryInfo,
                key: entry_key,
            })?;
        Ok(entry_info)
    }
    pub fn set_entry_info(&self, parent: Ino, name: &str, entry_info: EntryInfo) -> Result<()> {
        let entry_key = key::entry_info(parent, name);
        let entry_buf = bincode::serialize(&entry_info).context(model_err::CorruptionSnafu {
            kind: ModelKind::EntryInfo,
            key: entry_key.clone(),
        })?;
        self.db
            .put_cf(&self.cf_handle, &entry_key, &entry_buf)
            .context(RocksdbSnafu)?;
        Ok(())
    }

    pub fn set_symlink(&self, inode: Ino, path: String) -> Result<()> {
        let symlink_key = key::symlink(inode);
        self.db
            .put_cf(&self.cf_handle, &symlink_key, path.into_bytes())
            .context(RocksdbSnafu)?;
        Ok(())
    }
    pub fn get_symlink(&self, inode: Ino) -> Result<String> {
        let symlink_key = key::symlink(inode);
        let path_buf = self
            .db
            .get_pinned_cf(&self.cf_handle, &symlink_key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::Symlink,
                key: symlink_key.clone(),
            })?;
        Ok(String::from_utf8_lossy(path_buf.as_ref()).to_string())
    }
}
