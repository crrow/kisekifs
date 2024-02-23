use std::{
    cmp::{max, min},
    fmt::{Debug, Formatter},
    ops::Sub,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use kiseki_common::ChunkIndex;
use kiseki_types::{
    attr::InodeAttr,
    entry::DEntry,
    ino::{Ino, TRASH_INODE},
    setting::Format,
    slice::Slices,
    stat::DirStat,
    FileType,
};
use rocksdb::{DBAccess, DBPinnableSlice, MultiThreaded, WriteBatchWithTransaction};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use tracing::{debug, info};

use super::{key, key::Counter, Backend};
use crate::{
    context::FuseContext,
    err::{
        model_err, model_err::ModelKind, InvalidSettingSnafu, LibcSnafu, ModelSnafu, Result,
        RocksdbSnafu, UninitializedEngineSnafu,
    },
    Error,
};

#[derive(Debug, Default)]
pub struct Builder {
    path: PathBuf,
}

impl Builder {
    pub fn with_path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    pub fn build(&self) -> Result<Arc<dyn Backend>> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);

        let db = rocksdb::OptimisticTransactionDB::open(&opts, &self.path).context(RocksdbSnafu)?;
        Ok(Arc::new(RocksdbBackend { db }))
    }
}

pub(crate) struct RocksdbBackend {
    db: rocksdb::OptimisticTransactionDB<MultiThreaded>,
}

impl Debug for RocksdbBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbEngine");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

impl RocksdbBackend {
    fn do_get_dentry<Layer: DBAccess>(db: &Layer, parent: Ino, name: &str) -> Result<DEntry> {
        let entry_key = key::dentry(parent, name);
        let entry_buf = db
            .get_pinned_opt(&entry_key, &rocksdb::ReadOptions::default())
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::DEntry,
                key:  String::from_utf8_lossy(&entry_key).to_string(),
            })
            .context(ModelSnafu)?;

        let entry_info: DEntry = bincode::deserialize(&entry_buf)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::DEntry,
                key:  String::from_utf8_lossy(&entry_key).to_string(),
            })
            .context(ModelSnafu)?;
        Ok(entry_info)
    }

    fn do_get_attr<Layer: DBAccess>(db: &Layer, inode: Ino) -> Result<InodeAttr> {
        let attr_key = key::attr(inode);
        let buf = db
            .get_pinned_opt(&attr_key, &rocksdb::ReadOptions::default())
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::Attr,
                key:  String::from_utf8_lossy(&attr_key).to_string(),
            })
            .context(ModelSnafu)?;

        let attr: InodeAttr = bincode::deserialize(&buf)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::Attr,
                key:  String::from_utf8_lossy(&attr_key).to_string(),
            })
            .context(ModelSnafu)?;
        Ok(attr)
    }

    fn do_check_exist_children<DB>(txn: &rocksdb::Transaction<DB>, parent: Ino) -> Result<bool> {
        let prefix = key::dentry_prefix(parent);
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_range(rocksdb::PrefixRange(prefix.as_slice()));
        let mut iter = txn.raw_iterator_opt(ro);
        iter.seek_to_first();
        Ok(iter.valid())
    }
}

fn txn_put_attr<DB>(txn: &rocksdb::Transaction<DB>, inode: Ino, attr: &InodeAttr) -> Result<()> {
    let attr_key = key::attr(inode);
    let buf = bincode::serialize(attr)
        .context(model_err::CorruptionSnafu {
            kind: ModelKind::Attr,
            key:  String::from_utf8_lossy(&attr_key).to_string(),
        })
        .context(ModelSnafu)?;
    txn.put(&attr_key, &buf).context(RocksdbSnafu)?;
    Ok(())
}

impl Backend for RocksdbBackend {
    // TODO: merge the exists format
    fn set_format(&self, format: &Format) -> Result<()> {
        let setting_buf = bincode::serialize(format)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::Setting,
                key:  key::CURRENT_FORMAT.to_string(),
            })
            .context(ModelSnafu)?;

        self.db
            .put(key::CURRENT_FORMAT, setting_buf)
            .context(RocksdbSnafu)?;
        Ok(())
    }

    fn load_format(&self) -> Result<Format> {
        let setting_buf = self
            .db
            .get_pinned(key::CURRENT_FORMAT)
            .context(RocksdbSnafu)?
            .context(UninitializedEngineSnafu)?;
        let setting: Format = bincode::deserialize(&setting_buf)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::Setting,
                key:  key::CURRENT_FORMAT.to_string(),
            })
            .context(ModelSnafu)?;
        Ok(setting)
    }

    fn increase_count_by(&self, counter: Counter, step: usize) -> Result<u64> {
        let key: Vec<u8> = counter.into();
        let transaction = self.db.transaction();
        let current = transaction
            .get(&key)
            .context(RocksdbSnafu)?
            .map(|v| {
                bincode::deserialize(&v)
                    .context(model_err::CorruptionSnafu {
                        kind: ModelKind::Counter,
                        key:  String::from_utf8_lossy(&key).to_string(),
                    })
                    .context(ModelSnafu)
            })
            .transpose()?
            .unwrap_or(0u64);

        let new = current + step as u64;
        let new_buf = bincode::serialize(&new)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::Counter,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        transaction.put(&key, new_buf).context(RocksdbSnafu)?;
        transaction.commit().context(RocksdbSnafu)?;
        Ok(new)
    }

    fn load_count(&self, counter: Counter) -> Result<u64> {
        let key: Vec<u8> = counter.into();
        let buf = self
            .db
            .get_pinned(&key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::Counter,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        let count: u64 = bincode::deserialize(&buf)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::Counter,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        Ok(count)
    }

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr> { Self::do_get_attr(&self.db, inode) }

    fn set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()> {
        let attr_key = key::attr(inode);
        let buf = bincode::serialize(attr)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::Attr,
                key:  String::from_utf8_lossy(&attr_key).to_string(),
            })
            .context(ModelSnafu)?;
        self.db.put(&attr_key, &buf).context(RocksdbSnafu)?;
        Ok(())
    }

    fn get_dentry(&self, parent: Ino, name: &str) -> Result<DEntry> {
        Self::do_get_dentry(&self.db, parent, name)
    }

    fn set_dentry(&self, parent: Ino, name: &str, inode: Ino, typ: FileType) -> Result<()> {
        let entry_key = key::dentry(parent, name);
        let entry_buf = bincode::serialize(&DEntry {
            parent,
            name: name.to_string(),
            inode,
            typ,
        })
        .context(model_err::CorruptionSnafu {
            kind: ModelKind::DEntry,
            key:  String::from_utf8_lossy(&entry_key).to_string(),
        })
        .context(ModelSnafu)?;
        self.db.put(&entry_key, entry_buf).context(RocksdbSnafu)?;
        Ok(())
    }

    fn list_dentry(&self, parent: Ino, limit: i64) -> Result<Vec<DEntry>> {
        let prefix = key::dentry_prefix(parent);
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_range(rocksdb::PrefixRange(prefix.as_slice()));
        let mut iter = self.db.raw_iterator_opt(ro);
        iter.seek_to_first();
        let mut res = Vec::default();
        // Scan the keys in the iterator
        while iter.valid() {
            // Check the scan limit
            if limit == -1 || res.len() < limit as usize {
                // Get the key and value
                let (k, v) = (iter.key(), iter.value());
                // Check the key and value
                if let (Some(k), Some(v)) = (k, v) {
                    let dentry: DEntry = bincode::deserialize(v)
                        .context(model_err::CorruptionSnafu {
                            kind: ModelKind::DEntry,
                            key:  String::from_utf8_lossy(k).to_string(),
                        })
                        .context(ModelSnafu)?;
                    res.push(dentry);
                    iter.next();
                    continue;
                }
            }
            // Exit
            break;
        }
        Ok(res)
    }

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()> {
        let symlink_key = key::symlink(inode);
        self.db
            .put(&symlink_key, path.into_bytes())
            .context(RocksdbSnafu)?;
        Ok(())
    }

    fn get_symlink(&self, inode: Ino) -> Result<String> {
        let symlink_key = key::symlink(inode);
        let path_buf = self
            .db
            .get_pinned(&symlink_key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::Symlink,
                key:  String::from_utf8_lossy(&symlink_key).to_string(),
            })
            .context(ModelSnafu)?;
        Ok(String::from_utf8_lossy(path_buf.as_ref()).to_string())
    }

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()> {
        let key = key::chunk_slices(inode, chunk_index);
        let buf = bincode::serialize(&slices)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::ChunkSlices,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        self.db.put(&key, &buf).context(RocksdbSnafu)?;
        Ok(())
    }

    fn set_raw_chunk_slices(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
        buf: Vec<u8>,
    ) -> Result<()> {
        let key = key::chunk_slices(inode, chunk_index);
        self.db.put(&key, &buf).context(RocksdbSnafu)?;
        assert!(buf.len() > 0, "slices is empty");
        Ok(())
    }

    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Option<Vec<u8>>> {
        let key = key::chunk_slices(inode, chunk_index);
        let buf = self.db.get(&key).context(RocksdbSnafu)?;
        Ok(buf)
    }

    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices> {
        let key = key::chunk_slices(inode, chunk_index);
        let buf = self
            .db
            .get_pinned(&key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::ChunkSlices,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        let slices = Slices::decode(&buf).unwrap();

        assert!(buf.len() > 0, "slices is empty");
        assert!(slices.0.len() > 0, "slices is empty");
        debug!("get_chunk_slices: key: {:?}", String::from_utf8_lossy(&key));
        for slice in slices.0.iter() {
            debug!("get_chunk_slices: slice: {:?}", slice);
        }
        Ok(slices)
    }

    fn set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()> {
        let key = key::dir_stat(inode);
        let buf = bincode::serialize(&dir_stat)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::DirStat,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        self.db.put(&key, &buf).context(RocksdbSnafu)?;
        Ok(())
    }

    fn get_dir_stat(&self, inode: Ino) -> Result<DirStat> {
        let key = key::dir_stat(inode);
        let buf = self
            .db
            .get_pinned(&key)
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: ModelKind::DirStat,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        let dir_stat = bincode::deserialize::<DirStat>(&buf)
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::DirStat,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
            .context(ModelSnafu)?;
        Ok(dir_stat)
    }

    fn do_mknod(
        &self,
        ctx: Arc<FuseContext>,
        mut new_inode: Ino,
        mut new_inode_attr: InodeAttr,
        parent: Ino,
        name: &str,
        typ: FileType,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        let txn = self.db.transaction();
        debug!("get attr {} from backend", parent);
        let mut parent_attr = Self::do_get_attr(&txn, parent)?;
        ensure!(
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        // check if the parent is trash
        ensure!(
            !parent_attr.parent.is_trash(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );
        // check if the parent have the permission
        ctx.check_access(&parent_attr, kiseki_common::MODE_MASK_W)?;
        ensure!(
            !kiseki_types::attr::Flags::from_bits(parent_attr.flags as u8)
                .unwrap()
                .contains(kiseki_types::attr::Flags::IMMUTABLE),
            LibcSnafu { errno: libc::EPERM }
        );

        // check if the entry already exists
        if let Ok(found_entry) = Self::do_get_dentry(&txn, parent, name) {
            if matches!(found_entry.typ, FileType::Directory | FileType::RegularFile) {
                // load the exists inode attr
                match Self::do_get_attr(&txn, found_entry.inode) {
                    Ok(found_attr) => {
                        new_inode_attr = found_attr;
                    }
                    Err(e) => {
                        // not found inode attr
                        if !e.is_not_found() {
                            return Err(e);
                        }
                        // use the exists inode attr
                        new_inode = found_entry.inode;
                    }
                }
            }
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail();
        }

        // check if we need to update the parent
        let mut update_parent_attr = false;
        if !parent.is_trash() && typ == FileType::Directory {
            parent_attr.set_nlink(parent_attr.nlink + 1);

            let now = SystemTime::now();
            parent_attr.mtime = now;
            parent_attr.ctime = now;
            update_parent_attr = true;
        };

        let now = SystemTime::now();
        new_inode_attr.set_atime(now);
        new_inode_attr.set_mtime(now);
        new_inode_attr.set_ctime(now);

        #[cfg(target_os = "darwin")]
        {
            attr.set_gid(parent_attr.gid);
        }

        // TODO: review the logic here
        #[cfg(target_os = "linux")]
        {
            // if the parent directory has the set group ID (SGID) bit (02000) set in its
            // mode (pattr.Mode). If so, it sets the group ID (attr.Gid) of the new node to
            // the group ID of the parent directory (pattr.Gid).
            if parent_attr.mode & 0o2000 != 0 {
                new_inode_attr.set_gid(parent_attr.gid);
            }
            // If the type of the node being created is a directory (_type ==
            // TypeDirectory), it sets the SGID bit (02000) in the mode (attr.Mode) of the
            // new node. This ensures that newly created directories inherit the group ID of
            // their parent directory.
            if typ == FileType::Directory {
                new_inode_attr.mode |= 0o2000;
            } else if new_inode_attr.mode & 0o2010 == 0o2010
                && ctx.uid != 0
                && !ctx.gid_list.contains(&parent_attr.gid)
            {
                // If the mode of the new node has both the set group ID bit (02000) and the set
                // group execute bit (010) (attr.Mode&02010 == 02010), and if the user ID
                // (ctx.Uid()) is not 0 (i.e., the user is not root), it further checks if the
                // user belongs to the group of the parent directory (pattr.Gid). If not, it
                // removes the SGID bit from the mode of the new node (attr.Mode &=
                // ^uint16(02000)).
                new_inode_attr.mode &= !0o2010;
            }
        }

        let mut batch = txn.get_writebatch();
        let entry_key = key::dentry(parent, name);
        // insert entry
        batch.put(
            &entry_key,
            bincode::serialize(&DEntry {
                parent,
                name: name.to_string(),
                inode: new_inode,
                typ,
            })
            .context(model_err::CorruptionSnafu {
                kind: ModelKind::DEntry,
                key:  String::from_utf8_lossy(&entry_key).to_string(),
            })
            .context(ModelSnafu)?,
        );
        // insert attr
        let attr_key = key::attr(new_inode);
        batch.put(
            &attr_key,
            bincode::serialize(&new_inode_attr)
                .context(model_err::CorruptionSnafu {
                    kind: ModelKind::Attr,
                    key:  String::from_utf8_lossy(&attr_key).to_string(),
                })
                .context(ModelSnafu)?,
        );
        if update_parent_attr {
            // update parent attr
            let parent_attr_key = key::attr(parent);
            batch.put(
                &parent_attr_key,
                bincode::serialize(&parent_attr)
                    .context(model_err::CorruptionSnafu {
                        kind: ModelKind::Attr,
                        key:  String::from_utf8_lossy(&parent_attr_key).to_string(),
                    })
                    .context(ModelSnafu)?,
            );
        }
        if typ == FileType::Symlink {
            let symlink_key = key::symlink(new_inode);
            batch.put(&symlink_key, path.into_bytes());
        } else if typ == FileType::Directory {
            // TODO: maybe store the dir stats
        };
        self.db.write(batch).context(RocksdbSnafu)?;
        txn.commit().context(RocksdbSnafu)?;
        Ok((new_inode, new_inode_attr))
    }

    fn do_rmdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        skip_dir_mtime: Duration,
    ) -> Result<(DEntry, InodeAttr)> {
        let txn = self.db.transaction();
        let entry_info = Self::do_get_dentry(&txn, parent, name)?;
        ensure!(
            entry_info.typ == FileType::Directory,
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        // get parent and child's attr
        let mut parent_attr = Self::do_get_attr(&txn, parent)?;
        ensure!(
            // parent must be dir.
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        let child_attr = Self::do_get_attr(&txn, entry_info.inode)?;
        ensure!(
            // child must be dir. check again in case of we found that the entry info tells
            // the different story.
            child_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );

        ctx.check_access(
            &parent_attr,
            kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
        )?;
        let parent_flag = kiseki_types::attr::Flags::from_bits(parent_attr.flags as u8).unwrap();
        ensure!(
            !parent_flag.contains(kiseki_types::attr::Flags::APPEND)
                && !parent_flag.contains(kiseki_types::attr::Flags::IMMUTABLE),
            LibcSnafu { errno: libc::EPERM }
        );
        ensure!(
            !Self::do_check_exist_children(&txn, entry_info.inode)?,
            LibcSnafu {
                errno: libc::ENOTEMPTY,
            }
        );
        // The octal number 01000 specifically represents the setuid bit in POSIX
        // permissions. In a file's mode, the setuid bit is represented by the fourth
        // bit from the left. When set, it indicates that the file should be executed
        // with the privileges of its owner, rather than the privileges of the user who
        // executed it. This is commonly used for executable files that need special
        // permissions to perform certain tasks, such as changing passwords or accessing
        // sensitive resources.
        if ctx.uid != 0
            && parent_attr.mode & 0o1000 != 0
            && ctx.uid != parent_attr.uid
            && ctx.uid != child_attr.uid
        {
            return LibcSnafu {
                errno: libc::EACCES,
            }
            .fail();
        }
        parent_attr.nlink -= 1;
        let now = SystemTime::now();

        let need_update_parent_attr = if now
            .duration_since(parent_attr.mtime)
            .expect("found mtime in the future")
            >= skip_dir_mtime
        {
            parent_attr.mtime = now;
            parent_attr.ctime = now;
            true
        } else {
            false
        };

        let mut batch = txn.get_writebatch();
        // delete entry
        batch.delete(key::dentry(parent, name));
        // delete inode attr
        batch.delete(key::attr(entry_info.inode));
        if need_update_parent_attr {
            let parent_attr_key = key::attr(parent);
            // update parent attr
            batch.put(
                &parent_attr_key,
                bincode::serialize(&parent_attr)
                    .context(model_err::CorruptionSnafu {
                        kind: ModelKind::Attr,
                        key:  String::from_utf8_lossy(&parent_attr_key).to_string(),
                    })
                    .context(ModelSnafu)?,
            );
        }
        self.db.write(batch).context(RocksdbSnafu)?;
        // TODO: do we need to call commit after we call write batch with transaction?
        // txn.commit().context(RocksdbSnafu)?;
        Ok((entry_info, child_attr))
    }

    fn do_truncate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        length: u64,
        skip_perm_check: bool,
    ) -> Result<InodeAttr> {
        let txn = self.db.transaction();
        let mut old_attr = Self::do_get_attr(&txn, inode)?;
        ensure!(
            matches!(old_attr.kind, FileType::RegularFile),
            LibcSnafu { errno: libc::EPERM }
        );
        let flags = kiseki_types::attr::Flags::from_bits(old_attr.flags as u8).unwrap();
        if flags.contains(kiseki_types::attr::Flags::IMMUTABLE)
            || flags.contains(kiseki_types::attr::Flags::APPEND)
            || old_attr.parent.is_trash()
        {
            return LibcSnafu { errno: libc::EPERM }.fail();
        }
        if !skip_perm_check {
            ctx.check_access(&old_attr, kiseki_common::MODE_MASK_W)?;
        }
        assert_ne!(length, old_attr.length, "length is the same");
        old_attr.update_length(length);

        // TODO: review me, juicefs make hole manually here, by appending empty slices
        // info.

        txn_put_attr(&txn, inode, &old_attr)?;
        txn.commit().context(RocksdbSnafu)?;
        Ok(old_attr.clone())
    }
}

#[cfg(feature = "meta-rocksdb")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tempdir.path()).unwrap();
        let backend = RocksdbBackend { db };
        // it should be empty at first
        let exist =
            RocksdbBackend::do_check_exist_children(&backend.db.transaction(), Ino(1)).unwrap();
        assert_eq!(exist, false);

        // it should be empty after we insert a key-value pair
        backend.set_attr(Ino(1), &InodeAttr::default()).unwrap();
        let exist =
            RocksdbBackend::do_check_exist_children(&backend.db.transaction(), Ino(1)).unwrap();
        assert_eq!(exist, false);

        // now create a new inode under the inode 1
        backend.set_attr(Ino(2), &InodeAttr::default()).unwrap();
        // insert a dentry
        backend
            .set_dentry(Ino(1), "test", Ino(2), FileType::RegularFile)
            .unwrap();
        // now it should exist
        let exist =
            RocksdbBackend::do_check_exist_children(&backend.db.transaction(), Ino(1)).unwrap();
        assert_eq!(exist, true);

        backend
            .list_dentry(Ino(1), -1)
            .unwrap()
            .iter()
            .for_each(|e| println!("{:?}", e));
        backend
            .list_dentry(Ino(2), -1)
            .unwrap()
            .iter()
            .for_each(|e| println!("{:?}", e));
    }
}
