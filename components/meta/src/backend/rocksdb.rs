use std::{
    cmp::{max, min},
    fmt::{Debug, Formatter},
    ops::Sub,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bitflags::Flags;
use kiseki_common::ChunkIndex;
use kiseki_types::{
    attr::InodeAttr,
    entry::DEntry,
    ino::{Ino, ZERO_INO},
    setting::Format,
    slice::Slices,
    stat::DirStat,
    FileType,
};
use kiseki_utils::align::align4k;
use rocksdb::{DBAccess, DBPinnableSlice, MultiThreaded, WriteBatchWithTransaction};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use tracing::{debug, error, info};

use super::{key, key::Counter, Backend, RenameResult, UnlinkResult};
use crate::{
    context::FuseContext,
    engine::RenameFlags,
    err::{
        model_err, model_err::ModelKind, InvalidSettingSnafu, LibcSnafu, ModelSnafu, Result,
        RocksdbSnafu, UninitializedEngineSnafu,
    },
    open_files::OpenFilesRef,
    Error,
};

#[derive(Debug, Default)]
pub struct Builder {
    path:           PathBuf,
    skip_dir_mtime: Duration,
}

impl Builder {
    pub fn with_path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    pub fn with_skip_dir_mtime(&mut self, d: Duration) -> &mut Self {
        self.skip_dir_mtime = d;
        self
    }

    pub fn build(&self) -> Result<Arc<dyn Backend>> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);

        let db = rocksdb::OptimisticTransactionDB::open(&opts, &self.path).context(RocksdbSnafu)?;
        Ok(Arc::new(RocksdbBackend {
            db,
            skip_dir_mtime: self.skip_dir_mtime,
        }))
    }
}

pub(crate) struct RocksdbBackend {
    db:             rocksdb::OptimisticTransactionDB<MultiThreaded>,
    skip_dir_mtime: Duration,
}

impl Debug for RocksdbBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbEngine");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

fn do_get_hard_link_count<Layer: DBAccess>(db: &Layer, inode: Ino, parent: Ino) -> Result<u64> {
    let key = key::parent(inode, parent);

    let buf = db
        .get_pinned_opt(&key, &rocksdb::ReadOptions::default())
        .context(RocksdbSnafu)?;
    match buf {
        Some(buf) => {
            let count: u64 = bincode::deserialize(&buf)
                .context(model_err::CorruptionSnafu {
                    kind: ModelKind::HardLinkCount,
                    key:  String::from_utf8_lossy(&key).to_string(),
                })
                .context(ModelSnafu)?;
            Ok(count)
        }
        None => Ok(0),
    }
}

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

fn set_dentry_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    parent: Ino,
    name: &str,
    inode: Ino,
    typ: FileType,
) -> Result<()> {
    let entry_key = key::dentry(parent, name);
    set_value_in_write_batch(
        batch,
        ModelKind::DEntry,
        entry_key.as_slice(),
        &DEntry {
            parent,
            name: name.to_string(),
            inode,
            typ,
        },
    )
}

fn set_attr_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    inode: Ino,
    attr: &InodeAttr,
) -> Result<()> {
    let attr_key = key::attr(inode);
    set_value_in_write_batch(batch, ModelKind::Attr, attr_key.as_slice(), attr)
}

fn set_hard_link_count_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    inode: Ino,
    parent: Ino,
    cnt: u64,
) -> Result<()> {
    let key = key::parent(inode, parent);
    set_value_in_write_batch(batch, ModelKind::HardLinkCount, key.as_slice(), cnt)
}

fn set_sustained_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    session_id: u64,
    inode: Ino,
    sustained: u64,
) -> Result<()> {
    let key = key::sustained(session_id, inode);
    set_value_in_write_batch(batch, ModelKind::Sustained, key.as_slice(), sustained)
}

// set_delete_chunk_after_in_write_batch write a notification that we need to
// delete the chunk after a while.
fn set_delete_chunk_after_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    inode: Ino,
) -> Result<()> {
    let key = key::delete_chunk_after(inode);
    set_value_in_write_batch(
        batch,
        ModelKind::DeleteInode,
        key.as_slice(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
}

fn set_value_in_write_batch<const TRANSACTION: bool, V>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    kind: ModelKind,
    key: &[u8],
    value: V,
) -> Result<()>
where
    V: Serialize,
{
    let buf = bincode::serialize(&value)
        .context(model_err::CorruptionSnafu {
            kind,
            key: String::from_utf8_lossy(key).to_string(),
        })
        .context(ModelSnafu)?;
    batch.put(key, buf);
    Ok(())
}

fn delete_prefix_in_txn_write_batch(
    txn: &rocksdb::Transaction<rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    batch: &mut rocksdb::WriteBatchWithTransaction<true>,
    prefix: &[u8],
) {
    let mut ro = rocksdb::ReadOptions::default();
    ro.set_iterate_range(rocksdb::PrefixRange(prefix));
    let mut iter = txn.raw_iterator_opt(ro);
    iter.seek_to_first();
    // Scan the keys in the iterator
    while iter.valid() {
        let k = iter.key();
        if matches!(k, Some(k) if k.starts_with(prefix)) {
            batch.delete(k.unwrap());
            iter.next();
            continue;
        }
        break;
    }
}

#[async_trait::async_trait]
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

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr> { do_get_attr(&self.db, inode) }

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
        do_get_dentry(&self.db, parent, name)
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
        let mut parent_attr = do_get_attr(&txn, parent)?;
        ensure!(
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
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
        if let Ok(found_entry) = do_get_dentry(&txn, parent, name) {
            if matches!(found_entry.typ, FileType::Directory | FileType::RegularFile) {
                // load the exists inode attr
                match do_get_attr(&txn, found_entry.inode) {
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
        if typ == FileType::Directory {
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
        }

        let mut batch = txn.get_writebatch();
        // insert entry
        set_dentry_in_write_batch(&mut batch, parent, name, new_inode, typ)?;
        // insert attr
        set_attr_in_write_batch(&mut batch, new_inode, &new_inode_attr)?;

        if update_parent_attr {
            // update parent attr
            set_attr_in_write_batch(&mut batch, parent, &parent_attr)?;
        }
        if typ == FileType::Symlink {
            let symlink_key = key::symlink(new_inode);
            batch.put(&symlink_key, path.into_bytes());
        }
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
        let entry_info = do_get_dentry(&txn, parent, name)?;
        ensure!(
            entry_info.typ == FileType::Directory,
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        // get parent and child's attr
        let mut parent_attr = do_get_attr(&txn, parent)?;
        ensure!(
            // parent must be dir.
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        let child_attr = do_get_attr(&txn, entry_info.inode)?;
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
            !do_check_exist_children(&txn, entry_info.inode)?,
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
        let mut old_attr = do_get_attr(&txn, inode)?;
        ensure!(
            matches!(old_attr.kind, FileType::RegularFile),
            LibcSnafu { errno: libc::EPERM }
        );
        let flags = kiseki_types::attr::Flags::from_bits(old_attr.flags as u8).unwrap();
        if flags.contains(kiseki_types::attr::Flags::IMMUTABLE)
            || flags.contains(kiseki_types::attr::Flags::APPEND)
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

    fn do_link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<InodeAttr> {
        let txn = self.db.transaction();

        // get parent and child's attr
        let mut parent_attr = do_get_attr(&txn, new_parent)?;
        ensure!(
            // parent must be dir.
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        ctx.check_access(&parent_attr, kiseki_common::MODE_MASK_W)?;
        ensure!(
            !parent_attr.is_immutable(),
            LibcSnafu { errno: libc::EPERM }
        );

        let mut child_attr = do_get_attr(&txn, inode)?;
        ensure!(!child_attr.is_dir(), LibcSnafu { errno: libc::EPERM });
        ensure!(child_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
        ensure!(
            // the target inode must be empty
            do_get_dentry(&txn, new_parent, new_name).is_err(),
            LibcSnafu {
                errno: libc::EEXIST,
            }
        );
        // check if we need to update the parent
        let now = SystemTime::now();
        let need_update_parent_attr =
            parent_attr.update_modification_time_if(now, self.skip_dir_mtime);
        let old_parent = child_attr.parent;
        child_attr.ctime = now;
        child_attr.nlink += 1;
        child_attr.parent = ZERO_INO;

        let mut batch = txn.get_writebatch();
        // 1. create an entry that points to the original inode.
        set_dentry_in_write_batch(&mut batch, new_parent, new_name, inode, child_attr.kind)?;
        if need_update_parent_attr {
            // 2. update parent attr
            set_attr_in_write_batch(&mut batch, new_parent, &parent_attr)?;
        }
        // 3. update child attr
        set_attr_in_write_batch(&mut batch, inode, &child_attr)?;
        if !child_attr.parent.is_zero() {
            let cnt = do_get_hard_link_count(&txn, inode, old_parent)?;
            set_hard_link_count_in_write_batch(&mut batch, inode, old_parent, cnt + 1)?;
        }
        let cnt = do_get_hard_link_count(&txn, inode, new_parent)?;
        set_hard_link_count_in_write_batch(&mut batch, inode, new_parent, cnt + 1)?;
        self.db.write(batch).context(RocksdbSnafu)?;
        txn.commit().unwrap();
        Ok(child_attr)
    }

    async fn do_unlink(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: String,
        session_id: u64,
        open_files_ref: OpenFilesRef,
    ) -> Result<UnlinkResult> {
        let txn = self.db.transaction();

        let entry = do_get_dentry(&txn, parent, &name)?;
        ensure!(
            !matches!(entry.typ, FileType::Directory),
            LibcSnafu { errno: libc::EPERM }
        );
        // get parent and child's attr
        let mut parent_attr = do_get_attr(&txn, parent)?;
        ensure!(
            // parent must be dir.
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        ctx.check_access(
            &parent_attr,
            kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
        )?;
        ensure!(parent_attr.is_normal(), LibcSnafu { errno: libc::EPERM });

        let now = SystemTime::now();
        let mut opened = false;
        let mut new_nlink_cnt = 0;
        let mut attr_place_holder = InodeAttr::empty();
        // the target exist
        if let Ok(mut attr) = do_get_attr(&txn, entry.inode) {
            if ctx.uid != 0
                && parent_attr.mode & 0o1000 != 0
                && ctx.uid != parent_attr.uid
                && ctx.uid != attr.uid
            {
                return LibcSnafu {
                    errno: libc::EACCES,
                }
                .fail();
            }
            ensure!(attr.is_normal(), LibcSnafu { errno: libc::EPERM });
            attr.ctime = now;
            attr.nlink -= 1;
            new_nlink_cnt = attr.nlink;
            if attr.is_file() && attr.nlink == 0 {
                if let Some(of) = open_files_ref.load(&entry.inode).await {
                    opened = of.is_opened().await;
                }
            };
            attr_place_holder = attr;
        }

        let mut batch = txn.get_writebatch();
        if parent_attr.update_modification_time_if(now, self.skip_dir_mtime) {
            set_attr_in_write_batch(&mut batch, parent, &parent_attr)?;
        }
        // delete the entry
        batch.delete(key::dentry(parent, &name));
        let mut free_inode_cnt = 0;
        let mut free_space_size = 0;

        if attr_place_holder.nlink > 0 {
            set_attr_in_write_batch(&mut batch, entry.inode, &attr_place_holder)?;
            if attr_place_holder.parent.is_zero() {
                let cnt = do_get_hard_link_count(&txn, entry.inode, parent)?;
                if cnt > 0 {
                    set_hard_link_count_in_write_batch(&mut batch, entry.inode, parent, cnt - 1)?;
                }
            }
        } else {
            if matches!(attr_place_holder.kind, FileType::RegularFile) {
                if opened {
                    // update the inode attr
                    set_attr_in_write_batch(&mut batch, entry.inode, &attr_place_holder)?;
                    set_sustained_in_write_batch(&mut batch, session_id, entry.inode, 1)?;
                } else {
                    // make a notification that we need to delete the chunk after a while.
                    set_delete_chunk_after_in_write_batch(&mut batch, entry.inode)?;
                    // delete inode attr
                    batch.delete(key::attr(entry.inode));
                    free_inode_cnt += 1;
                    free_space_size += attr_place_holder.length;
                }
            } else {
                if matches!(attr_place_holder.kind, FileType::Symlink) {
                    batch.delete(key::symlink(entry.inode));
                }
                batch.delete(key::attr(entry.inode));
                free_inode_cnt += 1;
                free_space_size += 4096;
            }
            // delete xattr
            delete_prefix_in_txn_write_batch(&txn, &mut batch, &key::xattr_prefix(entry.inode));
            if attr_place_holder.parent.is_zero() {
                // delete hardlinks
                delete_prefix_in_txn_write_batch(
                    &txn,
                    &mut batch,
                    &key::parent_prefix(entry.inode),
                );
            }
        }
        self.db.write(batch).context(RocksdbSnafu)?;
        let mut r = UnlinkResult {
            inode:       entry.inode,
            removed:     None,
            freed_space: free_space_size,
            freed_inode: free_inode_cnt,
            is_opened:   opened,
        };
        if attr_place_holder.nlink == 0 && attr_place_holder.is_file() {
            r.removed = Some(attr_place_holder);
        }
        txn.commit().unwrap();
        Ok(r)
    }

    fn do_delete_chunks(&self, inode: Ino) {
        let txn = self.db.transaction();
        let mut batch = txn.get_writebatch();
        let mut ro = rocksdb::ReadOptions::default();

        let prefix = key::chunk_slices_prefix(inode);
        let prefix = prefix.as_slice();
        ro.set_iterate_range(rocksdb::PrefixRange(prefix));
        let mut iter = txn.raw_iterator_opt(ro);
        iter.seek_to_first();
        // Scan the keys in the iterator
        while iter.valid() {
            if let Some(k) = iter.key() {
                if k.starts_with(prefix) {
                    // at present, we delete the slice directly, since we haven't implemented the
                    // borrow mechanism.
                    batch.delete(k);
                    iter.next();
                    continue;
                }
            }
            break;
        }
        drop(iter);
        // clear the delete notification
        batch.delete(key::delete_chunk_after(inode));

        if let Err(e) = self.db.write(batch) {
            error!("write batch failed when do_delete_chunks: {:?}", e);
            return;
        }
        if let Err(e) = txn.commit() {
            error!("commit failed in do_delete_chunks: {:?}", e);
        }
    }

    async fn do_rename(
        &self,
        ctx: Arc<FuseContext>,
        session_id: u64,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: RenameFlags,
        open_files_ref: OpenFilesRef,
    ) -> Result<RenameResult> {
        let txn = self.db.transaction();
        let old_entry = do_get_dentry(&txn, old_parent, old_name)?;
        let mut rename_result = RenameResult {
            need_delete: None,
            freed_inode: 0,
            freed_space: 0,
        };
        if old_parent == new_parent && old_name == new_name {
            return Ok(rename_result);
        }

        let mut old_parent_attr = do_get_attr(&txn, old_parent)?;
        {
            // check access permission
            ensure!(
                old_parent_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            ctx.check_access(
                &old_parent_attr,
                kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
            )?;
        }

        let mut new_parent_attr = do_get_attr(&txn, new_parent)?;
        {
            ensure!(
                new_parent_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            ctx.check_access(
                &new_parent_attr,
                kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
            )?;
            ensure!(
                old_entry.inode != new_parent && old_entry.inode != new_parent_attr.parent,
                LibcSnafu { errno: libc::EPERM }
            );
        }

        let mut old_inode_attr = do_get_attr(&txn, old_entry.inode)?;
        {
            ensure!(old_inode_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
            if old_parent != new_parent
                && old_parent_attr.mode & 0o1000 != 0
                && ctx.uid != 0
                && ctx.uid != old_inode_attr.uid
                && (ctx.uid != old_parent_attr.uid || old_inode_attr.is_dir())
            {
                return LibcSnafu {
                    errno: libc::EACCES,
                }
                .fail();
            }

            if ctx.uid != 0
                && (old_parent_attr.mode & 0o1000) != 0
                && ctx.uid != old_parent_attr.uid
                && ctx.uid != old_inode_attr.uid
            {
                return LibcSnafu {
                    errno: libc::EACCES,
                }
                .fail();
            }
        }

        let (
            mut update_new_parent,
            mut opened,
            mut need_invalid_attr,
            mut dst_dentry_opt,
            mut dst_attr_opt,
        ) = (false, false, false, None, None);
        match do_get_dentry(&txn, new_parent, new_name) {
            Ok(dst_entry) => {
                // dst exists
                ensure!(
                    !flags.contains(RenameFlags::NOREPLACE),
                    LibcSnafu {
                        errno: libc::EEXIST,
                    }
                );

                let mut dst_attr = do_get_attr(&txn, dst_entry.inode)?;
                ensure!(dst_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
                dst_attr.ctime = SystemTime::now();

                if matches!(flags, RenameFlags::EXCHANGE) {
                    if old_parent != new_parent {
                        if matches!(dst_entry.typ, FileType::Directory) {
                            dst_attr.parent = old_parent;
                            new_parent_attr.nlink -= 1;
                            old_parent_attr.nlink += 1;
                        } else if !dst_attr.parent.is_zero() {
                            dst_attr.parent = old_parent;
                        }
                    }
                } else {
                    if matches!(dst_entry.typ, FileType::Directory) {
                        ensure!(
                            !do_check_exist_children(&txn, dst_entry.inode)?,
                            LibcSnafu {
                                errno: libc::ENOTEMPTY,
                            }
                        );
                        new_parent_attr.nlink -= 1;
                        update_new_parent = true;
                    } else {
                        dst_attr.nlink -= 1;
                        if matches!(dst_entry.typ, FileType::RegularFile) && dst_attr.nlink == 0 {
                            if let Some(of) = open_files_ref.load(&dst_entry.inode).await {
                                opened = of.is_opened().await;
                            }
                            need_invalid_attr = true;
                        }
                    }
                }

                if ctx.uid != 0
                    && (old_parent_attr.mode & 0o1000) == 0
                    && ctx.uid != new_parent_attr.uid
                    && ctx.uid != dst_attr.uid
                {
                    return LibcSnafu {
                        errno: libc::EACCES,
                    }
                    .fail();
                }

                dst_dentry_opt = Some(dst_entry);
                dst_attr_opt = Some(dst_attr);
            }
            Err(e) => {
                if !e.is_not_found() {
                    return Err(e);
                }
                ensure!(
                    !matches!(flags, RenameFlags::EXCHANGE),
                    LibcSnafu {
                        errno: libc::ENOENT,
                    }
                );
            }
        }

        if old_parent != new_parent {
            old_inode_attr.parent = new_parent;
            old_parent_attr.nlink -= 1;
            new_parent_attr.nlink += 1;
        }
        let now = SystemTime::now();
        let update_old_parent =
            old_parent_attr.update_modification_time_if(now, self.skip_dir_mtime);
        if update_new_parent {
            new_parent_attr.update_modification_time_with(now);
        } else {
            update_new_parent =
                new_parent_attr.update_modification_time_if(now, self.skip_dir_mtime);
        }
        old_inode_attr.ctime = now;

        let mut write_batch = txn.get_writebatch();
        match flags {
            RenameFlags::EXCHANGE => {
                // safety: we have checked before.
                let dst_dentry = dst_dentry_opt.unwrap();
                set_dentry_in_write_batch(
                    &mut write_batch,
                    old_parent,
                    old_name,
                    dst_dentry.inode,
                    dst_dentry.typ,
                )?;
                let dst_attr = dst_attr_opt.unwrap();
                set_attr_in_write_batch(&mut write_batch, dst_dentry.inode, &dst_attr)?;
                if old_parent != new_parent && dst_attr.parent.is_zero() {
                    let cnt = do_get_hard_link_count(&txn, dst_dentry.inode, old_parent)?;
                    set_hard_link_count_in_write_batch(
                        &mut write_batch,
                        dst_dentry.inode,
                        old_parent,
                        cnt + 1,
                    )?;
                    let cnt = {
                        let mut cnt = do_get_hard_link_count(&txn, dst_dentry.inode, new_parent)?;
                        if cnt > 0 {
                            cnt -= 1;
                        }
                        cnt
                    };
                    set_hard_link_count_in_write_batch(
                        &mut write_batch,
                        dst_dentry.inode,
                        new_parent,
                        cnt,
                    )?;
                }
            }
            _ => {
                write_batch.delete(key::dentry(old_parent, old_name));
                if let Some(dst_attr) = dst_attr_opt {
                    let dst_entry = dst_dentry_opt.unwrap();
                    if !matches!(dst_attr.kind, FileType::Directory) && dst_attr.nlink > 0 {
                        set_attr_in_write_batch(&mut write_batch, dst_entry.inode, &dst_attr)?;
                        if dst_attr.parent.is_zero() {
                            let cnt = do_get_hard_link_count(&txn, dst_entry.inode, old_parent)?;
                            if cnt > 0 {
                                set_hard_link_count_in_write_batch(
                                    &mut write_batch,
                                    dst_entry.inode,
                                    old_parent,
                                    cnt - 1,
                                )?;
                            }
                        }
                    } else {
                        if matches!(dst_attr.kind, FileType::RegularFile) {
                            if opened {
                                set_attr_in_write_batch(
                                    &mut write_batch,
                                    dst_entry.inode,
                                    &dst_attr,
                                )?;
                                set_sustained_in_write_batch(
                                    &mut write_batch,
                                    session_id,
                                    dst_entry.inode,
                                    1,
                                )?;
                            } else {
                                set_delete_chunk_after_in_write_batch(
                                    &mut write_batch,
                                    dst_entry.inode,
                                )?;
                                write_batch.delete(key::attr(dst_entry.inode));
                                rename_result.freed_space += align4k(dst_attr.length) as u64;
                                rename_result.freed_inode += 1;
                            }
                            rename_result.need_delete = Some((dst_entry.inode, opened));
                        } else {
                            if matches!(dst_attr.kind, FileType::Symlink) {
                                write_batch.delete(key::symlink(dst_entry.inode));
                            }
                            write_batch.delete(key::attr(dst_entry.inode));
                            rename_result.freed_space += 4096;
                            rename_result.freed_inode += 1;
                        }

                        delete_prefix_in_txn_write_batch(
                            &txn,
                            &mut write_batch,
                            &key::xattr_prefix(dst_entry.inode),
                        );
                        if dst_attr.parent.is_zero() {
                            delete_prefix_in_txn_write_batch(
                                &txn,
                                &mut write_batch,
                                &key::parent_prefix(dst_entry.inode),
                            );
                        }
                    }
                }
            }
        }

        if new_parent != old_parent {
            if update_old_parent {
                set_attr_in_write_batch(&mut write_batch, old_parent, &old_parent_attr)?;
            }
            if old_inode_attr.parent.is_zero() {
                let cnt = do_get_hard_link_count(&txn, old_entry.inode, new_parent)?;
                set_hard_link_count_in_write_batch(
                    &mut write_batch,
                    old_entry.inode,
                    new_parent,
                    cnt + 1,
                )?;
                let mut cnt = do_get_hard_link_count(&txn, old_entry.inode, old_parent)?;
                if cnt > 0 {
                    cnt -= 1;
                }
                set_hard_link_count_in_write_batch(
                    &mut write_batch,
                    old_entry.inode,
                    old_parent,
                    cnt,
                )?;
            }
        }

        set_attr_in_write_batch(&mut write_batch, old_entry.inode, &old_inode_attr)?;
        set_dentry_in_write_batch(
            &mut write_batch,
            new_parent,
            new_name,
            old_entry.inode,
            old_inode_attr.kind,
        )?;
        if update_new_parent {
            set_attr_in_write_batch(&mut write_batch, new_parent, &new_parent_attr)?;
        }

        self.db.write(write_batch).context(RocksdbSnafu)?;
        txn.commit().context(RocksdbSnafu)?;
        Ok(rename_result)
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
        let backend = RocksdbBackend {
            db,
            skip_dir_mtime: Duration::from_millis(100),
        };
        // it should be empty at first
        let exist = do_check_exist_children(&backend.db.transaction(), Ino(1)).unwrap();
        assert_eq!(exist, false);

        // it should be empty after we insert a key-value pair
        backend.set_attr(Ino(1), &InodeAttr::default()).unwrap();
        let exist = do_check_exist_children(&backend.db.transaction(), Ino(1)).unwrap();
        assert_eq!(exist, false);

        // now create a new inode under the inode 1
        backend.set_attr(Ino(2), &InodeAttr::default()).unwrap();
        // insert a dentry
        backend
            .set_dentry(Ino(1), "test", Ino(2), FileType::RegularFile)
            .unwrap();
        // now it should exist
        let exist = do_check_exist_children(&backend.db.transaction(), Ino(1)).unwrap();
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
