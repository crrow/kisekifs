use std::{
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
};

use kiseki_common::ChunkIndex;
use kiseki_types::{
    attr::InodeAttr, entry::DEntry, ino::Ino, setting::Format, slice::Slices, stat::DirStat,
    FileType,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use tracing::debug;

use super::{key, key::Counter, Backend};
use crate::err::{
    model_err, model_err::ModelKind, InvalidSettingSnafu, ModelSnafu, Result, RocksdbSnafu,
    UninitializedEngineSnafu,
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

    pub fn build(self) -> Result<Arc<dyn Backend>> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);

        let db = rocksdb::OptimisticTransactionDB::open(&opts, &self.path).context(RocksdbSnafu)?;
        Ok(Arc::new(RocksdbBackend { db }))
    }
}

pub(crate) struct RocksdbBackend {
    db: rocksdb::OptimisticTransactionDB,
}

impl Debug for RocksdbBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbEngine");
        ds.field("path", &self.db.path());
        ds.finish()
    }
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

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        let attr_key = key::attr(inode);
        let buf = self
            .db
            .get_pinned(&attr_key)
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

    fn get_entry_info(&self, parent: Ino, name: &str) -> Result<DEntry> {
        let entry_key = key::dentry(parent, name);
        let entry_buf = self
            .db
            .get_pinned(&entry_key)
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

    fn list_entry_info(&self, parent: Ino, limit: i64) -> Result<Vec<DEntry>> {
        let prefix = key::dentry_prefix(parent);
        let range = prefix.as_slice()..;
        let ro = rocksdb::ReadOptions::default();
        // Create the iterator
        let mut iter = self.db.raw_iterator_opt(ro);
        // Seek to the start key
        iter.seek(&range.start);
        let start = range.start;
        let mut res = Vec::default();
        // Scan the keys in the iterator
        while iter.valid() {
            // Check the scan limit
            if limit == -1 || res.len() < limit as usize {
                // Get the key and value
                let (k, v) = (iter.key(), iter.value());
                // Check the key and value
                if let (Some(k), Some(v)) = (k, v) {
                    if !k.starts_with(start) {
                        break;
                    }
                    if k >= start {
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
            }
            // Exit
            break;
        }

        // // let mut iter = self.db.prefix_iterator(&prefix);
        // let mut res = Vec::default();
        // while let Some(e) = iter.next() {
        //     let (key, value) = e.context(RocksdbSnafu)?;
        //     let dentry: DEntry = bincode::deserialize(value.as_ref())
        //         .context(model_err::CorruptionSnafu {
        //             kind: ModelKind::DEntry,
        //             key,
        //         })
        //         .context(ModelSnafu)?;
        //     res.push(dentry);
        // }
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
}
