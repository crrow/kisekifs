use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use rocksdb::{LogLevel, OptimisticTransactionDB, Options};
use snafu::ResultExt;

use crate::err::{Result, RocksdbSnafu};
use crate::store::{Store, Txn, Val, DEFAULT_DB_PATH};

pub struct Builder {
    path: PathBuf,
}

impl Builder {
    fn with_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.path = path.into();
        self
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            path: PathBuf::from_str(DEFAULT_DB_PATH).unwrap(),
        }
    }
}

impl Builder {
    pub fn build(self) -> Result<Backend> {
        // Configure custom options
        let mut opts = Options::default();
        /* General */
        opts.set_log_level(LogLevel::Error);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus::get() as i32);
        // Create the datastore
        Ok(Backend {
            db: Arc::new(OptimisticTransactionDB::open(&opts, self.path).context(RocksdbSnafu)?),
        })
    }
}

struct Backend {
    db: Arc<OptimisticTransactionDB>,
}

impl Backend {}

impl Store for Backend {
    type Transactional = Transaction;

    fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: Fn(&Self::Transactional) -> Result<T>,
    {
        let inner = self.db.transaction();
        let inner = unsafe {
            std::mem::transmute::<
                rocksdb::Transaction<'_, OptimisticTransactionDB>,
                rocksdb::Transaction<'static, OptimisticTransactionDB>,
            >(inner)
        };
        let txn = Transaction { inner };
        f(&txn)
    }
}

struct RocksdbIter<'d> {
    prefix: &'d [u8],
    inner:
        rocksdb::DBRawIteratorWithThreadMode<'d, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>,
}

struct Transaction {
    inner: rocksdb::Transaction<'static, OptimisticTransactionDB>,
}

impl Txn for Transaction {
    type Iter = RocksdbIter<'static>;

    fn get(&self, key: &[u8]) -> Result<Option<Val>> {
        self.inner.get(key).context(RocksdbSnafu)
    }

    fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.inner.put(key, val).context(RocksdbSnafu)
    }

    fn del(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key).context(RocksdbSnafu)
    }

    fn list(&self, prefix: &[u8]) -> Result<Self::Iter> {
        let v = self
            .inner
            .prefix_iterator(prefix)
            .flatten()
            .filter(|r| r.0.as_ref().starts_with(prefix))
            .map(|r| (r.0.to_vec(), r.1.to_vec()));
        Ok(Box::new(v))
    }

    fn scan(&self, range: Range<&[u8]>, limit: usize) -> Result<Vec<(Val, Val)>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = Builder::default()
            .with_path(temp_dir.path())
            .build()
            .unwrap();
        let v = store
            .transaction(|txn| {
                let key = b"hello".as_slice();
                let val = b"world".as_slice();

                let v = txn.get(key)?;
                assert_eq!(v, None);
                txn.set(key, val)?;
                let v = txn.get(key)?;
                assert_eq!(v, Some(val.to_vec()));
                Ok(v.unwrap())
            })
            .unwrap();
        assert_eq!(v, b"world");
    }
}
