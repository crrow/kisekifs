use crate::err::Result;
use kiseki_types::ino::Ino;

#[derive(Debug)]
pub(crate) struct RocksdbEngine {
    db: rocksdb::OptimisticTransactionDB,
}

impl RocksdbEngine {
    pub(crate) fn get_attr(&self, inode: Ino) -> Result<()> {
        todo!()
    }
}
