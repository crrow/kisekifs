pub mod key;
#[cfg(feature = "meta-rocksdb")]
mod rocksdb;

pub struct Store {}

enum Inner {
    #[cfg(feature = "meta-rocksdb")]
    Rocksdb(rocksdb::RocksdbEngine),
}
