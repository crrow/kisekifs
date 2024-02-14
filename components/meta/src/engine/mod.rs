#[cfg(feature = "meta-rocksdb")]
mod rocksdb;

pub struct Store {}

enum Inner {
    Rocksdb(rocksdb::RocksdbEngine),
}
