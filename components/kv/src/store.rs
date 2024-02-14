use crate::err::Result;
use futures::future::BoxFuture;
use std::ops::Range;

pub const DEFAULT_DB_PATH: &str = "/tmp/kiseki.meta.db";
pub type Val = Vec<u8>;

pub trait Store {
    type Transactional: Txn;
    fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: Fn(&Self::Transactional) -> Result<T>;
}

pub trait Txn {
    type Iter: Iterator<Item = (Val, Val)>;
    /// Get a key-value pair from the store.
    fn get(&self, key: &[u8]) -> Result<Option<Val>>;
    /// Set a key-value pair in the store.
    /// Return the previous value if it exists.
    fn set(&self, key: &[u8], val: &[u8]) -> Result<()>;
    /// Delete a key-value pair from the store.
    /// Return the previous value if it exists.
    fn del(&self, key: &[u8]) -> Result<()>;
    /// List all key-value pairs with a given prefix.
    fn list(&self, prefix: &[u8]) -> Result<Self::Iter>;
    /// Scan all key-value pairs within a given range.
    fn scan(&self, range: Range<&[u8]>, limit: usize) -> Result<Vec<(Val, Val)>>;
}
