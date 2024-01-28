use std::fmt::Debug;
use std::sync::Arc;

use opendal::Operator;
use snafu::{ResultExt, Snafu};

use super::err::*;

/// StoEngine is a trait for backend storage engines.
pub(crate) trait StoEngine: 'static + Debug + Send + Sync {
    /// Put data into storage.
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;
    /// Delete data from storage.
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// Remove data from storage.
    fn remove(&self, key: &str) -> Result<()>;
}

/// The very underlying storage engine.
#[derive(Debug)]
pub(crate) struct ObjectSto {
    operator: Operator,
}

pub(crate) fn new_debug_sto() -> Arc<dyn StoEngine> {
    Arc::new(ObjectSto::new_memory())
}

impl ObjectSto {
    pub(crate) fn new_memory() -> Self {
        let mut builder = opendal::services::Memory::default();
        let op = Operator::new(builder).unwrap().finish();
        Self { operator: op }
    }
}

impl StoEngine for ObjectSto {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.operator
            .blocking()
            .write(key, data)
            .context(ObjectStorageSnafu)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.operator
            .blocking()
            .read(key)
            .context(ObjectStorageSnafu)
    }

    fn remove(&self, key: &str) -> Result<()> {
        self.operator
            .blocking()
            .delete(key)
            .context(ObjectStorageSnafu)
    }
}
