use tokio::sync::RwLock;

use crate::{
    backend::{key::Counter, BackendRef},
    err::Result,
};

/// A table for allocating inode numbers.
/// It starts at 2 since the root inode is 1.
pub struct IdTable {
    next_max_pair: RwLock<(u64, u64)>,
    backend:       BackendRef,
    counter:       Counter,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub(crate) fn new(backend: BackendRef, counter: Counter) -> Self {
        Self {
            next_max_pair: RwLock::new((0, 0)),
            backend,
            counter,
        }
    }

    /// Return the next unused ID from the table.
    pub async fn next(&self) -> Result<u64> {
        let mut next_max_pair = self.next_max_pair.write().await;
        if next_max_pair.0 >= next_max_pair.1 {
            let step = self.counter.get_step();
            let new_max = self.backend.increase_count_by(self.counter.clone(), step)?;
            next_max_pair.0 = new_max - step as u64;
            next_max_pair.1 = new_max;
        }
        let mut next = next_max_pair.0;
        next_max_pair.0 += 1;
        while next <= 1 {
            next = next_max_pair.0;
            next_max_pair.0 += 1;
        }
        Ok(next)
    }
}
