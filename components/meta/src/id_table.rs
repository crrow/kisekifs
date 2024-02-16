use crossbeam::atomic::AtomicCell;
use tracing::debug;

use crate::{
    backend::{key::Counter, BackendRef},
    err::Result,
};

/// A table for allocating inode numbers.
/// It starts at 2 since the root inode is 1.
pub struct IdTable {
    next_max_pair: AtomicCell<(u64, u64)>,
    backend: BackendRef,
    counter: Counter,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub(crate) fn new(backend: BackendRef, counter: Counter) -> Self {
        Self {
            next_max_pair: AtomicCell::new((0, 0)),
            backend,
            counter,
        }
    }

    /// Return the next unused ID from the table.
    pub fn next(&self) -> Result<u64> {
        let old = self.next_max_pair.take();
        debug!("old: {:?}", old);
        let mut new_next_max_pair = old;
        if new_next_max_pair.0 >= new_next_max_pair.1 {
            let step = self.counter.get_step();
            let new_max = self.backend.increase_count_by(self.counter, step)?;
            new_next_max_pair.0 = new_max - step as u64;
            new_next_max_pair.1 = new_max;
        }
        debug!("new_nest_max_pair: {:?}", new_next_max_pair);
        let mut next = new_next_max_pair.0;
        new_next_max_pair.0 += 1;
        while next <= 1 {
            next = new_next_max_pair.0;
            new_next_max_pair.0 += 1;
        }
        debug!(
            "next: {:?}, new_nest_max_pair: {:?}",
            next, new_next_max_pair
        );

        // in case of compare_exchange failed, we need to retry
        if old != new_next_max_pair {
            while let Err(new_old) = self.next_max_pair.compare_exchange(old, new_next_max_pair) {
                new_next_max_pair = new_old;
                next = new_next_max_pair.0;
                new_next_max_pair.0 += 1;
                while next <= 1 {
                    next = new_next_max_pair.0;
                    new_next_max_pair.0 += 1;
                }
            }
        }
        Ok(next)
    }
}
