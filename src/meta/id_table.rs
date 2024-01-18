use crate::meta::Counter;
use opendal::Operator;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A table for allocating inode numbers.
/// It starts at 2 since the root inode is 1.
pub struct IdTable {
    next_max_pair: RwLock<(u64, u64)>,
    operator: Arc<Operator>,
    counter: Counter,
    step: u64,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new(operator: Arc<Operator>, counter: Counter, step: u64) -> Self {
        Self {
            next_max_pair: RwLock::new((0, 0)),
            operator,
            counter,
            step,
        }
    }

    /// Return the next unused ID from the table.
    pub async fn next(&self) -> Result<u64, opendal::Error> {
        let mut next_max_pair = self.next_max_pair.write().await;
        if next_max_pair.0 >= next_max_pair.1 {
            let new_max = self
                .counter
                .increment_by(self.operator.clone(), self.step)
                .await?;
            next_max_pair.0 = new_max - self.step;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::INODE_BATCH;

    #[tokio::test]
    async fn id_table_alloc() {
        let mut builder = opendal::services::Memory::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.root(tempdir_path);

        let op = Arc::new(Operator::new(builder).unwrap().finish());

        let counter = Counter::NextInode;
        let id_table = IdTable::new(op, counter, INODE_BATCH);
        let x = id_table.next().await.unwrap();
        assert_eq!(x, 2);
    }
}
