mod disk_pool;
pub mod memory_pool;

use std::sync::Arc;

use kiseki_types::{PAGE_BUFFER_SIZE, PAGE_SIZE};
use lazy_static::lazy_static;
use memory_pool::MemoryPagePool;

lazy_static! {
    pub static ref GLOBAL_MEMORY_PAGE_POOL: Arc<MemoryPagePool> =
        MemoryPagePool::new(PAGE_SIZE, PAGE_BUFFER_SIZE);
}

pub trait Pool {}

pub trait Page {}
