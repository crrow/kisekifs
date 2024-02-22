pub mod err;
mod pool;

pub fn get_pool_free_ratio() -> f64 { pool::GLOBAL_HYBRID_PAGE_POOL.free_ratio() }

pub mod slice_buffer;

pub mod cache;

// pub mod raw_buffer;

pub struct Storage {}

impl Storage {}
