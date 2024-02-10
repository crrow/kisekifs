mod disk_pool;
pub mod memory_pool;

use std::fmt::Display;
use std::sync::Arc;

use kiseki_types::{PAGE_BUFFER_SIZE, PAGE_SIZE};
use lazy_static::lazy_static;
use memory_pool::MemoryPagePool;

use crate::error::Result;

lazy_static! {
    pub static ref GLOBAL_MEMORY_PAGE_POOL: Arc<MemoryPagePool> =
        MemoryPagePool::new(PAGE_SIZE, PAGE_BUFFER_SIZE);
}

#[async_trait::async_trait]
pub trait Pool: Display + Clone + Send + Sync {
    type Page: Page;

    /// try to acquire a page from the pool.
    fn try_acquire_page(&self) -> Option<Self::Page>;

    /// acquire a page from the pool, this function may block.
    async fn acquire_page(&self) -> Self::Page;

    /// how many pages are available.
    fn remain(&self);

    /// total page count.
    fn total_page_cnt(&self) -> usize;
}

#[async_trait::async_trait]
pub trait Page: Clone + Send + Sync {
    /// copy the page's data to the writer [offset .. offset + length)
    async fn copy_to_writer<W>(&self, offset: usize, length: usize, writer: &mut W) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin + ?Sized;

    /// copy the reader's data to the page [offset .. offset + length)
    async fn copy_from_reader<R>(&self, offset: usize, length: usize, reader: &mut R) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized;
}
