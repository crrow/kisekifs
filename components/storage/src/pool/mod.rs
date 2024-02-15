pub mod disk_pool;
pub mod memory_pool;

use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
    thread,
};

use kiseki_common::{PAGE_BUFFER_SIZE, PAGE_SIZE};
use kiseki_utils::readable_size::ReadableSize;
use lazy_static::lazy_static;

use crate::err::Result;

lazy_static! {
    pub static ref GLOBAL_MEMORY_PAGE_POOL: Arc<memory_pool::MemoryPagePool> =
        memory_pool::MemoryPagePool::new(PAGE_SIZE, PAGE_BUFFER_SIZE);
    pub static ref GLOBAL_HYBRID_PAGE_POOL: Arc<HybridPagePool> = thread::spawn(|| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.handle().block_on(async {
            Arc::new(
                PagePoolBuilder::default()
                    .with_page_size(PAGE_SIZE)
                    .with_memory_capacity(1 << 30)
                    .with_disk_capacity(1 << 30)
                    .build()
                    .await
                    .unwrap(),
            )
        })
    })
    .join()
    .unwrap();
}

const DEFAULT_DISK_PAGE_POOL_PATH: &str = "/tmp/kiseki.page_pool";

#[derive(Debug, Default)]
pub struct PagePoolBuilder {
    page_size: usize,
    memory_capacity: usize,
    // disk page pool is optional
    disk_capacity: Option<usize>,
}

impl PagePoolBuilder {
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }
    pub fn with_memory_capacity(mut self, memory_capacity: usize) -> Self {
        self.memory_capacity = memory_capacity;
        self
    }
    pub fn with_disk_capacity(mut self, disk_capacity: usize) -> Self {
        self.disk_capacity = Some(disk_capacity);
        self
    }
    pub async fn build(self) -> Result<HybridPagePool> {
        let mut total_page_cnt = self.memory_capacity / self.page_size;
        let memory_pool = memory_pool::MemoryPagePool::new(self.page_size, self.memory_capacity);
        let (disk_pool, disk_capacity) = if let Some(disk_capacity) = self.disk_capacity {
            total_page_cnt += disk_capacity / self.page_size;
            let disk_pool = disk_pool::DiskPagePool::new(
                DEFAULT_DISK_PAGE_POOL_PATH,
                self.page_size,
                disk_capacity,
            )
            .await?;
            (Some(disk_pool), disk_capacity)
        } else {
            (None, 0)
        };

        Ok(HybridPagePool {
            page_size: self.page_size,
            memory_capacity: self.memory_capacity,
            disk_capacity,
            total_page_cnt,
            memory_pool,
            disk_pool,
        })
    }
}

/// HybridPagePool is a hybrid page pool that can store pages in memory and on
/// disk. It is used to store pages in memory when the memory is sufficient, and
/// to store pages on disk when the memory is insufficient.
pub struct HybridPagePool {
    page_size: usize,
    memory_capacity: usize,
    disk_capacity: usize,
    total_page_cnt: usize,

    memory_pool: Arc<memory_pool::MemoryPagePool>,
    disk_pool: Option<Arc<disk_pool::DiskPagePool>>,
}

impl Debug for HybridPagePool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HybridPagePool {{ page_size: {}, memory_capacity: {}, disk_capacity: {}, disk_path: {}, remain_page_cnt: {}, total_page_cnt: {} }}",
            ReadableSize(self.page_size as u64),
            ReadableSize(self.memory_capacity as u64),
            ReadableSize(self.disk_capacity as u64),
            DEFAULT_DISK_PAGE_POOL_PATH,
            self.remain(),
            self.total_page_cnt,
        )
    }
}

impl HybridPagePool {
    pub fn try_acquire_page(self: &Arc<Self>) -> Option<Page> {
        if let Some(page) = self.memory_pool.try_acquire_page() {
            return Some(Page::Memory(page));
        }

        if let Some(disk_pool) = &self.disk_pool {
            if let Some(page) = disk_pool.try_acquire_page() {
                return Some(Page::Disk(page));
            }
        }

        None
    }
    /// acquire_page will wait and  acquire a page from the page pool.
    pub async fn acquire_page(self: &Arc<Self>) -> Page {
        // let disk_pool = self.disk_pool.as_ref().unwrap();
        // let page = disk_pool.acquire_page().await;
        // return Page::Disk(page);

        if self.memory_pool.remain_page_cnt() > 0 {
            if let Some(page) = self.try_acquire_page() {
                return page;
            }
        }

        if let Some(disk_pool) = &self.disk_pool {
            let page = disk_pool.acquire_page().await;
            return Page::Disk(page);
        }
        let page = self.memory_pool.acquire_page().await;
        Page::Memory(page)
    }

    pub fn remain(&self) -> usize {
        self.memory_pool.remain_page_cnt()
            + self
                .disk_pool
                .as_ref()
                .map_or(0, |pool| pool.remain_page_cnt())
    }

    pub fn total_page_cnt(&self) -> usize {
        self.total_page_cnt
    }

    pub fn capacity(&self) -> usize {
        self.memory_capacity + self.disk_capacity
    }

    pub fn free_ratio(&self) -> f64 {
        self.remain() as f64 / self.total_page_cnt as f64
    }
}

pub enum Page {
    Memory(memory_pool::Page),
    Disk(disk_pool::Page),
}

impl Page {
    pub(crate) async fn copy_to_writer<W>(
        &self,
        offset: usize,
        length: usize,
        writer: &mut W,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin + ?Sized,
    {
        match self {
            Page::Memory(page) => page.copy_to_writer(offset, length, writer).await,
            Page::Disk(page) => page.copy_to_writer(offset, length, writer).await,
        }
    }
    pub(crate) async fn copy_from_reader<R>(
        &self,
        offset: usize,
        length: usize,
        reader: &mut R,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        match self {
            Page::Memory(page) => page.copy_from_reader(offset, length, reader).await,
            Page::Disk(page) => page.copy_from_reader(offset, length, reader).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, thread};

    use kiseki_utils::logger::install_fmt_log;
    use tokio::time::Instant;
    use tokio_util::io::StreamReader;
    use tracing::debug;

    use super::*;

    #[tokio::test]
    async fn basic() {
        install_fmt_log();

        let start = Instant::now();

        let pool = Arc::new(
            PagePoolBuilder::default()
                .with_page_size(PAGE_SIZE)
                .with_memory_capacity(300 << 20)
                .with_disk_capacity(1 << 30)
                .build()
                .await
                .unwrap(),
        );

        let total_page_cnt = pool.total_page_cnt();
        let handles = (0..total_page_cnt)
            .map(|i| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    let page = pool.acquire_page().await;
                    let mut data = Vec::from(format!("hello {}", i));
                    let data_len = data.len();
                    let mut cursor = Cursor::new(&mut data);
                    page.copy_from_reader(0, data_len, &mut cursor)
                        .await
                        .unwrap();

                    let mut dst = vec![0u8; data_len];
                    let mut writer = Cursor::new(&mut dst);
                    page.copy_to_writer(0, data_len, &mut writer).await.unwrap();
                    assert_eq!(dst, data);
                })
            })
            .collect::<Vec<_>>();

        futures::future::join_all(handles).await;

        debug!(
            "total time: {:?} for {}",
            start.elapsed(),
            ReadableSize(pool.capacity() as u64)
        );
    }
}
