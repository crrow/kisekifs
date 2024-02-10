use std::{
    fmt::{Display, Formatter},
    mem,
    ops::{Deref, DerefMut},
    ptr,
    sync::Arc,
};

use crossbeam_queue::ArrayQueue;
use kiseki_utils::readable_size::ReadableSize;
use lazy_static::lazy_static;
use tokio::{sync::Notify, time::Instant};
use tracing::debug;

pub struct MemoryPagePool {
    page_size: usize,
    capacity: usize,
    queue: ArrayQueue<Vec<u8>>,
    notify: Notify,
}

impl MemoryPagePool {
    pub fn new(page_size: usize, capacity: usize) -> Arc<Self> {
        let start_at = Instant::now();
        debug_assert!(
            page_size > 0 && capacity > 0 && capacity % page_size == 0 && capacity > page_size,
            "invalid page pool"
        );

        debug!(
            "page pool: page_size: {}, capacity: {}",
            ReadableSize(page_size as u64),
            ReadableSize(capacity as u64)
        );
        let page_cnt = capacity / page_size;
        let pool = Arc::new(Self {
            page_size,
            capacity,
            queue: ArrayQueue::new(page_cnt),
            notify: Default::default(),
        });

        (0..page_cnt).for_each(|_| {
            pool.queue.push(vec![0; page_size]).unwrap();
        });

        debug!(
            "{} initialize finished, cost: {:?}",
            &pool,
            start_at.elapsed(),
        );
        pool
    }

    pub fn try_acquire_page(self: &Arc<Self>) -> Option<Page> {
        let r = self.queue.pop()?;
        Some(Page {
            data: mem::ManuallyDrop::new(r),
            _pool: self.clone(),
        })
    }

    pub async fn acquire_page(self: &Arc<Self>) -> Page {
        let mut r = self.queue.pop();
        while let None = r {
            self.notify.notified().await;
            r = self.queue.pop();
        }
        Page {
            data: mem::ManuallyDrop::new(r.unwrap()),
            _pool: self.clone(),
        }
    }

    fn notify_page_ready(self: &Arc<Self>) {
        self.notify.notify_one();
    }

    fn recycle(self: &Arc<Self>, data: Vec<u8>) {
        self.queue.push(data).unwrap();
        self.notify_page_ready();
    }

    pub fn remain_page_cnt(&self) -> usize {
        self.queue.len()
    }

    #[inline]
    pub fn total_page_cnt(&self) -> usize {
        self.capacity / self.page_size
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Display for MemoryPagePool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PagePool {{ page_size: {}, capacity: {}, remain: {}, total_cnt: {} }}",
            ReadableSize(self.page_size as u64),
            ReadableSize(self.capacity as u64),
            self.remain_page_cnt(),
            self.total_page_cnt(),
        )
    }
}

/// The value returned by an allocation of the pool.
/// When it is dropped the memory gets returned into the pool, and is not
/// zeroed. If that is a concern, you must clear the data yourself.
pub struct Page {
    data: mem::ManuallyDrop<Vec<u8>>,
    _pool: Arc<MemoryPagePool>,
}

impl Drop for Page {
    fn drop(&mut self) {
        let mut data = mem::ManuallyDrop::into_inner(unsafe { ptr::read(&self.data) });
        data.clear();
        unsafe { data.set_len(self._pool.page_size) };
        self._pool.recycle(data);
    }
}

impl Deref for Page {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data.deref()
    }
}

impl DerefMut for Page {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.deref_mut()
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, time::Duration};

    use kiseki_utils::logger::install_fmt_log;
    use tracing::info;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        install_fmt_log();

        let pool = MemoryPagePool::new(128 << 10, 300 << 20);
        let page = pool.acquire_page().await;
        assert_eq!(page.len(), 128 << 10);
        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt() - 1);
        drop(page);
        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
    }

    #[tokio::test]
    async fn get_page_concurrently() {
        install_fmt_log();
        let pool = MemoryPagePool::new(128 << 10, 300 << 20);

        let start = std::time::Instant::now();
        let mut handles = vec![];
        for _ in 0..pool.total_page_cnt() {
            let pool = pool.clone();
            let handle = tokio::spawn(async move {
                let mut page = pool.acquire_page().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                let mut buf = page.as_mut_slice();
                let write_len = buf.write(b"hello").unwrap();
                assert_eq!(write_len, 5);
            });
            handles.push(handle);
        }

        assert!(pool.remain_page_cnt() <= pool.total_page_cnt());
        let _ = futures::future::join_all(handles).await;

        info!(
            "fill the whole pool {} cost: {:?}",
            ReadableSize(pool.capacity() as u64),
            start.elapsed(),
        );

        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
    }
}
