use std::{
    cell::UnsafeCell,
    collections::HashMap,
    fmt::{Display, Formatter},
    io::{Cursor, Write},
    mem,
    ops::{Deref, DerefMut},
    ptr,
    sync::{atomic::AtomicU8, Arc},
};

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use kiseki_utils::readable_size::ReadableSize;
use lazy_static::lazy_static;
use snafu::ResultExt;
use tokio::{
    io::AsyncReadExt,
    sync::{Notify, RwLock},
    time::Instant,
};
use tracing::debug;

use crate::err::{DiskPoolMmapSnafu, UnknownIOSnafu};

pub struct MemoryPagePool {
    page_size: usize,
    capacity: usize,
    queue: ArrayQueue<u64>,
    raw_pages: Box<[Slot]>,
    notify: Notify,
}

struct Slot {
    inner: UnsafeCell<&'static mut [u8]>,
    page_size: usize,
}

unsafe impl Send for Slot {}

unsafe impl Sync for Slot {}

impl Slot {
    fn get_inner_slice(&self, offset: usize, len: usize) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                ((&*self.inner.get()).as_ptr() as usize + offset) as *const u8,
                len,
            )
        }
    }

    fn get_mut_inner_slice(&self, offset: usize, len: usize) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                ((&mut *self.inner.get()).as_mut_ptr() as usize + offset) as *mut u8,
                len,
            )
        }
    }

    fn clear(&self) {
        unsafe {
            let mut slice = std::slice::from_raw_parts_mut(
                ((&mut *self.inner.get()).as_mut_ptr() as usize) as *mut u8,
                self.page_size,
            );
            slice.fill(0);
        };
    }
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

        let page_buffer = Box::leak(vec![0u8; page_cnt * page_size].into_boxed_slice());
        let slots = page_buffer
            .chunks_exact_mut(page_size)
            .map(|chunk| {
                let buf: &mut [u8] = chunk.try_into().unwrap();
                Slot {
                    inner: UnsafeCell::new(buf),
                    page_size,
                }
            })
            .collect();

        let pool = Arc::new(Self {
            page_size,
            capacity,
            queue: ArrayQueue::new(page_cnt),
            raw_pages: slots,
            notify: Default::default(),
        });

        (0..page_cnt as u64).for_each(|page_id| {
            pool.queue.push(page_id).unwrap();
        });

        debug!(
            "{} initialize finished, cost: {:?}",
            &pool,
            start_at.elapsed(),
        );
        pool
    }

    pub fn try_acquire_page(self: &Arc<Self>) -> Option<Page> {
        Some(Page {
            page_id: self.queue.pop()?,
            _pool: self.clone(),
        })
    }

    pub async fn acquire_page(self: &Arc<Self>) -> Page {
        loop {
            if let Some(page_id) = self.queue.pop() {
                return Page {
                    page_id,
                    _pool: self.clone(),
                };
            }
            self.notify.notified().await;
        }
    }

    fn notify_page_ready(self: &Arc<Self>) {
        self.notify.notify_one();
    }

    fn recycle(self: &Arc<Self>, page_id: u64) {
        let slot = &self.raw_pages[page_id as usize];
        slot.clear();
        self.queue.push(page_id).unwrap();
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
    page_id: u64,
    _pool: Arc<MemoryPagePool>,
}

impl Page {
    pub(crate) async fn copy_to_writer<W>(
        &self,
        offset: usize,
        length: usize,
        writer: &mut W,
    ) -> crate::err::Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin + ?Sized,
    {
        let slot = &self._pool.raw_pages[self.page_id as usize];
        let slice = slot.get_inner_slice(offset, length);
        let mut cursor = Cursor::new(slice);
        let copy_len = tokio::io::copy(&mut cursor, writer)
            .await
            .context(UnknownIOSnafu)?;
        debug_assert_eq!(copy_len as usize, length);
        Ok(())
    }
    pub(crate) async fn copy_from_reader<R>(
        &self,
        offset: usize,
        length: usize,
        reader: &mut R,
    ) -> crate::err::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        let slot = &self._pool.raw_pages[self.page_id as usize];
        let slice = slot.get_mut_inner_slice(offset, length);
        let mut cursor = Cursor::new(slice);
        let copy_len = tokio::io::copy(reader, &mut cursor)
            .await
            .context(UnknownIOSnafu)?;
        debug_assert_eq!(copy_len as usize, length);
        Ok(())
    }

    pub(crate) fn size(&self) -> usize {
        self._pool.page_size
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        self._pool.recycle(self.page_id);
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, time::Duration};

    use kiseki_utils::logger::install_fmt_log;
    use tracing::info;

    use super::*;

    #[test]
    fn slot() {
        let page_size = 1024;
        let page_cnt = 10;
        let page_buffer = Box::leak(vec![0u8; page_cnt * page_size].into_boxed_slice());
        let slots: Box<[Slot]> = page_buffer
            .chunks_exact_mut(page_size)
            .map(|chunk| {
                let buf: &mut [u8] = chunk.try_into().unwrap();
                Slot {
                    inner: UnsafeCell::new(buf),
                    page_size,
                }
            })
            .collect();

        let slot = &slots[0];
        let mut buf = slot.get_mut_inner_slice(0, 5);
        buf.write_all(b"hello").unwrap();
        let slice = slot.get_inner_slice(0, 5);
        assert_eq!(slice, b"hello");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        install_fmt_log();

        let pool = MemoryPagePool::new(128 << 10, 300 << 20);
        let page = pool.acquire_page().await;
        assert_eq!(page.size(), 128 << 10);
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
                let page2 = pool.acquire_page().await;
                let page = pool.acquire_page().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                let mut cursor = Cursor::new(b"hello");
                page.copy_from_reader(0, 5, &mut cursor).await.unwrap();
                // let mut buf = page.as_mut_slice();
                // let write_len = buf.write(b"hello").unwrap();
                // assert_eq!(write_len, 5);
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
