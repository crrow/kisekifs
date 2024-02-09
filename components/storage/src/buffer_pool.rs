use std::{
    io::Read,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use lazy_static::lazy_static;
use thingbuf::{Recycle, Ref, StaticThingBuf};
use tokio::sync::Notify;
use tracing::info;

use crate::PAGE_SIZE;

/// HybridBufferPool is a hybrid buffer pool,
/// when we cannot get a buffer from the memory pool,
/// we will produce a file handle for writing.
struct HybridBufferPool {
    memory_pool: Option<Ref<'static, Vec<u8>>>,
    notify: Arc<Notify>,
}

/// BufferPool is a pre-allocated buffer pool.
pub fn new_memory_buffer_pool<const BUFFER_SIZE: usize, const CAP: usize>()
-> StaticThingBuf<Vec<u8>, CAP, PageRecycler> {
    let pool = StaticThingBuf::<Vec<u8>, CAP, PageRecycler>::with_recycle(PageRecycler {
        page_size: BUFFER_SIZE,
    });

    while let Ok(mut slot) = pool.push_ref() {
        unsafe {
            slot.set_len(BUFFER_SIZE);
        }
    }

    assert_eq!(pool.remaining(), 0);
    pool
}

const PAGE_CNT: usize = 300 << 20 / PAGE_SIZE;

/// The global buffer pool.
lazy_static! {
    static ref GLOBAL_PAGE_POOL: StaticThingBuf<Vec<u8>, PAGE_CNT, PageRecycler> =
        new_memory_buffer_pool::<PAGE_SIZE, PAGE_CNT>();
    static ref AVAILABLE_NOTIFY: Arc<Notify> = Arc::new(Notify::new());
}

/// PageRecycler does nothing.
struct PageRecycler {
    page_size: usize,
}

impl Recycle<Vec<u8>> for PageRecycler {
    fn new_element(&self) -> Vec<u8> {
        vec![0; self.page_size]
    }

    fn recycle(&self, element: &mut Vec<u8>) {
        element.clear();
        unsafe {
            element.set_len(self.page_size);
        }
    }
}

pub struct Page {
    inner: Option<Ref<'static, Vec<u8>>>,
    notify: Arc<Notify>,
}

impl Deref for Page {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("inner is None")
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("inner is None")
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        let mut inner = self.inner.take().expect("inner is None");
        drop(inner);

        assert!(GLOBAL_PAGE_POOL.push_ref().is_ok());

        // Notify someone to get a page.
        self.notify.notify_one();
    }
}

/// We will block until we get a page from the pool.
pub async fn get_page() -> Page {
    let mut r = GLOBAL_PAGE_POOL.pop_ref();
    while let None = r {
        AVAILABLE_NOTIFY.notified().await;
        r = GLOBAL_PAGE_POOL.pop_ref();
    }
    Page {
        inner: Some(r.unwrap()),
        notify: AVAILABLE_NOTIFY.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Cursor, Read, Write},
        sync::Mutex,
        time::Instant,
    };

    use rand::{thread_rng, Rng};
    use tracing::info;

    use super::*;

    #[tokio::test]
    async fn pool_has_initialized() {
        kiseki_utils::logger::install_fmt_log();

        let page = get_page().await;
        assert_eq!(GLOBAL_PAGE_POOL.len(), PAGE_CNT - 1);
        drop(page);
        assert_eq!(GLOBAL_PAGE_POOL.len(), PAGE_CNT);
    }

    #[test]
    fn get_page_from_empty() {
        let pool = StaticThingBuf::<Vec<u8>, 1>::new();
        pool.push_ref().unwrap();
        assert!(pool.pop_ref().is_some());
        assert!(pool.pop_ref().is_none());
    }

    #[tokio::test]
    async fn basic() {
        kiseki_utils::logger::install_fmt_log();

        let data = random_data(PAGE_SIZE);
        let start = Instant::now();

        for _ in 0..PAGE_CNT {
            let mut page = get_page().await;
            let mut buf = page.as_mut_slice();
            buf.copy_from_slice(&data);

            let s = page.as_slice();
            let mut reader = Cursor::new(s);
            let mut buf = vec![0u8; PAGE_SIZE];
            reader.read_exact(&mut buf).unwrap();
            assert_eq!(buf, data);
        }

        info!(
            "expect_cnt: {}, initialize and write data total cost: {:?}",
            PAGE_CNT,
            start.elapsed(),
        );

        let x = GLOBAL_PAGE_POOL.push_ref();
        assert!(x.is_err());

        // should be able to get a page again
        for _ in 0..PAGE_CNT {
            let mut page = get_page().await;
            let mut buf = page.as_mut_slice();
            buf.copy_from_slice(&data);

            let s = page.as_slice();
            let mut reader = Cursor::new(s);
            let mut buf = vec![0u8; PAGE_SIZE];
            reader.read_exact(&mut buf).unwrap();
            assert_eq!(buf, data);
        }
    }

    #[test]
    fn raw_vec_vec() {
        kiseki_utils::logger::install_fmt_log();
        const TOTAL: usize = 300 << 20;
        const PAGE_SIZE: usize = 128 << 10;
        const PAGE_CNT: usize = TOTAL / PAGE_SIZE;
        let mut vec_pool = Mutex::new(vec![vec![0u8; PAGE_SIZE]; PAGE_CNT]);
        let data = random_data(PAGE_SIZE);
        let start = Instant::now();
        for i in 0..PAGE_CNT {
            let mut pool = vec_pool.lock().unwrap();
            let v = &mut pool[i];
            let buf = v.as_mut_slice();
            buf.copy_from_slice(&data);

            let mut reader = Cursor::new(v.as_slice());
            let mut buf = vec![0u8; PAGE_SIZE];
            reader.read_exact(&mut buf).unwrap();
            assert_eq!(buf, data);
        }
        info!(
            "expect_cnt: {}, initialize and write data total cost: {:?}",
            PAGE_CNT,
            start.elapsed(),
        );
    }
    fn random_data(size: usize) -> Vec<u8> {
        let mut data = vec![0u8; size];
        let mut rng = thread_rng();
        rng.fill(data.as_mut_slice());
        data
    }
}
