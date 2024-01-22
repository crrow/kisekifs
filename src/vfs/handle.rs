use std::fmt::Debug;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use libc::{EBADF, EPERM};
use snafu::Snafu;
use std::sync::RwLock;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio::{select, time::Instant};
use tracing::debug;

use crate::{
    meta::types::{Entry, Ino},
    vfs::{err::Result, reader::FileReader, writer::FileWriter, KisekiVFS, VFSError},
};

impl KisekiVFS {
    pub(crate) fn next_fh(&self) -> u64 {
        self._next_fh
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn new_handle(&self, inode: Ino) -> u64 {
        let fh = self.next_fh();
        let h = Handle::new(fh, inode);
        match self.handles.get_mut(&inode) {
            None => {
                let fh_handle_map = DashMap::new();
                fh_handle_map.insert(fh, h);
                self.handles.insert(inode, fh_handle_map);
            }
            Some(mut fh_handle_map) => {
                let l = fh_handle_map.value_mut();
                l.insert(fh, h);
            }
        };
        fh
    }

    pub(crate) fn new_file_handle(&self, inode: Ino, length: u64, flags: i32) -> Result<u64> {
        let fh = self.next_fh();
        match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                Handle::new_with(fh, inode, |h| {
                    h.reader = Some(self.reader.open(inode, length));
                });
            }
            libc::O_WRONLY | libc::O_RDWR => {
                Handle::new_with(fh, inode, |h| {
                    h.reader = Some(self.reader.open(inode, length));
                    h.writer = Some(self.writer.open(inode, length));
                });
            }
            _ => return Err(VFSError::ErrLIBC { kind: EPERM }),
        }

        Ok(fh)
    }

    pub(crate) fn find_handle(&self, ino: Ino, fh: u64) -> Option<Handle> {
        let list = self.handles.get(&ino).unwrap();
        let l = list.value();
        return l.get(&fh).map(|h| h.value().clone());
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Handle {
    fh: u64,                                    // cannot be changed
    inode: Ino,                                 // cannot be changed
    pub(crate) inner: Arc<RwLock<HandleInner>>, // status lock

    // we can only acquire writer when the readers is 0, then we mark the
    // readers to -1, then no writer or reader is allowed to acquire the lock again.
    //
    // we can acquire reader when the reader is not -1;
    readers: Arc<AtomicI64>,
    notify: Arc<Notify>,
    timeout: Duration,

    reader: Option<FileReader>, // TODO: how to make it concurrent safe ?
    writer: Option<FileWriter>,
}

impl Handle {
    pub(crate) fn new(fh: u64, inode: Ino) -> Self {
        let inner = HandleInner::new();
        let notify = Arc::new(Notify::new());

        Self {
            fh,
            inode,
            inner: Arc::new(RwLock::new(inner)),
            notify,
            readers: Default::default(),
            timeout: Duration::from_secs(1),
            reader: None,
            writer: None,
        }
    }
    pub(crate) fn new_with<F: FnMut(&mut Handle)>(fh: u64, inode: Ino, mut f: F) -> Self {
        let mut h = Self {
            fh,
            inode,
            inner: Arc::new(RwLock::new(HandleInner::new())),
            notify: Arc::new(Notify::new()),
            readers: Default::default(),
            timeout: Duration::from_secs(1),
            reader: None,
            writer: None,
        };
        f(&mut h);
        h
    }
    pub(crate) fn fh(&self) -> u64 {
        self.fh
    }
    pub(crate) fn inode(&self) -> Ino {
        self.inode
    }

    pub(crate) fn can_write(&self) -> bool {
        return self.writer.is_some();
    }
    pub(crate) fn can_read(&self) -> bool {
        return self.reader.is_some();
    }

    pub(crate) async fn acquire_read_lock(&self) -> Result<HandleReadGuard> {
        // we can only acquire reader when the readers is not -1,
        let start = Instant::now();
        let seq;
        loop {
            let elapsed = Instant::now().checked_duration_since(start).unwrap();
            if elapsed > self.timeout {
                return Err(VFSError::ErrTimeout {
                    timeout: self.timeout,
                });
            }
            let readers = self.readers.load(Ordering::SeqCst);
            if readers >= 0 {
                match self.readers.compare_exchange(
                    readers,
                    readers + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(r) => {
                        debug!("acquired read lock, current reader count {}", r + 1);
                        seq = r + 1;
                        break; // we get the lock
                    }
                    Err(e) => {
                        debug!("failed to acquire read lock {}", e);
                        self.notify.notified().await; // wait some to wake me up.
                    }
                }
            } else {
                debug!(
                    "exists writer, we should wait for it to finish. {}",
                    readers
                );
                // exists writer, we should wait for it to finish.
                self.notify.notified().await; // wait for someone to wake me up.
            }
        }

        // get the lock already, we should build a guard for it.
        Ok(HandleReadGuard { inner: self, seq })
    }

    pub(crate) async fn acquire_write_lock(&self) -> Result<HandleWriteGuard> {
        let seq;
        let start = Instant::now();
        // we can only acquire writer when the readers is 0,
        loop {
            let elapsed = Instant::now().checked_duration_since(start).unwrap();
            if elapsed > self.timeout {
                return Err(VFSError::ErrTimeout {
                    timeout: self.timeout,
                });
            }
            let current = self.readers.load(Ordering::SeqCst);
            if current != 0 {
                debug!(
                    "exists reader or writer, we should wait for it to finish. {}",
                    current
                );
                self.notify.notified().await; // wait for all readers to wake me up.
                continue;
            }

            match self
                .readers
                .compare_exchange(current, -1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(r) => {
                    debug!("acquired write lock, current write {}", r - 1);
                    seq = r - 1;
                    break; // we get the lock
                }
                Err(e) => {
                    debug!("failed to acquire write lock {}", e);
                    // exists writer, we should wait for it to finish.
                    self.notify.notified().await; // wait some to wake me up.
                }
            }
        }

        // get the lock already, we should build a guard for it.
        Ok(HandleWriteGuard { inner: self, seq })
    }

    pub(crate) fn write(
        &self,
        handle_write_guard: &HandleWriteGuard,
        offset: u64,
        data: &[u8],
    ) -> Result<u32> {
        todo!()
    }
}

#[derive(Debug)]
pub(crate) struct HandleInner {
    pub(crate) children: Vec<Entry>,
    pub(crate) read_at: Option<Instant>,
}

impl HandleInner {
    pub(crate) fn new() -> Self {
        Self {
            children: Vec::new(),
            read_at: None,
        }
    }
}

pub(crate) struct HandleReadGuard<'a> {
    seq: i64,
    inner: &'a Handle,
}

impl<'a> Debug for HandleReadGuard<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandleReadGuard")
            .field("seq", &self.seq)
            .finish()
    }
}

impl<'a> Drop for HandleReadGuard<'a> {
    fn drop(&mut self) {
        // we should notify the writer to wake up.
        let previous = self.inner.readers.fetch_sub(1, Ordering::SeqCst);
        if previous < 1 {
            panic!("the readers should at least be 1 while we hold the guard, but it is not.");
        }

        self.inner.notify.notify_one();
    }
}
pub(crate) struct HandleWriteGuard<'a> {
    seq: i64,
    inner: &'a Handle,
}

impl<'a> Debug for HandleWriteGuard<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandleWriteGuard")
            .field("seq", &self.seq)
            .finish()
    }
}

impl<'a> Drop for HandleWriteGuard<'a> {
    fn drop(&mut self) {
        // we should notify the writer to wake up.
        if let Err(_) =
            self.inner
                .readers
                .compare_exchange(-1, 0, Ordering::SeqCst, Ordering::SeqCst)
        {
            // we should panic here.
            panic!("the readers should be -1 while we hold the guard, but it is not.");
        }
        // we should notify one waiter to wake up.
        self.inner.notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::SliceRandom;
    use std::thread::sleep;

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn rwlock_basic_read() {
        let handle = Handle::new(0, Ino(1));
        assert_eq!(handle.readers.load(Ordering::SeqCst), 0);

        let guard = handle.acquire_read_lock().await;
        assert!(guard.is_ok());
        assert_eq!(handle.readers.load(Ordering::SeqCst), 1);
        drop(guard);
        assert_eq!(handle.readers.load(Ordering::SeqCst), 0);

        let guard1 = handle.acquire_read_lock().await;
        assert!(guard1.is_ok());
        assert_eq!(handle.readers.load(Ordering::SeqCst), 1);

        let guard2 = handle.acquire_read_lock().await;
        assert!(guard2.is_ok());
        assert_eq!(handle.readers.load(Ordering::SeqCst), 2);

        drop(guard1);
        assert_eq!(handle.readers.load(Ordering::SeqCst), 1);
        drop(guard2);
        assert_eq!(handle.readers.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn rwlock() {
        let handle = Handle::new(0, Ino(1));
        let rh = handle.clone();
        let read_handle = tokio::spawn(async move {
            let guard = rh.acquire_read_lock().await;
            assert!(guard.is_ok());
            assert_eq!(rh.readers.load(Ordering::SeqCst), 1);
            println!("acquired read lock {:?}", guard.unwrap());
            return ();
        });

        let wh = handle.clone();
        let write_handle = tokio::spawn(async move {
            let guard = wh.acquire_write_lock().await;
            assert!(guard.is_ok());
            assert_eq!(wh.readers.load(Ordering::SeqCst), -1);
            println!("acquired write lock {:?}", guard.unwrap());
            return ();
        });

        let _ = tokio::join!(read_handle, write_handle);
    }

    #[derive(Clone, Copy)]
    struct A {
        inner: *mut i32,
    }
    unsafe impl Send for crate::vfs::handle::tests::A {}
    unsafe impl Sync for crate::vfs::handle::tests::A {}
    impl crate::vfs::handle::tests::A {
        fn read(&self) -> i32 {
            unsafe { *self.inner }
        }
        fn write(&self, x: i32) {
            unsafe {
                *self.inner += x;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn rwlock_concurrent() {
        let handle = Handle::new(0, Ino(1));
        let mut jhs = vec![];
        let mut x = 0i32;
        let ptr = &mut x as *mut _;
        let ptr = A { inner: ptr };

        for i in 0..5000 {
            // let mut data = data.clone();
            let handle = handle.clone();
            let ptr = ptr.clone();
            let jh = tokio::spawn(async move {
                let wh = handle.acquire_write_lock().await;
                assert!(wh.is_ok());
                ptr.write(1);
                drop(wh);
            });
            jhs.push(jh);
        }

        for i in 0..3000 {
            let rh = handle.acquire_read_lock().await;
            assert!(rh.is_ok());
            let data = ptr.read();
            println!("read data : {}", data)
        }

        let rng = &mut rand::thread_rng();
        jhs.shuffle(rng);

        for t in jhs {
            let _ = t.await;
        }

        assert_eq!(ptr.read(), 5000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn timeout() {
        let handle = Handle::new(0, Ino(1));

        let handle1 = handle.clone();
        let wh = tokio::spawn(async move {
            let wh = handle1.acquire_write_lock().await;
            assert!(wh.is_ok());
            sleep(Duration::from_secs(2));
            drop(wh);
        });

        let handle2 = handle.clone();
        let wh2 = tokio::spawn(async move {
            let wh = handle2.acquire_write_lock().await;
            assert!(wh.is_err());
            println!("acquired write lock err {:?}", wh.unwrap_err())
        });

        assert!(wh.await.is_ok());
        let r = wh2.await.unwrap();
    }
}
