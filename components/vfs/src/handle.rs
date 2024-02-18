// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use kiseki_common::FH;
use kiseki_meta::context::FuseContext;
use kiseki_types::{entry::Entry, ino::Ino};
use kiseki_utils::readable_size::ReadableSize;
use libc::clone;
use snafu::ResultExt;
use tokio::{
    sync::{Notify, RwLock},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, Instrument};

use crate::{
    data_manager::DataManagerRef,
    err::{Error, JoinErrSnafu, LibcSnafu, Result},
    reader::FileReader,
    writer::FileWriter,
};

pub(crate) type HandleTableRef = Arc<HandleTable>;
pub(crate) struct HandleTable {
    data_manager: DataManagerRef,
    handles: DashMap<Ino, DashMap<FH, Handle>>,
    _next_fh: AtomicU64,
}

impl HandleTable {
    pub(crate) fn new(data_manager_ref: DataManagerRef) -> HandleTableRef {
        Arc::new(HandleTable {
            data_manager: data_manager_ref,
            handles: DashMap::new(),
            _next_fh: AtomicU64::new(1),
        })
    }
    fn next_fh(&self) -> FH {
        self._next_fh.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn new_dir_handle(self: &Arc<Self>, inode: Ino) -> FH {
        let fh = self.next_fh();
        let e = self.handles.entry(inode).or_insert(DashMap::new());
        e.insert(
            fh,
            Handle::Dir(Arc::new(DirHandle {
                fh,
                inode,
                inner: RwLock::new(DirHandleInner {
                    children: Vec::new(),
                    read_at: None,
                    ofd_owner: 0,
                }),
            })),
        );
        fh
    }

    pub(crate) fn new_file_handle(&self, inode: Ino, length: u64, flags: i32) -> Result<FH> {
        let fh = self.next_fh();
        let h = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => FileHandle::new(
                inode,
                fh,
                self.data_manager
                    .new_file_reader(inode, fh, length as usize),
                None,
            ),
            libc::O_WRONLY | libc::O_RDWR => FileHandle::new(
                inode,
                fh,
                self.data_manager
                    .new_file_reader(inode, fh, length as usize),
                Some(self.data_manager.open_file_writer(inode, length)),
            ),
            _ => LibcSnafu { errno: libc::EPERM }.fail()?,
        };
        self.handles
            .entry(inode)
            .or_default()
            .insert(fh, Handle::File(Arc::new(h)));

        Ok(fh)
    }

    pub(crate) fn find_handle(&self, ino: Ino, fh: u64) -> Option<Handle> {
        self.handles.get(&ino)?.get(&fh).map(|h| h.value().clone())
    }

    // after writes it waits for data sync, so do it after everything
    pub(crate) async fn release_file_handle(&self, inode: Ino, fh: FH) {
        if let Some(h) = self.handles.get(&inode) {
            if let Some((_, h)) = h.remove(&fh) {
                // remove the handle from the handle table.
                if let Some(fh) = h.as_file_handle() {
                    if let Err(e) = fh.wait_all_operations_done(None).await {
                        error!("wait all operations done failed: {}", e);
                    };
                    fh.close().await;
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum Handle {
    File(Arc<FileHandle>),
    Dir(Arc<DirHandle>),
}

impl Handle {
    pub(crate) fn get_fh(&self) -> FH {
        match self {
            Handle::File(h) => h.fh,
            Handle::Dir(h) => h.fh,
        }
    }
    pub(crate) fn get_inode(&self) -> Ino {
        match self {
            Handle::File(h) => h.inode,
            Handle::Dir(h) => h.inode,
        }
    }

    pub(crate) fn as_file_handle(&self) -> Option<Arc<FileHandle>> {
        match self {
            Handle::File(h) => Some(h.clone()),
            _ => None,
        }
    }

    pub(crate) fn as_dir_handle(&self) -> Option<Arc<DirHandle>> {
        match self {
            Handle::Dir(h) => Some(h.clone()),
            _ => None,
        }
    }

    pub(crate) async fn wait_all_operations_done(&self, ctx: Arc<FuseContext>) -> Result<()> {
        match self {
            Handle::File(h) => h.wait_all_operations_done(Some(ctx)).await,
            Handle::Dir(_) => return Ok(()),
        }
    }
}

pub(crate) struct FileHandle {
    fh: FH,     // cannot be changed
    inode: Ino, // cannot be changed

    reader: Arc<FileReader>,
    reader_cnt: Arc<AtomicUsize>,
    reader_notify: Arc<Notify>,

    // The underlying data structure for flushing and writing.
    writer: Option<Arc<FileWriter>>,
    // record how many write operations are waiting
    write_wait_cnt: Arc<AtomicUsize>,
    // is someone holding the exclusive lock right now?
    exclusive_locking: Arc<AtomicBool>,
    // notify when exclusive lock is released
    exclusive_lock_notify: Arc<Notify>,
    // pid -> FuseContext
    operations: RwLock<HashMap<u32, HashMap<u64, Arc<FuseContext>>>>,

    // posix-lock
    pub(crate) locks: AtomicU8,
    flock_owner: AtomicU64, // kernel 3.1- does not pass lock_owner in release()
    ofd_owner: AtomicU64,

    closed: AtomicBool,
}

impl FileHandle {
    pub(crate) fn new(
        inode: Ino,
        fh: FH,
        fr: Arc<FileReader>,
        fw: Option<Arc<FileWriter>>,
    ) -> Self {
        FileHandle {
            fh,
            inode,
            reader: fr,
            reader_cnt: Arc::new(AtomicUsize::new(0)),
            reader_notify: Arc::new(Default::default()),
            writer: fw,
            write_wait_cnt: Arc::new(AtomicUsize::new(0)),
            exclusive_locking: Arc::new(Default::default()),
            exclusive_lock_notify: Arc::new(Default::default()),
            operations: Default::default(),
            locks: Default::default(),
            flock_owner: Default::default(),
            ofd_owner: Default::default(),
            closed: Default::default(),
        }
    }
    pub(crate) fn try_set_ofd_owner(&self, lock_owner: u64) {
        let _ = self
            .ofd_owner
            .compare_exchange(lock_owner, 0, Ordering::AcqRel, Ordering::Relaxed);
    }

    pub(crate) fn has_writer(&self) -> bool {
        self.writer.is_some()
    }

    pub(crate) async fn read_lock(&self, ctx: Arc<FuseContext>) -> Option<FileHandleReadGuard> {
        let cancel_token = ctx.cancellation_token.clone();
        while self.write_wait_cnt.load(Ordering::Acquire) > 0
            || self.exclusive_locking.load(Ordering::Acquire)
        {
            // wait for exclusive lock to be released
            tokio::select! {
                _ = self.exclusive_lock_notify.notified() => {
                    debug!("exclusive lock is released, go back to check");
                }
                _ = cancel_token.cancelled() => {
                    error!("read lock is cancelled");
                    return None;
                }
            }
        }

        // add reader count
        self.reader_cnt.fetch_add(1, Ordering::AcqRel);
        if cancel_token.is_cancelled() {
            self.reader_cnt.fetch_sub(1, Ordering::AcqRel);
            return None;
        };
        let mut write_guard = self.operations.write().await;
        write_guard
            .entry(ctx.pid)
            .or_default()
            .insert(ctx.unique, ctx.clone());

        Some(FileHandleReadGuard {
            reader: self.reader.clone(),
            reader_cnt: self.reader_cnt.clone(),
            reader_notify: self.reader_notify.clone(),
            ctx,
        })
    }

    #[instrument(skip(self))]
    pub(crate) async fn write_lock(&self, ctx: Arc<FuseContext>) -> Option<FileHandleWriteGuard> {
        if self.writer.is_none() {
            return None;
        }
        let cancel_token = ctx.cancellation_token.clone();
        // 1. increase the write wait count
        self.write_wait_cnt.fetch_add(1, Ordering::AcqRel);
        loop {
            // check if exists readers
            while self.reader_cnt.load(Ordering::Acquire) > 0
                // check if someone is holding the exclusive lock
                || self.exclusive_locking.load(Ordering::Acquire)
            {
                // wait for they notify that exclusive lock has been released or reader has been released.
                tokio::select! {
                    _ =self.exclusive_lock_notify.notified() => {
                        debug!("exclusive lock is released")
                    }
                    _ = self.reader_notify.notified() => {
                        debug!("reader is released")
                    }
                    _ = cancel_token.cancelled() => {
                        // decrease the write wait count
                        self.write_wait_cnt.fetch_sub(1, Ordering::AcqRel);
                        error!("write lock is cancelled");
                        return None;
                    }
                }
            }
            // if no one is holding the exclusive lock, then we can hold it.
            // but we may not be the first one to hold it, so we need to check again
            if self
                .exclusive_locking
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                debug!("exclusive lock is hold");
                break;
            }
        }
        self.write_wait_cnt.fetch_sub(1, Ordering::AcqRel);
        if ctx.is_cancelled() {
            // we get the exclusive lock, but the write lock is cancelled
            self.exclusive_locking.store(false, Ordering::Release);
            return None;
        }

        let mut write_guard = self.operations.write().await;
        write_guard
            .entry(ctx.pid)
            .or_default()
            .insert(ctx.unique, ctx.clone());

        Some(FileHandleWriteGuard {
            file_writer: self.writer.as_ref().unwrap().clone(),
            exclusive_locking: self.exclusive_locking.clone(),
            exclusive_lock_notify: self.exclusive_lock_notify.clone(),
            ctx,
        })
    }

    pub(crate) fn get_posix_lock_info(&self) -> (u8, u64, u64) {
        (
            self.locks.load(Ordering::Acquire),
            self.flock_owner.load(Ordering::Acquire),
            self.ofd_owner.load(Ordering::Acquire),
        )
    }

    pub(crate) async fn cancel_operations(&self, pid: &u32) {
        let mut write_guard = self.operations.write().await;
        if let Some(ctxes) = write_guard.remove(pid) {
            for (_, ctx) in ctxes {
                ctx.cancellation_token.cancel();
            }
        }
    }

    pub(crate) async fn remove_operation(&self, ctx: &Arc<FuseContext>) {
        let mut write_guard = self.operations.write().await;
        if let Some(ctxes) = write_guard.get_mut(&ctx.pid) {
            ctxes.remove(&ctx.unique);
        }
    }

    pub(crate) async fn wait_all_operations_done(
        &self,
        ctx: Option<Arc<FuseContext>>,
    ) -> Result<()> {
        let cancel_token = ctx.map(|ctx| ctx.cancellation_token.clone());
        while self.reader_cnt.load(Ordering::Acquire) > 0
            // check if someone is holding the exclusive lock
            || self.write_wait_cnt.load(Ordering::Acquire) > 0 || self.exclusive_locking.load(Ordering::Acquire)
        {
            // wait for they notify that exclusive lock has been released or reader has been released.
            if cancel_token.is_some() {
                let cancel_token = cancel_token.as_ref().unwrap().clone();
                tokio::select! {
                    _ = self.exclusive_lock_notify.notified() => {
                        debug!("exclusive lock is released")
                    }
                    _ = self.reader_notify.notified() => {
                        debug!("reader is released")
                    }
                    _ = cancel_token.cancelled() => {
                        error!("wait all operations done is cancelled");
                        return LibcSnafu { errno: libc::ECANCELED }.fail();
                    }
                }
            } else {
                tokio::select! {
                    _ = self.exclusive_lock_notify.notified() => {
                        debug!("exclusive lock is released")
                    }
                    _ = self.reader_notify.notified() => {
                        debug!("reader is released")
                    }
                }
            }
        }
        return Ok(());
    }

    pub(crate) async fn close(&self) {
        self.reader.close();
        if let Some(writer) = &self.writer {
            writer.close().await;
        }
    }

    // Flush without lock
    pub(crate) async fn unsafe_flush(&self) -> Result<()> {
        if let Some(writer) = &self.writer {
            writer.flush().await
        } else {
            Ok(())
        }
    }
}

pub(crate) struct FileHandleWriteGuard {
    file_writer: Arc<FileWriter>,
    exclusive_locking: Arc<AtomicBool>,
    exclusive_lock_notify: Arc<Notify>,
    ctx: Arc<FuseContext>,
}

impl FileHandleWriteGuard {
    pub(crate) async fn write(&self, offset: usize, src: &[u8]) -> Result<usize> {
        self.file_writer.write(offset, src).await
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        self.file_writer.flush().await
    }

    pub(crate) fn get_length(&self) -> usize {
        self.file_writer.get_length()
    }
}

impl Drop for FileHandleWriteGuard {
    fn drop(&mut self) {
        self.exclusive_locking.store(false, Ordering::Release);
        self.exclusive_lock_notify.notify_waiters();
    }
}

pub(crate) struct FileHandleReadGuard {
    reader: Arc<FileReader>,
    reader_cnt: Arc<AtomicUsize>,
    reader_notify: Arc<Notify>,
    ctx: Arc<FuseContext>,
}

impl FileHandleReadGuard {
    pub(crate) async fn read(&self, offset: usize, dst: &mut [u8]) -> Result<usize> {
        self.reader.read(offset, dst).await
    }
}

impl Drop for FileHandleReadGuard {
    fn drop(&mut self) {
        self.reader_cnt.fetch_sub(1, Ordering::AcqRel);
        self.reader_notify.notify_waiters()
    }
}

pub(crate) struct DirHandle {
    fh: FH,     // cannot be changed
    inode: Ino, // cannot be changed
    pub(crate) inner: RwLock<DirHandleInner>,
}

pub(crate) struct DirHandleInner {
    pub(crate) children: Vec<Entry>,
    pub(crate) read_at: Option<Instant>,
    pub(crate) ofd_owner: u64, // OFD lock
}
