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

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use kiseki_common::FH;
use kiseki_types::{entry::Entry, ino::Ino};
use kiseki_utils::readable_size::ReadableSize;
use libc::clone;
use snafu::ResultExt;
use tokio::{
    sync::{Notify, RwLock},
    time::Instant,
};
use tracing::{debug, Instrument};

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

    pub(crate) async fn flush_if_exists(&self, ino: Ino) -> Result<()> {
        if let Some(handles) = self.handles.get(&ino) {
            let mut join_handles = vec![];
            for h in handles.value() {
                if let Some(fh) = h.as_file_handle() {
                    if let Some(fw) = fh.writer.as_ref() {
                        let fw_cloned = fw.clone();
                        let handle = tokio::spawn(async move {
                            fw_cloned.flush().in_current_span().await?;
                            Ok::<(), Error>(())
                        });
                        join_handles.push(handle);
                        debug!(
                            "{} file writer exists and flush success, current_len: {}",
                            ino,
                            ReadableSize(fw.get_length() as u64)
                        );
                    }
                }
            }
            for v in futures::future::join_all(join_handles).await {
                v.context(JoinErrSnafu)??;
            }
        }
        Ok(())
    }

    // after writes it waits for data sync, so do it after everything
    pub(crate) async fn release_file_handle(&self, inode: Ino, fh: FH) {
        if let Some(h) = self.handles.get(&inode) {
            if let Some((_, h)) = h.remove(&fh) {
                if let Some(fh) = h.as_file_handle() {
                    fh.wait_operation_done().await;
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

    pub(crate) async fn wait_all_operations_done(&self) {
        match self {
            Handle::File(h) => h.wait_operation_done().await,
            Handle::Dir(_) => {}
        }
    }
}

pub(crate) struct FileHandle {
    fh: FH,     // cannot be changed
    inode: Ino, // cannot be changed

    reader: Arc<FileReader>,
    reader_cnt: Arc<AtomicUsize>,
    reader_notify: Arc<Notify>,

    writer: Option<Arc<FileWriter>>,
    writer_cnt: Arc<AtomicUsize>,
    writer_notify: Arc<Notify>,

    in_flushing: Arc<AtomicBool>,
    flush_notify: Arc<Notify>,

    /* lock */
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
            writer_cnt: Arc::new(AtomicUsize::new(0)),
            writer_notify: Arc::new(Default::default()),
            in_flushing: Default::default(),
            flush_notify: Arc::new(Default::default()),
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

    pub(crate) fn read(&self) -> FileHandleReadGuard {
        self.reader_cnt.fetch_add(1, Ordering::AcqRel);
        FileHandleReadGuard {
            reader: self.reader.clone(),
            reader_cnt: self.reader_cnt.clone(),
            reader_notify: self.reader_notify.clone(),
        }
    }

    pub(crate) fn write(&self) -> Option<FileHandleWriteGuard> {
        if self.writer.is_none() {
            return None;
        }
        self.writer_cnt.fetch_add(1, Ordering::AcqRel);
        Some(FileHandleWriteGuard {
            file_writer: self.writer.as_ref().unwrap().clone(),
            writer_cnt: self.writer_cnt.clone(),
            writer_notify: self.writer_notify.clone(),
        })
    }

    pub(crate) async fn try_flush(&self) -> Result<()> {
        if let Some(fw) = self.writer.as_ref() {
            while self
                .in_flushing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                // wait for flush to complete
                self.flush_notify.notified().await;
            }

            let in_flushing = self.in_flushing.clone();
            let flush_notify = self.flush_notify.clone();
            let fw = fw.clone();
            tokio::spawn(async move {
                fw.flush().in_current_span().await?;
                in_flushing.store(false, Ordering::Release);
                flush_notify.notify_waiters();
                Ok::<(), Error>(())
            })
            .await
            .context(JoinErrSnafu)??;
        }

        Ok(())
    }
    async fn wait_operation_done(&self) {
        while self.reader_cnt.load(Ordering::Acquire) > 0
            || self.writer_cnt.load(Ordering::Acquire) > 0
        {
            tokio::select! {
                _ = self.reader_notify.notified() => {}
                _ = self.writer_notify.notified() => {}
            };
        }
        while self.in_flushing.load(Ordering::Acquire) {
            self.flush_notify.notified().await;
        }
    }

    pub(crate) fn get_locks(&self) -> (u8, u64, u64) {
        (
            self.locks.load(Ordering::Acquire),
            self.flock_owner.load(Ordering::Acquire),
            self.ofd_owner.load(Ordering::Acquire),
        )
    }

    pub(crate) async fn close(&self) {
        self.reader.close();
        if let Some(writer) = &self.writer {
            writer.close().await;
        }
    }
}

pub(crate) struct FileHandleWriteGuard {
    file_writer: Arc<FileWriter>,
    writer_cnt: Arc<AtomicUsize>,
    writer_notify: Arc<Notify>,
}

impl FileHandleWriteGuard {
    pub(crate) async fn write(&self, offset: usize, src: &[u8]) -> Result<usize> {
        self.file_writer.write(offset, src).in_current_span().await
    }

    pub(crate) fn get_length(&self) -> usize {
        self.file_writer.get_length()
    }
}

impl Drop for FileHandleWriteGuard {
    fn drop(&mut self) {
        self.writer_cnt.fetch_sub(1, Ordering::AcqRel);
        self.writer_notify.notify_waiters();
    }
}

pub(crate) struct FileHandleReadGuard {
    reader: Arc<FileReader>,
    reader_cnt: Arc<AtomicUsize>,
    reader_notify: Arc<Notify>,
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
