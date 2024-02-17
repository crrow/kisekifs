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
    handles: DashMap<Ino, DashMap<FH, Arc<HandleV2>>>,
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
            Arc::new(HandleV2::Dir(DirHandle {
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
            libc::O_RDONLY => FileHandle {
                fh,
                inode,
                reader: self
                    .data_manager
                    .new_file_reader(inode, fh, length as usize),
                writer: None,
                locks: 0,
                flock_owner: 0,
                ofd_owner: Default::default(),
            },
            libc::O_WRONLY | libc::O_RDWR => FileHandle {
                fh,
                inode,
                reader: self
                    .data_manager
                    .new_file_reader(inode, fh, length as usize),
                writer: Some(self.data_manager.open_file_writer(inode, length)),
                locks: 0,
                flock_owner: 0,
                ofd_owner: Default::default(),
            },
            _ => LibcSnafu { errno: libc::EPERM }.fail()?,
        };
        self.handles
            .entry(inode)
            .or_default()
            .insert(fh, Arc::new(HandleV2::File(h)));

        Ok(fh)
    }

    pub(crate) fn find_handle(&self, ino: Ino, fh: u64) -> Option<Arc<HandleV2>> {
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
}

pub(crate) enum HandleV2 {
    File(FileHandle),
    Dir(DirHandle),
}

impl HandleV2 {
    pub(crate) fn get_fh(&self) -> FH {
        match self {
            HandleV2::File(h) => h.fh,
            HandleV2::Dir(h) => h.fh,
        }
    }
    pub(crate) fn get_inode(&self) -> Ino {
        match self {
            HandleV2::File(h) => h.inode,
            HandleV2::Dir(h) => h.inode,
        }
    }

    pub(crate) fn as_file_handle(&self) -> Option<&FileHandle> {
        match self {
            HandleV2::File(h) => Some(h),
            _ => None,
        }
    }

    pub(crate) fn as_dir_handle(&self) -> Option<&DirHandle> {
        match self {
            HandleV2::Dir(h) => Some(h),
            _ => None,
        }
    }
}

pub(crate) struct FileHandle {
    fh: FH,     // cannot be changed
    inode: Ino, // cannot be changed
    pub(crate) reader: Arc<FileReader>,
    pub(crate) writer: Option<Arc<FileWriter>>,
    pub(crate) locks: u8,
    flock_owner: u64, // kernel 3.1- does not pass lock_owner in release()
    pub(crate) ofd_owner: AtomicU64,
}

impl FileHandle {
    pub(crate) fn try_set_ofd_owner(&self, lock_owner: u64) {
        let _ = self
            .ofd_owner
            .compare_exchange(lock_owner, 0, Ordering::AcqRel, Ordering::Relaxed);
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

// pub(crate) struct Handle {
//     fh: FH,                                // cannot be changed
//     inode: Ino,                            // cannot be changed
//     pub(crate) inner: RwLock<HandleInner>, // status lock
//     pub(crate) reader: Arc<FileReader>,
//     pub(crate) writer: Arc<FileWriter>,
//
//     pub(crate) locks: u8,
//     flock_owner: u64, // kernel 3.1- does not pass lock_owner in release()
// }
//
// impl Handle {
//     pub(crate) fn new(fh: u64, inode: Ino) -> Self {
//         let inner = HandleInner::new();
//         let notify = Arc::new(Notify::new());
//
//         Self {
//             fh,
//             inode,
//             inner: RwLock::new(inner),
//             locks: 0,
//             flock_owner: 0,
//         }
//     }
//     pub(crate) fn new_with<F: FnMut(&mut Handle)>(fh: u64, inode: Ino, mut f:
// F) -> Self {         let mut h = Self {
//             fh,
//             inode,
//             inner: RwLock::new(HandleInner::new()),
//             locks: 0,
//             flock_owner: 0,
//         };
//         f(&mut h);
//         h
//     }
//     pub(crate) fn fh(&self) -> u64 {
//         self.fh
//     }
//
//     pub(crate) fn inode(&self) -> Ino {
//         self.inode
//     }
//
//     pub(crate) async fn get_ofd_owner(&self) -> u64 {
//         self.inner.read().await.ofd_owner
//     }
//
//     pub(crate) async fn set_ofd_owner(&self, value: u64) {
//         self.inner.write().await.ofd_owner = value;
//     }
// }
//
// pub(crate) struct HandleInner {
//     pub(crate) children: Vec<Entry>,
//     pub(crate) read_at: Option<Instant>,
//     pub(crate) ofd_owner: u64, // OFD lock
// }
//
// impl HandleInner {
//     pub(crate) fn new() -> Self {
//         Self {
//             children: Vec::new(),
//             read_at: None,
//             ofd_owner: 0,
//         }
//     }
// }
