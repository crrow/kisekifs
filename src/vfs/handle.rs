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
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use dashmap::DashMap;
use kiseki_types::{entry::FullEntry, ino::Ino};
use libc::EPERM;
use tokio::{
    sync::{Notify, RwLock},
    time::Instant,
};
use tracing::instrument;

use crate::vfs::{
    err::{ErrLIBCSnafu, Result},
    KisekiVFS,
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
                fh_handle_map.insert(fh, Arc::new(h));
                self.handles.insert(inode, fh_handle_map);
            }
            Some(mut fh_handle_map) => {
                let l = fh_handle_map.value_mut();
                l.insert(fh, Arc::new(h));
            }
        };
        fh
    }

    #[instrument(skip(self))]
    pub(crate) fn new_file_handle(&self, inode: Ino, length: u64, flags: i32) -> Result<u64> {
        let fh = self.next_fh();
        let h = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => Handle::new_with(fh, inode, |_h| {
                self.data_engine.new_file_reader(inode, fh, length as usize);
            }),
            libc::O_WRONLY | libc::O_RDWR => Handle::new_with(fh, inode, |_h| {
                self.data_engine.new_file_reader(inode, fh, length as usize);
                self.data_engine.new_file_writer(inode, length);
            }),
            _ => return ErrLIBCSnafu { kind: EPERM }.fail()?,
        };
        self.handles
            .entry(inode)
            .or_default()
            .insert(fh, Arc::new(h));

        Ok(fh)
    }

    pub(crate) fn find_handle(&self, ino: Ino, fh: u64) -> Option<Arc<Handle>> {
        let list = self.handles.get(&ino).unwrap();
        let l = list.value();
        return l.get(&fh).map(|h| h.value().clone());
    }
}

#[derive(Debug)]
pub(crate) struct Handle {
    fh: u64,                               // cannot be changed
    inode: Ino,                            // cannot be changed
    pub(crate) inner: RwLock<HandleInner>, // status lock

    // we can only acquire writer when the readers is 0, then we mark the
    // readers to -1, then no writer or reader is allowed to acquire the lock again.
    //
    // we can acquire reader when the reader is not -1;
    readers: Arc<AtomicI64>,
    notify: Arc<Notify>,
    timeout: Duration,

    pub(crate) locks: u8,
    flock_owner: u64, // kernel 3.1- does not pass lock_owner in release()
}

impl Handle {
    pub(crate) fn new(fh: u64, inode: Ino) -> Self {
        let inner = HandleInner::new();
        let notify = Arc::new(Notify::new());

        Self {
            fh,
            inode,
            inner: RwLock::new(inner),
            notify,
            readers: Default::default(),
            timeout: Duration::from_secs(1),
            locks: 0,
            flock_owner: 0,
        }
    }
    pub(crate) fn new_with<F: FnMut(&mut Handle)>(fh: u64, inode: Ino, mut f: F) -> Self {
        let mut h = Self {
            fh,
            inode,
            inner: RwLock::new(HandleInner::new()),
            notify: Arc::new(Notify::new()),
            readers: Default::default(),
            timeout: Duration::from_secs(1),
            locks: 0,
            flock_owner: 0,
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

    pub(crate) async fn get_ofd_owner(&self) -> u64 {
        self.inner.read().await.ofd_owner
    }

    pub(crate) async fn set_ofd_owner(&self, value: u64) {
        self.inner.write().await.ofd_owner = value;
    }
}

#[derive(Debug)]
pub(crate) struct HandleInner {
    pub(crate) children: Vec<FullEntry>,
    pub(crate) read_at: Option<Instant>,
    pub(crate) ofd_owner: u64, // OFD lock
}

impl HandleInner {
    pub(crate) fn new() -> Self {
        Self {
            children: Vec::new(),
            read_at: None,
            ofd_owner: 0,
        }
    }
}
