use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kiseki_common::ChunkIndex;
use kiseki_types::{attr::InodeAttr, ino::Ino, slice::Slices};
use tokio::{sync::RwLock, time::Instant};

/// [OpenFile] represents an opened file in the cache.
/// It is used for accelerating the query of the file's slices and attr.
#[derive(Clone)]
pub(crate) struct OpenFile(Arc<RwLock<OpenFileInner>>);

impl OpenFile {
    async fn refresh_access(&self, attr: &mut InodeAttr) {
        let mut write_guard = self.0.write().await;
        if attr.mtime != write_guard.attr.mtime {
            write_guard.chunks.clear();
        } else {
            attr.keep_cache = write_guard.attr.keep_cache;
        }
        write_guard.attr = attr.clone();
        write_guard.last_check = SystemTime::now();
    }

    async fn refresh_slices(&self, chunk_index: ChunkIndex, slices: Arc<Slices>) {
        let mut write_guard = self.0.write().await;
        write_guard.chunks.insert(chunk_index, slices);
        write_guard.last_check = UNIX_EPOCH;
    }

    async fn invalid_slices(&self, chunk_index: ChunkIndex) {
        let mut write_guard = self.0.write().await;
        write_guard.chunks.remove(&chunk_index);
    }

    async fn invalid_all_chunk(&self) {
        let mut write_guard = self.0.write().await;
        write_guard.chunks.clear();
    }

    async fn invalid_attr(&self) {
        let mut write_guard = self.0.write().await;
        write_guard.attr.keep_cache = false;
        write_guard.last_check = UNIX_EPOCH;
    }

    // decreases the reference count of the open file.
    async fn decrease_ref(&self) -> usize {
        let mut write_guard = self.0.write().await;
        write_guard.reference_count -= 1;
        write_guard.reference_count
    }

    pub(crate) async fn read_guard(&self) -> tokio::sync::RwLockReadGuard<OpenFileInner> {
        self.0.read().await
    }

    pub(crate) async fn is_opened(&self) -> bool {
        let read_guard = self.0.read().await;
        read_guard.reference_count > 0
    }
}

pub(crate) struct OpenFileInner {
    pub(crate) attr: InodeAttr,
    reference_count: usize,
    last_check:      SystemTime,
    chunks:          HashMap<usize, Arc<Slices>>,
}

pub(crate) type OpenFilesRef = Arc<OpenFiles>;

pub(crate) struct OpenFiles {
    ttl:   Duration,
    limit: usize,
    files: RwLock<HashMap<Ino, OpenFile>>,
    // TODO: background clean up
}

impl OpenFiles {
    pub(crate) fn new(ttl: Duration, limit: usize) -> Self {
        Self {
            ttl,
            limit,
            files: Default::default(),
        }
    }

    /// [load] fetches the [OpenFile] from the cache.
    pub(crate) async fn load(&self, inode: &Ino) -> Option<OpenFile> {
        let read_guard = self.files.read().await;
        let of = read_guard.get(inode).map(|v| v.clone());
        drop(read_guard); // explicit drop to release the lock
        of
    }

    /// [open] create a new [OpenFile] if it does not exist, otherwise increase
    /// the reference count.
    pub(crate) async fn open(&self, inode: Ino, attr: &mut InodeAttr) {
        let read_guard = self.files.read().await;
        let of = match read_guard.get(&inode) {
            Some(of) => {
                let of = of.clone();
                drop(read_guard);
                of
            }
            None => {
                drop(read_guard);
                let mut outer_write_guard = self.files.write().await;
                // check again
                let of = match outer_write_guard.get(&inode) {
                    None => {
                        outer_write_guard.insert(
                            inode,
                            OpenFile(Arc::new(RwLock::new(OpenFileInner {
                                attr:            attr.keep_cache().clone(),
                                reference_count: 1,
                                last_check:      SystemTime::now(),
                                chunks:          Default::default(),
                            }))),
                        );
                        return;
                    }
                    Some(of) => of.clone(),
                };
                of
            }
        };
        // exists case
        let read_guard = of.0.read().await;
        if read_guard.attr.mtime == attr.mtime {
            attr.keep_cache = read_guard.attr.keep_cache;
        }
        drop(read_guard);
        let mut write_guard = of.0.write().await;
        write_guard.attr.keep_cache = true;
        write_guard.reference_count += 1;
        write_guard.last_check = SystemTime::now();
    }

    /// [load_attr] fetches the [InodeAttr] from the cache, if it is not
    /// expired.
    pub(crate) async fn load_attr(&self, ino: Ino, add_ref: bool) -> Option<InodeAttr> {
        let outer_read_guard = self.files.read().await;
        if let Some(of) = outer_read_guard.get(&ino).map(|v| v.clone()) {
            drop(outer_read_guard);

            let read_guard = of.0.read().await;
            if read_guard.last_check.elapsed().unwrap() < self.ttl {
                let attr = read_guard.attr.clone();
                drop(read_guard);
                if add_ref {
                    let mut write_guard = of.0.write().await;
                    write_guard.reference_count += 1;
                }
                return Some(attr);
            }
        }
        None
    }

    /// [load_slices] fetches the [Slices] from the cache, if it is not expired.
    pub(crate) async fn load_slices(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
    ) -> Option<Arc<Slices>> {
        let outer_read_guard = self.files.read().await;
        if let Some(of) = outer_read_guard.get(&inode).map(|of| of.clone()) {
            drop(outer_read_guard);

            let read_guard = of.0.read().await;
            if read_guard.last_check.elapsed().unwrap() < self.ttl {
                return read_guard.chunks.get(&chunk_index).map(|s| s.clone());
            }
        }
        None
    }

    /// [refresh_attr] refresh the open file's [InodeAttr].
    pub(crate) async fn refresh_attr(&self, ino: Ino, attr: &mut InodeAttr) {
        let read_guard = self.files.read().await;
        if let Some(of) = read_guard.get(&ino).map(|of| of.clone()) {
            drop(read_guard);
            of.refresh_access(attr).await;
        }
    }

    pub(crate) async fn refresh_slices(&self, ino: Ino, chunk_idx: ChunkIndex, views: Arc<Slices>) {
        let read_guard = self.files.read().await;
        if let Some(of) = read_guard.get(&ino) {
            let of = of.clone();
            drop(read_guard);
            of.refresh_slices(chunk_idx, views).await;
        }
    }

    pub(crate) async fn invalid(&self, inode: Ino, req: InvalidReq) {
        let read_guard = self.files.read().await;
        if let Some(of) = read_guard.get(&inode).map(|of| of.clone()) {
            drop(read_guard);
            match req {
                InvalidReq::OneChunk(idx) => {
                    of.invalid_slices(idx).await;
                }
                InvalidReq::All => {
                    of.invalid_all_chunk().await;
                }
                InvalidReq::OnlyAttr => {
                    of.invalid_attr().await;
                }
            }
        }
    }

    /// [close] a file, under the hood, it just decreases the reference count.
    pub(crate) async fn close(&self, ino: Ino) -> bool {
        let read_guard = self.files.read().await;
        if let Some(of) = read_guard.get(&ino) {
            let of = of.clone();
            drop(read_guard);

            let new_ref_cnt = of.decrease_ref().await;
            return new_ref_cnt <= 0;
        }
        true
    }
}

pub(crate) enum InvalidReq {
    OneChunk(ChunkIndex),
    All,
    OnlyAttr,
}
