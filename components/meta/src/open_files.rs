use std::{collections::HashMap, sync::Arc, time::Duration};

use kiseki_types::{attr::InodeAttr, ino::Ino, slice::Slices};
use tokio::sync::RwLock;

pub(crate) struct OpenFile {
    pub attr: InodeAttr,
    pub reference_count: usize,
    pub last_check: std::time::Instant,
    pub chunks: HashMap<usize, Arc<Slices>>, // should we add lock on it ?
}

pub(crate) struct OpenFiles {
    ttl: Duration,
    limit: usize,
    pub(crate) files: RwLock<HashMap<Ino, OpenFile>>,
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

    pub(crate) async fn check(&self, ino: Ino) -> Option<InodeAttr> {
        let read_guard = self.files.read().await;
        read_guard.get(&ino).and_then(|f| {
            if f.last_check.elapsed() < self.ttl {
                Some(f.attr.clone())
            } else {
                None
            }
        })
    }

    pub(crate) async fn update(&self, ino: Ino, attr: &mut InodeAttr) -> bool {
        let mut write_guard = self.files.write().await;
        write_guard
            .get_mut(&ino)
            .map(|mut open_file| {
                if attr.mtime != open_file.attr.mtime {
                    open_file.chunks = HashMap::new();
                } else {
                    attr.keep_cache = open_file.attr.keep_cache;
                }
                open_file.attr = attr.clone();
                open_file.last_check = std::time::Instant::now();
                Some(())
            })
            .is_some()
    }

    pub(crate) async fn open(&self, inode: Ino, attr: &mut InodeAttr) {
        let mut write_guard = self.files.write().await;
        match write_guard.get_mut(&inode) {
            None => {
                write_guard.insert(
                    inode,
                    OpenFile {
                        attr: attr.keep_cache().clone(),
                        reference_count: 1,
                        last_check: std::time::Instant::now(),
                        chunks: HashMap::new(),
                    },
                );
            }
            Some(mut op) => {
                if op.attr.mtime == attr.mtime {
                    attr.keep_cache = op.attr.keep_cache;
                }
                op.attr.keep_cache = true;
                op.reference_count += 1;
                op.last_check = std::time::Instant::now();
            }
        }
    }

    pub(crate) async fn open_check(&self, ino: Ino) -> Option<InodeAttr> {
        let mut write_guard = self.files.write().await;
        if let Some(mut of) = write_guard.get_mut(&ino) {
            if of.last_check.elapsed() < self.ttl {
                of.reference_count += 1;
                return Some(of.attr.clone());
            }
        }

        None
    }

    pub(crate) async fn update_chunk_slices_info(
        &self,
        ino: Ino,
        chunk_idx: usize,
        views: Arc<Slices>,
    ) {
        let mut write_guard = self.files.write().await;
        if let Some(mut of) = write_guard.get_mut(&ino) {
            of.chunks.insert(chunk_idx, views);
        }
    }

    /// close a file
    pub(crate) async fn close(&self, ino: Ino) -> bool {
        let mut write_guard = self.files.write().await;
        if let Some(mut of) = write_guard.get_mut(&ino) {
            of.reference_count -= 1;
            return of.reference_count <= 0;
        }
        true
    }
}
