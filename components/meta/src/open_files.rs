use dashmap::DashMap;
use kiseki_types::attr::InodeAttr;
use kiseki_types::ino::Ino;
use kiseki_types::slice::Slices;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct OpenFile {
    pub attr: InodeAttr,
    pub reference_count: usize,
    pub last_check: std::time::Instant,
    pub chunks: HashMap<usize, Arc<Slices>>, // should we add lock on it ?
}

pub(crate) struct OpenFiles {
    ttl: Duration,
    limit: usize,
    pub(crate) files: DashMap<Ino, OpenFile>,
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

    pub(crate) fn check(&self, ino: Ino) -> Option<InodeAttr> {
        self.files.get(&ino).and_then(|f| {
            if f.last_check.elapsed() < self.ttl {
                Some(f.attr.clone())
            } else {
                None
            }
        })
    }

    pub(crate) fn update(&self, ino: Ino, attr: &mut InodeAttr) -> bool {
        self.files
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
    pub(crate) fn open(&self, inode: Ino, attr: &mut InodeAttr) {
        match self.files.get_mut(&inode) {
            None => {
                self.files.insert(
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
                let op = op.value_mut();
                if op.attr.mtime == attr.mtime {
                    attr.keep_cache = op.attr.keep_cache;
                }
                op.attr.keep_cache = true;
                op.reference_count += 1;
                op.last_check = std::time::Instant::now();
            }
        }
    }

    pub(crate) fn open_check(&self, ino: Ino) -> Option<InodeAttr> {
        if let Some(mut of) = self.files.get_mut(&ino) {
            let of = of.value_mut();
            if of.last_check.elapsed() < self.ttl {
                of.reference_count += 1;
                return Some(of.attr.clone());
            }
        }

        None
    }

    pub(crate) fn update_chunk_slices_info(&self, ino: Ino, chunk_idx: usize, views: Arc<Slices>) {
        if let Some(mut of) = self.files.get_mut(&ino) {
            let of = of.value_mut();
            of.chunks.insert(chunk_idx, views);
        }
    }
}
