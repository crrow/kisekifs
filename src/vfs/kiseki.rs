use crate::common;
use crate::common::err::ToErrno;
use crate::meta::engine::access;
use crate::meta::types::*;
use crate::meta::util::*;
use crate::meta::{MetaContext, MetaEngine};
use crate::vfs::config::VFSConfig;
use crate::vfs::handle::Handle;
use crate::vfs::reader::DataReader;
use crate::vfs::writer::DataWriter;
use common::err::Result;
use dashmap::DashMap;
use fuser::FileType;
use libc::c_int;
use snafu::prelude::*;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;
use std::time;
use tracing::trace;

#[derive(Debug, Snafu)]
pub enum VFSError {}

impl From<VFSError> for common::err::Error {
    fn from(value: VFSError) -> Self {
        common::err::Error::VFSError { source: value }
    }
}

impl ToErrno for VFSError {
    fn to_errno(&self) -> c_int {
        todo!()
    }
}

#[derive(Debug)]
pub struct KisekiVFS {
    config: VFSConfig,
    meta: MetaEngine,
    internal_nodes: PreInternalNodes,
    writer: DataWriter,
    reader: DataReader,
    modified_at: DashMap<Ino, time::Instant>,
    _next_fh: AtomicU64,
    handles: DashMap<Ino, Vec<Handle>>,
}

impl Display for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiVFS {
    pub fn create(vfs_config: VFSConfig, meta: MetaEngine) -> Result<Self> {
        Ok(Self {
            internal_nodes: PreInternalNodes::new((
                vfs_config.entry_timeout,
                vfs_config.dir_entry_timeout,
            )),
            config: vfs_config,
            meta,
            writer: DataWriter::default(),
            reader: DataReader::default(),
            modified_at: DashMap::new(),
            _next_fh: AtomicU64::new(1),
            handles: DashMap::new(),
        })
    }

    pub async fn stat_fs<I: Into<Ino>>(&self, ctx: &MetaContext, ino: I) -> Result<FSStates> {
        let ino = ino.into();
        trace!("fs:stat_fs with ino {:?}", ino);
        let r = self.meta.stat_fs(ctx, ino).await?;
        Ok(r)
    }

    pub async fn lookup(&self, ctx: &MetaContext, parent: Ino, name: &str) -> Result<Entry> {
        trace!("fs:lookup with parent {:?} name {:?}", parent, name);
        // TODO: handle the special case
        if parent == ROOT_INO || name.eq(CONTROL_INODE_NAME) {
            if let Some(n) = self.internal_nodes.get_internal_node_by_name(name) {
                return Ok(n.into());
            }
        }
        if parent.is_special() && name == "." {
            if let Some(n) = self.internal_nodes.get_internal_node(parent) {
                return Ok(n.into());
            }
        }
        let (inode, attr) = self.meta.lookup(ctx, parent, name, true).await?;
        Ok(Entry {
            inode,
            name: name.to_string(),
            ttl: self.get_entry_ttl(&attr),
            attr,
            generation: 1,
        })
    }

    pub fn get_entry_ttl(&self, attr: &InodeAttr) -> time::Duration {
        if attr.is_dir() {
            self.config.dir_entry_timeout
        } else {
            self.config.entry_timeout
        }
    }

    pub fn update_length(&self, entry: &mut Entry) {
        if entry.attr.full && entry.is_file() {
            let len = self.writer.get_length(entry.inode);
            if len > entry.attr.length {
                entry.attr.length = len;
            }
            self.reader.truncate(entry.inode, entry.attr.length);
        }
    }

    pub fn modified_since(&self, inode: Ino, start_at: time::Instant) -> bool {
        match self.modified_at.get(&inode) {
            Some(v) => v.value() > &start_at,
            None => false,
        }
    }

    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        trace!("vfs:get_attr with inode {:?}", inode);
        let attr = self.meta.get_attr(inode).await?;
        Ok(attr)
    }

    pub fn get_ttl(&self, kind: FileType) -> time::Duration {
        if kind == FileType::Directory {
            self.config.dir_entry_timeout
        } else {
            self.config.entry_timeout
        }
    }

    pub async fn open_dir<I: Into<Ino>>(
        &self,
        ctx: &MetaContext,
        inode: I,
        flags: i32,
    ) -> Result<u64> {
        let inode = inode.into();
        trace!("vfs:open_dir with inode {:?}", inode);
        if ctx.check_permission {
            let mmask =
                match flags as libc::c_int & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
                    libc::O_RDONLY => MODE_MASK_R,
                    libc::O_WRONLY => MODE_MASK_W,
                    libc::O_RDWR => MODE_MASK_R | MODE_MASK_W,
                    _ => 0, // do nothing, // Handle unexpected flags
                };
            let attr = self.meta.get_attr(inode).await?;
            access(ctx, inode, &attr, mmask)?;
        }
        Ok(self.new_handle(inode))
    }

    fn new_handle(&self, inode: Ino) -> u64 {
        let fh = self.next_fh();
        let h = Handle::new(fh, inode);
        match self.handles.get_mut(&inode) {
            None => {
                self.handles.insert(inode, vec![h]);
            }
            Some(mut list) => {
                let l = list.value_mut();
                l.push(h)
            }
        };
        fh
    }

    fn next_fh(&self) -> u64 {
        self._next_fh
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
