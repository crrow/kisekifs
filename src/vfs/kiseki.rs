use std::{
    fmt::{Display, Formatter},
    sync::{atomic::AtomicU64, Arc},
    time,
};

use common::err::Result;
use dashmap::DashMap;
use fuser::FileType;
use libc::c_int;
use snafu::prelude::*;
use tokio::{sync::Mutex, time::Instant};
use tracing::{debug, info, instrument, trace};

use crate::{
    common,
    common::err::ToErrno,
    meta::{
        engine::{access, MetaEngine},
        internal_nodes::{PreInternalNodes, CONFIG_INODE_NAME, CONTROL_INODE_NAME},
        types::*,
        MetaContext, MODE_MASK_R, MODE_MASK_W,
    },
    vfs::{
        config::VFSConfig, handle::Handle, reader::DataReader, writer::DataWriter,
        VFSError::ErrBadFileHandle,
    },
};

#[derive(Debug, Snafu)]
pub enum VFSError {
    #[snafu(display("bad file handle: inode {:?} fh {:?}", inode, fh))]
    ErrBadFileHandle { inode: Ino, fh: u64 },
}

impl From<VFSError> for common::err::Error {
    fn from(value: VFSError) -> Self {
        common::err::Error::VFSError { source: value }
    }
}

impl ToErrno for VFSError {
    fn to_errno(&self) -> c_int {
        match self {
            ErrBadFileHandle { .. } => libc::EBADF,
        }
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
    handles: DashMap<Ino, DashMap<u64, Arc<Mutex<Handle>>>>,
}

impl Display for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiVFS {
    pub fn create(vfs_config: VFSConfig, meta: MetaEngine) -> Result<Self> {
        let mut internal_nodes =
            PreInternalNodes::new((vfs_config.entry_timeout, vfs_config.dir_entry_timeout));
        let config_inode = internal_nodes
            .get_mut_internal_node_by_name(CONFIG_INODE_NAME)
            .unwrap();
        let config_buf = bincode::serialize(&vfs_config).expect("unable to serialize vfs config");
        config_inode.0.attr.set_length(config_buf.len() as u64);
        if let Some(_) = &meta.config.sub_dir {
            // don't show trash directory
            internal_nodes.remove_trash_node();
        }
        if vfs_config.prefix_internal {
            internal_nodes.add_prefix();
        }

        let vfs = Self {
            internal_nodes,
            config: vfs_config,
            meta,
            writer: DataWriter::default(),
            reader: DataReader::default(),
            modified_at: DashMap::new(),
            _next_fh: AtomicU64::new(1),
            handles: DashMap::new(),
        };

        // TODO: spawn a background task to clean up modified time.

        Ok(vfs)
    }

    pub async fn init(&self, ctx: &MetaContext) -> Result<()> {
        debug!("vfs:init");
        let format = self.meta.load_format(false).await?;
        if let Some(sub_dir) = &self.meta.config.sub_dir {
            self.meta.chroot(ctx, sub_dir).await?;
        }

        // TODO: handle the meta format
        Ok(())
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
        let ttl = self.get_entry_ttl(&attr);
        let e = Entry::new_with_attr(inode, name, attr)
            .set_ttl(ttl)
            .set_generation(1);
        Ok(e)
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
        debug!("vfs:get_attr with inode {:?}", inode);
        if inode.is_special() {
            if let Some(n) = self.internal_nodes.get_internal_node(inode) {
                return Ok(n.get_attr());
            }
        }
        let attr = self.meta.get_attr(inode).await?;
        debug!("vfs:get_attr with inode {:?} attr {:?}", inode, attr);
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
        let h = Arc::new(Mutex::new(Handle::new(fh, inode)));
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

    fn next_fh(&self) -> u64 {
        self._next_fh
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn read_dir<I: Into<Ino>>(
        &self,
        ctx: &MetaContext,
        inode: I,
        fh: u64,
        offset: i64,
    ) -> Result<Vec<Entry>> {
        let inode = inode.into();
        debug!(
            "fs:readdir with ino {:?} fh {:?} offset {:?}",
            inode, fh, offset
        );

        let h = self
            .find_handle(inode, fh)
            .ok_or_else(|| ErrBadFileHandle { inode, fh })?;

        let mut h = h.lock().await;
        if h.children.is_empty() || offset == 0 {
            h.read_at = Some(Instant::now());
            let children = match self.meta.read_dir(ctx, inode, true).await {
                Ok(children) => children,
                Err(e) => {
                    if e.to_errno() == libc::EACCES {
                        let children = self.meta.read_dir(ctx, inode, false).await?;
                        children
                    } else {
                        return Err(e)?;
                    }
                }
            };
            h.children = children;
        }

        if (offset as usize) < h.children.len() {
            return Ok(h.children.drain(offset as usize..).collect::<Vec<_>>());
        }
        return Ok(Vec::new());
    }

    fn find_handle(&self, ino: Ino, fh: u64) -> Option<Arc<Mutex<Handle>>> {
        let list = self.handles.get(&ino).unwrap();
        let l = list.value();
        return l.get(&fh).map(|h| h.value().clone());
    }
}
