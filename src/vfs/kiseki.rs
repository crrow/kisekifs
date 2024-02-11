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
    fmt::{Debug, Display, Formatter},
    sync::{atomic::AtomicU64, Arc},
    time,
    time::SystemTime,
};

use bytes::Bytes;
use dashmap::DashMap;
use fuser::{FileType, TimeOrNow};
use kiseki_types::{
    ino::{Ino, CONTROL_INODE, ROOT_INO},
    MAX_FILE_SIZE,
};
use libc::{mode_t, EACCES, EBADF, EFBIG, EINVAL, EPERM};
use snafu::{location, Location, ResultExt};
use tokio::time::Instant;
use tracing::{debug, error, info, instrument, trace};

use crate::{
    common::{err::ToErrno, new_fs_sto, new_memory_sto},
    meta::{
        engine::{access, MetaEngine},
        internal_nodes::{PreInternalNodes, CONFIG_INODE_NAME, CONTROL_INODE_NAME},
        types::*,
        MetaContext, SetAttrFlags, MAX_NAME_LENGTH, MODE_MASK_R, MODE_MASK_W,
    },
    vfs::{
        config::VFSConfig,
        err::{ErrLIBCSnafu, JoinSnafu, Result},
        handle::Handle,
        storage::Engine,
        VFSError::ErrLIBC,
        FH,
    },
};

pub struct KisekiVFS {
    config: VFSConfig,
    meta: Arc<MetaEngine>,
    internal_nodes: PreInternalNodes,
    pub(crate) data_engine: Arc<Engine>,
    modified_at: DashMap<Ino, time::Instant>,
    pub(crate) _next_fh: AtomicU64,
    pub(crate) handles: DashMap<Ino, DashMap<FH, Arc<Handle>>>,
}

impl Debug for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl Display for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiVFS {
    pub fn new(vfs_config: VFSConfig, meta: MetaEngine) -> Result<Self> {
        let mut internal_nodes =
            PreInternalNodes::new((vfs_config.entry_timeout, vfs_config.dir_entry_timeout));
        let config_inode = internal_nodes
            .get_mut_internal_node_by_name(CONFIG_INODE_NAME)
            .unwrap();
        let config_buf = bincode::serialize(&vfs_config).expect("unable to serialize vfs config");
        config_inode.0.attr.set_length(config_buf.len() as u64);
        if meta.config.sub_dir.is_some() {
            // don't show trash directory
            internal_nodes.remove_trash_node();
        }
        if vfs_config.prefix_internal {
            internal_nodes.add_prefix();
        }

        let meta = Arc::new(meta);
        let object_storage = new_fs_sto();
        let storage_engine = Arc::new(Engine::new(
            Arc::new(vfs_config.engine_config.clone()),
            object_storage,
            meta.clone(),
        )?);

        let vfs = Self {
            config: vfs_config,
            internal_nodes,
            data_engine: storage_engine,
            meta: meta.clone(),
            modified_at: DashMap::new(),
            _next_fh: AtomicU64::new(1),
            handles: DashMap::new(),
        };

        // TODO: spawn a background task to clean up modified time.

        Ok(vfs)
    }

    pub async fn init(&self, ctx: &MetaContext) -> Result<()> {
        debug!("vfs:init");
        let _format = self.meta.load_format(false).await?;
        if let Some(sub_dir) = &self.meta.config.sub_dir {
            self.meta.chroot(ctx, sub_dir).await?;
        }

        // TODO: handle the meta format
        Ok(())
    }

    pub(crate) async fn stat_fs<I: Into<Ino>>(
        &self,
        ctx: &MetaContext,
        ino: I,
    ) -> Result<FSStates> {
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
            let len = self.data_engine.get_length(entry.inode);
            if len > entry.attr.length {
                entry.attr.length = len;
            }
            self.data_engine
                .truncate_reader(entry.inode, entry.attr.length);
        }
    }

    pub fn update_length_by_attr(&self, inode: Ino, attr: &mut InodeAttr) {
        if attr.full && attr.is_file() {
            let len = self.data_engine.get_length(inode);
            if len > attr.length {
                attr.length = len;
            }
            self.data_engine.truncate_reader(inode, attr.length);
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

        let h = match self.find_handle(inode, fh) {
            None => return ErrLIBCSnafu { kind: EBADF }.fail()?,
            Some(h) => h,
        };

        let mut h = h.inner.write().await;
        if h.children.is_empty() || offset == 0 {
            h.read_at = Some(Instant::now());
            let children = match self.meta.read_dir(ctx, inode, true).await {
                Ok(children) => children,
                Err(e) => {
                    if e.to_errno() == libc::EACCES {
                        self.meta.read_dir(ctx, inode, false).await?
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
        Ok(Vec::new())
    }

    pub async fn mknod(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: String,
        mode: mode_t,
        cumask: u16,
        rdev: u32,
    ) -> Result<Entry> {
        if parent.is_root() && self.internal_nodes.contains_name(&name) {
            return ErrLIBCSnafu { kind: libc::EEXIST }.fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return ErrLIBCSnafu {
                kind: libc::ENAMETOOLONG,
            }
            .fail()?;
        }
        let file_type = get_file_type(mode)?;
        let mode = mode as u16 & 0o777;

        let (ino, attr) = self
            .meta
            .mknod(
                ctx,
                parent,
                &name,
                file_type,
                mode,
                cumask,
                rdev,
                String::new(),
            )
            .await?;
        let ttl = self.get_entry_ttl(&attr);
        Ok(Entry::new_with_attr(ino, &name, attr)
            .with_generation(1)
            .with_ttl(ttl)
            .to_owned())
    }

    pub async fn create(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: libc::c_int,
    ) -> Result<(Entry, u64)> {
        debug!("fs:create with parent {:?} name {:?}", parent, name);
        if parent.is_root() && self.internal_nodes.contains_name(name) {
            return ErrLIBCSnafu { kind: libc::EEXIST }.fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return ErrLIBCSnafu {
                kind: libc::ENAMETOOLONG,
            }
            .fail()?;
        };

        let (inode, attr) = self
            .meta
            .create(ctx, parent, name, mode & 0o777, cumask, flags)
            .await?;

        let ttl = self.get_entry_ttl(&attr);
        let mut e = Entry::new_with_attr(inode, name, attr)
            .with_generation(1)
            .with_ttl(ttl)
            .to_owned();
        self.update_length(&mut e);
        let fh = self.new_file_handle(inode, e.attr.length, flags)?;
        Ok((e, fh))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_attr(
        &self,
        ctx: &MetaContext,
        ino: Ino,
        flags: u32,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        fh: Option<u64>,
    ) -> Result<Entry> {
        info!(
            "fs:setattr with ino {:?} flags {:?} atime {:?} mtime {:?}",
            ino, flags, atime, mtime
        );

        if ino.is_special() {
            return if let Some(n) = self.internal_nodes.get_internal_node(ino) {
                Ok(n.into())
            } else {
                return ErrLIBCSnafu { kind: EPERM }.fail()?;
            };
        }

        let mut new_attr = InodeAttr::default();
        let flags = SetAttrFlags::from_bits(flags).expect("invalid set attr flags");
        if flags.contains(SetAttrFlags::SIZE) {
            if let Some(size) = size {
                new_attr = self.truncate(ino, size, fh).await?;
            } else {
                return ErrLIBCSnafu { kind: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::MODE) {
            if let Some(mode) = mode {
                new_attr.perm = mode as u16 & 0o777;
            } else {
                return ErrLIBCSnafu { kind: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::UID) {
            if let Some(uid) = uid {
                new_attr.uid = uid;
            } else {
                return ErrLIBCSnafu { kind: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::GID) {
            if let Some(gid) = gid {
                new_attr.gid = gid;
            } else {
                return ErrLIBCSnafu { kind: EINVAL }.fail()?;
            }
        }
        let mut need_update = false;
        if flags.contains(SetAttrFlags::ATIME) {
            if let Some(atime) = atime {
                new_attr.atime = match atime {
                    TimeOrNow::SpecificTime(st) => st,
                    TimeOrNow::Now => SystemTime::now(),
                };
                need_update = true;
            } else {
                return ErrLIBCSnafu { kind: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::MTIME) {
            if let Some(mtime) = mtime {
                new_attr.mtime = match mtime {
                    TimeOrNow::SpecificTime(st) => st,
                    TimeOrNow::Now => {
                        need_update = true;
                        SystemTime::now()
                    }
                };
            } else {
                return ErrLIBCSnafu { kind: EINVAL }.fail()?;
            }
        }
        if need_update {
            if ctx.check_permission {
                self.meta
                    .check_set_attr(ctx, ino, flags, &mut new_attr)
                    .await?;
            }
            let mtime = match mtime.unwrap() {
                TimeOrNow::SpecificTime(st) => st,
                TimeOrNow::Now => SystemTime::now(),
            };
            if flags.contains(SetAttrFlags::MTIME) || flags.contains(SetAttrFlags::MTIME_NOW) {
                // TODO: whats wrong with this?
                self.data_engine.update_mtime(ino, mtime)?;
            }
        }

        let entry = self
            .meta
            .set_attr(ctx, flags, ino, &mut new_attr)
            .await
            .map(|mut entry| {
                self.update_length(&mut entry);
                let ttl = self.get_entry_ttl(&new_attr);
                entry.with_ttl(ttl);
                entry
            })?;

        // TODO: invalid open_file cache

        Ok(entry)
    }

    async fn truncate(&self, _ino: Ino, _size: u64, _fh: Option<u64>) -> Result<InodeAttr> {
        // let attr = self.meta.get_attr(ino).await?;
        // TODO: fix me
        Ok(InodeAttr::default())
    }

    pub async fn mkdir(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: &str,
        mode: u16,
        umask: u16,
    ) -> Result<Entry> {
        debug!("fs:mkdir with parent {:?} name {:?}", parent, name);
        if parent.is_root() && self.internal_nodes.contains_name(name) {
            return ErrLIBCSnafu { kind: libc::EEXIST }.fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return ErrLIBCSnafu {
                kind: libc::ENAMETOOLONG,
            }
            .fail()?;
        };
        let (ino, attr) = self.meta.mkdir(ctx, parent, name, mode, umask).await?;
        let ttl = self.get_entry_ttl(&attr);
        Ok(Entry::new_with_attr(ino, name, attr)
            .with_generation(1)
            .with_ttl(ttl)
            .to_owned())
    }

    pub async fn open(&self, ctx: &MetaContext, inode: Ino, flags: i32) -> Result<Opened> {
        debug!(
            "fs:open with ino {:?} flags {:#b} pid {:?}",
            inode, flags, ctx.pid
        );

        if inode.is_special() {
            // TODO: at present, we don't implement the same logic as the juicefs.
            return ErrLIBCSnafu { kind: EACCES }.fail()?;
            // if inode != CONTROL_INODE && flags & libc::O_ACCMODE !=
            // libc::O_RDONLY { }
        }

        let mut attr = self.meta.open_inode(ctx, inode, flags).await?;
        self.update_length_by_attr(inode, &mut attr);
        let opened_fh = self.new_file_handle(inode, attr.length, flags)?;
        let ttl = self.get_ttl(attr.kind);
        let entry = Entry::new_with_attr(inode, "", attr)
            .with_generation(1)
            .with_ttl(ttl)
            .to_owned();

        let opened_flags = if inode.is_special() {
            fuser::consts::FOPEN_DIRECT_IO
        } else if entry.attr.keep_cache {
            fuser::consts::FOPEN_KEEP_CACHE
        } else {
            0
        };

        Ok(Opened {
            fh: opened_fh,
            flags: opened_flags,
            entry,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn read(
        &self,
        _ctx: &MetaContext,
        ino: Ino,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
    ) -> Result<Bytes> {
        debug!(
            "fs:read with ino {:?} fh {:?} offset {:?} size {:?}",
            ino, fh, offset, size
        );

        if ino.is_special() {
            todo!()
        }

        // just convert it.
        // TODO: review me, is it correct? it may be negative.
        let offset = offset as u64;
        let size = size as u64;

        let _handle = self.find_handle(ino, fh).ok_or(ErrLIBC {
            kind: EBADF,
            location: location!(),
        })?;
        if offset >= MAX_FILE_SIZE as u64 || offset + size >= MAX_FILE_SIZE as u64 {
            return ErrLIBCSnafu { kind: EFBIG }.fail()?;
        }
        let fr = self.data_engine.find_file_reader(ino, fh).ok_or(ErrLIBC {
            kind: EBADF,
            location: location!(),
        })?;
        self.data_engine.flush_if_exists(ino).await?;
        let mut buf = vec![0u8; size as usize];
        let _read_len = fr.read(offset as usize, buf.as_mut_slice()).await?;
        debug!(
            "vfs:read with ino {:?} fh {:?} offset {:?} expected_read_size {:?} actual_read_len: {:?}",
            ino, fh, offset, size, _read_len
        );
        Ok(Bytes::from(buf))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        _ctx: &MetaContext,
        ino: Ino,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<u32> {
        let size = data.len();
        debug!(
            "fs:write with {:?} fh {:?} offset {:?} size {:?}",
            ino, fh, offset, size
        );

        let offset = offset as usize;
        if offset >= MAX_FILE_SIZE || offset + size >= MAX_FILE_SIZE {
            return ErrLIBCSnafu { kind: EFBIG }.fail()?;
        }
        let _handle = self.find_handle(ino, fh).ok_or(ErrLIBC {
            kind: EBADF,
            location: location!(),
        })?;
        if ino == CONTROL_INODE {
            todo!()
        }
        if !self.data_engine.check_file_writer(ino) {
            error!(
                "fs:write with ino {:?} fh {:?} offset {:?} size {:?} failed; maybe open flag contains problem",
                ino, fh, offset, size
            );
            return ErrLIBCSnafu { kind: EBADF }.fail()?;
        }

        let len = self.data_engine.write(ino, offset, data).await?;
        self.data_engine
            .truncate_reader(ino, self.data_engine.get_length(ino));
        Ok(len as u32)
    }

    pub async fn flush(&self, ctx: &MetaContext, ino: Ino, fh: u64, lock_owner: u64) -> Result<()> {
        debug!("do flush manually on ino {:?} fh {:?}", ino, fh);
        let h = self.find_handle(ino, fh).ok_or(ErrLIBC {
            kind: EBADF,
            location: location!(),
        })?;
        if ino.is_special() {
            return Ok(());
        };

        if let Some(fw) = self.data_engine.find_file_writer(ino) {
            fw.do_flush().await?;
        }

        if lock_owner != h.get_ofd_owner().await {
            h.set_ofd_owner(0).await;
        }

        if h.locks & 2 != 0 {
            self.meta
                .set_lk(
                    ctx,
                    ino,
                    lock_owner,
                    false,
                    libc::F_UNLCK,
                    0,
                    0x7FFFFFFFFFFFFFFF,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn fsync(
        &self,
        _ctx: &MetaContext,
        ino: Ino,
        fh: u64,
        _data_sync: bool,
    ) -> Result<()> {
        if ino.is_special() {
            return Ok(());
        }

        self.find_handle(ino, fh).ok_or(ErrLIBC {
            kind: EBADF,
            location: Location::default(),
        })?;
        if let Some(fw) = self.data_engine.find_file_writer(ino) {
            fw.do_flush().await?;
        }

        Ok(())
    }
}

/// Reply to a `open` or `opendir` call
#[derive(Debug)]
pub struct Opened {
    pub fh: u64,
    pub flags: u32,
    pub entry: Entry,
}

// TODO: review me, use a better way.
fn get_file_type(mode: mode_t) -> Result<FileType> {
    match mode & (libc::S_IFMT & 0xffff) {
        libc::S_IFIFO => Ok(FileType::NamedPipe),
        libc::S_IFSOCK => Ok(FileType::Socket),
        libc::S_IFLNK => Ok(FileType::Symlink),
        libc::S_IFREG => Ok(FileType::RegularFile),
        libc::S_IFBLK => Ok(FileType::BlockDevice),
        libc::S_IFDIR => Ok(FileType::Directory),
        libc::S_IFCHR => Ok(FileType::CharDevice),
        _ => ErrLIBCSnafu { kind: EPERM }.fail()?,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::install_fmt_log,
        meta::{Format, MetaConfig},
    };

    async fn make_vfs() -> KisekiVFS {
        let meta_engine = MetaConfig::test_config().open().unwrap();
        let format = Format::default();
        meta_engine.init(format, false).await.unwrap();
        KisekiVFS::new(VFSConfig::default(), meta_engine).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn setup_tests() {
        install_fmt_log();

        let vfs = make_vfs().await;
        basic_write(&vfs).await;
    }

    async fn basic_write(vfs: &KisekiVFS) {
        let meta_ctx = MetaContext::background();
        let (entry, fh) = vfs
            .create(&meta_ctx, ROOT_INO, "f", 0o755, 0, libc::O_RDWR)
            .await
            .unwrap();

        let write_len = vfs
            .write(&meta_ctx, entry.inode, fh, 0, b"hello", 0, 0, None)
            .await
            .unwrap();
        assert_eq!(write_len, 5);

        vfs.fsync(&meta_ctx, entry.inode, fh, true).await.unwrap();

        let write_len = vfs
            .write(&meta_ctx, entry.inode, fh, 100 << 20, b"world", 0, 0, None)
            .await
            .unwrap();
        assert_eq!(write_len, 5);

        vfs.fsync(&meta_ctx, entry.inode, fh, true).await.unwrap();

        sequential_write(vfs, entry.inode, fh).await;
    }
    async fn sequential_write(vfs: &KisekiVFS, inode: Ino, fh: FH) {
        let meta_ctx = MetaContext::background();
        let data = vec![0u8; 128 << 10];
        for _i in 0..=1000 {
            let write_len = vfs
                .write(&meta_ctx, inode, fh, 128 << 10, &data, 0, 0, None)
                .await
                .unwrap();
            assert_eq!(write_len, 128 << 10);
        }
    }
}
