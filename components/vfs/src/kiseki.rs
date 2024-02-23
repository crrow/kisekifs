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
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use dashmap::DashMap;
use fuser::{FileType, TimeOrNow};
use kiseki_common::{DOT, DOT_DOT, FH, MAX_FILE_SIZE, MAX_NAME_LENGTH, MODE_MASK_R, MODE_MASK_W};
use kiseki_meta::{context::FuseContext, MetaEngineRef};
use kiseki_storage::slice_buffer::SliceBuffer;
use kiseki_types::{
    attr::{InodeAttr, SetAttrFlags},
    entry::{Entry, FullEntry},
    ino::{Ino, CONTROL_INODE, ROOT_INO},
    internal_nodes::{InternalNodeTable, CONFIG_INODE_NAME, CONTROL_INODE_NAME, TRASH_INODE_NAME},
    slice::SliceID,
    ToErrno,
};
use kiseki_utils::{object_storage, object_storage::ObjectStorage};
use libc::{mode_t, EACCES, EBADF, EFBIG, EINTR, EINVAL, ENOENT, EPERM};
use scopeguard::defer;
use snafu::{ensure, location, Location, OptionExt, ResultExt};
use tokio::{task::JoinHandle, time::Instant};
use tracing::{debug, error, info, instrument, trace, Instrument};

use crate::{
    config::Config,
    data_manager::{DataManager, DataManagerRef},
    err::{
        Error, Error::LibcError, JoinErrSnafu, LibcSnafu, MetaSnafu, ObjectStorageSnafu,
        OpenDalSnafu, Result, StorageSnafu,
    },
    handle::{FileHandleWriteGuard, Handle, HandleTable, HandleTableRef},
    writer::{FileWriter, FileWritersRef},
};

pub struct KisekiVFS {
    pub config: Config,

    // Runtime status
    internal_nodes:          InternalNodeTable,
    modified_at:             DashMap<Ino, std::time::Instant>,
    pub(crate) handle_table: HandleTableRef,
    pub(crate) data_manager: DataManagerRef,

    // Dependencies
    pub(crate) meta: MetaEngineRef,
}

impl Debug for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta)
    }
}

impl KisekiVFS {
    pub fn new(vfs_config: Config, meta: MetaEngineRef) -> Result<Self> {
        let mut internal_nodes =
            InternalNodeTable::new((vfs_config.file_entry_timeout, vfs_config.dir_entry_timeout));
        let config_inode = internal_nodes
            .get_mut_internal_node_by_name(CONFIG_INODE_NAME)
            .unwrap();
        let config_buf = bincode::serialize(&vfs_config).expect("unable to serialize vfs config");
        config_inode.0.attr.set_length(config_buf.len() as u64);
        // if meta.config.sub_dir.is_some() {
        //     don't show trash directory
        // internal_nodes.remove_trash_node();
        // }
        if vfs_config.prefix_internal {
            internal_nodes.add_prefix();
        }

        // let object_storage =
        //     kiseki_utils::object_storage::new_sled_store(&vfs_config.
        // object_storage_dsn)         .context(OpenDalSnafu)?;
        let object_storage =
            kiseki_utils::object_storage::new_minio_store().context(ObjectStorageSnafu)?;

        // let object_storage = kiseki_utils::object_storage::new_memory_object_store();
        // let object_storage =
        //     kiseki_utils::object_storage::new_local_object_store(&vfs_config.
        // object_storage_dsn)         .context(ObjectStorageSnafu)?;

        let data_manager = Arc::new(DataManager::new(
            vfs_config.page_size,
            vfs_config.block_size,
            vfs_config.chunk_size,
            meta.clone(),
            object_storage,
        ));

        let vfs = Self {
            config: vfs_config,
            internal_nodes,
            modified_at: DashMap::new(),
            handle_table: HandleTable::new(data_manager.clone()),
            data_manager,
            meta,
        };

        // TODO: spawn a background task to clean up modified time.

        Ok(vfs)
    }

    pub async fn init(&self, ctx: &FuseContext) -> Result<()> {
        debug!("vfs:init");
        // let _format = self.meta.get_format().await?;
        // if let Some(sub_dir) = &self.meta.config.sub_dir {
        //     self.meta.chroot(ctx, sub_dir).await?;
        // }

        // TODO: handle the meta format
        Ok(())
    }

    pub fn stat_fs<I: Into<Ino>>(
        self: &Arc<Self>,
        ctx: Arc<FuseContext>,
        ino: I,
    ) -> Result<kiseki_types::stat::FSStat> {
        let ino = ino.into();
        trace!("fs:stat_fs with ino {:?}", ino);
        let h = self.meta.stat_fs(ctx, ino)?;
        Ok(h)
    }

    pub async fn lookup(&self, ctx: &FuseContext, parent: Ino, name: &str) -> Result<FullEntry> {
        trace!("fs:lookup with parent {:?} name {:?}", parent, name);
        // TODO: handle the special case
        if parent == ROOT_INO || name.eq(CONTROL_INODE_NAME) {
            if let Some(n) = self.internal_nodes.get_internal_node_by_name(name) {
                return Ok(n.0.clone());
            }
        }
        if parent.is_special() && name == DOT {
            if let Some(n) = self.internal_nodes.get_internal_node(parent) {
                return Ok(n.0.clone());
            }
        }
        let (inode, attr) = self.meta.lookup(ctx, parent, name, true).await?;
        Ok(FullEntry {
            inode,
            name: name.to_string(),
            attr,
        })
    }

    /// [get_entry_ttl]
    pub fn get_entry_ttl(&self, kind: FileType) -> &Duration {
        if kind == FileType::Directory {
            &self.config.dir_entry_timeout
        } else {
            &self.config.file_entry_timeout
        }
    }

    /// [try_update_file_reader_length] is used for checking and update the file
    /// reader length.
    pub async fn try_update_file_reader_length(&self, inode: Ino, attr: &mut InodeAttr) {
        if attr.is_file() {
            let len = self.data_manager.get_length(inode);
            if len > attr.length {
                attr.length = len;
            }
            if len < attr.length {
                self.data_manager.truncate_reader(inode, attr.length).await;
            }
        }
    }

    pub fn modified_since(&self, inode: Ino, start_at: std::time::Instant) -> bool {
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

    pub async fn open_dir<I: Into<Ino>>(
        &self,
        ctx: &FuseContext,
        inode: I,
        flags: i32,
    ) -> Result<FH> {
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
            ctx.check(&attr, mmask)?;
        }
        Ok(self.handle_table.new_dir_handle(inode))
    }

    pub async fn read_dir<I: Into<Ino>>(
        &self,
        ctx: &FuseContext,
        inode: I,
        fh: u64,
        offset: i64,
        plus: bool,
    ) -> Result<Vec<Entry>> {
        let inode = inode.into();
        debug!(
            "fs:readdir with ino {:?} fh {:?} offset {:?}",
            inode, fh, offset
        );

        let h = self
            .handle_table
            .find_handle(inode, fh)
            .context(LibcSnafu { errno: EBADF })?;
        let h = h.as_dir_handle().context(LibcSnafu { errno: EBADF })?;

        let mut read_guard = h.inner.read().await;
        if read_guard.children.is_empty() || offset == 0 {
            drop(read_guard);
            let mut write_guard = h.inner.write().await;
            // FIXME
            write_guard.read_at = Some(Instant::now());
            write_guard.children = self
                .meta
                .read_dir(ctx, inode, plus)
                .await
                .context(MetaSnafu)?;
            if (offset as usize) < write_guard.children.len() {
                let result = write_guard.children[offset as usize..]
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                return Ok(result);
            } else {
                return Ok(vec![]);
            }
        }
        if (offset as usize) < read_guard.children.len() {
            let result = read_guard.children[offset as usize..]
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            return Ok(result);
        }
        Ok(Vec::new())
    }

    pub async fn mknod(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: String,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> Result<FullEntry> {
        if parent.is_root() && self.internal_nodes.contains_name(&name) {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
            .fail()?;
        }
        let file_type = get_file_type(mode)?;

        let (ino, attr) = self
            .meta
            .mknod(
                ctx,
                parent,
                &name,
                file_type,
                mode & 0o7777,
                umask,
                rdev,
                String::new(),
            )
            .await
            .context(MetaSnafu)?;
        Ok(FullEntry::new(ino, &name, attr))
    }

    pub async fn create(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
        flags: libc::c_int,
    ) -> Result<(FullEntry, FH)> {
        debug!("fs:create with parent {:?} name {:?}", parent, name);
        if parent.is_root() && self.internal_nodes.contains_name(name) {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
            .fail()?;
        };

        let (inode, attr) = self
            .meta
            .create(ctx, parent, name, mode & 0o7777, umask, flags)
            .await
            .context(MetaSnafu)?;

        let mut e = FullEntry::new(inode, name, attr);
        self.try_update_file_reader_length(inode, &mut e.attr).await;
        let fh = self
            .handle_table
            .new_file_handle(inode, e.attr.length, flags)
            .await?;
        Ok((e, fh))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_attr(
        &self,
        ctx: &FuseContext,
        ino: Ino,
        flags: u32,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        fh: Option<u64>,
    ) -> Result<InodeAttr> {
        info!(
            "fs:setattr with ino {:?} flags {:?} atime {:?} mtime {:?}",
            ino, flags, atime, mtime
        );

        if ino.is_special() {
            return if let Some(n) = self.internal_nodes.get_internal_node(ino) {
                Ok(n.get_attr())
            } else {
                return LibcSnafu { errno: EPERM }.fail()?;
            };
        }

        let mut new_attr = InodeAttr::default();
        let flags = SetAttrFlags::from_bits(flags).expect("invalid set attr flags");
        if flags.contains(SetAttrFlags::SIZE) {
            if let Some(size) = size {
                new_attr = self.truncate(ino, size, fh).await?;
            } else {
                return LibcSnafu { errno: EPERM }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::MODE) {
            if let Some(mode) = mode {
                new_attr.mode = mode & 0o7777;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::UID) {
            if let Some(uid) = uid {
                new_attr.uid = uid;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::GID) {
            if let Some(gid) = gid {
                new_attr.gid = gid;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
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
                return LibcSnafu { errno: EINVAL }.fail()?;
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
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if need_update {
            if ctx.check_permission {
                self.meta
                    .check_set_attr(ctx, ino, flags, &mut new_attr)
                    .await
                    .context(MetaSnafu)?;
            }
            let mtime = match mtime.unwrap() {
                TimeOrNow::SpecificTime(st) => st,
                TimeOrNow::Now => SystemTime::now(),
            };
            if flags.contains(SetAttrFlags::MTIME) || flags.contains(SetAttrFlags::MTIME_NOW) {
                // TODO: whats wrong with this?
                self.data_manager.update_mtime(ino, mtime)?;
            }
        }

        self.meta
            .set_attr(ctx, flags, ino, &mut new_attr)
            .await
            .context(MetaSnafu)?;

        self.try_update_file_reader_length(ino, &mut new_attr).await;

        // TODO: invalid open_file cache

        Ok(new_attr)
    }

    async fn truncate(&self, _ino: Ino, _size: u64, _fh: Option<u64>) -> Result<InodeAttr> {
        // let attr = self.meta.get_attr(ino).await?;
        // TODO: fix me
        Ok(InodeAttr::default())
    }

    pub async fn mkdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
    ) -> Result<FullEntry> {
        debug!("fs:mkdir with parent {:?} name {:?}", parent, name);
        if parent.is_root() && self.internal_nodes.contains_name(name) {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
            .fail()?;
        };

        let (ino, attr) = self
            .meta
            .mkdir(ctx, parent, name, mode, umask)
            .await
            .context(MetaSnafu)?;
        Ok(FullEntry::new(ino, name, attr))
    }

    pub async fn rmdir(&self, ctx: Arc<FuseContext>, parent: Ino, name: &str) -> Result<()> {
        debug!("fs:rmdir with parent {:?} name {:?}", parent, name);
        ensure!(name != DOT, LibcSnafu { errno: EINVAL });
        ensure!(name != DOT_DOT, LibcSnafu { errno: EINVAL });
        ensure!(
            name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        if parent == ROOT_INO && name == TRASH_INODE_NAME || parent.is_trash() && ctx.uid != 0 {
            return LibcSnafu { errno: EPERM }.fail()?;
        }
        self.meta.rmdir(ctx, parent, name).await.context(MetaSnafu)?;

        Ok(())
    }

    pub async fn open(&self, ctx: &FuseContext, inode: Ino, flags: i32) -> Result<Opened> {
        debug!(
            "fs:open with ino {:?} flags {:#b} pid {:?}",
            inode, flags, ctx.pid
        );

        if inode.is_special() {
            // TODO: at present, we don't implement the same logic as the juicefs.
            return LibcSnafu { errno: EACCES }.fail()?;
            // if inode != CONTROL_INODE && flags & libc::O_ACCMODE !=
            // libc::O_RDONLY { }
        }

        let mut attr = self
            .meta
            .open_inode(ctx, inode, flags)
            .await
            .context(MetaSnafu)?;
        self.try_update_file_reader_length(inode, &mut attr).await;
        let opened_fh = self
            .handle_table
            .new_file_handle(inode, attr.length, flags)
            .await?;
        // TODO: review me
        let entry = FullEntry::new(inode, "", attr);

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
        ctx: Arc<FuseContext>,
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
        ensure!(
            offset + size < MAX_FILE_SIZE as u64,
            LibcSnafu { errno: EFBIG }
        );

        let handle = self
            .handle_table
            .find_handle(ino, fh)
            .context(LibcSnafu { errno: EBADF })?;
        let file_handle = handle
            .as_file_handle()
            .context(LibcSnafu { errno: EBADF })?;

        let mut buf = vec![0u8; size as usize];
        let read_guard = file_handle
            .read_lock(ctx.clone())
            .await
            .context(LibcSnafu { errno: EINTR })?; // failed to lock

        self.data_manager.direct_flush(ino).await?;

        let _read_len = read_guard.read(offset as usize, buf.as_mut_slice()).await?;
        file_handle.remove_operation(&ctx).await;
        debug!(
            "vfs:read with ino {:?} fh {:?} offset {:?} expected_read_size {:?} actual_read_len: \
             {:?}",
            ino, fh, offset, size, _read_len
        );
        Ok(Bytes::from(buf))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        ctx: Arc<FuseContext>,
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
        ensure!(offset + size < MAX_FILE_SIZE, LibcSnafu { errno: EFBIG });

        let _handle = self
            .handle_table
            .find_handle(ino, fh)
            .context(LibcSnafu { errno: EBADF })?;
        if ino == CONTROL_INODE {
            todo!()
        }

        let handle = _handle
            .as_file_handle()
            .context(LibcSnafu { errno: EBADF })?;

        let write_guard = handle
            .write_lock(ctx.clone())
            .await
            .context(LibcSnafu { errno: EINTR })?;
        let write_len = write_guard.write(offset, data).await?;
        handle.remove_operation(&ctx).await;

        self.data_manager
            .truncate_reader(ino, write_guard.get_length() as u64);

        Ok(write_len as u32)
    }

    #[instrument(skip(self), fields(ino, fh))]
    pub async fn flush(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        fh: u64,
        lock_owner: u64,
    ) -> Result<()> {
        let h = self
            .handle_table
            .find_handle(ino, fh)
            .context(LibcSnafu { errno: ENOENT })?;
        let h = h.as_file_handle().context(LibcSnafu { errno: EBADF })?;
        if ino.is_special() {
            return Ok(());
        };

        if h.has_writer() {
            let guard = loop {
                match h.write_lock(ctx.clone()).await {
                    Some(guard) => break Some(guard),
                    None => {
                        if !ctx.is_cancelled() {
                            // as long as it's not cancelled, we should continue to wait
                            h.cancel_operations(&ctx.pid).await;
                            continue; // go back to acquire lock again.
                        } else {
                            // we get cancelled while waiting for the lock
                            break None;
                        }
                    }
                }
            };
            let guard = guard.context(LibcSnafu { errno: EINTR })?;
            guard.flush().await?;
            h.remove_operation(&ctx).await;
        } else {
            // still need to cancel
            h.cancel_operations(&ctx.pid).await;
        }

        h.try_set_ofd_owner(lock_owner);
        if h.locks.load(Ordering::Acquire) & 2 != 0 {
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
                .await
                .context(MetaSnafu)?;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(ino, fh))]
    pub async fn fsync(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        fh: u64,
        _data_sync: bool,
    ) -> Result<()> {
        if ino.is_special() {
            return Ok(());
        }

        let handle = self
            .handle_table
            .find_handle(ino, fh)
            .context(LibcSnafu { errno: EBADF })?;
        if let Some(fh) = handle.as_file_handle() {
            let write_guard = fh
                .write_lock(ctx.clone())
                .await
                .context(LibcSnafu { errno: EINTR })?;

            write_guard.flush().await?;
            fh.remove_operation(&ctx).await;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(ino, fh))]
    pub async fn fallocate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        fh: FH,
        offset: i64,
        length: i64,
        mode: u8,
    ) -> Result<()> {
        ensure!(offset >= 0 && length > 0, LibcSnafu { errno: EINVAL });
        ensure!(!inode.is_special(), LibcSnafu { errno: EPERM });
        let offset = offset as usize;
        let length = length as usize;
        ensure!(offset + length < MAX_FILE_SIZE, LibcSnafu { errno: EFBIG });
        let h = self
            .handle_table
            .find_handle(inode, fh)
            .context(LibcSnafu { errno: EBADF })?;
        let _ = h.as_file_handle().context(LibcSnafu { errno: EBADF })?;
        self.meta.fallocate(inode, offset, length, mode).await?;

        todo!()
    }

    pub async fn release(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        fh: FH,
    ) -> Result<JoinHandle<()>> {
        if inode.is_special() {
            todo!()
        }
        if let Some(handle) = self.handle_table.find_handle(inode, fh) {
            handle.wait_all_operations_done(ctx.clone()).await?;
            if let Some(fh) = handle.as_file_handle() {
                if fh.has_writer() {
                    fh.unsafe_flush().await?;
                    self.invalidate_length(inode);
                }
                let (locks, fowner, powner) = fh.get_posix_lock_info();
                if locks & 1 != 0 {
                    self.meta
                        .flock(ctx.clone(), inode, fowner, libc::F_UNLCK)
                        .await?;
                }
                if locks & 2 != 0 && powner != 0 {
                    self.meta
                        .set_lk(
                            ctx,
                            inode,
                            powner,
                            false,
                            libc::F_UNLCK,
                            0,
                            0x7FFFFFFFFFFFFFFF,
                        )
                        .await?;
                }
            }
        }
        self.meta.close(inode).await?;
        let ht = self.handle_table.clone();
        Ok(tokio::spawn(async move {
            ht.release_file_handle(inode, fh).await;
        }))
    }

    fn invalidate_length(&self, ino: Ino) {
        self.modified_at
            .get_mut(&ino)
            .map(|mut v| *v = std::time::Instant::now());
    }
}

/// Reply to a `open` or `opendir` call
#[derive(Debug)]
pub struct Opened {
    pub fh:    u64,
    pub flags: u32,
    pub entry: FullEntry,
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
        _ => LibcSnafu { errno: EPERM }.fail()?,
    }
}

#[cfg(test)]
mod tests {
    use kiseki_utils::logger::install_fmt_log;

    use super::*;

    async fn make_vfs() -> KisekiVFS {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = kiseki_meta::MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", tempdir.path().to_str().unwrap()));
        let mut format = kiseki_types::setting::Format::default();
        format.with_name("test-kiseki");
        kiseki_meta::update_format(&meta_config.dsn, format, true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let vfs_config = Config::default();
        KisekiVFS::new(vfs_config, meta_engine).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vfs_basic_io() {
        install_fmt_log();

        let vfs = Arc::new(make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let (entry, fh) = vfs
            .create(ctx.clone(), ROOT_INO, "f", 0o755, 0, libc::O_RDWR)
            .await
            .unwrap();

        let write_len = vfs
            .write(ctx.clone(), entry.inode, fh, 0, b"hello", 0, 0, None)
            .await
            .unwrap();
        assert_eq!(write_len, 5);

        vfs.fsync(ctx.clone(), entry.inode, fh, true).await.unwrap();

        let write_len = vfs
            .write(
                ctx.clone(),
                entry.inode,
                fh,
                100 << 20,
                b"world",
                0,
                0,
                None,
            )
            .await
            .unwrap();
        assert_eq!(write_len, 5);

        vfs.fsync(ctx.clone(), entry.inode, fh, true).await.unwrap();
        let fw = vfs.data_manager.find_file_writer(entry.inode).unwrap();
        assert_eq!(fw.get_reference_count(), 1);

        {
            // small write
            let buf = vec![1u8; 5 << 10];
            for i in 32..0 {
                let write_len = vfs
                    .write(
                        ctx.clone(),
                        entry.inode,
                        fh,
                        i * (4 << 10),
                        &buf,
                        0,
                        0,
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(write_len, buf.len() as u32);
            }
        }

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let release_waiter = vfs.release(ctx, entry.inode, fh).await.unwrap();
        // wait for the release to finish
        release_waiter.await.unwrap();
        assert_eq!(fw.get_reference_count(), 0);
        assert_eq!(
            vfs.data_manager.file_writers.contains_key(&entry.inode),
            false
        );

        // sequential_write(&vfs, entry.inode, fh).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vfs_basic() -> Result<()> {
        install_fmt_log();

        let vfs = Arc::new(make_vfs().await);
        let ctx = Arc::new(FuseContext::background());

        let stat = vfs.stat_fs(ctx.clone(), ROOT_INO)?;
        debug!("{:?}", stat);

        let dir1 = vfs.mkdir(ctx.clone(), ROOT_INO, "d1", 0o755, 0).await?;
        let dir2 = vfs.mkdir(ctx.clone(), dir1.inode, "d2", 0o755, 0).await?;

        Ok(())
    }
}
