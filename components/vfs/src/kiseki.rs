// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt::{Debug, Display, Formatter},
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use dashmap::DashMap;
use fuser::{FileType, TimeOrNow};
use kiseki_common::{
    DOT, DOT_DOT, FH, MAX_FILE_SIZE, MAX_NAME_LENGTH, MAX_SYMLINK_LEN, MODE_MASK_R, MODE_MASK_W,
    MODE_MASK_X,
};
use kiseki_meta::{context::FuseContext, MetaEngineRef};
use kiseki_types::{
    attr::{InodeAttr, SetAttrFlags},
    entry::{Entry, FullEntry},
    ino::{Ino, CONTROL_INODE, ROOT_INO},
    internal_nodes::{InternalNodeTable, CONFIG_INODE_NAME, CONTROL_INODE_NAME},
    ToErrno,
};
use libc::{mode_t, EACCES, EBADF, EFBIG, EINTR, EINVAL, ENOENT, EPERM};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::{task::JoinHandle, time::Instant};
use tracing::{debug, info, instrument, trace, Instrument};

use crate::{
    config::Config,
    data_manager::{DataManager, DataManagerRef},
    err::{LibcSnafu, MetaSnafu, ObjectStorageSnafu, Result},
    handle::{HandleTable, HandleTableRef},
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

// All helper functions for KisekiVFS
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

    async fn truncate(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        size: u64,
        _fh: Option<u64>,
    ) -> Result<InodeAttr> {
        ensure!(!ino.is_special(), LibcSnafu { errno: EPERM });
        ensure!((size as usize) < MAX_FILE_SIZE, LibcSnafu { errno: EFBIG });

        // acquire a big lock for all file handles of the inode
        let handles = self.handle_table.get_handles(ino).await;
        // when we release the guards, the lock will be released automatically.
        let mut _guards = Vec::with_capacity(handles.len());
        for h in handles {
            if let Some(fh) = h.as_file_handle() {
                if let Some(write_guard) = fh.write_lock(ctx.clone()).await {
                    _guards.push(write_guard);
                } else if matches!(_fh, Some(_fh) if _fh == h.get_fh()) {
                    // not find FileWriter
                    return LibcSnafu { errno: EACCES }.fail();
                }
            } else {
                // Try to truncate a DirHandle
                if matches!(_fh, Some(_fh) if _fh == h.get_fh()) {
                    return LibcSnafu { errno: EBADF }.fail();
                }
            }
        }

        // safety: we hold all the locks for the exists file handles
        self.data_manager.direct_flush(ino).await?;
        // TODO: call meta to truncate the file length
        let attr = self
            .meta
            .truncate(ctx, ino, size, _fh.is_some())
            .await
            .context(MetaSnafu)?;
        Ok(attr)
    }

    /// [get_entry_ttl] return the entry timeout according to the given file
    /// type.
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

    fn invalidate_length(&self, ino: Ino) {
        self.modified_at
            .get_mut(&ino)
            .map(|mut v| *v = std::time::Instant::now());
    }

    #[cfg(test)]
    fn check_access(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        attr: &InodeAttr,
        mask: libc::c_int,
    ) -> Result<()> {
        let mut my_mask = 0;
        if mask & libc::R_OK != 0 {
            my_mask |= MODE_MASK_R;
        }
        if mask & libc::W_OK != 0 {
            my_mask |= MODE_MASK_W;
        }
        if mask & libc::X_OK != 0 {
            my_mask |= MODE_MASK_X;
        }
        ctx.check_access(attr, my_mask)?;
        Ok(())
    }
}

impl KisekiVFS {
    pub async fn init(&self, _ctx: &FuseContext) -> Result<()> {
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

    pub async fn lookup(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
    ) -> Result<FullEntry> {
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

    #[allow(clippy::too_many_arguments)]
    pub async fn set_attr(
        &self,
        ctx: Arc<FuseContext>,
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
                new_attr = self.truncate(ctx.clone(), ino, size, fh).await?;
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
                    .check_set_attr(&ctx, ino, flags, &mut new_attr)
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
            .set_attr(&ctx, flags, ino, &mut new_attr)
            .await
            .context(MetaSnafu)?;

        self.try_update_file_reader_length(ino, &mut new_attr).await;

        // TODO: invalid open_file cache

        Ok(new_attr)
    }

    pub async fn open(&self, ctx: &FuseContext, inode: Ino, flags: i32) -> Result<Opened> {
        debug!(
            "fs:open with ino {:?} flags {:#b} pid {:?}",
            inode, flags, ctx.pid
        );

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

        let opened_flags = if inode.is_special() {
            fuser::consts::FOPEN_DIRECT_IO
        } else if attr.keep_cache {
            fuser::consts::FOPEN_KEEP_CACHE
        } else {
            0
        };

        Ok(Opened {
            fh: opened_fh,
            flags: opened_flags,
            inode,
            attr,
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
            .await
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
            .await
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
            .truncate_reader(ino, write_guard.get_length() as u64)
            .await;

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
            .await
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
            .await
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
            .await
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
        if let Some(handle) = self.handle_table.find_handle(inode, fh).await {
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
}

// Dir
impl KisekiVFS {
    pub async fn open_dir<I: Into<Ino>>(
        &self,
        ctx: &FuseContext,
        inode: I,
        flags: i32,
    ) -> Result<FH> {
        let inode = inode.into();
        trace!("vfs:open_dir with {:?}, flags: {:o}", inode, flags);
        if ctx.check_permission {
            let mmask =
                match flags as libc::c_int & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
                    libc::O_RDONLY => MODE_MASK_R,
                    libc::O_WRONLY => MODE_MASK_W,
                    libc::O_RDWR => MODE_MASK_R | MODE_MASK_W,
                    _ => 0, // do nothing, // Handle unexpected flags
                };
            let attr = self.meta.get_attr(inode).await?;
            ctx.check_access(&attr, mmask)?;
        }
        Ok(self.handle_table.new_dir_handle(inode).await)
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
            .await
            .context(LibcSnafu { errno: EBADF })?;
        let h = h.as_dir_handle().context(LibcSnafu { errno: EBADF })?;

        let read_guard = h.inner.read().await;
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
                let result = write_guard.children[offset as usize..].to_vec();
                return Ok(result);
            } else {
                return Ok(vec![]);
            }
        }
        if (offset as usize) < read_guard.children.len() {
            let result = read_guard.children[offset as usize..].to_vec();
            return Ok(result);
        }
        Ok(Vec::new())
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
        self.meta
            .rmdir(ctx, parent, name)
            .await
            .context(MetaSnafu)?;

        Ok(())
    }

    pub async fn release_dir(&self, inode: Ino, fh: FH) -> Result<()> {
        if let Some(dh) = self.handle_table.find_handle(inode, fh).await {
            let _ = dh.as_dir_handle().context(LibcSnafu { errno: EBADF })?;
            self.handle_table.release_file_handle(inode, fh).await;
        }
        Ok(())
    }
}

// File
impl KisekiVFS {
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
}

// Link
impl KisekiVFS {
    pub async fn link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<FullEntry> {
        ensure!(
            new_name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        ensure!(
            !new_name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );

        let attr = self.meta.link(ctx, inode, new_parent, new_name).await?;
        let e = FullEntry::new(inode, new_name, attr);
        Ok(e)
    }

    /// [KisekiVFS::unlink] deletes a name and possibly the file it refers to.
    ///
    /// Deletes a name from the filesystem. <br>
    ///
    /// If that name was the last link to a file and no processes have the<br>
    /// file open, the file is deleted and the space it was using is<br>
    /// made available for reuse.
    ///
    /// # Arguments
    /// * `ctx`: [FuseContext] - the fuse context, including some additional
    ///   information like uid, gid, etc.
    ///
    /// # Examples
    ///
    /// # References
    /// * [unlink(2)](https://man7.org/linux/man-pages/man2/unlink.2.html)
    pub async fn unlink(&self, ctx: Arc<FuseContext>, parent: Ino, name: &str) -> Result<()> {
        ensure!(
            name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        ensure!(
            !name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );

        self.meta.unlink(ctx, parent, name).await?;
        Ok(())
    }

    /// [KisekiVFS::symlink] makes a new name for a file.
    ///
    /// Symbolic links are interpreted at run time as if the contents of
    /// the link had been substituted into the path being followed to
    /// find a file or directory.
    ///
    /// A symbolic link (also known as a soft link) may point to an existing
    /// file or to a nonexistent one; the latter case is known as a dangling
    /// link.
    ///
    /// # Arguments
    /// * `ctx`: [FuseContext] - the fuse context, including some additional
    ///   information like uid, gid, etc.
    /// * `parent`: [Ino] - the inode number of the parent directory where the
    ///   symbolic link will be created.
    /// * `link_name`: [&str] - the name of the symbolic link.
    /// * `target`: [&Path] - the path to the file or directory that the
    ///   symbolic link
    ///
    /// # Returns
    /// * [FullEntry] - the inode number and the attribute of the symbolic link.
    ///
    /// # Examples
    /// For example, let's say you have a file named **foo.txt** in the current
    /// directory, and you want to create a symbolic link named **foo_link**
    /// that points to **foo.txt**. You would use the following command:
    /// ```shell
    /// ln -s foo.txt foo_link
    /// ```
    ///
    /// # References
    /// * [symlink(2)](https://man7.org/linux/man-pages/man2/symlink.2.html)
    pub async fn symlink(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        link_name: &str,
        target: &Path,
    ) -> Result<FullEntry> {
        ensure!(
            link_name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        ensure!(
            target.to_string_lossy().len() < MAX_SYMLINK_LEN,
            LibcSnafu { errno: EINVAL }
        );

        let (inode, attr) = self.meta.symlink(ctx, parent, link_name, target).await?;
        Ok(FullEntry::new(inode, link_name, attr))
    }

    /// [KisekiVFS::readlink] reads value of a symbolic link.
    ///
    /// # Arguments
    /// * `ctx`: [FuseContext] - the fuse context, including some additional
    ///   information like uid, gid, etc.
    /// * `inode`: [Ino] - inode number of the symbolic link for which you want
    ///   to read the target.
    ///
    /// # Returns
    /// * `TargetPath`: [Bytes] - the target of the symbolic link.
    ///
    /// # References
    /// * [readlink(2)](https://man7.org/linux/man-pages/man2/readlink.2.html)
    pub async fn readlink(&self, ctx: Arc<FuseContext>, inode: Ino) -> Result<Bytes> {
        let target = self.meta.readlink(ctx, inode).await?;
        Ok(target)
    }
}

// Rename
impl KisekiVFS {
    pub async fn rename(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: u32,
    ) -> Result<()> {
        ensure!(
            !name.is_empty() && !new_name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );
        ensure!(
            name.len() < MAX_NAME_LENGTH && new_name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );

        self.meta
            .rename(ctx, parent, name, new_parent, new_name, flags)
            .await
            .context(MetaSnafu)?;
        Ok(())
    }
}

/// Reply to a `open` or `opendir` call
#[derive(Debug)]
pub struct Opened {
    pub fh:    u64,
    pub flags: u32,
    pub inode: Ino,
    pub attr:  InodeAttr,
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

        let read_content = vfs
            .read(ctx.clone(), entry.inode, fh, 0, 5, 0, None)
            .await
            .unwrap();
        assert_eq!(read_content.as_ref(), b"hello".as_slice());

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

        let root = vfs.get_attr(ROOT_INO).await?;
        assert_eq!(root.mode, 511);

        // dirs
        let dir1 = vfs.mkdir(ctx.clone(), ROOT_INO, "d1", 0o755, 0).await?;
        assert_eq!(dir1.attr.mode, 493);
        let dir2 = vfs.mkdir(ctx.clone(), dir1.inode, "d2", 0o755, 0).await?;
        assert_eq!(dir2.attr.mode, 493);

        let root_dir_handle = vfs.open_dir(&ctx, ROOT_INO, libc::O_RDONLY).await?;
        let entries = vfs
            .read_dir(&ctx, ROOT_INO, root_dir_handle, 0, true)
            .await?;
        assert_eq!(entries.len(), 1);
        let dir1_handle = vfs.open_dir(&ctx, dir1.inode, libc::O_RDONLY).await?;
        let entries = vfs.read_dir(&ctx, dir1.inode, dir1_handle, 0, true).await?;
        assert_eq!(entries.len(), 1);

        assert_eq!(
            vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name)
                .await
                .unwrap_err()
                .to_errno(),
            libc::ENOTEMPTY
        );
        vfs.rmdir(ctx.clone(), dir1.inode, &dir2.name).await?;

        // files
        let f1 = vfs
            .mknod(
                ctx.clone(),
                dir1.inode,
                "f1".to_string(),
                0o644 | libc::S_IFREG,
                0,
                0,
            )
            .await?;
        assert_eq!(
            vfs.check_access(ctx.clone(), f1.inode, &f1.attr, libc::X_OK)
                .unwrap_err()
                .to_errno(),
            libc::EACCES
        );

        let time = SystemTime::now();
        let f1_attr = vfs
            .set_attr(
                ctx.clone(),
                f1.inode,
                (SetAttrFlags::MODE
                    | SetAttrFlags::UID
                    | SetAttrFlags::GID
                    | SetAttrFlags::SIZE
                    | SetAttrFlags::ATIME
                    | SetAttrFlags::MTIME)
                    .bits(),
                Some(TimeOrNow::SpecificTime(time)),
                Some(TimeOrNow::SpecificTime(time)),
                Some(0o755),
                Some(1),
                Some(3),
                Some(1024),
                None,
            )
            .await?;
        assert_eq!(f1_attr.mode, 0o755);
        assert_eq!(f1_attr.uid, 1);
        assert_eq!(f1_attr.gid, 3);
        assert_eq!(f1_attr.atime, time);
        assert_eq!(f1_attr.mtime, time);
        assert_eq!(f1_attr.length, 1024);

        vfs.check_access(ctx.clone(), f1.inode, &f1_attr, libc::X_OK)?;

        // link root/f2 -> d1/f1
        let f2_entry = vfs.link(ctx.clone(), f1.inode, ROOT_INO, "f2").await?;
        let f1_attr = vfs.get_attr(f1.inode).await?;
        assert_eq!(f1_attr.nlink, 2);

        // unlink d1/f1
        vfs.unlink(ctx.clone(), dir1.inode, &f1.name).await?;
        // lookup root/f2
        if let Ok(e) = vfs.lookup(ctx.clone(), ROOT_INO, &f2_entry.name).await {
            assert_eq!(e.attr.nlink, 1);
        } else {
            panic!("lookup f2 failed");
        }

        // rename root/f2 -> root/f3
        vfs.rename(ctx.clone(), ROOT_INO, &f2_entry.name, ROOT_INO, "f3", 0)
            .await?;
        // we should not be able to find the root/f2
        assert_eq!(
            vfs.lookup(ctx.clone(), ROOT_INO, &f2_entry.name)
                .await
                .unwrap_err()
                .to_errno(),
            libc::ENOENT
        );

        // open f3
        let f3 = vfs.open(&ctx, f2_entry.inode, libc::O_RDONLY).await?;
        assert_eq!(f3.inode, f2_entry.inode);
        vfs.flush(ctx.clone(), f3.inode, f3.fh, 0).await?;
        vfs.release(ctx.clone(), f3.inode, f3.fh).await?;

        // symlink f2_sym -> f2
        let symlink = vfs
            .symlink(ctx.clone(), ROOT_INO, "f2_sym", Path::new("f2"))
            .await?;
        assert_eq!(symlink.name, "f2_sym");
        let target = vfs.readlink(ctx.clone(), symlink.inode).await?;
        assert_eq!(target.as_ref(), b"f2");

        Ok(())
    }
}
