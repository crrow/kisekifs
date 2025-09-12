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
    fmt::{Debug, Formatter},
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use dashmap::DashMap;
use fuser::{FileType, TimeOrNow};
use kiseki_common::{
    DOT, DOT_DOT, FH, MAX_FILE_SIZE, MAX_NAME_LENGTH, MAX_SYMLINK_LEN, MODE_MASK_R, MODE_MASK_W,
};
use kiseki_meta::{MetaEngineRef, context::FuseContext};
use kiseki_types::{
    ToErrno,
    attr::{InodeAttr, SetAttrFlags},
    entry::{Entry, FullEntry},
    ino::{CONTROL_INODE, Ino, ROOT_INO},
    internal_nodes::{CONFIG_INODE_NAME, CONTROL_INODE_NAME, InternalNodeTable},
};
use libc::{EACCES, EBADF, EFBIG, EINTR, EINVAL, ENOENT, EPERM, mode_t};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::{task::JoinHandle, time::Instant};
use tracing::{debug, info, instrument, trace};

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
    /// 验证文件名是否有效
    /// 拒绝空字符串、"." 和 ".." 这些系统保留名称
    fn validate_filename(name: &str) -> Result<()> {
        if name.is_empty() {
            return LibcSnafu {
                errno: libc::EINVAL,
            }
            .fail();
        }
        if name == "." || name == ".." {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail();
        }
        Ok(())
    }

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
        _inode: Ino,
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
            my_mask |= (libc::S_IXUSR | libc::S_IXGRP | libc::S_IXOTH) as u8;
        }
        ctx.check_access(attr, my_mask)?;
        Ok(())
    }
}

impl KisekiVFS {
    /// Initialize the KisekiVFS filesystem.
    ///
    /// This function initializes the virtual file system and prepares it for
    /// operation. It performs initial setup tasks such as validating the
    /// meta format and handling any subdirectory chroot operations if
    /// configured.
    ///
    /// # Arguments
    /// * `_ctx`: [FuseContext] - The FUSE context containing request metadata
    ///   like uid, gid, process information, etc. Currently unused in
    ///   initialization.
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful initialization, or an
    ///   error if initialization fails.
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The metadata engine format validation fails
    /// - Subdirectory chroot operation fails (when configured)
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_meta::context::FuseContext;
    ///
    /// let ctx = FuseContext::background();
    /// vfs.init(&ctx).await?;
    /// ```
    ///
    /// # References
    /// * [FUSE init](https://libfuse.github.io/doxygen/structfuse__operations.html#a39a048c8a19c7b02fec706f43ec68b29)
    pub async fn init(&self, ctx: &FuseContext) -> Result<()> {
        debug!("vfs:init - starting KisekiFS initialization");

        // 1. Validate filesystem format and version
        let format = self.meta.get_format();
        info!(
            "Initializing KisekiFS: {} (chunk_size={}MB, block_size={}KB)",
            format.name,
            format.chunk_size / (1024 * 1024),
            format.block_size / 1024
        );

        // 2. Validate root directory integrity
        let root_attr = self.meta.get_attr(ROOT_INO).await.context(MetaSnafu)?;
        ensure!(
            root_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        debug!("Root directory validation passed (inode: {})", ROOT_INO.0);

        // 3. Validate internal nodes consistency
        if self.config.prefix_internal || !self.config.hide_internal {
            debug!("Internal nodes are enabled, validating consistency");
            // Internal nodes are already initialized in new(), just log their status
            // Check that key internal nodes are accessible
            let control_node = self.internal_nodes.get_internal_node(CONTROL_INODE);
            let config_node = self
                .internal_nodes
                .get_internal_node_by_name(CONFIG_INODE_NAME);
            ensure!(control_node.is_some(), LibcSnafu { errno: libc::EIO });
            ensure!(config_node.is_some(), LibcSnafu { errno: libc::EIO });
            debug!("Internal nodes validated successfully");
        }

        // 4. Initialize data manager cache and buffers
        debug!(
            "Initializing data manager with {} total buffer capacity",
            self.config.total_buffer_capacity
        );

        // 5. Log configuration summary
        info!("KisekiFS configuration:");
        info!(
            "  - Chunk size: {}MB",
            self.config.chunk_size / (1024 * 1024)
        );
        info!("  - Block size: {}KB", self.config.block_size / 1024);
        info!("  - Page size: {}KB", self.config.page_size / 1024);
        info!(
            "  - Total buffer capacity: {}MB",
            self.config.total_buffer_capacity / (1024 * 1024)
        );
        info!("  - Cache capacity: {}KB", self.config.capacity / 1024);
        info!("  - Attr timeout: {:?}", self.config.attr_timeout);
        info!("  - Dir entry timeout: {:?}", self.config.dir_entry_timeout);
        info!(
            "  - File entry timeout: {:?}",
            self.config.file_entry_timeout
        );

        // 6. Validate filesystem statistics
        let fs_stat = self
            .meta
            .stat_fs(Arc::new(ctx.clone()), ROOT_INO)
            .context(MetaSnafu)?;
        debug!(
            "Filesystem statistics - Total size: {}MB, Used: {}MB, Files: {}",
            fs_stat.total_size / (1024 * 1024),
            fs_stat.used_size / (1024 * 1024),
            fs_stat.file_count
        );

        info!("KisekiFS initialization completed successfully");
        Ok(())
    }

    /// Get filesystem statistics.
    ///
    /// This function retrieves statistical information about the filesystem,
    /// such as total space, free space, inode counts, and other filesystem
    /// metadata. This corresponds to the statfs(2) system call.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `ino`: [I: Into<Ino>] - The inode number to query. Can be any valid
    ///   inode, though typically the root inode is used for filesystem
    ///   statistics.
    ///
    /// # Returns
    /// * [Result<kiseki_types::stat::FSStat>] - Returns filesystem statistics
    ///   including:
    ///   - Total blocks and free blocks
    ///   - Total inodes and free inodes
    ///   - Block size and fragment size
    ///   - Maximum filename length
    ///   - Filesystem flags
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode is invalid or inaccessible
    /// - The metadata engine fails to retrieve statistics
    /// - Permission denied for the requested inode
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// let stats = vfs.stat_fs(ctx.clone(), ROOT_INO)?;
    /// println!("Total space: {} bytes", stats.blocks * stats.block_size);
    /// ```
    ///
    /// # References
    /// * [statfs(2)](https://man7.org/linux/man-pages/man2/statfs.2.html)
    /// * [FUSE statfs](https://libfuse.github.io/doxygen/structfuse__operations.html#a1589a59c8d9ef9a0b94dac33e6e7b1c3)
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

    /// Look up a directory entry by name.
    ///
    /// This function searches for a file or directory with the given name
    /// within the specified parent directory. It returns the inode number
    /// and attributes of the found entry. This operation is equivalent to
    /// the lookup operation in FUSE and corresponds to path resolution in
    /// traditional filesystems.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata such as uid, gid, and process information for permission
    ///   checking
    /// * `parent`: [Ino] - The inode number of the parent directory to search
    ///   in
    /// * `name`: [&str] - The name of the file or directory to look up
    ///
    /// # Returns
    /// * [Result<FullEntry>] - Returns a `FullEntry` containing:
    ///   - `inode`: The inode number of the found entry
    ///   - `name`: The name of the entry (same as input)
    ///   - `attr`: Complete inode attributes including type, permissions,
    ///     timestamps, etc.
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The parent inode does not exist (`ENOENT`)
    /// - The parent is not a directory (`ENOTDIR`)
    /// - The requested name does not exist in the parent directory (`ENOENT`)
    /// - Permission denied to search the parent directory (`EACCES`)
    /// - The name is too long (`ENAMETOOLONG`)
    ///
    /// # Special Cases
    /// - Handles internal nodes (control and config inodes) specially
    /// - Supports "." (current directory) lookups for special inodes
    /// - Internal nodes are synthetic and don't exist in the metadata store
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// // Look up a file in the root directory
    /// let entry = vfs.lookup(ctx.clone(), ROOT_INO, "myfile.txt").await?;
    /// println!("Found inode: {}, type: {:?}", entry.inode, entry.attr.kind);
    /// ```
    ///
    /// # References
    /// * [FUSE lookup](https://libfuse.github.io/doxygen/structfuse__operations.html#a37c513e31e4fc7a12c2da39b7e2490e8)
    pub async fn lookup(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
    ) -> Result<FullEntry> {
        trace!("fs:lookup with parent {:?} name {:?}", parent, name);
        // TODO: handle the special case
        if (parent == ROOT_INO || name.eq(CONTROL_INODE_NAME))
            && let Some(n) = self.internal_nodes.get_internal_node_by_name(name)
        {
            return Ok(n.0.clone());
        }
        if parent.is_special()
            && name == DOT
            && let Some(n) = self.internal_nodes.get_internal_node(parent)
        {
            return Ok(n.0.clone());
        }
        let (inode, attr) = self.meta.lookup(ctx, parent, name, true).await?;
        Ok(FullEntry {
            inode,
            name: name.to_string(),
            attr,
        })
    }

    /// Get inode attributes.
    ///
    /// This function retrieves the complete attributes of an inode, including
    /// file type, permissions, size, timestamps, ownership information, and
    /// other metadata. This corresponds to the stat(2) system call and the
    /// getattr operation in FUSE.
    ///
    /// # Arguments
    /// * `inode`: [Ino] - The inode number to retrieve attributes for
    ///
    /// # Returns
    /// * [Result<InodeAttr>] - Returns inode attributes including:
    ///   - File type (regular file, directory, symlink, etc.)
    ///   - Access permissions (mode)
    ///   - File size in bytes
    ///   - Number of hard links
    ///   - Owner user ID (uid) and group ID (gid)
    ///   - Access, modification, and change timestamps
    ///   - Block count and block size
    ///   - Device numbers for special files
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode does not exist (`ENOENT`)
    /// - The inode is corrupted or invalid (`EIO`)
    /// - Permission denied to access inode attributes (`EACCES`)
    /// - The metadata engine fails to retrieve the attributes
    ///
    /// # Special Cases
    /// - For special internal nodes (control and config inodes), returns
    ///   synthetic attributes without querying the metadata engine
    /// - Internal nodes have predefined attributes and behavior
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// let attr = vfs.get_attr(ROOT_INO).await?;
    /// println!("Root directory size: {} bytes", attr.length);
    /// println!("Permissions: {:o}", attr.mode);
    /// ```
    ///
    /// # References
    /// * [stat(2)](https://man7.org/linux/man-pages/man2/stat.2.html)
    /// * [FUSE getattr](https://libfuse.github.io/doxygen/structfuse__operations.html#a1c4a3c6b982ba57d5d96acf49b1a6c8b)
    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        debug!("vfs:get_attr with inode {:?}", inode);
        if inode.is_special()
            && let Some(n) = self.internal_nodes.get_internal_node(inode)
        {
            return Ok(n.get_attr());
        }
        let attr = self.meta.get_attr(inode).await?;
        debug!("vfs:get_attr with inode {:?} attr {:?}", inode, attr);
        Ok(attr)
    }

    /// Set inode attributes.
    ///
    /// This function modifies various attributes of an inode, such as
    /// permissions, size, timestamps, and ownership. Only the attributes
    /// specified by the flags parameter will be modified. This corresponds
    /// to operations like chmod, chown, truncate, and utime system calls,
    /// and the setattr operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata such as uid, gid for permission checking
    /// * `ino`: [Ino] - The inode number to modify attributes for
    /// * `flags`: [u32] - Bitmask specifying which attributes to modify
    ///   (SetAttrFlags)
    /// * `atime`: [Option<TimeOrNow>] - New access time, if ATIME flag is set
    /// * `mtime`: [Option<TimeOrNow>] - New modification time, if MTIME flag is
    ///   set
    /// * `mode`: [Option<u32>] - New file permissions, if MODE flag is set
    /// * `uid`: [Option<u32>] - New owner user ID, if UID flag is set
    /// * `gid`: [Option<u32>] - New owner group ID, if GID flag is set
    /// * `size`: [Option<u64>] - New file size for truncation, if SIZE flag is
    ///   set
    /// * `fh`: [Option<u64>] - Optional file handle for size operations
    ///
    /// # Returns
    /// * [Result<InodeAttr>] - Returns the updated inode attributes after
    ///   modification
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode does not exist (`ENOENT`)
    /// - Permission denied for the requested operation (`EPERM`, `EACCES`)
    /// - Invalid parameters (`EINVAL`)
    /// - File too large when setting size (`EFBIG`)
    /// - Cannot truncate due to open file handles (`EACCES`)
    /// - Trying to truncate a directory (`EBADF`)
    ///
    /// # Special Behavior
    /// - For special internal nodes, returns synthetic attributes without
    ///   modification
    /// - Size changes trigger file truncation through the data manager
    /// - Timestamp changes may update file writer metadata
    /// - All file handles must be locked during truncation operations
    /// - Permission checks are performed when ctx.check_permission is true
    ///
    /// # Examples
    /// ```rust,no_run
    /// use fuser::TimeOrNow;
    /// use kiseki_types::attr::SetAttrFlags;
    ///
    /// // Change file permissions
    /// let new_attr = vfs
    ///     .set_attr(
    ///         ctx.clone(),
    ///         inode,
    ///         SetAttrFlags::MODE.bits(),
    ///         None,
    ///         None,
    ///         Some(0o644), // new permissions
    ///         None,
    ///         None,
    ///         None,
    ///         None,
    ///     )
    ///     .await?;
    ///
    /// // Truncate file to 1024 bytes
    /// let new_attr = vfs
    ///     .set_attr(
    ///         ctx.clone(),
    ///         inode,
    ///         SetAttrFlags::SIZE.bits(),
    ///         None,
    ///         None,
    ///         None,
    ///         None,
    ///         None,
    ///         Some(1024), // new size
    ///         None,
    ///     )
    ///     .await?;
    /// ```
    ///
    /// # References
    /// * [chmod(2)](https://man7.org/linux/man-pages/man2/chmod.2.html)
    /// * [chown(2)](https://man7.org/linux/man-pages/man2/chown.2.html)
    /// * [truncate(2)](https://man7.org/linux/man-pages/man2/truncate.2.html)
    /// * [utimes(2)](https://man7.org/linux/man-pages/man2/utime.2.html)
    /// * [FUSE setattr](https://libfuse.github.io/doxygen/structfuse__operations.html#a603adb23c9e46b4b8c1b956cc0de99c9)
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

    /// Open a file for I/O operations.
    ///
    /// This function opens a file and returns a file handle that can be used
    /// for subsequent read, write, and other I/O operations. It corresponds
    /// to the open(2) system call and the open operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [&FuseContext] - The FUSE context containing request metadata
    /// * `inode`: [Ino] - The inode number of the file to open
    /// * `flags`: [i32] - Open flags (O_RDONLY, O_WRONLY, O_RDWR, etc.)
    ///
    /// # Returns
    /// * [Result<Opened>] - Returns an `Opened` structure containing:
    ///   - `fh`: File handle identifier for subsequent operations
    ///   - `flags`: Open flags for the file handle
    ///   - `inode`: The inode number that was opened
    ///   - `attr`: Current file attributes
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode does not exist (`ENOENT`)
    /// - Permission denied for the requested access mode (`EACCES`)
    /// - Too many files are already open (`EMFILE`, `ENFILE`)
    /// - The file is a directory and write access was requested (`EISDIR`)
    ///
    /// # Behavior
    /// - Updates file reader length before returning
    /// - Creates a new file handle in the handle table
    /// - Sets appropriate open flags (FOPEN_DIRECT_IO for special inodes,
    ///   FOPEN_KEEP_CACHE for cacheable files)
    /// - Delegates permission checking to the metadata engine
    ///
    /// # Examples
    /// ```rust,no_run
    /// use libc::{O_CREAT, O_RDWR};
    ///
    /// let opened = vfs.open(&ctx, inode, O_RDWR).await?;
    /// println!("Opened file with handle: {}", opened.fh);
    /// ```
    ///
    /// # References
    /// * [open(2)](https://man7.org/linux/man-pages/man2/open.2.html)
    /// * [FUSE open](https://libfuse.github.io/doxygen/structfuse__operations.html#a14b98b31a494b4cb0479819871c2ddbd)
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

    /// Read data from a file.
    ///
    /// This function reads data from an open file at the specified offset.
    /// It corresponds to the read(2) and pread(2) system calls and the
    /// read operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to read from
    /// * `fh`: [u64] - File handle returned by open operation
    /// * `offset`: [i64] - Byte offset within the file to start reading from
    /// * `size`: [u32] - Number of bytes to read
    /// * `_flags`: [i32] - Read flags (currently unused)
    /// * `_lock`: [Option<u64>] - File lock information (currently unused)
    ///
    /// # Returns
    /// * [Result<Bytes>] - Returns the read data as a `Bytes` object. The
    ///   actual number of bytes read may be less than requested if EOF is
    ///   reached.
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - The inode does not exist (`ENOENT`)
    /// - Permission denied for read access (`EACCES`)
    /// - The offset + size would exceed maximum file size (`EFBIG`)
    /// - The operation was interrupted (`EINTR`)
    /// - I/O error occurred during read (`EIO`)
    ///
    /// # Behavior
    /// - Special inodes are handled separately (TODO: not yet implemented)
    /// - Flushes any pending writes before reading to ensure consistency
    /// - Uses file handle locking to coordinate with concurrent operations
    /// - Reads through the data manager for caching and buffering
    ///
    /// # Examples
    /// ```rust,no_run
    /// let data = vfs.read(ctx.clone(), inode, fh, 0, 1024, 0, None).await?;
    /// println!("Read {} bytes", data.len());
    /// ```
    ///
    /// # References
    /// * [read(2)](https://man7.org/linux/man-pages/man2/read.2.html)
    /// * [pread(2)](https://man7.org/linux/man-pages/man2/pread.2.html)
    /// * [FUSE read](https://libfuse.github.io/doxygen/structfuse__operations.html#a2a1c6b56565c0bb1b7e5a0d5b36b2d0b)
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

    /// Write data to a file.
    ///
    /// This function writes data to an open file at the specified offset.
    /// It corresponds to the write(2) and pwrite(2) system calls and the
    /// write operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to write to
    /// * `fh`: [u64] - File handle returned by open operation
    /// * `offset`: [i64] - Byte offset within the file to start writing at
    /// * `data`: [&[u8]] - The data to write to the file
    /// * `_write_flags`: [u32] - Write flags (currently unused)
    /// * `_flags`: [i32] - Additional flags (currently unused)
    /// * `_lock_owner`: [Option<u64>] - Lock owner information (currently
    ///   unused)
    ///
    /// # Returns
    /// * [Result<u32>] - Returns the number of bytes actually written
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - Permission denied for write access (`EACCES`)
    /// - The file is opened read-only (`EBADF`)
    /// - The offset + data size would exceed maximum file size (`EFBIG`)
    /// - No space left on device (`ENOSPC`)
    /// - The operation was interrupted (`EINTR`)
    /// - I/O error occurred during write (`EIO`)
    ///
    /// # Behavior
    /// - Control inode writes are handled specially (TODO: not yet implemented)
    /// - Uses file handle write locking to coordinate with concurrent
    ///   operations
    /// - Writes through the data manager for buffering and caching
    /// - Updates file reader length after write to maintain consistency
    /// - May extend the file size if writing beyond current EOF
    ///
    /// # Examples
    /// ```rust,no_run
    /// let data = b"Hello, world!";
    /// let written = vfs
    ///     .write(ctx.clone(), inode, fh, 0, data, 0, 0, None)
    ///     .await?;
    /// println!("Wrote {} bytes", written);
    /// ```
    ///
    /// # References
    /// * [write(2)](https://man7.org/linux/man-pages/man2/write.2.html)
    /// * [pwrite(2)](https://man7.org/linux/man-pages/man2/pwrite.2.html)
    /// * [FUSE write](https://libfuse.github.io/doxygen/structfuse__operations.html#a897dd1b109e81c65b1ad7fc1e5de160a)
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

    /// Flush cached data for a file.
    ///
    /// This function flushes any cached data for an open file to the underlying
    /// storage. It's called when a file descriptor is closed or when an
    /// explicit flush is requested. This corresponds to the close(2) system
    /// call behavior and the flush operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to flush
    /// * `fh`: [u64] - File handle to flush
    /// * `lock_owner`: [u64] - Lock owner identifier for POSIX lock management
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful flush
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - The inode does not exist (`ENOENT`)
    /// - The operation was interrupted while waiting for locks (`EINTR`)
    /// - I/O error occurred during flush (`EIO`)
    /// - Insufficient space to complete flush (`ENOSPC`)
    ///
    /// # Behavior
    /// - Special inodes are handled without flushing
    /// - Acquires write lock before flushing to ensure consistency
    /// - Handles operation cancellation gracefully
    /// - Manages POSIX file locks during flush operation
    /// - Cancels pending operations on interruption
    /// - Sets OFD (Open File Description) lock owner
    /// - Releases POSIX locks if held by the lock owner
    ///
    /// # Examples
    /// ```rust,no_run
    /// vfs.flush(ctx.clone(), inode, fh, lock_owner).await?;
    /// println!("File flushed successfully");
    /// ```
    ///
    /// # References
    /// * [close(2)](https://man7.org/linux/man-pages/man2/close.2.html)
    /// * [FUSE flush](https://libfuse.github.io/doxygen/structfuse__operations.html#a5c2ff30b08d9e2ecf70b697ae0aeb949)
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
                    libc::F_UNLCK.into(),
                    0,
                    0x7FFFFFFFFFFFFFFF,
                )
                .await
                .context(MetaSnafu)?;
        }
        Ok(())
    }

    /// Synchronize file data to storage.
    ///
    /// This function forces a write of all cached data for the given file to
    /// the underlying storage device. It corresponds to the fsync(2) and
    /// fdatasync(2) system calls and the fsync operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to synchronize
    /// * `fh`: [u64] - File handle to synchronize
    /// * `_data_sync`: [bool] - If true, sync only data (not metadata) like
    ///   fdatasync(2) (currently unused, always performs full sync)
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful synchronization
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - The inode does not exist (`ENOENT`)
    /// - The operation was interrupted while waiting for locks (`EINTR`)
    /// - I/O error occurred during synchronization (`EIO`)
    /// - Insufficient space to complete sync (`ENOSPC`)
    ///
    /// # Behavior
    /// - Special inodes return immediately without syncing
    /// - Acquires write lock to ensure exclusive access during sync
    /// - Flushes all buffered data through the data manager
    /// - Ensures data persistence on the underlying storage device
    /// - Coordinates with concurrent operations through file handle locking
    ///
    /// # Examples
    /// ```rust,no_run
    /// vfs.fsync(ctx.clone(), inode, fh, false).await?;
    /// println!("File data synchronized to storage");
    /// ```
    ///
    /// # References
    /// * [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)
    /// * [fdatasync(2)](https://man7.org/linux/man-pages/man2/fdatasync.2.html)
    /// * [FUSE fsync](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
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
                        .flock(ctx.clone(), inode, fowner, libc::F_UNLCK.into())
                        .await?;
                }
                if locks & 2 != 0 && powner != 0 {
                    self.meta
                        .set_lk(
                            ctx,
                            inode,
                            powner,
                            false,
                            libc::F_UNLCK.into(),
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

        // 检查inode是否为目录类型
        let attr = self.meta.get_attr(inode).await?;
        if !attr.is_dir() {
            return LibcSnafu {
                errno: libc::ENOTDIR,
            }
            .fail();
        }

        if ctx.check_permission {
            let mmask =
                match flags as libc::c_int & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
                    libc::O_RDONLY => MODE_MASK_R,
                    libc::O_WRONLY => MODE_MASK_W,
                    libc::O_RDWR => MODE_MASK_R | MODE_MASK_W,
                    _ => 0, // do nothing, // Handle unexpected flags
                };
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

    /// Create a new directory.
    ///
    /// This function creates a new directory with the specified name in the
    /// given parent directory. It corresponds to the mkdir(2) system call
    /// and the mkdir operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `parent`: [Ino] - The inode number of the parent directory
    /// * `name`: [&str] - The name of the new directory to create
    /// * `mode`: [u32] - The file mode (permissions) for the new directory
    /// * `umask`: [u32] - The umask to apply when setting permissions
    ///
    /// # Returns
    /// * [Result<FullEntry>] - Returns a `FullEntry` containing the new
    ///   directory's inode number, name, and attributes
    ///
    /// # Errors
    /// This function may return errors if:
    /// - A file or directory with the same name already exists (`EEXIST`)
    /// - The parent directory does not exist (`ENOENT`)
    /// - Permission denied to create in parent directory (`EACCES`)
    /// - The name is too long (`ENAMETOOLONG`)
    /// - The parent is not a directory (`ENOTDIR`)
    /// - No space left on device (`ENOSPC`)
    ///
    /// # Special Cases
    /// - Cannot create directories with internal node names in root directory
    /// - Name length is limited by MAX_NAME_LENGTH
    /// - Permissions are modified by umask before setting
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// let new_dir = vfs
    ///     .mkdir(ctx.clone(), ROOT_INO, "my_directory", 0o755, 0)
    ///     .await?;
    /// println!("Created directory with inode: {}", new_dir.inode);
    /// ```
    ///
    /// # References
    /// * [mkdir(2)](https://man7.org/linux/man-pages/man2/mkdir.2.html)
    /// * [FUSE mkdir](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
    pub async fn mkdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
    ) -> Result<FullEntry> {
        debug!("fs:mkdir with parent {:?} name {:?}", parent, name);

        // 验证文件名是否有效（拒绝空字符串、"." 和 ".."）
        Self::validate_filename(name)?;

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
        let file_type = get_file_type(mode as mode_t)?;

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

    /// Create and open a new file.
    ///
    /// This function creates a new regular file with the specified name in the
    /// given parent directory and immediately opens it for I/O operations.
    /// This is an atomic operation that combines file creation and opening,
    /// corresponding to the open(2) system call with O_CREAT flag and the
    /// create operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `parent`: [Ino] - The inode number of the parent directory
    /// * `name`: [&str] - The name of the new file to create
    /// * `mode`: [u32] - The file mode (permissions) for the new file
    /// * `umask`: [u32] - The umask to apply when setting permissions
    /// * `flags`: [libc::c_int] - Open flags for the created file (O_RDWR,
    ///   O_WRONLY, etc.)
    ///
    /// # Returns
    /// * [Result<(FullEntry, FH)>] - Returns a tuple containing:
    ///   - `FullEntry`: The newly created file's inode, name, and attributes
    ///   - `FH`: File handle for immediate I/O operations
    ///
    /// # Errors
    /// This function may return errors if:
    /// - A file or directory with the same name already exists (`EEXIST`)
    /// - The parent directory does not exist (`ENOENT`)
    /// - Permission denied to create in parent directory (`EACCES`)
    /// - The name is too long (`ENAMETOOLONG`)
    /// - The parent is not a directory (`ENOTDIR`)
    /// - No space left on device (`ENOSPC`)
    /// - Too many files are open (`EMFILE`, `ENFILE`)
    ///
    /// # Special Cases
    /// - Cannot create files with internal node names in root directory
    /// - Name length is limited by MAX_NAME_LENGTH
    /// - File is immediately opened with the specified flags
    /// - Updates file reader length before returning the handle
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_types::ino::ROOT_INO;
    /// use libc::O_RDWR;
    ///
    /// let (entry, fh) = vfs
    ///     .create(ctx.clone(), ROOT_INO, "newfile.txt", 0o644, 0, O_RDWR)
    ///     .await?;
    /// println!("Created file {} with handle {}", entry.name, fh);
    /// ```
    ///
    /// # References
    /// * [open(2)](https://man7.org/linux/man-pages/man2/open.2.html)
    /// * [FUSE create](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
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

        // 验证文件名是否有效（拒绝空字符串、"." 和 ".."）
        Self::validate_filename(name)?;

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
    /// Rename or move a file or directory.
    ///
    /// This function renames a file or directory from one name to another,
    /// potentially moving it between directories. This corresponds to the
    /// rename(2) system call and the rename operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: [Arc<FuseContext>] - The FUSE context containing request
    ///   metadata
    /// * `parent`: [Ino] - The inode number of the current parent directory
    /// * `name`: [&str] - The current name of the file or directory
    /// * `new_parent`: [Ino] - The inode number of the target parent directory
    /// * `new_name`: [&str] - The new name for the file or directory
    /// * `flags`: [u32] - Rename flags (RENAME_EXCHANGE, RENAME_NOREPLACE,
    ///   etc.)
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful rename
    ///
    /// # Errors
    /// This function may return errors if:
    /// - Either the old or new name is empty (`ENOENT`)
    /// - Either name is too long (`ENAMETOOLONG`)
    /// - The source file or directory does not exist (`ENOENT`)
    /// - Permission denied for the operation (`EACCES`)
    /// - The target already exists and cannot be replaced (`EEXIST`)
    /// - The source and target are on different filesystems (`EXDEV`)
    /// - The operation would create a directory loop (`EINVAL`)
    /// - No space left on device (`ENOSPC`)
    ///
    /// # Behavior
    /// - Can move files between directories within the same filesystem
    /// - May replace existing files depending on flags and file types
    /// - Atomic operation - either succeeds completely or fails without changes
    /// - Updates directory modification times
    /// - Handles both file and directory renaming
    ///
    /// # Examples
    /// ```rust,no_run
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// // Rename a file in the same directory
    /// vfs.rename(
    ///     ctx.clone(),
    ///     ROOT_INO,
    ///     "oldname.txt",
    ///     ROOT_INO,
    ///     "newname.txt",
    ///     0,
    /// )
    /// .await?;
    ///
    /// // Move a file to a different directory
    /// vfs.rename(ctx.clone(), src_dir, "file.txt", dst_dir, "file.txt", 0)
    ///     .await?;
    /// ```
    ///
    /// # References
    /// * [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)
    /// * [FUSE rename](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
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
    match mode & libc::S_IFMT {
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
    use proptest::prelude::*;
    use rstest::rstest;
    use serial_test::serial;

    use super::*;

    /// 测试工具模块 - 提供通用的测试辅助函数和fixtures
    mod test_utils {
        use rstest::fixture;

        use super::*;

        /// VFS测试环境，包含VFS实例和临时目录
        pub struct VfsTestEnv {
            pub vfs:      Arc<KisekiVFS>,
            pub ctx:      Arc<FuseContext>,
            pub _tempdir: tempfile::TempDir, // 保持临时目录不被删除
        }

        /// 创建用于测试的VFS实例 - 传统方式（向后兼容）
        pub async fn make_vfs() -> KisekiVFS {
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

        /// 创建用于测试的VFS实例 - rstest fixture
        #[fixture]
        pub async fn vfs_env() -> VfsTestEnv {
            let tempdir = tempfile::tempdir().unwrap();
            let mut meta_config = kiseki_meta::MetaConfig::default();
            meta_config.with_dsn(&format!("rocksdb://:{}", tempdir.path().to_str().unwrap()));
            let mut format = kiseki_types::setting::Format::default();
            format.with_name("test-kiseki");
            kiseki_meta::update_format(&meta_config.dsn, format, true).unwrap();

            let meta_engine = kiseki_meta::open(meta_config).unwrap();
            let vfs_config = Config::default();
            let vfs = Arc::new(KisekiVFS::new(vfs_config, meta_engine).unwrap());
            let ctx = Arc::new(FuseContext::background());

            // 初始化VFS
            vfs.init(&ctx).await.unwrap();

            VfsTestEnv {
                vfs,
                ctx,
                _tempdir: tempdir,
            }
        }

        /// 创建测试文件内容生成器
        pub fn generate_test_data(size: usize) -> Vec<u8> {
            (0..size).map(|i| (i % 256) as u8).collect()
        }

        /// 生成随机文件名
        #[allow(dead_code)]
        pub fn generate_filename(prefix: &str) -> String {
            use std::time::SystemTime;
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("{}_file_{}", prefix, timestamp % 10000)
        }

        /// 创建测试文件的辅助函数
        pub async fn create_test_file(
            vfs: &KisekiVFS,
            ctx: Arc<FuseContext>,
            parent: Ino,
            name: &str,
            mode: u32,
        ) -> Result<(FullEntry, FH)> {
            vfs.create(ctx, parent, name, mode, 0, libc::O_RDWR).await
        }

        /// 验证文件属性的辅助函数
        #[allow(dead_code)]
        pub fn assert_file_attr(attr: &InodeAttr, expected_mode: u32, expected_size: u64) {
            assert!(attr.is_file(), "Expected file type");
            assert_eq!(attr.mode, expected_mode, "Mode mismatch");
            assert_eq!(attr.length, expected_size, "Size mismatch");
        }

        /// 验证目录属性的辅助函数  
        pub fn assert_dir_attr(attr: &InodeAttr, expected_mode: u32) {
            assert!(attr.is_dir(), "Expected directory type");
            assert_eq!(attr.mode, expected_mode, "Mode mismatch");
        }
    }

    /// 基础功能测试模块
    mod basic_operations {
        use super::{test_utils::vfs_env, *};

        #[rstest]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_vfs_initialization(#[future] vfs_env: test_utils::VfsTestEnv) -> Result<()> {
            let env = vfs_env.await;

            // Verify filesystem is operational after init
            let root_attr = env.vfs.get_attr(ROOT_INO).await?;
            test_utils::assert_dir_attr(&root_attr, 511);

            // Verify filesystem stats are accessible
            let fs_stat = env.vfs.stat_fs(env.ctx.clone(), ROOT_INO)?;
            assert!(fs_stat.total_size > 0, "文件系统总大小应大于0");

            Ok(())
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_filesystem_stats(#[future] vfs_env: test_utils::VfsTestEnv) -> Result<()> {
            let env = vfs_env.await;
            let stat = env.vfs.stat_fs(env.ctx.clone(), ROOT_INO)?;

            // 验证基本统计信息
            assert!(stat.total_size > 0, "总大小应大于0");
            assert!(stat.used_size <= stat.total_size, "已用大小不应超过总大小");
            // 验证文件数是有效的（u64不能为负）
            assert!(stat.file_count <= 1_000_000, "文件数应在合理范围内");

            Ok(())
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_root_directory_operations(
            #[future] vfs_env: test_utils::VfsTestEnv,
        ) -> Result<()> {
            let env = vfs_env.await;

            // 测试根目录属性获取
            let root_attr = env.vfs.get_attr(ROOT_INO).await?;
            test_utils::assert_dir_attr(&root_attr, 511);

            // 测试lookup不存在的文件（应该失败）
            // 注意："." 和 ".." 是有效的目录项，所以我们测试一个不存在的文件名
            let lookup_result = env
                .vfs
                .lookup(env.ctx.clone(), ROOT_INO, "nonexistent_file")
                .await;
            assert!(lookup_result.is_err(), "查找不存在的文件应该失败");

            Ok(())
        }
    }

    /// 文件操作测试模块
    mod file_operations {
        use super::{test_utils::vfs_env, *};

        #[rstest]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_basic_file_io(#[future] vfs_env: test_utils::VfsTestEnv) -> Result<()> {
            let env = vfs_env.await;

            // 创建文件
            let (entry, fh) = test_utils::create_test_file(
                &env.vfs,
                env.ctx.clone(),
                ROOT_INO,
                "test_file",
                0o755,
            )
            .await?;

            // 写入数据
            let test_data = b"hello world";
            let write_len = env
                .vfs
                .write(env.ctx.clone(), entry.inode, fh, 0, test_data, 0, 0, None)
                .await?;
            assert_eq!(write_len, test_data.len() as u32, "写入长度应匹配");

            // 同步数据
            env.vfs
                .fsync(env.ctx.clone(), entry.inode, fh, true)
                .await?;

            // 读取数据
            let read_content = env
                .vfs
                .read(
                    env.ctx.clone(),
                    entry.inode,
                    fh,
                    0,
                    test_data.len() as u32,
                    0,
                    None,
                )
                .await?;
            assert_eq!(read_content.as_ref(), test_data, "读取内容应匹配写入内容");

            // 清理资源
            env.vfs.release(env.ctx, entry.inode, fh).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_large_file_write() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            let (entry, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "large_file", 0o644)
                    .await?;

            // 测试大文件写入 - 在100MB偏移处写入数据
            let large_offset = 100 << 20; // 100 MB
            let test_data = b"large_file_content";

            let write_len = vfs
                .write(
                    ctx.clone(),
                    entry.inode,
                    fh,
                    large_offset,
                    test_data,
                    0,
                    0,
                    None,
                )
                .await?;
            assert_eq!(write_len, test_data.len() as u32);

            // 同步并读取
            vfs.fsync(ctx.clone(), entry.inode, fh, true).await?;
            let read_content = vfs
                .read(
                    ctx.clone(),
                    entry.inode,
                    fh,
                    large_offset,
                    test_data.len() as u32,
                    0,
                    None,
                )
                .await?;
            assert_eq!(read_content.as_ref(), test_data);

            // 验证writer引用计数
            let fw = vfs
                .data_manager
                .find_file_writer(entry.inode)
                .ok_or_else(|| {
                    crate::err::LibcSnafu {
                        errno: libc::ENOENT,
                    }
                    .build()
                })?;
            assert_eq!(fw.get_reference_count(), 1, "Writer引用计数应为1");

            // 清理
            vfs.release(ctx, entry.inode, fh).await?;

            // 验证writer已清理 - 由于writer清理可能是异步的，我们给它一些时间
            // 或者检查引用计数是否降为0
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // 检查writer是否仍然存在，如果存在，引用计数应该为0
            if let Some(fw) = vfs.data_manager.find_file_writer(entry.inode) {
                // 如果writer仍然存在，那么引用计数应该为0，表示没有活跃的引用
                assert_eq!(
                    fw.get_reference_count(),
                    0,
                    "Writer引用计数应为0表示无活跃引用"
                );
            }
            // 否则writer已被完全清理，这也是可接受的

            Ok(())
        }

        #[rstest]
        #[case(1024, "小文件_1KB")]
        #[case(4096, "页面大小_4KB")]
        #[case(65536, "大文件_64KB")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_various_file_sizes(
            #[future] vfs_env: test_utils::VfsTestEnv,
            #[case] size: usize,
            #[case] _desc: &str,
        ) -> Result<()> {
            let env = vfs_env.await;
            let filename = format!("file_{}_bytes", size);
            let (entry, fh) =
                test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, &filename, 0o644)
                    .await?;

            // 生成测试数据
            let test_data = test_utils::generate_test_data(size);

            // 写入数据
            let write_len = env
                .vfs
                .write(env.ctx.clone(), entry.inode, fh, 0, &test_data, 0, 0, None)
                .await?;
            assert_eq!(write_len, size as u32);

            // 读取并验证
            let read_content = env
                .vfs
                .read(env.ctx.clone(), entry.inode, fh, 0, size as u32, 0, None)
                .await?;
            assert_eq!(read_content.as_ref(), test_data.as_slice());

            // 清理
            env.vfs.release(env.ctx, entry.inode, fh).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_file_flush_and_sync() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            let (entry, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "sync_test_file", 0o644)
                    .await?;

            // 写入数据
            let test_data = b"sync test data";
            vfs.write(ctx.clone(), entry.inode, fh, 0, test_data, 0, 0, None)
                .await?;

            // 测试flush操作
            vfs.flush(ctx.clone(), entry.inode, fh, 0).await?;

            // 测试fsync操作
            vfs.fsync(ctx.clone(), entry.inode, fh, false).await?;
            vfs.fsync(ctx.clone(), entry.inode, fh, true).await?;

            // 清理
            vfs.release(ctx, entry.inode, fh).await?;

            Ok(())
        }
    }

    /// 目录操作测试模块
    mod directory_operations {
        use super::{test_utils::vfs_env, *};

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_directory_creation_and_removal() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建目录
            let dir1 = vfs
                .mkdir(ctx.clone(), ROOT_INO, "test_dir1", 0o755, 0)
                .await?;
            test_utils::assert_dir_attr(&dir1.attr, 493); // 0o755 & !S_IFMT + S_IFDIR

            // 创建嵌套目录
            let dir2 = vfs
                .mkdir(ctx.clone(), dir1.inode, "test_dir2", 0o755, 0)
                .await?;
            test_utils::assert_dir_attr(&dir2.attr, 493);

            // 测试删除非空目录（应该失败）
            let rmdir_result = vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name).await;
            assert!(rmdir_result.is_err(), "删除非空目录应该失败");
            assert_eq!(rmdir_result.unwrap_err().to_errno(), libc::ENOTEMPTY);

            // 先删除子目录，再删除父目录
            vfs.rmdir(ctx.clone(), dir1.inode, &dir2.name).await?;
            vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_directory_listing() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建多个目录和文件
            let _dir1 = vfs
                .mkdir(ctx.clone(), ROOT_INO, "list_test_dir1", 0o755, 0)
                .await?;
            let _dir2 = vfs
                .mkdir(ctx.clone(), ROOT_INO, "list_test_dir2", 0o755, 0)
                .await?;

            let (file1, fh1) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "list_test_file1", 0o644)
                    .await?;
            vfs.release(ctx.clone(), file1.inode, fh1).await?;

            // 打开根目录并读取条目
            let root_handle = vfs.open_dir(&ctx, ROOT_INO, libc::O_RDONLY).await?;
            let entries = vfs.read_dir(&ctx, ROOT_INO, root_handle, 0, true).await?;

            // 验证条目数量（应该有3个：两个目录 + 一个文件）
            assert_eq!(entries.len(), 3, "应该有3个条目");

            // 验证条目名称
            let entry_names: std::collections::HashSet<String> =
                entries.iter().map(|e| e.get_name().to_string()).collect();
            assert!(entry_names.contains("list_test_dir1"));
            assert!(entry_names.contains("list_test_dir2"));
            assert!(entry_names.contains("list_test_file1"));

            // 释放目录句柄
            vfs.release_dir(ROOT_INO, root_handle).await?;

            Ok(())
        }

        #[rstest]
        #[case("简单目录", "中文目录名")]
        #[case("带_下划线_的_目录", "包含下划线的目录名")]
        #[case("Dir123", "英文数字混合目录名")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_directory_names(
            #[future] vfs_env: test_utils::VfsTestEnv,
            #[case] dirname: &str,
            #[case] _desc: &str,
        ) -> Result<()> {
            let env = vfs_env.await;

            // 创建目录
            let dir = env
                .vfs
                .mkdir(env.ctx.clone(), ROOT_INO, dirname, 0o755, 0)
                .await?;
            assert_eq!(dir.name, dirname, "目录名应匹配");

            // 通过lookup验证目录存在
            let lookup_result = env.vfs.lookup(env.ctx.clone(), ROOT_INO, dirname).await?;
            assert_eq!(lookup_result.inode, dir.inode, "Lookup结果应匹配");

            // 清理
            env.vfs.rmdir(env.ctx.clone(), ROOT_INO, dirname).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_nested_directory_operations() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建深层嵌套目录结构 a/b/c/d
            let dir_a = vfs.mkdir(ctx.clone(), ROOT_INO, "a", 0o755, 0).await?;
            let dir_b = vfs.mkdir(ctx.clone(), dir_a.inode, "b", 0o755, 0).await?;
            let dir_c = vfs.mkdir(ctx.clone(), dir_b.inode, "c", 0o755, 0).await?;
            let dir_d = vfs.mkdir(ctx.clone(), dir_c.inode, "d", 0o755, 0).await?;

            // 在最深层目录中创建文件
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), dir_d.inode, "deep_file", 0o644)
                    .await?;

            // 写入并读取数据验证深层文件操作
            let test_data = b"deep file content";
            vfs.write(ctx.clone(), file.inode, fh, 0, test_data, 0, 0, None)
                .await?;
            let read_data = vfs
                .read(
                    ctx.clone(),
                    file.inode,
                    fh,
                    0,
                    test_data.len() as u32,
                    0,
                    None,
                )
                .await?;
            assert_eq!(read_data.as_ref(), test_data);

            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 先删除文件，再删除目录（目录必须为空才能删除）
            vfs.unlink(ctx.clone(), dir_d.inode, &file.name).await?;

            // 自底向上删除目录结构
            vfs.rmdir(ctx.clone(), dir_c.inode, &dir_d.name).await?;
            vfs.rmdir(ctx.clone(), dir_b.inode, &dir_c.name).await?;
            vfs.rmdir(ctx.clone(), dir_a.inode, &dir_b.name).await?;
            vfs.rmdir(ctx.clone(), ROOT_INO, &dir_a.name).await?;

            Ok(())
        }
    }

    /// 链接操作测试模块
    mod link_operations {
        use super::*;

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_hard_link_operations() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建原始文件
            let dir1 = vfs
                .mkdir(ctx.clone(), ROOT_INO, "link_test_dir", 0o755, 0)
                .await?;
            let (original_file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), dir1.inode, "original_file", 0o644)
                    .await?;

            // 写入数据
            let test_data = b"original content";
            vfs.write(
                ctx.clone(),
                original_file.inode,
                fh,
                0,
                test_data,
                0,
                0,
                None,
            )
            .await?;
            vfs.release(ctx.clone(), original_file.inode, fh).await?;

            // 创建硬链接
            let hard_link = vfs
                .link(ctx.clone(), original_file.inode, ROOT_INO, "hard_link")
                .await?;

            // 验证链接计数
            let file_attr = vfs.get_attr(original_file.inode).await?;
            assert_eq!(file_attr.nlink, 2, "硬链接计数应为2");

            // 通过硬链接读取数据验证内容一致性
            let link_opened = vfs.open(&ctx, hard_link.inode, libc::O_RDONLY).await?;
            let read_data = vfs
                .read(
                    ctx.clone(),
                    link_opened.inode,
                    link_opened.fh,
                    0,
                    test_data.len() as u32,
                    0,
                    None,
                )
                .await?;
            assert_eq!(read_data.as_ref(), test_data, "通过硬链接读取的数据应一致");
            vfs.release(ctx.clone(), link_opened.inode, link_opened.fh)
                .await?;

            // 删除原始文件，硬链接应该仍然存在
            vfs.unlink(ctx.clone(), dir1.inode, &original_file.name)
                .await?;

            // 验证硬链接仍可访问且链接计数减少
            let lookup_result = vfs.lookup(ctx.clone(), ROOT_INO, &hard_link.name).await?;
            assert_eq!(lookup_result.attr.nlink, 1, "删除原文件后链接计数应为1");

            // 清理
            vfs.unlink(ctx.clone(), ROOT_INO, &hard_link.name).await?;
            vfs.rmdir(ctx.clone(), ROOT_INO, &dir1.name).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_symbolic_link_operations() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建目标文件
            let (target_file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "symlink_target", 0o644)
                    .await?;
            vfs.release(ctx.clone(), target_file.inode, fh).await?;

            // 创建符号链接
            let symlink = vfs
                .symlink(
                    ctx.clone(),
                    ROOT_INO,
                    "test_symlink",
                    Path::new("symlink_target"),
                )
                .await?;

            // 验证符号链接属性
            let symlink_attr = vfs.get_attr(symlink.inode).await?;
            assert!(
                matches!(symlink_attr.kind, fuser::FileType::Symlink),
                "应该是符号链接类型"
            );

            // 读取符号链接目标
            let link_target = vfs.readlink(ctx.clone(), symlink.inode).await?;
            assert_eq!(
                link_target.as_ref(),
                b"symlink_target",
                "符号链接目标应匹配"
            );

            // 清理
            vfs.unlink(ctx.clone(), ROOT_INO, &symlink.name).await?;
            vfs.unlink(ctx.clone(), ROOT_INO, &target_file.name).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_rename_operations() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建测试文件
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "rename_source", 0o644)
                    .await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 重命名文件
            vfs.rename(
                ctx.clone(),
                ROOT_INO,
                &file.name,
                ROOT_INO,
                "rename_target",
                0,
            )
            .await?;

            // 验证原文件名不存在
            let old_lookup = vfs.lookup(ctx.clone(), ROOT_INO, &file.name).await;
            assert!(old_lookup.is_err(), "原文件名应不存在");
            assert_eq!(old_lookup.unwrap_err().to_errno(), libc::ENOENT);

            // 验证新文件名存在
            let new_lookup = vfs.lookup(ctx.clone(), ROOT_INO, "rename_target").await?;
            assert_eq!(new_lookup.inode, file.inode, "重命名后的inode应一致");

            // 清理
            vfs.unlink(ctx.clone(), ROOT_INO, "rename_target").await?;

            Ok(())
        }
    }

    /// 错误处理测试模块
    mod error_handling {
        use super::{test_utils::vfs_env, *};

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_file_not_found_errors() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 尝试打开不存在的文件
            let open_result = vfs.open(&ctx, Ino(999999), libc::O_RDONLY).await;
            assert!(open_result.is_err(), "打开不存在的文件应该失败");

            // 尝试lookup不存在的文件
            let lookup_result = vfs.lookup(ctx.clone(), ROOT_INO, "nonexistent_file").await;
            assert!(lookup_result.is_err(), "查找不存在的文件应该失败");
            assert_eq!(lookup_result.unwrap_err().to_errno(), libc::ENOENT);

            // 尝试删除不存在的文件
            let unlink_result = vfs.unlink(ctx.clone(), ROOT_INO, "nonexistent_file").await;
            assert!(unlink_result.is_err(), "删除不存在的文件应该失败");
            assert_eq!(unlink_result.unwrap_err().to_errno(), libc::ENOENT);

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_permission_errors() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建只读文件
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "readonly_file", 0o444)
                    .await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 验证权限检查
            let file_attr = vfs.get_attr(file.inode).await?;
            let access_result = vfs.check_access(ctx.clone(), file.inode, &file_attr, libc::W_OK);
            assert!(access_result.is_err(), "对只读文件的写权限检查应该失败");
            assert_eq!(access_result.unwrap_err().to_errno(), libc::EACCES);

            // 测试执行权限
            let exec_result = vfs.check_access(ctx.clone(), file.inode, &file_attr, libc::X_OK);
            assert!(exec_result.is_err(), "对非可执行文件的执行权限检查应该失败");
            assert_eq!(exec_result.unwrap_err().to_errno(), libc::EACCES);

            // 清理
            vfs.unlink(ctx.clone(), ROOT_INO, &file.name).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_invalid_operations() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 尝试在文件上执行目录操作
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "regular_file", 0o644)
                    .await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 尝试对文件进行mkdir操作（应该失败）
            let mkdir_result = vfs.mkdir(ctx.clone(), file.inode, "subdir", 0o755, 0).await;
            assert!(mkdir_result.is_err(), "在文件中创建目录应该失败");

            // 尝试对文件进行目录列表操作
            let opendir_result = vfs.open_dir(&ctx, file.inode, libc::O_RDONLY).await;
            assert!(opendir_result.is_err(), "对文件打开目录句柄应该失败");

            // 清理
            vfs.unlink(ctx.clone(), ROOT_INO, &file.name).await?;

            Ok(())
        }

        #[rstest]
        #[case("", "空文件名")]
        #[case(".", "当前目录符号")]
        #[case("..", "父目录符号")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_invalid_file_names(
            #[future] vfs_env: test_utils::VfsTestEnv,
            #[case] invalid_name: &str,
            #[case] _desc: &str,
        ) -> Result<()> {
            let env = vfs_env.await;

            // 尝试创建具有无效名称的文件
            let create_result = env
                .vfs
                .create(
                    env.ctx.clone(),
                    ROOT_INO,
                    invalid_name,
                    0o644,
                    0,
                    libc::O_RDWR,
                )
                .await;
            assert!(create_result.is_err(), "创建具有无效名称的文件应该失败");

            // 尝试创建具有无效名称的目录
            let mkdir_result = env
                .vfs
                .mkdir(env.ctx.clone(), ROOT_INO, invalid_name, 0o755, 0)
                .await;
            assert!(mkdir_result.is_err(), "创建具有无效名称的目录应该失败");

            Ok(())
        }
    }

    /// 并发操作测试模块
    mod concurrent_operations {
        use tokio::task::JoinSet;

        use super::*;

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_concurrent_file_access() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建文件
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "concurrent_file", 0o644)
                    .await?;

            // 写入初始数据
            let initial_data = b"initial content";
            vfs.write(ctx.clone(), file.inode, fh, 0, initial_data, 0, 0, None)
                .await?;
            vfs.fsync(ctx.clone(), file.inode, fh, true).await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 启动多个并发读取任务
            let mut tasks: JoinSet<Result<()>> = JoinSet::new();
            for i in 0..10 {
                let vfs_clone = vfs.clone();
                let ctx_clone = ctx.clone();
                let file_inode = file.inode;

                tasks.spawn(async move {
                    let opened = vfs_clone
                        .open(&ctx_clone, file_inode, libc::O_RDONLY)
                        .await?;
                    let read_data = vfs_clone
                        .read(
                            ctx_clone.clone(),
                            opened.inode,
                            opened.fh,
                            0,
                            initial_data.len() as u32,
                            0,
                            None,
                        )
                        .await?;
                    vfs_clone
                        .release(ctx_clone, opened.inode, opened.fh)
                        .await?;

                    assert_eq!(
                        read_data.as_ref(),
                        initial_data,
                        "任务{}读取的数据应一致",
                        i
                    );
                    Ok(())
                });
            }

            // 等待所有任务完成
            while let Some(result) = tasks.join_next().await {
                result.unwrap()?;
            }

            // 清理
            vfs.unlink(ctx, ROOT_INO, &file.name).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[serial] // 使用serial确保测试不会相互干扰
        async fn test_concurrent_directory_operations() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            let mut tasks: JoinSet<Result<String>> = JoinSet::new();

            // 并发创建多个目录
            for i in 0..5 {
                let vfs_clone = vfs.clone();
                let ctx_clone = ctx.clone();

                tasks.spawn(async move {
                    let dir_name = format!("concurrent_dir_{}", i);
                    let dir = vfs_clone
                        .mkdir(ctx_clone.clone(), ROOT_INO, &dir_name, 0o755, 0)
                        .await?;

                    // 在每个目录中创建一个文件
                    let (file, fh) = test_utils::create_test_file(
                        &vfs_clone,
                        ctx_clone.clone(),
                        dir.inode,
                        "file_in_dir",
                        0o644,
                    )
                    .await?;
                    vfs_clone.release(ctx_clone, file.inode, fh).await?;

                    Ok(dir_name)
                });
            }

            // 收集创建的目录名
            let mut created_dirs = Vec::new();
            while let Some(result) = tasks.join_next().await {
                let dir_name = result.unwrap()?;
                created_dirs.push(dir_name);
            }

            // 验证所有目录都已创建
            assert_eq!(created_dirs.len(), 5, "应该创建5个目录");

            // 清理所有创建的目录
            for dir_name in &created_dirs {
                // 先删除目录中的文件
                let dir_lookup = vfs.lookup(ctx.clone(), ROOT_INO, dir_name).await?;
                vfs.unlink(ctx.clone(), dir_lookup.inode, "file_in_dir")
                    .await?;
                // 再删除目录
                vfs.rmdir(ctx.clone(), ROOT_INO, dir_name).await?;
            }

            Ok(())
        }
    }

    /// 边界条件测试模块
    mod boundary_conditions {
        use super::{test_utils::vfs_env, *};

        proptest! {
                #[test]
                fn test_filename_generation(name in "[a-zA-Z0-9_-]{1,100}") {
                    // 属性测试：验证生成的文件名符合预期格式
                    prop_assert!(name.len() <= 100);
                    prop_assert!(name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-'));
                }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_maximum_filename_length() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 测试接近最大文件名长度的情况
            let long_name = "a".repeat(MAX_NAME_LENGTH - 1);
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, &long_name, 0o644)
                    .await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 验证文件可以正常查找
            let lookup_result = vfs.lookup(ctx.clone(), ROOT_INO, &long_name).await?;
            assert_eq!(lookup_result.inode, file.inode);

            // 清理
            vfs.unlink(ctx, ROOT_INO, &long_name).await?;

            Ok(())
        }

        #[rstest]
        #[case(0, "空文件")]
        #[case(1, "单字节文件")]
        #[case(4095, "页面边界减一")]
        #[case(4097, "页面边界加一")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_edge_file_sizes(
            #[future] vfs_env: test_utils::VfsTestEnv,
            #[case] size: usize,
            #[case] _desc: &str,
        ) -> Result<()> {
            let env = vfs_env.await;
            let filename = format!("edge_file_{}", size);
            let (file, fh) =
                test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, &filename, 0o644)
                    .await?;

            if size > 0 {
                let test_data = test_utils::generate_test_data(size);
                let write_len = env
                    .vfs
                    .write(env.ctx.clone(), file.inode, fh, 0, &test_data, 0, 0, None)
                    .await?;
                assert_eq!(write_len, size as u32);

                // 读取并验证
                let read_data = env
                    .vfs
                    .read(env.ctx.clone(), file.inode, fh, 0, size as u32, 0, None)
                    .await?;
                assert_eq!(read_data.as_ref(), test_data.as_slice());
            }

            // 验证文件属性
            let attr = env.vfs.get_attr(file.inode).await?;
            assert_eq!(attr.length, size as u64);

            // 清理
            env.vfs.release(env.ctx.clone(), file.inode, fh).await?;
            env.vfs.unlink(env.ctx, ROOT_INO, &filename).await?;

            Ok(())
        }
    }

    /// 属性操作测试模块
    mod attribute_operations {
        use super::{test_utils::vfs_env, *};

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_set_file_attributes() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            // 创建测试文件
            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "attr_test_file", 0o644)
                    .await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            let test_time = SystemTime::now();

            // 设置普通用户可以修改的属性（避免UID/GID权限问题）
            // 注意：在实际系统中，修改UID/GID需要root权限，这里只测试普通属性
            let new_attr = vfs
                .set_attr(
                    ctx.clone(),
                    file.inode,
                    (SetAttrFlags::MODE
                        | SetAttrFlags::SIZE
                        | SetAttrFlags::ATIME
                        | SetAttrFlags::MTIME)
                        .bits(),
                    Some(TimeOrNow::SpecificTime(test_time)),
                    Some(TimeOrNow::SpecificTime(test_time)),
                    Some(0o755),
                    None, // 不设置UID，避免权限问题
                    None, // 不设置GID，避免权限问题
                    Some(2048),
                    None,
                )
                .await?;

            // 验证属性设置成功
            assert_eq!(new_attr.mode & 0o777, 0o755, "文件模式应已更新");
            assert_eq!(new_attr.length, 2048, "文件大小应已更新");
            assert_eq!(new_attr.atime, test_time, "访问时间应已更新");
            assert_eq!(new_attr.mtime, test_time, "修改时间应已更新");
            // UID/GID保持原值，因为我们没有root权限修改它们

            // 清理
            vfs.unlink(ctx, ROOT_INO, &file.name).await?;

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_set_privileged_file_attributes() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);

            // 创建一个模拟的root context用于测试权限操作
            let mut root_ctx = FuseContext::background();
            root_ctx.uid = 0; // root用户
            root_ctx.gid = 0; // root组
            root_ctx.check_permission = false; // 跳过权限检查以简化测试
            let ctx = Arc::new(root_ctx);

            // 创建测试文件
            let (file, fh) = test_utils::create_test_file(
                &vfs,
                ctx.clone(),
                ROOT_INO,
                "privileged_attr_test_file",
                0o644,
            )
            .await?;
            vfs.release(ctx.clone(), file.inode, fh).await?;

            // 设置需要特殊权限的属性（UID/GID）
            let new_attr = vfs
                .set_attr(
                    ctx.clone(),
                    file.inode,
                    (SetAttrFlags::UID | SetAttrFlags::GID).bits(),
                    None,
                    None,
                    None,
                    Some(1001), // 设置UID
                    Some(1002), // 设置GID
                    None,
                    None,
                )
                .await?;

            // 验证特权属性设置成功
            assert_eq!(new_attr.uid, 1001, "用户ID应已更新");
            assert_eq!(new_attr.gid, 1002, "组ID应已更新");

            // 清理
            vfs.unlink(ctx, ROOT_INO, &file.name).await?;

            Ok(())
        }

        #[rstest]
        #[case(0o000, "无权限")]
        #[case(0o444, "只读权限")]
        #[case(0o644, "用户读写_组只读")]
        #[case(0o755, "用户全权限_组读执行")]
        #[case(0o777, "所有用户全权限")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_file_permissions(
            #[future] vfs_env: test_utils::VfsTestEnv,
            #[case] mode: u32,
            #[case] _desc: &str,
        ) -> Result<()> {
            let env = vfs_env.await;
            let filename = format!("perm_test_{:o}", mode);
            let (file, fh) =
                test_utils::create_test_file(&env.vfs, env.ctx.clone(), ROOT_INO, &filename, mode)
                    .await?;
            env.vfs.release(env.ctx.clone(), file.inode, fh).await?;

            // 获取并验证权限
            let attr = env.vfs.get_attr(file.inode).await?;
            // 文件创建时会应用umask，所以我们只验证用户有意设置的权限位
            // 注意：实际的mode包含文件类型位，我们只比较权限位部分(0o777)
            assert_eq!(attr.mode & 0o777, mode & 0o777, "权限位应匹配设置的权限");

            // 清理
            env.vfs.unlink(env.ctx, ROOT_INO, &filename).await?;

            Ok(())
        }
    }

    /// 性能测试模块
    mod performance_tests {
        use std::time::Instant;

        use super::*;

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_file_creation_performance() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            let start = Instant::now();
            let file_count = 100;

            // 创建多个文件并测量性能
            for i in 0..file_count {
                let filename = format!("perf_file_{}", i);
                let (file, fh) =
                    test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, &filename, 0o644)
                        .await?;
                vfs.release(ctx.clone(), file.inode, fh).await?;
            }

            let elapsed = start.elapsed();
            let files_per_sec = file_count as f64 / elapsed.as_secs_f64();

            // 验证性能至少达到合理水平 (比如每秒50个文件)
            assert!(
                files_per_sec > 50.0,
                "文件创建性能太低：{:.2} files/sec",
                files_per_sec
            );

            println!("文件创建性能：{:.2} files/sec", files_per_sec);

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_directory_operations_performance() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            let start = Instant::now();
            let dir_count = 50;

            // 创建多个目录
            for i in 0..dir_count {
                let dirname = format!("perf_dir_{}", i);
                let _dir = vfs.mkdir(ctx.clone(), ROOT_INO, &dirname, 0o755, 0).await?;
            }

            let create_elapsed = start.elapsed();

            // 测试目录列举性能
            let list_start = Instant::now();
            let root_handle = vfs.open_dir(&ctx, ROOT_INO, libc::O_RDONLY).await?;
            let entries = vfs.read_dir(&ctx, ROOT_INO, root_handle, 0, true).await?;
            let list_elapsed = list_start.elapsed();

            vfs.release_dir(ROOT_INO, root_handle).await?;

            let dirs_per_sec = dir_count as f64 / create_elapsed.as_secs_f64();

            // 验证目录创建性能
            assert!(
                dirs_per_sec > 100.0,
                "目录创建性能太低：{:.2} dirs/sec",
                dirs_per_sec
            );

            // 验证目录列举性能 (应该很快)
            assert!(
                list_elapsed.as_millis() < 100,
                "目录列举太慢：{:?}",
                list_elapsed
            );

            // 验证列举结果正确
            assert_eq!(entries.len(), dir_count, "列举的目录数应匹配");

            println!("目录创建性能：{:.2} dirs/sec", dirs_per_sec);
            println!("目录列举性能：{:?}", list_elapsed);

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_file_io_performance() -> Result<()> {
            let vfs = Arc::new(test_utils::make_vfs().await);
            let ctx = Arc::new(FuseContext::background());

            let (file, fh) =
                test_utils::create_test_file(&vfs, ctx.clone(), ROOT_INO, "perf_io_file", 0o644)
                    .await?;

            // 测试写入性能 - 1MB数据
            let data_size = 1024 * 1024; // 1MB
            let test_data = test_utils::generate_test_data(data_size);

            let write_start = Instant::now();
            let written = vfs
                .write(ctx.clone(), file.inode, fh, 0, &test_data, 0, 0, None)
                .await?;
            let write_elapsed = write_start.elapsed();

            assert_eq!(written, data_size as u32);

            // 测试读取性能
            let read_start = Instant::now();
            let read_data = vfs
                .read(ctx.clone(), file.inode, fh, 0, data_size as u32, 0, None)
                .await?;
            let read_elapsed = read_start.elapsed();

            assert_eq!(read_data.len(), data_size);

            // 计算吞吐量 (MB/s)
            let write_throughput =
                (data_size as f64) / (1024.0 * 1024.0) / write_elapsed.as_secs_f64();
            let read_throughput =
                (data_size as f64) / (1024.0 * 1024.0) / read_elapsed.as_secs_f64();

            // 验证I/O性能至少达到1MB/s (这是很保守的要求)
            assert!(
                write_throughput > 1.0,
                "写入吞吐量太低：{:.2} MB/s",
                write_throughput
            );
            assert!(
                read_throughput > 1.0,
                "读取吞吐量太低：{:.2} MB/s",
                read_throughput
            );

            println!("写入性能：{:.2} MB/s", write_throughput);
            println!("读取性能：{:.2} MB/s", read_throughput);

            // 清理
            vfs.release(ctx.clone(), file.inode, fh).await?;
            vfs.unlink(ctx, ROOT_INO, &file.name).await?;

            Ok(())
        }
    }
}
