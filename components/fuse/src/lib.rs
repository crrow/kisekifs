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
    cmp::max,
    ffi::{OsStr, OsString},
    sync::Arc,
    time::{Duration, SystemTime},
};

pub use config::FuseConfig;
use fuser::{
    FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    TimeOrNow,
};
use kiseki_common::{BLOCK_SIZE, MAX_NAME_LENGTH};
use kiseki_meta::context::{FuseContext, EMPTY_CONTEXT};
use kiseki_types::{
    attr::InodeAttr,
    entry::{Entry, FullEntry},
    ino::Ino,
    stat::FSStat,
    ToErrno,
};
use kiseki_utils::readable_size::ReadableSize;
use kiseki_vfs::KisekiVFS;
use libc::{__u64, c_int};
use snafu::{ResultExt, Snafu, Whatever};
use tokio::runtime;
use tracing::{debug, error, field, info, instrument, Instrument};

use crate::err::Error;

mod config;
mod err;
pub mod null;

#[derive(Debug)]
pub struct KisekiFuse {
    config:  FuseConfig,
    vfs:     Arc<KisekiVFS>,
    runtime: runtime::Runtime,
}

impl KisekiFuse {
    pub fn create(fuse_config: FuseConfig, vfs: KisekiVFS) -> Result<Self, Whatever> {
        let runtime = runtime::Builder::new_multi_thread()
            .worker_threads(fuse_config.async_work_threads)
            .thread_name("kiseki-fuse-async-runtime")
            .thread_stack_size(32 << 20)
            .enable_all()
            .build()
            .with_whatever_context(|e| format!("unable to built tokio runtime {e} "))?;
        info!(
            "build tokio runtime with {} working threads",
            fuse_config.async_work_threads
        );
        Ok(Self {
            config: fuse_config,
            vfs: Arc::new(vfs),
            runtime,
        })
    }

    fn reply_entry(&self, ctx: &FuseContext, reply: ReplyEntry, mut entry: FullEntry) {
        let inode = entry.inode;
        if !inode.is_special()
            && entry.attr.is_file()
            && self.vfs.modified_since(inode, ctx.start_at)
        {
            debug!("refresh attr for {:?}", inode);
            match self
                .runtime
                .block_on(self.vfs.get_attr(inode).in_current_span())
            {
                Ok(new_attr) => {
                    debug!("refresh attr for {:?} to {:?}", inode, new_attr);
                    entry.attr = new_attr;
                }
                Err(e) => {
                    debug!("failed to refresh attr for {:?} {:?}", inode, e);
                }
            }
        }
        self.runtime.block_on(
            self.vfs
                .try_update_file_reader_length(inode, &mut entry.attr)
                .in_current_span(),
        );

        reply.entry(
            self.vfs.get_entry_ttl(entry.attr.kind),
            &entry.attr.to_fuse_attr(entry.inode),
            1,
        );
    }

    fn reply_attr(
        &self,
        ctx: &FuseContext,
        reply: ReplyAttr,
        inode: Ino,
        mut attr: InodeAttr,
        // we reply the attr directly without refresh check.
        directly: bool,
    ) {
        if !directly
            && !inode.is_special()
            && attr.is_file()
            && self.vfs.modified_since(inode, ctx.start_at)
        {
            debug!("refresh attr for {:?}", inode);
            match self
                .runtime
                .block_on(self.vfs.get_attr(inode).in_current_span())
            {
                Ok(new_attr) => {
                    debug!("refresh attr for {:?} to {:?}", inode, new_attr);
                    attr = new_attr;
                }
                Err(e) => {
                    debug!("failed to refresh attr for {:?} {:?}", inode, e);
                }
            }
            self.runtime.block_on(
                self.vfs
                    .try_update_file_reader_length(inode, &mut attr)
                    .in_current_span(),
            );
        }

        reply.attr(self.vfs.get_entry_ttl(attr.kind), &attr.to_fuse_attr(inode))
    }
}

impl Filesystem for KisekiFuse {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig
    /// object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        let ctx = FuseContext::from(_req);
        match self.runtime.block_on(self.vfs.init(&ctx).in_current_span()) {
            Ok(_) => {
                // TODO
            }
            Err(_) => {
                // TODO
            }
        }
        Ok(())
    }

    #[instrument(level = "info", skip_all, fields(req = _req.unique(), ino = parent, name = ? name))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let ctx = Arc::new(FuseContext::from(_req));
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if name.len() > MAX_NAME_LENGTH {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        let entry = match self.runtime.block_on(
            self.vfs
                .lookup(ctx.clone(), Ino::from(parent), name)
                .in_current_span(),
        ) {
            Ok(n) => n,
            Err(e) => {
                // TODO: handle this error
                reply.error(e.to_errno());
                return;
            }
        };

        debug!("lookup {:?} {:?}", parent, entry);

        self.reply_entry(&ctx, reply, entry);
    }

    #[instrument(level = "info", skip_all, fields(req = _req.unique(), ino = ino, name = field::Empty))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self
            .runtime
            .block_on(self.vfs.get_attr(Ino::from(ino)).in_current_span())
        // .block_on(self.vfs.get_attr(Ino::from(ino)))
        {
            Ok(attr) => self.reply_attr(&EMPTY_CONTEXT, reply, Ino(ino), attr, true),
            Err(e) => {
                error!("getattr {:?} {:?}", ino, e);
                reply.error(e.to_errno())
            }
        };
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), ino = ino, name = field::Empty))]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let ctx = Arc::new(FuseContext::from(_req));
        match self.runtime.block_on(
            self.vfs
                .set_attr(
                    ctx.clone(),
                    Ino(ino),
                    flags.unwrap_or(0),
                    atime,
                    mtime,
                    mode,
                    uid,
                    gid,
                    size,
                    fh,
                )
                .in_current_span(),
        ) {
            Ok(new_attr) => self.reply_attr(&ctx, reply, Ino(ino), new_attr, false),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    /// In UNIX-like operating systems, when a new file or directory is created,
    /// its permissions are typically set based on the process's umask value.
    ///
    /// The umask is a bitmask that specifies which permissions should be
    /// removed from the default permissions assigned to newly created files and
    /// directories.
    ///
    /// For example, if mode is set to 0666 (read and write
    /// permissions for owner, group, and others), and umask is set to
    /// 0200, it might indicate that the write permission for the owner
    /// should be removed, resulting in a final mode of 0466 (read-only for
    /// owner, read and write for group and others).
    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), parent = parent, name = ? name))]
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        // mode_t is u32 on Linux but u16 on macOS, so cast it here
        let ctx = Arc::new(FuseContext::from(_req));
        let name = name.to_string_lossy().to_string();

        match self.runtime.block_on(
            self.vfs
                .mknod(ctx.clone(), Ino(parent), name, mode, umask, rdev)
                .in_current_span(),
        ) {
            Ok(entry) => self.reply_entry(&ctx, reply, entry),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), parent = parent, name = ? name))]
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let ctx = Arc::new(FuseContext::from(_req));
        let name = name.to_string_lossy().to_string();
        match self.runtime.block_on(
            self.vfs
                .mkdir(ctx.clone(), Ino(parent), &name, mode, umask)
                .in_current_span(),
        ) {
            Ok(entry) => self.reply_entry(&ctx, reply, entry),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let ctx = Arc::new(FuseContext::from(_req));
        match self.runtime.block_on(
            self.vfs
                .unlink(ctx, Ino(parent), name.to_str().unwrap())
                .in_current_span(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), parent = parent, name = ? name))]
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let ctx = Arc::new(FuseContext::from(_req));
        match self.runtime.block_on(
            self.vfs
                .rmdir(ctx, Ino(parent), name.to_str().unwrap())
                .in_current_span(),
        ) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        };
    }

    #[instrument(level = "info",
    skip_all,
    fields(req = req.unique(),
    old_parent = parent,
    old_name = ?name,
    new_parent = newparent,
    new_name = ?newname))]
    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let ctx = Arc::new(FuseContext::from(req));
        match self.runtime.block_on(
            self.vfs
                .rename(
                    ctx,
                    Ino(parent),
                    name.to_str().unwrap(),
                    Ino(newparent),
                    newname.to_str().unwrap(),
                    flags,
                )
                .in_current_span(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), ino = ino, new_parent = new_parent, new_name = ? new_name))]
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        let ctx = Arc::new(FuseContext::from(_req));
        match self.runtime.block_on(
            self.vfs
                .link(
                    ctx.clone(),
                    Ino(ino),
                    Ino(new_parent),
                    new_name.to_str().unwrap(),
                )
                .in_current_span(),
        ) {
            Ok(entry) => self.reply_entry(&ctx, reply, entry),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), ino = _ino, pid = _req.pid(), name = field::Empty))]
    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        let ctx = FuseContext::from(_req);
        match self
            .runtime
            .block_on(self.vfs.open(&ctx, Ino(_ino), _flags).in_current_span())
        {
            Ok(opened) => reply.opened(opened.fh, opened.flags),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), ino = ino, fh = fh, offset = offset, size = size, name = field::Empty))]
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let ctx = Arc::new(FuseContext::from(_req));
        let mut bytes_read = 0;
        match self.runtime.block_on(
            self.vfs
                .read(ctx, Ino(ino), fh, offset, size, flags, lock_owner)
                .in_current_span(),
        ) {
            Ok(data) => {
                bytes_read = data.len();
                reply.data(&data);
            }
            Err(e) => {
                error!("read {:?} {:?}", Ino(ino), e);
                reply.error(e.to_errno())
            }
        }

        debug!(
            "read {:?} FH: {:?} offset: {:?} read_count: {:?}",
            Ino(ino),
            fh,
            offset,
            bytes_read
        );
    }

    #[instrument(level = "debug", skip_all, fields(req = _req.unique(), ino = ino, fh = fh, offset = ReadableSize(offset as u64).to_string(), length = ReadableSize(data.len() as u64).to_string(), pid = _req.pid(), name = field::Empty))]
    // #[instrument(fields(req=_req.unique(), ino=ino, fh=fh, offset=offset,
    // length=data.len(), pid=_req.pid(), name=field::Empty))]
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let ctx = Arc::new(FuseContext::from(_req));
        match self.runtime.block_on(
            self.vfs
                .write(
                    ctx,
                    Ino(ino),
                    fh,
                    offset,
                    data,
                    write_flags,
                    flags,
                    lock_owner,
                )
                .in_current_span(),
        ) {
            Ok(bytes_written) => {
                reply.written(bytes_written);
            }
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "info", skip_all, fields(req = req.unique(), ino = ino, fh = fh, pid = req.pid(), name = field::Empty))]
    fn flush(&mut self, req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        let ctx = Arc::new(FuseContext::from(req));
        match self.runtime.block_on(
            self.vfs
                .flush(ctx, Ino(ino), fh, lock_owner)
                .in_current_span(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "info", skip_all, fields(req = req.unique(), ino = ino, fh = fh, name = field::Empty))]
    fn release(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _ock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let ctx = Arc::new(FuseContext::from(req));
        match self
            .runtime
            .block_on(self.vfs.release(ctx, Ino(ino), fh).in_current_span())
        {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        };
    }

    #[instrument(level = "info", skip_all, fields(req = _req.unique(), ino = ino, fh = fh, datasync = datasync, name = field::Empty))]
    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, _reply: ReplyEmpty) {
        let ctx = Arc::new(FuseContext::from(_req));
        match self.runtime.block_on(
            self.vfs
                .fsync(ctx, Ino(ino), fh, datasync)
                .in_current_span(),
        ) {
            Ok(()) => _reply.ok(),
            Err(e) => _reply.error(e.to_errno()),
        }
    }

    // Open directory.
    // Unless the 'default_permissions' mount option is given,
    // this method should check if opendir is permitted for this directory.
    // Optionally opendir may also return an arbitrary filehandle in the
    // fuse_file_info structure, which will be passed to readdir, releasedir and
    // fsyncdir.
    #[instrument(level = "info", skip_all, fields(req = _req.unique(), ino = ino, flags = flags, name = field::Empty))]
    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let ctx = FuseContext::from(_req);
        match self
            .runtime
            .block_on(self.vfs.open_dir(&ctx, ino, flags).in_current_span())
        {
            Ok(fh) => reply.opened(fh, flags as u32),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), ino = ino, fh = fh, offset = offset))]
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let ctx = FuseContext::from(_req);
        let entries = match self.runtime.block_on(
            self.vfs
                .read_dir(&ctx, ino, fh, offset, false)
                .in_current_span(),
        ) {
            Ok(n) => n,
            Err(e) => {
                reply.error(e.to_errno());
                return;
            }
        };

        let mut offset = offset + 1;
        debug!("get entry length: { }", entries.len());
        for entry in entries.iter() {
            if reply.add(
                entry.get_inode().0,
                offset,
                entry.get_file_type(),
                &entry.get_name(),
            ) {
                break;
            } else {
                offset += 1;
            }
        }
        reply.ok();
    }

    #[instrument(level = "info", skip_all, fields(req = _req.unique(), ino = _ino, name = field::Empty))]
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        // 	http://man.he.net/man2/statfs
        // struct statfs {
        // __fsword_t f_type;    // Type of filesystem (see below)
        // __fsword_t f_bsize;   // Optimal transfer block size
        // fsblkcnt_t f_blocks;  // Total data blocks in filesystem
        // fsblkcnt_t f_bfree;   // Free blocks in filesystem
        // fsblkcnt_t f_bavail;  // Free blocks available to
        // unprivileged user
        // fsfilcnt_t f_files;   // Total file nodes in filesystem
        // fsfilcnt_t f_ffree;   // Free file nodes in filesystem
        // fsid_t     f_fsid;    // Filesystem ID
        // __fsword_t f_namelen; // Maximum length of filenames
        // __fsword_t f_frsize;  // Fragment size (since Linux 2.6)
        // __fsword_t f_flags;   // Mount flags of filesystem
        // (since Linux 2.6.36)
        // __fsword_t f_spare[xxx];
        // Padding bytes reserved for future use
        // };

        let ctx = Arc::new(FuseContext::from(_req));
        // in case we can't get the stat_fs, we just return a default one.
        let state = self.vfs.stat_fs(ctx, _ino).unwrap_or(FSStat::default());

        // Compute the total number of available blocks
        let total_blocks = max(state.total_size / BLOCK_SIZE as u64, 1);
        // Compute the total number of used blocks
        let used_blocks = state.used_size / BLOCK_SIZE as u64;
        // Compute the total number of remaining blocks
        let remain_blocks = max(total_blocks as i64 - used_blocks as i64, 0) as u64;

        reply.statfs(
            // blocks the total number of available blocks
            total_blocks,
            // bfree: Number of free blocks available for use.
            remain_blocks,
            // bavail: Number of blocks available to unprivileged users.
            remain_blocks,
            // files: Total number of inodes (file system objects) in the file system.
            u64::MAX,
            // ffree: Number of free inodes available for creating new files.
            u64::MAX - state.file_count,
            // bsize: Fundamental block size of the file system (in bytes).
            BLOCK_SIZE as u32,
            // namelen: Maximum length of a filename.
            MAX_NAME_LENGTH as u32,
            // frsize: Fragment size (if file system supports fragmentation).
            BLOCK_SIZE as u32,
        );
    }

    #[instrument(level = "warn", skip_all, fields(req = _req.unique(), parent = parent, name = ? name))]
    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let ctx = Arc::new(FuseContext::from(_req));
        let name = name.to_string_lossy().to_string();

        match self.runtime.block_on(
            self.vfs
                .create(ctx.clone(), Ino(parent), &name, mode, umask, flags)
                .in_current_span(),
        ) {
            Ok((entry, fh)) => reply.created(
                self.vfs.get_entry_ttl(entry.attr.kind),
                &entry.attr.to_fuse_attr(entry.inode),
                1,
                fh,
                flags as u32,
            ),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level = "info", skip_all, fields(req = req.unique(), ino = ino, name = field::Empty))]
    fn fallocate(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let ctx = Arc::new(FuseContext::from(req));
        match self.runtime.block_on(
            self.vfs
                .fallocate(ctx, Ino(ino), fh, offset, length, mode as u8)
                .in_current_span(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        }
    }
}
