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
    time::SystemTime,
};

use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use libc::c_int;
use snafu::{ResultExt, Snafu, Whatever};
use tokio::runtime;
use tracing::{debug, error, field, info, instrument, Instrument};

use crate::{
    common::err::ToErrno,
    fuse::config::FuseConfig,
    meta::{
        types::{Entry, Ino},
        MetaContext, MAX_NAME_LENGTH,
    },
    vfs::KisekiVFS,
};

const BLOCK_SIZE: u32 = 4096;

#[derive(Debug, Snafu)]
pub enum FuseError {
    #[snafu(display("invalid file name {:?}", name))]
    ErrInvalidFileName { name: OsString },
    #[snafu(display("file name too long {:?}", name))]
    ErrFilenameTooLong { name: OsString },
}

impl ToErrno for FuseError {
    fn to_errno(&self) -> c_int {
        match self {
            FuseError::ErrInvalidFileName { .. } => libc::EINVAL,
            FuseError::ErrFilenameTooLong { .. } => libc::ENAMETOOLONG,
        }
    }
}

#[derive(Debug)]
pub struct KisekiFuse {
    config: FuseConfig,
    vfs: KisekiVFS,
    runtime: runtime::Runtime,
}

impl KisekiFuse {
    pub fn create(fuse_config: FuseConfig, vfs: KisekiVFS) -> Result<Self, Whatever> {
        let runtime = runtime::Builder::new_multi_thread()
            .worker_threads(fuse_config.async_work_threads)
            .thread_name("kiseki-fuse-async-runtime")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .with_whatever_context(|e| format!("unable to built tokio runtime {e} "))?;
        info!(
            "build tokio runtime with {} working threads",
            fuse_config.async_work_threads
        );
        Ok(Self {
            config: fuse_config,
            vfs,
            runtime,
        })
    }

    fn reply_entry(&self, ctx: &MetaContext, reply: ReplyEntry, mut entry: Entry) {
        if !entry.is_special_inode()
            && entry.is_file()
            && self.vfs.modified_since(entry.inode, ctx.start_at)
        {
            debug!("refresh attr for {:?}", entry.inode);
            match self
                .runtime
                .block_on(self.vfs.get_attr(entry.inode).in_current_span())
            {
                Ok(new_attr) => {
                    debug!("refresh attr for {:?} to {:?}", entry.inode, new_attr);
                    entry.attr = new_attr;
                }
                Err(e) => {
                    debug!("failed to refresh attr for {:?} {:?}", entry.inode, e);
                }
            }
        }

        self.vfs.update_length(&mut entry);
        reply.entry(
            &entry.ttl.unwrap(),
            &entry.to_fuse_attr(),
            entry.generation.unwrap(),
        );
    }
    fn reply_attr(&self, ctx: &MetaContext, reply: ReplyAttr, mut entry: Entry) {
        if !entry.inode.is_special()
            && entry.is_file()
            && self.vfs.modified_since(entry.inode, ctx.start_at)
        {
            debug!("refresh attr for {:?}", entry.inode);
            match self
                .runtime
                .block_on(self.vfs.get_attr(entry.inode).in_current_span())
            {
                Ok(new_attr) => {
                    debug!("refresh attr for {:?} to {:?}", entry.inode, new_attr);
                    entry.attr = new_attr;
                }
                Err(e) => {
                    debug!("failed to refresh attr for {:?} {:?}", entry.inode, e);
                }
            }
        }
        self.vfs.update_length(&mut entry);
        reply.attr(&entry.ttl.unwrap(), &entry.attr.to_fuse_attr(entry.inode))
    }
}

impl Filesystem for KisekiFuse {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig
    /// object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        let ctx = MetaContext::from(_req);
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
    #[instrument(level="info", skip_all, fields(req=_req.unique(), ino=parent, name=?name))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let ctx = MetaContext::from(_req);
        let name = match name.to_str().ok_or_else(|| FuseError::ErrInvalidFileName {
            name: name.to_owned(),
        }) {
            Ok(n) => n,
            Err(e) => {
                reply.error(e.to_errno());
                return;
            }
        };

        // FIXME: tidy this error

        if name.len() > MAX_NAME_LENGTH {
            reply.error(
                FuseError::ErrFilenameTooLong {
                    name: OsString::from(name),
                }
                .to_errno(),
            );
            return;
        }

        let entry = match self.runtime.block_on(
            self.vfs
                .lookup(&ctx, Ino::from(parent), name)
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

    #[instrument(level="info", skip_all, fields(req=_req.unique(), ino=ino, name=field::Empty))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self
            .runtime
            .block_on(self.vfs.get_attr(Ino::from(ino)).in_current_span())
            // .block_on(self.vfs.get_attr(Ino::from(ino)))
        {
            Ok(attr) => reply.attr(&self.vfs.get_ttl(attr.kind), &attr.to_fuse_attr(ino)),
            Err(e) => {
                error!("getattr {:?} {:?}", ino, e);
                reply.error(e.to_errno())
            }
        };
    }

    #[instrument(level="info", skip_all, fields(req=_req.unique(), ino=_ino, name=field::Empty))]
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        let ctx = MetaContext::from(_req);
        // FIXME: use a better way
        let state = self
            .runtime
            .block_on(self.vfs.stat_fs(&ctx, _ino).in_current_span())
            .unwrap();

        reply.statfs(
            // BLOCKS: Number of free blocks available for use.
            // blocks:
            max(state.total_space / BLOCK_SIZE as u64, 1),
            // bfree: Number of free blocks available for use.
            state.avail_space / BLOCK_SIZE as u64,
            // bavail: Number of blocks available to unprivileged users.
            state.avail_space / BLOCK_SIZE as u64,
            // files: Total number of inodes (file system objects) in the file system.
            state.used_inodes + state.available_inodes,
            // ffree: Number of free inodes available for creating new files.
            state.available_inodes,
            // bsize: Fundamental block size of the file system (in bytes).
            BLOCK_SIZE,
            // namelen: Maximum length of a filename.
            MAX_NAME_LENGTH as u32,
            // frsize: Fragment size (if file system supports fragmentation).
            BLOCK_SIZE,
        );
    }

    // Open directory.
    // Unless the 'default_permissions' mount option is given,
    // this method should check if opendir is permitted for this directory.
    // Optionally opendir may also return an arbitrary filehandle in the
    // fuse_file_info structure, which will be passed to readdir, releasedir and
    // fsyncdir.
    #[instrument(level="info", skip_all, fields(req=_req.unique(), ino=_ino, name=field::Empty))]
    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        let ctx = MetaContext::from(_req);
        match self
            .runtime
            .block_on(self.vfs.open_dir(&ctx, _ino, _flags).in_current_span())
        {
            Ok(fh) => reply.opened(fh, _flags as u32),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=ino, fh=fh, offset=offset))]
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let ctx = MetaContext::from(_req);
        let entries = match self
            .runtime
            .block_on(self.vfs.read_dir(&ctx, ino, fh, offset).in_current_span())
        {
            Ok(n) => n,
            Err(e) => {
                reply.error(e.to_errno());
                return;
            }
        };

        let mut offset = offset + 1;
        debug!("get entry length: { }", entries.len());
        for entry in entries.iter() {
            if reply.add(entry.inode.into(), offset, entry.attr.kind, &entry.name) {
                break;
            } else {
                offset += 1;
            }
        }
        reply.ok();
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), parent=parent, name=?name))]
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
        let ctx = MetaContext::from(_req);
        let name = name.to_string_lossy().to_string();

        match self.runtime.block_on(
            self.vfs
                .mknod(
                    &ctx,
                    Ino(parent),
                    name,
                    mode as libc::mode_t,
                    umask as u16,
                    rdev,
                )
                .in_current_span(),
        ) {
            Ok(entry) => self.reply_entry(&ctx, reply, entry),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), parent=parent, name=?name))]
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
        let ctx = MetaContext::from(_req);
        let name = name.to_string_lossy().to_string();

        match self.runtime.block_on(
            self.vfs
                .create(&ctx, Ino(parent), &name, mode as u16, umask as u16, flags)
                .in_current_span(),
        ) {
            Ok((entry, fh)) => reply.created(
                &entry.ttl.unwrap(),
                &entry.to_fuse_attr(),
                entry.generation.unwrap(),
                fh,
                flags as u32,
            ),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=ino, name=field::Empty))]
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
        let ctx = MetaContext::from(_req);
        match self.runtime.block_on(
            self.vfs
                .set_attr(
                    &ctx,
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
            Ok(entry) => self.reply_attr(&ctx, reply, entry),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), parent=parent, name=?name))]
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let ctx = MetaContext::from(_req);
        let name = name.to_string_lossy().to_string();
        match self.runtime.block_on(
            self.vfs
                .mkdir(&ctx, Ino(parent), &name, mode as u16, umask as u16)
                .in_current_span(),
        ) {
            Ok(entry) => self.reply_entry(&ctx, reply, entry),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=_ino, pid=_req.pid(), name=field::Empty))]
    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        let ctx = MetaContext::from(_req);
        match self
            .runtime
            .block_on(self.vfs.open(&ctx, Ino(_ino), _flags).in_current_span())
        {
            Ok(opened) => reply.opened(opened.fh, opened.flags),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=ino, fh=fh, offset=offset, size=size, name=field::Empty))]
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
        let ctx = MetaContext::from(_req);
        let mut bytes_read = 0;
        match self.runtime.block_on(
            self.vfs
                .read(&ctx, Ino(ino), fh, offset, size, flags, lock_owner)
                .in_current_span(),
        ) {
            Ok(data) => {
                bytes_read = data.len();
                reply.data(&data);
            }
            Err(e) => reply.error(e.to_errno()),
        }

        debug!("read {:?} {:?} {:?} {:?}", ino, fh, offset, bytes_read);
    }

    #[instrument(level="debug", skip_all, fields(req=_req.unique(), ino=ino, fh=fh, offset=offset, length=data.len(), pid=_req.pid(), name=field::Empty))]
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
        let ctx = MetaContext::from(_req);
        match self.runtime.block_on(
            self.vfs
                .write(
                    &ctx,
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

    #[instrument(level="warn", skip_all, fields(req=req.unique(), ino=ino, fh=fh, pid=req.pid(), name=field::Empty))]
    fn flush(&mut self, req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        let ctx = MetaContext::from(req);
        match self.runtime.block_on(
            self.vfs
                .flush(&ctx, Ino(ino), fh, lock_owner)
                .in_current_span(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=ino, fh=fh, datasync=datasync, name=field::Empty))]
    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, _reply: ReplyEmpty) {
        todo!()
    }
}
