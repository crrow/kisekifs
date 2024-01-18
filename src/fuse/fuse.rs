use crate::common::err::ToErrno;
use crate::fuse::config::FuseConfig;
use crate::meta::types::{Entry, Ino};
use crate::meta::{MetaContext, MAX_NAME_LENGTH};
use crate::vfs::KisekiVFS;
use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEntry, ReplyOpen, ReplyStatfs,
    Request,
};
use libc::c_int;
use snafu::{ResultExt, Snafu, Whatever};
use std::cmp::max;
use std::ffi::{OsStr, OsString};
use std::fmt::Display;
use tokio::runtime;
use tracing::{debug, field, info, instrument, trace, Instrument};

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
}

impl Filesystem for KisekiFuse {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        match self.runtime.block_on(self.vfs.init().in_current_span()) {
            Ok(_) => {}
            Err(_) => {}
        }
        Ok(())
    }
    #[instrument(level="info", skip_all, fields(req=_req.unique(), ino=parent, name=?name))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let ctx = MetaContext::default();
        let name = match name.to_str().ok_or_else(|| FuseError::ErrInvalidFileName {
            name: name.to_owned(),
        }) {
            Ok(n) => n,
            Err(e) => {
                reply.error(e.to_errno());
                return;
            }
        };

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

    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=ino, name=field::Empty))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self
            .runtime
            .block_on(self.vfs.get_attr(Ino::from(ino)).in_current_span())
        {
            Ok(attr) => reply.attr(&self.vfs.get_ttl(attr.kind), &attr.to_fuse_attr(ino)),
            Err(e) => reply.error(e.to_errno()),
        };
    }

    #[instrument(level="info", skip_all, fields(req=_req.unique(), ino=_ino, name=field::Empty))]
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        let ctx = MetaContext::default();
        // FIXME: use a better way
        let state = self
            .runtime
            .block_on(self.vfs.stat_fs(&ctx, _ino).in_current_span())
            .unwrap();

        reply.statfs(
            // BLOCKS: Number of free blocks available for use.
            /* blocks:*/
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
    // Optionally opendir may also return an arbitrary filehandle in the fuse_file_info structure,
    // which will be passed to readdir, releasedir and fsyncdir.
    #[instrument(level="warn", skip_all, fields(req=_req.unique(), ino=_ino, name=field::Empty))]
    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        let ctx = MetaContext::default();
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
        let ctx = MetaContext::default();
        let mut entries = match self
            .runtime
            .block_on(self.vfs.read_dir(&ctx, ino, fh, offset).in_current_span())
        {
            Ok(n) => n,
            Err(e) => {
                reply.error(e.to_errno());
                return;
            }
        };

        for entry in entries.drain(..) {
            todo!()
            // if !reply.add(entry.inode.into(), entry.offset, entry.kind, entry.name) {
            //     break;
            // }
        }
        reply.ok();
    }
}
