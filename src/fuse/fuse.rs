use crate::common::err::ToErrno;
use crate::fuse::config::FuseConfig;
use crate::meta::types::{Entry, Ino};
use crate::meta::{MetaContext, MAX_NAME_LENGTH};
use crate::vfs::KisekiVFS;
use fuser::{Filesystem, KernelConfig, ReplyEntry, Request};
use libc::c_int;
use snafu::{ResultExt, Snafu, Whatever};
use std::ffi::{OsStr, OsString};
use std::fmt::Display;
use tokio::runtime;
use tracing::{debug, info, instrument, Instrument};

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
        reply.entry(&entry.ttl, &entry.to_fuse_attr(), entry.generation);
    }
}

impl Filesystem for KisekiFuse {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
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

        self.reply_entry(&ctx, reply, entry);
    }
}
