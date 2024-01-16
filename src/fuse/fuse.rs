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
use tracing::{debug, info};

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
    // fn reply_entry(&self, ctx: &mut FuseContext, reply: ReplyEntry, entry: &Entry) {
    //     let ttl = if entry.is_filetype(fuser::FileType::Directory) {
    //         &self.config.dir_entry_timeout
    //     } else {
    //         &self.config.entry_timeout
    //     };
    //
    //     if entry.is_special_inode() {
    //     } else if entry.is_filetype(fuser::FileType::RegularFile)
    //         && self.modified_since(entry.inode, ctx.start_at)
    //     {
    //         debug!("refresh attr for {:?}", entry.inode);
    //         // TODO: introduce another type to avoid messing up with fuse's methods.
    //         // self.getattr()
    //     }
    //
    //     reply.entry(&ttl, &entry.attr.borrow().inner, 1)
    // }
    // fn modified_since(&self, ino: Ino, since: SystemTime) -> bool {
    //     todo!()
    // }
}

impl Filesystem for KisekiFuse {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        Ok(())
    }
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

        let entry = match self
            .runtime
            .block_on(self.vfs.lookup(&ctx, Ino::from(parent), name))
        {
            Ok(n) => n,
            Err(e) => {
                // TODO: handle this error
                reply.error(e.to_errno());
                return;
            }
        };
    }
}

fn update_length(entry: &mut Entry) {}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::common::err::Result;
//     use crate::fuse::config::FsConfig;
//
//     #[test]
//     fn test_unmount() {
//         // Fetch a list of supported file systems.
//         // When mounting, a file system will be selected from this.
//         let supported = sys_mount::SupportedFilesystems::new().unwrap();
//         println!("is supported {:?}", supported.is_supported("kiseki"));
//         for fuse in supported.nodev_file_systems() {
//             println!("Supported file systems: {:?}", fuse);
//         }
//
//         let path = PathBuf::from("/tmp/kiseki");
//         unmount(&path).unwrap();
//     }
//
//     #[test]
//     fn mount() -> Result<()> {
//         let path = PathBuf::from("/tmp/kiseki");
//         unmount(&path)?;
//         let kfs = FsConfig::default().mount_point(&path).open()?;
//         let session = kfs.mount()?;
//         session.join();
//         Ok(())
//     }
// }
