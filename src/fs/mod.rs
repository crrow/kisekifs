pub mod config;
pub mod null;

pub const KISEKI: &str = "kiseki";

use crate::common;
use crate::fs::config::FsConfig;
use crate::meta::config::MetaConfig;
use crate::meta::types::{Entry, Ino, InodeAttr, PreInternalNodes, CONTROL_INODE_NAME};
use crate::meta::Meta;
use fuser::{Filesystem, KernelConfig, ReplyEntry, Request, FUSE_ROOT_ID};
use libc::c_int;
use snafu::{ResultExt, Snafu, Whatever};
use std::ffi::{OsStr, OsString};
use std::fmt::{Display, Formatter};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, info};

#[derive(Debug, Snafu)]
pub enum FsError {
    #[snafu(display("failed to mount kiseki on {:?}, {:?}", mount_point, source))]
    ErrMountFailed {
        mount_point: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("failed to prepare mount point dir {:?}, {:?}", mount_point, source))]
    ErrPrepareMountPointDirFailed {
        mount_point: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("failed to unmount kiseki on {:?}, {:?}", mount_point, source))]
    ErrUnmountFailed {
        mount_point: PathBuf,
        source: std::io::Error,
    },
}

/// Errors that can be converted to a raw OS error (errno)
pub trait ToErrno {
    fn to_errno(&self) -> libc::c_int;
}

impl ToErrno for InodeError {
    fn to_errno(&self) -> c_int {
        match self {
            InodeError::InvalidFileName { .. } => libc::EINVAL,
        }
    }
}
#[derive(Debug, Snafu)]
pub enum InodeError {
    #[snafu(display("invalid file name {:?}", name))]
    InvalidFileName { name: OsString },
}

impl From<FsError> for common::err::Error {
    fn from(value: FsError) -> Self {
        Self::GenericError {
            component: "kiseki-fs",
            source: Box::new(value),
        }
    }
}

#[derive(Debug)]
pub struct KisekiFS {
    config: FsConfig,
    meta: Meta,
    internal_nodes: PreInternalNodes,
}

impl Display for KisekiFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiFS {
    pub fn create(fs_config: FsConfig, meta_config: MetaConfig) -> Result<Self, Whatever> {
        let meta = meta_config
            .open()
            .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;

        Ok(Self {
            config: fs_config,
            meta,
            internal_nodes: PreInternalNodes::default(),
        })
    }
    fn reply_entry(&self, ctx: &mut FuseContext, reply: ReplyEntry, entry: &Entry) {
        let ttl = if entry.is_filetype(fuser::FileType::Directory) {
            &self.config.dir_entry_timeout
        } else {
            &self.config.entry_timeout
        };

        if entry.is_special_inode() {
        } else if entry.is_filetype(fuser::FileType::RegularFile)
            && self.modified_since(entry.inode, ctx.start_at)
        {
            debug!("refresh attr for {:?}", entry.inode);
            // TODO: introduce another type to avoid messing up with fuse's methods.
            // self.getattr()
        }

        reply.entry(&ttl, &entry.attr.borrow().inner, 1)
    }
    fn modified_since(&self, ino: Ino, since: SystemTime) -> bool {
        todo!()
    }
}

struct FuseContext {
    start_at: SystemTime,
}

impl FuseContext {
    fn new() -> Self {
        Self {
            start_at: SystemTime::now(),
        }
    }
}

impl Filesystem for KisekiFS {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        Ok(())
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let mut ctx = FuseContext::new();
        let name = match name.to_str().ok_or_else(|| InodeError::InvalidFileName {
            name: name.to_owned(),
        }) {
            Ok(n) => n,
            Err(e) => {
                reply.error(e.to_errno());
                return;
            }
        };

        if parent == FUSE_ROOT_ID || name.eq(CONTROL_INODE_NAME) {
            if let Some(n) = self.internal_nodes.get_internal_node_by_name(name) {
                self.reply_entry(&mut ctx, reply, n);
                return;
            }
        }
    }
}

fn update_length(entry: &mut Entry) {}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::common::err::Result;
//     use crate::fs::config::FsConfig;
//
//     #[test]
//     fn test_unmount() {
//         // Fetch a list of supported file systems.
//         // When mounting, a file system will be selected from this.
//         let supported = sys_mount::SupportedFilesystems::new().unwrap();
//         println!("is supported {:?}", supported.is_supported("kiseki"));
//         for fs in supported.nodev_file_systems() {
//             println!("Supported file systems: {:?}", fs);
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
