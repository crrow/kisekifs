pub mod config;
pub mod null;

pub const KISEKI: &str = "kiseki";

use crate::common;
use crate::fs::config::FsConfig;
use crate::meta::config::MetaConfig;
use crate::meta::Meta;
use fuser::{
    mount2, spawn_mount2, BackgroundSession, Filesystem, KernelConfig, MountOption, ReplyEntry,
    Request,
};
use libc::c_int;
use snafu::{ResultExt, Snafu, Whatever};
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
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
}

impl Display for KisekiFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiFS {
    pub(crate) fn new(fs_config: FsConfig, meta_config: MetaConfig) -> Result<Self, Whatever> {
        let meta = meta_config
            .new_meta()
            .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;

        Ok(Self {
            config: fs_config,
            meta,
        })
    }

    // pub fn mount(mut self) -> common::err::Result<BackgroundSession> {
    //     let mountpoint = self.get_mount_point();
    //     std::fs::create_dir_all(&mountpoint).context(ErrPrepareMountPointDirFailedSnafu {
    //         mount_point: mountpoint.clone(),
    //     })?;
    //     let options = [
    //         MountOption::FSName(String::from(KISEKI)),
    //         MountOption::AutoUnmount,
    //     ];
    //
    //     info!("try to mounted to {:?}", &mountpoint);
    //     let session = spawn_mount2(self, &mountpoint, &options).context(ErrMountFailedSnafu {
    //         mount_point: mountpoint,
    //     })?;
    //     Ok(session)
    // }
    //
    // pub fn block_mount(mut self) -> common::err::Result<()> {
    //     let mountpoint = self.get_mount_point();
    //     std::fs::create_dir_all(&mountpoint).context(ErrPrepareMountPointDirFailedSnafu {
    //         mount_point: mountpoint.clone(),
    //     })?;
    //     let options = [
    //         MountOption::FSName(String::from(KISEKI)),
    //         MountOption::AllowRoot,
    //     ];
    //
    //     info!("Mounted to {:?}", &mountpoint);
    //     mount2(self, &mountpoint, &options).context(ErrMountFailedSnafu {
    //         mount_point: mountpoint,
    //     })?;
    //     Ok(())
    // }
}

impl Filesystem for KisekiFS {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        Ok(())
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {}
}

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
