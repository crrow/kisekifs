use std::path::{Path, PathBuf};

use super::KISEKI;
use crate::common;
use crate::common::err::Result;
use fuser::{spawn_mount2, Filesystem, MountOption};
use snafu::{ResultExt, Snafu};

#[derive(Snafu, Debug)]
pub enum NullFsError {
    #[snafu(display("failed to mount null fs on {:?}, {:?}", path, source))]
    ErrTryMountFailed {
        source: std::io::Error,
        path: PathBuf,
    },
}

impl From<NullFsError> for common::err::Error {
    fn from(value: NullFsError) -> Self {
        Self::GenericError {
            component: "null-fs",
            source: Box::new(value),
        }
    }
}

/// An empty FUSE file system. It can be used in a mounting test aimed to determine whether or
/// not the real file system can be mounted as well. If the test fails, the application can fail
/// early instead of wasting time constructing the real file system.
struct NullFs {}

impl Filesystem for NullFs {}

pub fn mount_check<P: AsRef<Path>>(mountpoint: P) -> Result<()> {
    let mountpoint = mountpoint.as_ref();
    let options = [
        MountOption::FSName(String::from(KISEKI)),
        MountOption::AllowRoot,
    ];
    let session =
        spawn_mount2(NullFs {}, mountpoint, &options).context(ErrTryMountFailedSnafu {
            path: mountpoint.to_path_buf(),
        })?;
    drop(session);

    Ok(())
}
