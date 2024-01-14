use std::path::{Path, PathBuf};

use super::KISEKI;
use fuser::{spawn_mount2, Filesystem, MountOption};
use snafu::{ResultExt, Snafu};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to mount null fs on {:?}", path))]
    ErrTryMountFailed {
        source: std::io::Error,
        path: PathBuf,
    },
}

pub type Result = std::result::Result<(), Error>;

/// An empty FUSE file system. It can be used in a mounting test aimed to determine whether or
/// not the real file system can be mounted as well. If the test fails, the application can fail
/// early instead of wasting time constructing the real file system.
struct NullFs {}
impl Filesystem for NullFs {}

pub fn mount_check<P: AsRef<Path>>(mountpoint: P) -> Result {
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
