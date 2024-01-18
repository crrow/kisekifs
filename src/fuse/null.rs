use std::path::Path;

use fuser::{spawn_mount2, Filesystem, MountOption};
use snafu::{ResultExt, Whatever};

use super::KISEKI;

/// An empty FUSE file system. It can be used in a mounting test aimed to
/// determine whether or not the real file system can be mounted as well. If the
/// test fails, the application can fail early instead of wasting time
/// constructing the real file system.
struct NullFs {}

impl Filesystem for NullFs {}

pub fn mount_check<P: AsRef<Path>>(mountpoint: P) -> Result<(), Whatever> {
    let mountpoint = mountpoint.as_ref();
    let options = [
        MountOption::FSName(String::from(KISEKI)),
        MountOption::AllowRoot,
    ];
    let session = spawn_mount2(NullFs {}, mountpoint, &options).with_whatever_context(|e| {
        format!("failed to mount null fs on {}; {}", mountpoint.display(), e)
    })?;
    drop(session);

    Ok(())
}
