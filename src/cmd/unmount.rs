use std::path::PathBuf;

use clap::Parser;
use snafu::{ResultExt, Whatever};

#[derive(Debug, Parser, Clone)]
pub struct UmountArgs {
    #[clap(
        help = "Directory to umount the fs at",
        value_name = "DIRECTORY",
        default_value = "/tmp/kiseki"
    )]
    pub mount_point: PathBuf,
    #[clap(long, short, help = "Force unmount even if the directory is not empty")]
    pub force: bool,
}

impl UmountArgs {
    pub fn run(&self) -> Result<(), Whatever> {
        let path = &self.mount_point;
        sys_mount::unmount(path, sys_mount::UnmountFlags::empty())
            .with_whatever_context(|_| format!("could not umount { }", path.display()))?;
        Ok(())
    }
}
