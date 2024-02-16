use std::path::PathBuf;

use clap::Args;
use snafu::{ResultExt, Whatever};

#[derive(Debug, Clone, Args)]
#[command(long_about = r"

Unmount kiseki-fs from the specified directory.
")]
pub struct UmountArgs {
    #[arg(
        help = "Directory to umount the fs at",
        value_name = "MOUNT_POINT",
        default_value = "/tmp/kiseki"
    )]
    pub mount_point: PathBuf,
    #[arg(long, short, help = "Force unmount even if the directory is not empty")]
    pub force: bool,
}

impl UmountArgs {
    pub fn run(&self) -> Result<(), Whatever> {
        let path = &self.mount_point;
        rustix::mount::unmount(path, rustix::mount::UnmountFlags::FORCE)
            .with_whatever_context(|_| format!("could not umount { }", path.display()))?;
        Ok(())
    }
}
