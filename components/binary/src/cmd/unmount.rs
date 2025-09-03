// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{ffi::CString, path::PathBuf, process::Command};

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
    pub force:       bool,
}

impl UmountArgs {
    pub fn run(&self) -> Result<(), Whatever> {
        let path = &self.mount_point;
        self.fuser_unmount(path)
            .with_whatever_context(|_| format!("could not umount {}", path.display()))?;
        Ok(())
    }

    /// Unmount using fuser-compatible approach
    /// This follows the same logic as fuser's internal unmount implementation
    fn fuser_unmount(&self, mountpoint: &PathBuf) -> std::io::Result<()> {
        // Convert path to CString for libc calls
        let mountpoint_cstr =
            CString::new(mountpoint.canonicalize()?.as_os_str().as_encoded_bytes())?;

        // If direct unmount failed with permission error, try fusermount as fallback
        self.fusermount_unmount(&mountpoint_cstr)
    }

    /// Fallback unmount using fusermount command (same as fuser's approach)
    fn fusermount_unmount(&self, mountpoint: &CString) -> std::io::Result<()> {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

        let mountpoint_osstr = OsStr::from_bytes(mountpoint.as_bytes());

        // Try fusermount3 first (FUSE 3.x), then fusermount (FUSE 2.x)
        let fusermount_commands = ["fusermount3", "fusermount"];

        for cmd in &fusermount_commands {
            let mut command = Command::new(cmd);
            command.arg("-u").arg(mountpoint_osstr);

            if self.force {
                command.arg("-z"); // lazy unmount option
            }

            match command.status() {
                Ok(status) if status.success() => return Ok(()),
                Ok(_) => continue, // Try next fusermount command
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to unmount: fusermount command not found or failed",
        ))
    }
}
