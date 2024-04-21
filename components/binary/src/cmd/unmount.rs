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
    pub force:       bool,
}

impl UmountArgs {
    pub fn run(&self) -> Result<(), Whatever> {
        let path = &self.mount_point;
        rustix::mount::unmount(path, rustix::mount::UnmountFlags::FORCE)
            .with_whatever_context(|_| format!("could not umount { }", path.display()))?;
        Ok(())
    }
}
