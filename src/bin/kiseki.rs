// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;

use clap::{Parser, Subcommand};
use kisekifs::{
    build_info,
    cmd::{mount::MountArgs, unmount::UmountArgs, *},
};
use snafu::Whatever;

#[derive(Debug, Parser)]
#[clap(
name = "kiseki",
about= "kiseki-fs client",
author = build_info::AUTHOR,
version = build_info::FULL_VERSION)]
struct Cli {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Mount kiseki-fs to the specified directory.
    Mount {
        #[clap(flatten)]
        mount_args: MountArgs,
    },
    /// Unmount kiseki-fs from the specified directory.
    Umount {
        #[clap(flatten)]
        umount_args: UmountArgs,
    },
}

// TODO: handle logging
fn main() -> Result<(), Whatever> {
    let cli = Cli::parse();
    return match cli.commands {
        Commands::Mount { mount_args } => mount_args.run(),
        Commands::Umount { umount_args } => umount_args.run(),
    };
}
