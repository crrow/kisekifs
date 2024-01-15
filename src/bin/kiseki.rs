/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use clap::{Parser, Subcommand};
use fuser::{BackgroundSession, MountOption};
use kisekifs::fs::config::{FsConfig, FuseConfig};
use kisekifs::fs::KISEKI;
use kisekifs::meta::config::MetaConfig;
use kisekifs::{build_info, fs};
use snafu::{whatever, ResultExt, Whatever};
use std::path::{Path, PathBuf};
use tracing::{error, info};
const MOUNT_OPTIONS_HEADER: &str = "Mount options";
const LOGGING_OPTIONS_HEADER: &str = "Logging options";

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

#[derive(Debug, Parser, Clone)]
struct UmountArgs {
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
    fn run(&self) -> Result<(), Whatever> {
        let path = &self.mount_point;
        sys_mount::unmount(path, sys_mount::UnmountFlags::empty())
            .with_whatever_context(|_| format!("could not umount { }", path.display()))?;
        Ok(())
    }
}

#[derive(Debug, Parser, Clone)]
#[clap(name = "kiseki", about= "mount client for kiseki-fs",author = build_info::AUTHOR, version = build_info::FULL_VERSION)]
// #[command(name, author, version, about, long_about = None)]
struct MountArgs {
    #[clap(
        help = "Directory to mount the fs at",
        value_name = "DIRECTORY",
        default_value = "/tmp/kiseki"
    )]
    pub mount_point: PathBuf,

    #[clap(
    long,
    help = "Mount file system in read-only mode",
    help_heading = MOUNT_OPTIONS_HEADER
    )]
    pub read_only: bool,

    #[clap(long, help = "Automatically unmount on exit", help_heading = MOUNT_OPTIONS_HEADER)]
    pub auto_unmount: bool,

    #[clap(long, help = "Allow root user to access file system", help_heading = MOUNT_OPTIONS_HEADER)]
    pub allow_root: bool,

    #[clap(
    long,
    help = "Allow other users, including root, to access file system",
    help_heading = MOUNT_OPTIONS_HEADER,
    conflicts_with = "allow_root"
    )]
    pub allow_other: bool,

    #[clap(
    short,
    long,
    help = "Write log files to a directory [default: logs written to syslog]",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "DIRECTORY",
    )]
    pub log_directory: Option<PathBuf>,

    #[clap(short, long, help = "Enable debug logging for Mountpoint", help_heading = LOGGING_OPTIONS_HEADER)]
    pub debug: bool,

    #[clap(
    long,
    help = "Disable all logging. You will still see stdout messages.",
    help_heading = LOGGING_OPTIONS_HEADER,
    conflicts_with_all(["log_directory", "debug"])
    )]
    pub no_log: bool,

    #[clap(
        short,
        long,
        help = "Run as foreground process",
        default_value = "true"
    )]
    pub foreground: bool,
}

impl MountArgs {
    fn fuse_config(&self) -> FuseConfig {
        let mut options = vec![
            MountOption::DefaultPermissions,
            MountOption::FSName(KISEKI.to_string()),
            MountOption::NoAtime,
        ];
        if self.read_only {
            options.push(MountOption::RO);
        }
        if self.auto_unmount {
            options.push(MountOption::AutoUnmount);
        }
        if self.allow_root {
            options.push(MountOption::AllowRoot);
        }
        if self.allow_other {
            options.push(MountOption::AllowOther);
        }
        FuseConfig {
            mount_point: self.mount_point.clone(),
            mount_options: options,
        }
    }
    fn meta_config(&self) -> MetaConfig {
        todo!()
    }
    fn fs_config(&self) -> FsConfig {
        todo!()
    }

    fn run(self) -> Result<(), Whatever> {
        let successful_mount_msg =
            format!("{} is mounted at {}", KISEKI, self.mount_point.display());
        if self.foreground {
            let session = mount(self)?;
            println!("{successful_mount_msg}");
            session.join();
        }
        return Ok(());
    }
}

// TODO: handle logging
fn main() -> Result<(), Whatever> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    return match cli.commands {
        Commands::Mount { mount_args } => mount_args.run(),
        Commands::Umount { umount_args } => umount_args.run(),
    };
}

fn mount(args: MountArgs) -> Result<BackgroundSession, Whatever> {
    info!("try to mount kiseki on {:?}", &args.mount_point);
    validate_mount_point(&args.mount_point)?;

    let fuse_config = args.fuse_config();
    let meta_config = args.meta_config();
    let fs_config = args.fs_config();

    todo!()
}

fn validate_mount_point(path: impl AsRef<Path>) -> Result<(), Whatever> {
    let mount_point = path.as_ref();
    if !mount_point.exists() {
        whatever!("mount point {} does not exist", mount_point.display());
    }

    if !mount_point.is_dir() {
        whatever!("mount point {} is not a directory", mount_point.display());
    }

    #[cfg(target_os = "linux")]
    {
        use procfs::process::Process;

        // This is a best-effort validation, so don't fail if we can't read /proc/self/mountinfo for
        // some reason.
        let mounts = match Process::myself().and_then(|me| me.mountinfo()) {
            Ok(mounts) => mounts,
            Err(e) => {
                tracing::debug!(
                    "failed to read mountinfo, not checking for existing mounts: {e:?}"
                );
                return Ok(());
            }
        };

        if mounts
            .into_iter()
            .any(|mount| mount.mount_point == path.as_ref())
        {
            whatever!("mount point {} is already mounted", path.as_ref().display());
        }
    }

    Ok(())
}
