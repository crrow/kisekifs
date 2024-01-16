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

use clap::{arg, Parser, Subcommand, ValueEnum};
use fuser::MountOption;
use kisekifs::fuse::config::FuseConfig;
use kisekifs::fuse::{null, KISEKI};
use kisekifs::meta::config::MetaConfig;
use kisekifs::vfs::config::VFSConfig;
use kisekifs::{build_info, fuse, vfs};
use snafu::{whatever, ResultExt, Whatever};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::info;

const MOUNT_OPTIONS_HEADER: &str = "Mount options";
const LOGGING_OPTIONS_HEADER: &str = "Logging options";
const META_OPTIONS_HEADER: &str = "Meta options";

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

    #[clap(
    long,
    help = "Automatically unmount on exit",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "true",
    )]
    pub auto_unmount: bool,

    #[clap(long, help = "Allow root user to access file system", help_heading = MOUNT_OPTIONS_HEADER)]
    pub allow_root: bool,

    #[clap(
    long,
    help = "Allow other users, including root, to access file system",
    help_heading = MOUNT_OPTIONS_HEADER,
    conflicts_with = "allow_root",
    default_value = "true",
    )]
    pub allow_other: bool,

    #[arg(
    long,
    help = "Number of threads to use for tokio async runtime",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "4",
    )]
    pub async_work_threads: usize,

    #[clap(
    short,
    long,
    help = "Write log files to a directory [default: logs written to syslog]",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "DIRECTORY",
    )]
    pub log_directory: Option<PathBuf>,

    #[clap(
    short,
    long,
    help = "Enable debug logging for Mountpoint", 
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "bool",
    default_value = "true"
    )]
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

    #[clap(flatten)]
    pub meta_args: MetaArgs,
}

#[derive(Debug, Clone, Parser)]
struct MetaArgs {
    #[arg(
    long,
    help = "Specify the scheme of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value_t = opendal::Scheme::Sled.to_string(),
    )]
    pub scheme: String, // FIXME

    #[clap(
    long,
    help = "Specify the address of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value = "/tmp/kiseki-meta",
    )]
    pub data_dir: PathBuf,
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
            async_work_threads: self.async_work_threads,
        }
    }
    fn meta_config(&self) -> Result<MetaConfig, Whatever> {
        let mut mc = MetaConfig::default();
        mc.scheme = opendal::Scheme::from_str(&self.meta_args.scheme)
            .with_whatever_context(|_| format!("invalid scheme {}", &self.meta_args.scheme))?;
        mc.scheme_config.insert(
            "datadir".to_string(),
            self.meta_args.data_dir.to_string_lossy().to_string(),
        );
        Ok(mc)
    }
    fn vfs_config(&self) -> VFSConfig {
        VFSConfig::default()
    }

    fn run(self) -> Result<(), Whatever> {
        let successful_mount_msg =
            format!("{} is mounted at {}", KISEKI, self.mount_point.display());
        if self.foreground {
            mount(self)?;
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

fn mount(args: MountArgs) -> Result<(), Whatever> {
    info!("try to mount kiseki on {:?}", &args.mount_point);
    validate_mount_point(&args.mount_point)?;

    let fuse_config = args.fuse_config();
    let meta_config = args.meta_config()?;
    let fs_config = args.vfs_config();

    let meta = meta_config
        .open()
        .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;

    let file_system = vfs::KisekiVFS::create(fs_config, meta)
        .with_whatever_context(|e| format!("failed to create file system, {:?}", e))?;
    let fs = fuse::KisekiFuse::create(fuse_config.clone(), file_system)?;
    fuser::mount2(fs, &args.mount_point, &fuse_config.mount_options).with_whatever_context(
        |e| {
            format!(
                "failed to mount kiseki on {}; {}",
                args.mount_point.display(),
                e
            )
        },
    )?;
    Ok(())
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

    null::mount_check(path)?;

    Ok(())
}
