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

use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use clap::{Args, Parser};
use fuser::MountOption;
use kiseki_utils::logger::{LoggingOptions, DEFAULT_LOG_DIR};
use snafu::{whatever, ResultExt, Whatever};
use tracing::info;

use crate::{
    build_info, fuse,
    fuse::{config::FuseConfig, null, KISEKI},
    meta::MetaConfig,
    vfs,
    vfs::config::VFSConfig,
};

const MOUNT_OPTIONS_HEADER: &str = "Mount options";
const LOGGING_OPTIONS_HEADER: &str = "Logging options";
const META_OPTIONS_HEADER: &str = "Meta options";

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Mount the target volume at the mount point.
Examples:

# Mount in foreground
kiseki mount -f /tmp/kiseki
")]
pub struct MountArgs {
    #[arg(
        help = "Directory to mount the fs at",
        value_name = "MOUNT_POINT",
        default_value = "/tmp/kiseki"
    )]
    pub mount_point: PathBuf,

    #[arg(
    long,
    help = "Mount file system in read-only mode",
    help_heading = MOUNT_OPTIONS_HEADER
    )]
    pub read_only: bool,

    #[arg(
    long,
    help = "Automatically unmount on exit",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "true",
    )]
    pub auto_unmount: bool,

    #[arg(long, help = "Allow root user to access file system", help_heading = MOUNT_OPTIONS_HEADER)]
    pub allow_root: bool,

    #[arg(
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
    default_value = "8",
    )]
    pub async_work_threads: usize,

    #[clap(
    long,
    help = "Write log files to a directory [default: logs written to syslog]",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "DIRECTORY",
    default_value = "/tmp/kiseki.log"
    )]
    pub log_directory: String,

    #[clap(
    short,
    long,
    help = "Log level",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "LEVEL",
    default_value = "info"
    )]
    pub level: Option<String>,

    #[clap(
    long,
    help = "Enable OTLP tracing",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "true"
    )]
    pub enable_otlp_tracing: bool,

    #[clap(
    long,
    help = "Specify the OTLP endpoint",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "URL",
    default_value = "localhost:4317",
    )]
    pub otlp_endpoint: Option<String>,

    #[clap(
    long,
    help = "Specify the tracing sample ratio",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "0.5",
    value_name = "RATIO",
    )]
    pub tracing_sample_ratio: Option<f64>,

    #[clap(
    long,
    help = "Append stdout to log files",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "true",
    )]
    pub append_stdout: bool,

    #[clap(
    long,
    help = "Disable all logging. You will still see stdout messages.",
    help_heading = LOGGING_OPTIONS_HEADER,
    conflicts_with_all(["log_directory", "level", "enable_otlp_tracing"])
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
pub struct MetaArgs {
    #[arg(
    long,
    help = "Specify the scheme of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value_t = opendal::Scheme::Sled.to_string(),
    )]
    pub scheme: String, // FIXME

    #[arg(
    long,
    help = "Specify the address of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value = "/tmp/kiseki-meta",
    )]
    pub meta_address: String,
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
        mc.scheme_config
            .insert("datadir".to_string(), self.meta_args.meta_address.clone());
        Ok(mc)
    }

    fn load_logging_opts(&self) -> LoggingOptions {
        let mut opts = LoggingOptions {
            dir: self.log_directory.clone(),
            level: self.level.clone(),
            enable_otlp_tracing: self.enable_otlp_tracing.clone(),
            otlp_endpoint: self.otlp_endpoint.clone(),
            tracing_sample_ratio: self.tracing_sample_ratio,
            append_stdout: self.append_stdout,
        };
        opts
    }

    fn vfs_config(&self) -> VFSConfig {
        VFSConfig::default()
    }

    pub fn run(self) -> Result<(), Whatever> {
        if self.foreground {
            let opts = self.load_logging_opts();
            let _guard =
                kiseki_utils::logger::init_global_logging_without_runtime("kiseki-fuse", &opts);
            let _sentry_guard = kiseki_utils::sentry_init::init_sentry();

            mount(self)?;
        }
        Ok(())
    }
}

pub fn log_versions() {
    // Report app version as gauge.
    // APP_VERSION
    //     .with_label_values(&[short_version(), full_version()])
    //     .inc();

    // Log version and argument flags.
    info!(
        "PKG_VERSION: {}, FULL_VERSION: {}",
        build_info::PKG_VERSION,
        build_info::FULL_VERSION,
    );

    log_env_flags();
}

fn log_env_flags() {
    info!("command line arguments");
    for argument in std::env::args() {
        info!("argument: {}", argument);
    }
}

fn mount(args: MountArgs) -> Result<(), Whatever> {
    info!("try to mount kiseki on {:?}", &args.mount_point);
    log_versions();

    validate_mount_point(&args.mount_point)?;

    let fuse_config = args.fuse_config();
    let meta_config = args.meta_config()?;
    let vfs_config = args.vfs_config();

    let meta = meta_config
        .open()
        .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;

    let file_system = vfs::KisekiVFS::new(vfs_config, meta)
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

        // This is a best-effort validation, so don't fail if we can't read
        // /proc/self/mountinfo for some reason.
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
