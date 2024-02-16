mod build_info;
mod cmd;

use crate::cmd::format::FormatArgs;
use crate::cmd::mount::MountArgs;
use crate::cmd::unmount::UmountArgs;
use clap::Parser;
use clap::Subcommand;
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
    Mount(MountArgs),
    Umount(UmountArgs),
    Format(FormatArgs),
}

fn main() -> Result<(), Whatever> {
    let cli = Cli::parse();
    match cli.commands {
        Commands::Mount(mount_args) => mount_args.run(),
        Commands::Umount(umount_args) => umount_args.run(),
        Commands::Format(format_args) => format_args.run(),
    }
}
