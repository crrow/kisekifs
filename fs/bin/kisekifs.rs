use clap::{Parser, Subcommand};
use fs::null;
use std::path::PathBuf;
use tracing::{error, info};

fn main() {
    let cli = Cli::parse();
    // install global collector configured based o  n RUST_LOG env var.
    tracing_subscriber::fmt::init();

    match &cli.command {
        Some(commands) => match commands {
            Commands::Mount { target } => {
                if let Err(e) = null::mount_check(target) {
                    error!("failed to mount null: {}", e);
                    return;
                } else {
                    info!("mounting on {:?}", target);
                }
            }
            Commands::Unmount { target } => {
                info!("mounting on {:?}", target);
            }
        },
        None => {
            info!("no command");
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Mount filesystem to target.
    Mount {
        /// Target directory to mount the filesystem to.
        #[arg(short, long, value_name = "FILE")]
        target: PathBuf,
    },
    /// Unmount the filesystem from the target.
    Unmount {
        /// Target directory to unmount from.
        #[arg(short, long, value_name = "FILE")]
        target: PathBuf,
    },
}
