use std::path::PathBuf;

use clap::Parser;

pub mod mount;
pub mod unmount;

const META_OPTIONS_HEADER: &str = "Meta options";

#[derive(Debug, Clone, Parser)]
pub struct MetaArgs {
    #[arg(
    long,
    help = "Specify the scheme of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value_t = opendal::Scheme::Memory.to_string(),
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
