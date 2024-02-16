use std::str::FromStr;

use clap::Args;
use kiseki_types::setting::Format;
use kiseki_utils::{align::align_to_block, readable_size::ReadableSize};
use regex::Regex;
use snafu::{ensure, ensure_whatever, OptionExt, ResultExt, Whatever};
use tokio::runtime;
use tracing::{debug, info, level_filters::LevelFilter, warn, Instrument};

use kiseki_meta::MetaConfig;
use kiseki_utils::align::align4k;

const FORMAT_OPTIONS_HEADER: &str = "DATA FORMAT";
const MANAGEMENT_OPTIONS_HEADER: &str = "MANAGEMENT";

#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
#[command(flatten_help = true)]
#[command(long_about = r"

Create a new KisekiFS volume. Here META_ADDRESS is used to set up the
metadata engine location, the SCHEME is used to specific the meta sto
engine, and NAME is the prefix of all objects in data storage.
")]
pub struct FormatArgs {
    #[arg(
        value_name = "FILE_SYSTEM_NAME",
        help = r"Your file system name, like 'demo-fs'.",
        value_parser = validate_name,
        default_value = "hello-fs",
    )]
    pub name: String,

    #[arg(
        long,
        help = "Specify the address of the meta store",
        help_heading = FORMAT_OPTIONS_HEADER,
        default_value = "rocksdb://:/tmp/kiseki.meta",
    )]
    pub meta_dsn: Option<String>,

    #[arg(long, short, help = "overwrite existing format", help_heading = FORMAT_OPTIONS_HEADER)]
    pub force: bool,

    // #[arg(long, help = "compression algorithm", help_heading = FORMAT_OPTIONS_HEADER)]
    // pub compression: Option<Compression>,
    #[arg(
        long,
        help = "size of block in KiB",
        help_heading = FORMAT_OPTIONS_HEADER,
        default_value = "4M",
        value_parser = validate_block_size,
    )]
    pub block_size: String,

    #[arg(
        long,
        short,
        help = "hard quota of the volume limiting its usage of space in GiB",
        help_heading = MANAGEMENT_OPTIONS_HEADER,
    )]
    pub capacity: Option<usize>,

    #[arg(
        long,
        short,
        help = "hard quota of the volume limiting its number of inodes",
        help_heading = MANAGEMENT_OPTIONS_HEADER,
    )]
    pub inodes: Option<usize>,

    #[arg(
        long,
        short,
        help = "number of days after which removed files will be permanently deleted",
        help_heading = MANAGEMENT_OPTIONS_HEADER,
        default_value = "1",
        value_parser = validate_trash_day,
    )]
    pub trash_days: usize,
}
impl FormatArgs {
    fn generate_format(&self) -> Format {
        let mut format = Format::default();
        if let Some(cap) = self.capacity {
            format.max_capacity = Some(cap << 30);
        }
        if let Some(inodes) = self.inodes {
            format.max_inodes = Some(inodes);
        }
        format.trash_days = self.trash_days;
        let block_size = ReadableSize::from_str(&self.block_size)
            .expect("block size should be validated in the argument parser");
        format.block_size = block_size.as_bytes() as usize;
        format.name = self.name.clone();
        format
    }
    pub fn run(&self) -> Result<(), Whatever> {
        kiseki_utils::logger::install_fmt_log();
        let dsn = self
            .meta_dsn
            .clone()
            .expect("meta_dsn should be validated in the argument parser");
        let format = self.generate_format();
        kiseki_meta::update_format(dsn, format, true).unwrap();
        info!("format file system {:?} success", self.name);
        Ok(())
    }
}

const NAME_REGEX: &str = r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$";

// Validation function for file system names
fn validate_name(name: &str) -> Result<String, String> {
    if name.len() <= 3 {
        return Err(format!("File system name {:?} is too short", name));
    }
    if name.len() >= 30 {
        return Err(format!("File system name {:?} is too long", name));
    }
    let reg = Regex::new(NAME_REGEX).unwrap();
    if !reg.is_match(name) {
        return Err(format!("File system name {:?} is invalid", name));
    }

    Ok(name.to_string())
}

fn validate_trash_day(s: &str) -> Result<usize, String> {
    clap_num::number_range(s, 1, u64::MAX as usize)
}

fn validate_block_size(s: &str) -> Result<String, String> {
    let n = ReadableSize::from_str(s).map_err(|e| format!("invalid block size: {}", e))?;
    let n = n.as_bytes() as usize;
    if n < kiseki_common::MIN_BLOCK_SIZE {
        return Err(format!("block size {} too small", n));
    }
    if n > kiseki_common::MAX_BLOCK_SIZE {
        return Err(format!("block size {} too large", n));
    }

    Ok(ReadableSize(align_to_block(n) as u64).to_string())
}
