use std::{fmt::Display, str::FromStr};

use clap::Args;
use regex::Regex;
use snafu::{ResultExt, Whatever};
use tokio::runtime;
use tracing::{debug, info, level_filters::LevelFilter, warn, Instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::{
    meta,
    meta::{Compression, MetaConfig},
    metrics::metrics_tracing_span_layer,
};

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
        default_value = "/tmp/kiseki-meta",
    )]
    pub meta_address: Option<String>,

    #[arg(
        long,
        help = "Specify the scheme of the meta store",
        help_heading = FORMAT_OPTIONS_HEADER,
        default_value_t = opendal::Scheme::Sled.to_string(),
    )]
    pub scheme: String, // FIXME

    #[arg(long, short, help = "overwrite existing format", help_heading = FORMAT_OPTIONS_HEADER)]
    pub force: bool,

    #[arg(long, help = "compression algorithm", help_heading = FORMAT_OPTIONS_HEADER)]
    pub compression: Option<Compression>,

    #[arg(
        long,
        help = "size of block in KiB",
        help_heading = FORMAT_OPTIONS_HEADER,
        default_value = "4096",
        value_parser = validate_block_size,
    )]
    pub block_size: u64,

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
    pub trash_days: u64,
}
impl FormatArgs {
    fn meta_config(&self) -> Result<MetaConfig, Whatever> {
        let mut mc = MetaConfig::default();
        mc.scheme = opendal::Scheme::from_str(&self.scheme)
            .with_whatever_context(|_| format!("invalid scheme {}", &self.scheme))?;
        if let Some(meta_address) = &self.meta_address {
            mc.scheme_config
                .insert("datadir".to_string(), meta_address.clone());
        };
        Ok(mc)
    }
    fn generate_format(&self) -> meta::Format {
        let mut format = meta::Format::default();
        if let Some(cap) = self.capacity {
            format.capacity_in_bytes = (cap << 30) as u64;
        }
        if let Some(inodes) = self.inodes {
            format.inodes = inodes as u64;
        }
        format.trash_days = self.trash_days as u64;
        format.block_size = fix_block_size(self.block_size);
        format.compression = self.compression.clone();
        format.name = self.name.clone();
        format
    }
    pub fn run(&self) -> Result<(), Whatever> {
        self.std_log();

        let mc = self.meta_config()?;
        let meta = mc
            .open()
            .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;
        debug!("meta created");

        let format = self.generate_format();

        let runtime = runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kiseki-format-async-runtime")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .with_whatever_context(|e| format!("unable to built tokio runtime {e} "))?;

        match runtime.block_on(meta.init(format, self.force).in_current_span()) {
            Ok(_) => {
                info!("format successfully")
            }
            Err(e) => {
                warn!("failed to format meta, {:?}", e);
                return Err(e).with_whatever_context(|e| format!("failed to format meta, {:?}", e));
            }
        }

        Ok(())
    }

    fn std_log(&self) {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_ansi(supports_color::on(supports_color::Stream::Stdout).is_some())
            .with_filter(
                tracing_subscriber::filter::Targets::new().with_target("kiseki", LevelFilter::INFO),
            )
            .with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("kiseki", LevelFilter::DEBUG),
            );

        let registry = tracing_subscriber::registry()
            // .with(syslog_layer)
            .with(fmt_layer)
            // .with(file_layer)
            .with(metrics_tracing_span_layer());
        registry.init();
        let _metrics = crate::metrics::install();
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

fn validate_trash_day(s: &str) -> Result<u64, String> {
    clap_num::number_range(s, 1, u64::MAX)
}
fn validate_block_size(s: &str) -> Result<u64, String> {
    clap_num::number_range(s, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE)
}

const MIN_BLOCK_SIZE: u64 = 64;
const MAX_BLOCK_SIZE: u64 = 16 << 10; // 16 KiB
fn fix_block_size(s: u64) -> u64 {
    let mut s = s;
    let mut bits = 0;
    while s > 1 {
        bits += 1;
        s >>= 1;
    }

    let adjusted_size = s << bits;

    if adjusted_size < MIN_BLOCK_SIZE {
        warn!(
            "Block size is too small: {}, using {} instead",
            adjusted_size, MIN_BLOCK_SIZE
        );
        MIN_BLOCK_SIZE
    } else if adjusted_size > MAX_BLOCK_SIZE {
        warn!(
            "Block size is too large: {}, using {} instead",
            adjusted_size, MAX_BLOCK_SIZE
        );
        MAX_BLOCK_SIZE
    } else {
        adjusted_size
    }
}
