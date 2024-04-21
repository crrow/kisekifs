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

use std::str::FromStr;

use clap::Args;
use kiseki_meta::MetaConfig;
use kiseki_types::setting::Format;
use kiseki_utils::{
    align::{align4k, align_to_block},
    readable_size::ReadableSize,
};
use regex::Regex;
use snafu::{ensure, ensure_whatever, OptionExt, ResultExt, Whatever};
use tokio::runtime;
use tracing::{debug, info, level_filters::LevelFilter, warn, Instrument};

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
    default_value = kiseki_common::KISEKI_DEBUG_META_ADDR,
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
    pub block_size: ReadableSize,

    #[arg(
    long,
    short,
    help = "hard quota of the volume limiting its usage of space in GiB",
    help_heading = MANAGEMENT_OPTIONS_HEADER,
    value_parser = validate_capacity,
    )]
    pub capacity: Option<ReadableSize>,

    #[arg(
    long,
    short,
    help = "hard quota of the volume limiting its number of inodes",
    help_heading = MANAGEMENT_OPTIONS_HEADER,
    )]
    pub inodes: Option<usize>,
}

impl FormatArgs {
    fn generate_format(&self) -> Format {
        let mut format = Format::default();
        format.max_capacity = self.capacity.map(|s| s.as_bytes_usize());
        if let Some(inodes) = self.inodes {
            format.max_inodes = Some(inodes);
        }
        format.block_size = self.block_size.as_bytes_usize();
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
        kiseki_meta::update_format(&dsn, format, true).unwrap();
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

fn validate_block_size(s: &str) -> Result<ReadableSize, String> {
    let n = ReadableSize::from_str(s).map_err(|e| format!("invalid block size: {}", e))?;
    let n = n.as_bytes() as usize;
    let n = align_to_block(n);
    if n < kiseki_common::MIN_BLOCK_SIZE {
        return Err(format!("block size {} too small", n));
    }
    if n > kiseki_common::MAX_BLOCK_SIZE {
        return Err(format!("block size {} too large", n));
    }
    Ok(ReadableSize(n as u64))
}

fn validate_capacity(s: &str) -> Result<ReadableSize, String> {
    let n = ReadableSize::from_str(s).map_err(|e| format!("invalid capacity: {}", e))?;
    let n = n.as_bytes() as usize;

    Ok(ReadableSize(align4k(n as u64) as u64))
}
