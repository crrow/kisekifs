use crate::meta::MetaConfig;
use clap::Args;
use regex::Regex;
use snafu::{ensure, ensure_whatever, ResultExt, Whatever};
use std::str::FromStr;
use tracing::debug;

const FORMAT_OPTIONS_HEADER: &str = "Format options";

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
    default_value_t = opendal::Scheme::Memory.to_string(),
    )]
    pub scheme: String, // FIXME
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
    pub fn run(&self) -> Result<(), Whatever> {
        let mc = self.meta_config()?;
        let meta = mc
            .open()
            .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;
        debug!("meta created");
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
