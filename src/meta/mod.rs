pub mod config;
pub mod err;
pub mod kv_meta;

use crate::common::Ino;
use config::{Config, Format};
use err::Result;

/// Trait `Meta` describes a meta service for file system.
pub trait Meta {
    fn scheme() -> opendal::Scheme;
    /// Name of database
    fn name(&self) -> String;
    // Reset cleans up all metadata, VERY DANGEROUS!
    fn reset(&mut self) -> Result<()>;
    /// Init is used to initialize a meta service.
    fn init(&mut self, format: Format, force: bool) -> Result<()>;
    /// Load loads the existing setting of a formatted volume from meta service.
    fn load(&mut self, check_version: bool) -> Result<Format>;
}

pub(crate) struct Inner {
    config: Config,
    format: Format,
    root: Ino,
}
