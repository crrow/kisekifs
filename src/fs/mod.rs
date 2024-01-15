pub mod config;
pub mod null;

pub const KISEKI: &str = "kiseki";

use crate::meta::Meta;
use fuser::{Filesystem, KernelConfig, ReplyEntry, Request};
use libc::c_int;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use tracing::debug;

#[derive(Debug)]
pub struct KisekiFS {
    meta: Meta,
}

impl Display for KisekiFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiFS {
    pub(crate) fn new(meta: Meta) -> Self {
        Self { meta }
    }
}

impl Filesystem for KisekiFS {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        Ok(())
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::config::Config;

    #[test]
    fn new_fs() {
        let mut config = Config::default();
        let kfs = config.open().unwrap();
        println!("{ }", kfs)
    }
}
