use crate::common::err::Result;
use crate::fs::KisekiFS;
use crate::meta;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Config {
    meta_config: meta::config::Config,
}

impl Config {
    pub fn open(mut self) -> Result<KisekiFS> {
        let meta = self.meta_config.open()?;
        Ok(KisekiFS::new(meta))
    }
}
