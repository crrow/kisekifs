pub mod config;

use crate::meta::config::MetaConfig;
use crate::meta::types::PreInternalNodes;
use crate::meta::Meta;
use config::VFSConfig;
use snafu::{ResultExt, Whatever};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct KisekiVFS {
    config: VFSConfig,
    meta: Meta,
    internal_nodes: PreInternalNodes,
}

impl Display for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiVFS {
    pub fn create(vfs_config: VFSConfig, meta_config: MetaConfig) -> Result<Self, Whatever> {
        let meta = meta_config
            .open()
            .with_whatever_context(|e| format!("failed to create meta, {:?}", e))?;

        Ok(Self {
            config: vfs_config,
            meta,
            internal_nodes: PreInternalNodes::default(),
        })
    }
}
