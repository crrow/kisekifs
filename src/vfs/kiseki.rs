use crate::common;
use crate::common::err::ToErrno;
use crate::meta::types::*;
use crate::meta::{MetaContext, MetaEngine};
use crate::vfs::config::VFSConfig;
use common::err::Result;
use libc::c_int;
use snafu::prelude::*;
use std::fmt::{Display, Formatter};

#[derive(Debug, Snafu)]
pub enum VFSError {}

impl From<VFSError> for common::err::Error {
    fn from(value: VFSError) -> Self {
        common::err::Error::VFSError { source: value }
    }
}

impl ToErrno for VFSError {
    fn to_errno(&self) -> c_int {
        todo!()
    }
}

#[derive(Debug)]
pub struct KisekiVFS {
    config: VFSConfig,
    meta: MetaEngine,
    internal_nodes: PreInternalNodes,
}

impl Display for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiVFS {
    pub fn create(vfs_config: VFSConfig, meta: MetaEngine) -> Result<Self> {
        Ok(Self {
            config: vfs_config,
            meta,
            internal_nodes: PreInternalNodes::default(),
        })
    }

    pub async fn lookup(&self, ctx: &MetaContext, parent: Ino, name: &str) -> Result<Entry> {
        // TODO: handle the special case
        if parent == ROOT_INO || name.eq(CONTROL_INODE_NAME) {
            if let Some(n) = self.internal_nodes.get_internal_node_by_name(name) {
                return Ok(n.into());
            }
        }
        if parent.is_special() && name == "." {
            if let Some(n) = self.internal_nodes.get_internal_node(parent) {
                return Ok(n.into());
            }
        }
        let (inode, attr) = self.meta.lookup(ctx, parent, name, true).await?;
        Ok(Entry {
            inode,
            name: name.to_string(),
            attr: Some(attr),
        })
    }
}
