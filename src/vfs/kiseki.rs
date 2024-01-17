use crate::common;
use crate::common::err::ToErrno;
use crate::meta::types::*;
use crate::meta::{MetaContext, MetaEngine};
use crate::vfs::config::VFSConfig;
use crate::vfs::reader::DataReader;
use crate::vfs::writer::DataWriter;
use common::err::Result;
use dashmap::DashMap;
use libc::c_int;
use snafu::prelude::*;
use std::fmt::{Display, Formatter};
use std::time;
use tracing::trace;

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
    writer: DataWriter,
    reader: DataReader,
    modified_at: DashMap<Ino, time::Instant>,
}

impl Display for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta.config.scheme)
    }
}

impl KisekiVFS {
    pub fn create(vfs_config: VFSConfig, meta: MetaEngine) -> Result<Self> {
        Ok(Self {
            internal_nodes: PreInternalNodes::new((
                vfs_config.entry_timeout,
                vfs_config.dir_entry_timeout,
            )),
            config: vfs_config,
            meta,
            writer: DataWriter::default(),
            reader: DataReader::default(),
            modified_at: DashMap::new(),
        })
    }

    pub async fn lookup(&self, ctx: &MetaContext, parent: Ino, name: &str) -> Result<Entry> {
        trace!("fs:lookup with parent {:?} name {:?}", parent, name);
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
            ttl: self.get_entry_ttl(&attr),
            attr,
            generation: 1,
        })
    }

    pub fn get_entry_ttl(&self, attr: &InodeAttr) -> time::Duration {
        if attr.is_dir() {
            self.config.dir_entry_timeout
        } else {
            self.config.entry_timeout
        }
    }

    pub fn update_length(&self, entry: &mut Entry) {
        if entry.attr.full && entry.is_file() {
            let len = self.writer.get_length(entry.inode);
            if len > entry.attr.length {
                entry.attr.length = len;
            }
            self.reader.truncate(entry.inode, entry.attr.length);
        }
    }

    pub fn modified_since(&self, inode: Ino, start_at: time::Instant) -> bool {
        match self.modified_at.get(&inode) {
            Some(v) => v.value() > &start_at,
            None => false,
        }
    }

    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        trace!("vfs:get_attr with inode {:?}", inode);
        let attr = self.meta.get_attr(inode).await?;
        Ok(attr)
    }
}
