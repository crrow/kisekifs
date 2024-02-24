use std::{path::Path, str::FromStr, sync::Arc, time::Duration};

use kiseki_common::ChunkIndex;
use kiseki_types::{
    attr::InodeAttr, entry::DEntry, ino::Ino, setting::Format, slice::Slices, stat, stat::DirStat,
    FileType,
};
use snafu::ensure;
use strum_macros::EnumString;
use tracing::debug;

use crate::{backend::key::Counter, context::FuseContext, err::Result};

pub mod key;
#[cfg(feature = "meta-rocksdb")]
mod rocksdb;

use crate::err::UnsupportedMetaDSNSnafu;

// TODO: optimize me
pub fn open_backend(dsn: &str) -> Result<BackendRef> {
    let x = dsn.splitn(2, "://:").collect::<Vec<_>>();
    ensure!(x.len() == 2, UnsupportedMetaDSNSnafu { dsn: dsn.clone() });
    let backend_kind = x[0];
    let path = x[1];

    let backend = BackendKinds::from_str(backend_kind).expect("unsupported backend kind");
    backend.build(path)
}

#[derive(Debug, EnumString)]
enum BackendKinds {
    #[cfg(feature = "meta-rocksdb")]
    #[strum(serialize = "rocksdb", serialize = "Rocksdb")]
    Rocksdb,
}

impl BackendKinds {
    fn build(&self, path: &str) -> Result<BackendRef> {
        match self {
            #[cfg(feature = "meta-rocksdb")]
            BackendKinds::Rocksdb => {
                let mut builder = rocksdb::Builder::default();
                builder.with_path(path);
                debug!("backend [rocksdb] is built with path: {}", path);
                builder.build()
            }
            _ => unimplemented!("unsupported backend"),
        }
    }
}

pub type BackendRef = Arc<dyn Backend>;

pub trait Backend: Send + Sync + 'static {
    fn set_format(&self, format: &Format) -> Result<()>;
    fn load_format(&self) -> Result<Format>;

    fn increase_count_by(&self, counter: Counter, step: usize) -> Result<u64>;
    fn load_count(&self, counter: Counter) -> Result<u64>;

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr>;
    fn set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()>;

    fn get_dentry(&self, parent: Ino, name: &str) -> Result<DEntry>;
    fn set_dentry(&self, parent: Ino, name: &str, inode: Ino, typ: FileType) -> Result<()>;
    fn list_dentry(&self, parent: Ino, limit: i64) -> Result<Vec<DEntry>>;

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()>;
    fn get_symlink(&self, inode: Ino) -> Result<String>;

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()>;
    fn set_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, buf: Vec<u8>)
    -> Result<()>;
    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Option<Vec<u8>>>;
    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices>;

    fn set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()>;
    fn get_dir_stat(&self, inode: Ino) -> Result<DirStat>;

    /// [do_mknod] creates a node in a directory with given name, type and
    /// permissions.
    fn do_mknod(
        &self,
        ctx: Arc<FuseContext>,
        new_inode: Ino,
        new_inode_attr: InodeAttr,
        parent: Ino,
        name: &str,
        typ: FileType,
        path: String,
    ) -> Result<(Ino, InodeAttr)>;

    /// [do_rmdir] removes a directory from the filesystem. The directory must
    /// be empty. return the removed directory entry and its attribute
    fn do_rmdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        // skip updating attribute of a directory if the mtime difference is smaller
        // than this value
        skip_dir_mtime: Duration,
    ) -> Result<(DEntry, InodeAttr)>;

    /// [truncate] changes the length for given file.
    fn do_truncate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        length: u64,
        skip_perm_check: bool,
    ) -> Result<InodeAttr>;

    /// [do_link] creates an entry for the inode, return the new [InodeAttr].
    /// Creating another directory entry (filename) that points directly to the
    /// same inode as the original file.
    fn do_link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
        skip_dir_mtime: Duration,
    ) -> Result<InodeAttr>;
}
