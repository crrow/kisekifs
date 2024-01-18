use std::cmp::{max, min};
use std::fmt::{Debug, Formatter};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use crate::common::err::ToErrno;
use crate::meta::config::{Format, MetaConfig};
use crate::meta::types::{Ino, InternalNode, OpenFiles};
use crate::meta::util::*;
use crate::meta::{
    Counter, Entry, EntryInfo, FSStates, FSStatesInner, FreeID, InodeAttr, MetaContext, DOT,
    DOT_DOT, INODE_BATCH, ROOT_INO, TRASH_INODE, TRASH_INODE_NAME,
};

use crate::meta::id_table::IdTable;
use dashmap::DashMap;
use fuser::FileType;
use futures::TryStream;
use libc::c_int;
use opendal::{ErrorKind, Operator};
use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
use tracing::{error, trace, warn};

#[derive(Debug, Snafu)]
pub enum MetaError {
    #[snafu(display("invalid format version"))]
    ErrInvalidFormatVersion,
    #[snafu(display("failed to parse scheme: {}: {}", got, source))]
    FailedToParseScheme { source: opendal::Error, got: String },
    #[snafu(display("failed to open operator: {}", source))]
    FailedToOpenOperator { source: opendal::Error },
    #[snafu(display("bad access permission for inode:{inode}, want:{want}, grant:{grant}"))]
    ErrBadAccessPerm { inode: Ino, want: u8, grant: u8 },
    #[snafu(display("inode {inode} is not a directory"))]
    ErrNotDir { inode: Ino },
    #[snafu(display("failed to deserialize: {source}"))]
    ErrBincodeDeserializeFailed { source: bincode::Error },
    #[snafu(display("failed to read {key} from sto: {source}"))]
    ErrFailedToReadFromSto { key: String, source: opendal::Error },
    #[snafu(display("failed to list by opendal: {source}"))]
    ErrOpendalList { source: opendal::Error },
    #[snafu(display("failed to mknod: {kind}"))]
    ErrMknod { kind: libc::c_int },
    #[snafu(display("failed to do counter: {source}"))]
    ErrFailedToDoCounter { source: opendal::Error },
}

impl From<MetaError> for crate::common::err::Error {
    fn from(value: MetaError) -> Self {
        Self::MetaError { source: value }
    }
}

// TODO: review the errno mapping
impl ToErrno for MetaError {
    fn to_errno(&self) -> c_int {
        match self {
            MetaError::FailedToParseScheme { .. } => libc::EINVAL,
            MetaError::FailedToOpenOperator { .. } => libc::EIO,
            MetaError::ErrBadAccessPerm { .. } => libc::EACCES,
            MetaError::ErrNotDir { .. } => libc::ENOTDIR,
            MetaError::ErrBincodeDeserializeFailed { .. } => libc::EIO,
            MetaError::ErrFailedToReadFromSto { source, .. } => match source.kind() {
                ErrorKind::NotFound => libc::ENOENT,
                _ => {
                    error!("failed to read from sto: {}", source);
                    libc::EIO
                }
            },
            MetaError::ErrOpendalList { .. } => libc::EIO,
            MetaError::ErrInvalidFormatVersion => libc::EBADF, // TODO: review
            MetaError::ErrMknod { kind } => *kind,
            MetaError::ErrFailedToDoCounter { .. } => libc::EIO,
        }
    }
}

pub type Result<T> = std::result::Result<T, MetaError>;

/// MetaEngine describes a meta service for file system.
pub struct MetaEngine {
    pub config: MetaConfig,
    format: RwLock<Format>,
    root: Ino,
    operator: Arc<Operator>,
    sub_trash: Option<InternalNode>,
    open_files: OpenFiles,
    dir_parents: DashMap<Ino, Ino>,
    fs_states: FSStatesInner,
    free_inodes: IdTable,
}

impl MetaEngine {
    pub fn open(config: MetaConfig) -> Result<MetaEngine> {
        let op = Arc::new(
            Operator::via_map(config.scheme, config.scheme_config.clone())
                .context(FailedToOpenOperatorSnafu)?,
        );
        let m = MetaEngine {
            config: config.clone(),
            format: RwLock::new(Format::default()),
            root: ROOT_INO,
            operator: op.clone(),
            sub_trash: None,
            open_files: OpenFiles::new(config.open_cache, config.open_cache_limit),
            dir_parents: DashMap::new(),
            fs_states: Default::default(),
            free_inodes: IdTable::new(op.clone(), Counter::NextInode, INODE_BATCH),
        };
        Ok(m)
    }
    pub fn info(&self) -> String {
        format!("meta-{}", self.config.scheme)
    }

    /// Load loads the existing setting of a formatted volume from meta service.
    pub async fn load_format(&self, check_version: bool) -> Result<Format> {
        let format_key_str = Format::format_key_str();
        let format_buf = self.operator.blocking().read(&format_key_str).context(
            ErrFailedToReadFromStoSnafu {
                key: format_key_str,
            },
        )?;

        let format = Format::parse_from(&format_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        if check_version {
            format.check_version()?;
        }
        let mut guard = self.format.write().await;
        *guard = format.clone();
        Ok(format)
    }

    /// StatFS returns summary statistics of a volume.
    pub async fn stat_fs(&self, ctx: &MetaContext, inode: Ino) -> Result<FSStates> {
        let (state, no_error) = self.stat_root_fs().await;
        if !no_error {
            return Ok(state);
        }

        let inode = self.check_root(inode);
        if inode == ROOT_INO {
            return Ok(state);
        }

        let attr = self.get_attr(inode).await?;
        if let Err(_) = access(ctx, inode, &attr, MODE_MASK_R & MODE_MASK_X) {
            return Ok(state);
        }

        // TODO: quota check
        Ok(state)
    }

    async fn stat_root_fs(&self) -> (FSStates, bool) {
        let mut no_error = true;
        // Parallelize calls to get_counter()
        let (mut used_space, mut inodes) = match tokio::try_join!(
            timeout(
                Duration::from_millis(150),
                Counter::UsedSpace.load(self.operator.clone()),
            ),
            timeout(
                Duration::from_millis(150),
                Counter::TotalInodes.load(self.operator.clone()),
            )
        ) {
            Ok((used_space, total_inodes)) => {
                // the inner sto may return error
                no_error = used_space.is_ok() && total_inodes.is_ok();
                (
                    used_space.map_or(self.fs_states.used_space.load(Acquire), |x| x as i64),
                    total_inodes.map_or(self.fs_states.used_inodes.load(Acquire), |x| x as i64),
                )
            }
            Err(_) => {
                // timeout case
                no_error = false;
                (
                    self.fs_states.used_space.load(Acquire),
                    self.fs_states.used_inodes.load(Acquire),
                )
            }
        };

        used_space += self.fs_states.new_space.load(Acquire);
        inodes += self.fs_states.new_inodes.load(Acquire);
        used_space = max(used_space, 0);
        inodes = max(inodes, 0);
        let iused = inodes as u64;

        let format = self.format.read().await;

        let total_space = if format.capacity > 0 {
            min(format.capacity, used_space as u64)
        } else {
            let mut v = 1 << 50;
            let us = used_space as u64;
            while v * 8 < us * 10 {
                v *= 2;
            }
            v
        };
        let avail_space = total_space - used_space as u64;

        let available_inodes = if format.inodes > 0 {
            if iused > format.inodes {
                0
            } else {
                format.inodes - iused
            }
        } else {
            let mut available_inodes: u64 = 10 << 20;
            while available_inodes * 10 > (iused + available_inodes) * 8 {
                available_inodes *= 2;
            }
            available_inodes
        };

        (
            FSStates {
                total_space,
                avail_space,
                used_inodes: iused,
                available_inodes,
            },
            no_error,
        )
    }

    /// Lookup returns the inode and attributes for the given entry in a directory.
    pub async fn lookup(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: &str,
        check_perm: bool,
    ) -> Result<(Ino, InodeAttr)> {
        trace!(parent=?parent, ?name, "lookup");
        let parent = self.check_root(parent);
        if check_perm {
            let parent_attr = self.get_attr(parent).await?;
            access(ctx, parent, &parent_attr, MODE_MASK_X)?;
        }
        let mut name = name;
        if name == DOT_DOT {
            if parent == self.root {
                // If parent is already the root directory,
                // sets name to "." (current directory).
                name = DOT;
            } else {
                // Otherwise, retrieves attributes of parent.
                // Checks if parent is a directory using attr.Typ != TypeDirectory.
                // Returns syscall.ENOTDIR if not.
                let parent_attr = self.get_attr(parent).await?;
                if parent_attr.get_filetype() != fuser::FileType::Directory {
                    return Err(MetaError::ErrNotDir { inode: parent })?;
                }
                let attr = self.get_attr(parent_attr.parent).await?;
                return Ok((parent_attr.parent, attr));
            }
        }
        if name == DOT {
            let attr = self.get_attr(parent).await?;
            return Ok((parent, attr));
        }
        if parent == ROOT_INO && name == TRASH_INODE_NAME {
            return Ok((TRASH_INODE, self.get_attr(TRASH_INODE).await?));
        }
        let (inode, attr) = match self.do_lookup(parent, name).await {
            Ok(r) => r,
            Err(e) => match e {
                MetaError::ErrFailedToReadFromSto { .. } if self.config.case_insensitive => {
                    // TODO: this is an optimization point
                    self.resolve_case(&ctx, parent, name);
                    return Err(e);
                }
                _ => return Err(e),
            },
        };

        if attr.kind == FileType::Directory && !parent.is_trash() {
            self.dir_parents.insert(inode, parent);
        }

        return Ok((inode, attr));
    }

    // Verifies if the requested access mode (mmask) is permitted for the given user or group based on the file's actual permissions (mode).
    // Ensures access control based on file permissions.
    pub fn check_root(&self, inode: Ino) -> Ino {
        if inode.is_zero() {
            ROOT_INO // force using Root inode
        } else if inode == ROOT_INO {
            self.root
        } else {
            inode
        }
    }

    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        trace!("get_attr with inode {:?}", inode);
        let inode = self.check_root(inode);
        // check cache
        if let Some(attr) = self.open_files.check(inode) {
            return Ok(attr);
        }

        let mut attr = if inode.is_trash() || inode.is_root() {
            // call do_get_attr with timeout
            //
            // In the timeout case, we give the root and trash inodes a default hard code value.
            //
            // Availability: The Root and Trash inodes are critical for filesystem operations.
            // Providing default values guarantees that they're always accessible, even under slow or unreliable conditions.
            // Consistency: Ensuring consistent behavior for these inodes, even with timeouts, helps maintain filesystem integrity.
            timeout(Duration::from_millis(300), self.do_get_attr(inode))
                .await
                .unwrap_or(Ok(InodeAttr::hard_code_inode_attr(inode.is_trash())))?
        } else {
            self.do_get_attr(inode).await?
        };

        // update cache
        self.open_files.update(inode, &mut attr);
        if attr.is_filetype(FileType::Directory) && !inode.is_root() && !attr.parent.is_trash() {
            self.dir_parents.insert(inode, attr.parent);
        }
        Ok(attr)
    }

    async fn do_get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        // TODO: do we need transaction ?
        let inode_key = inode.generate_key_str();
        let attr_buf =
            self.operator
                .read(&inode_key)
                .await
                .context(ErrFailedToReadFromStoSnafu {
                    key: inode_key.to_string(),
                })?;
        let attr: InodeAttr =
            bincode::deserialize(&attr_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        Ok(attr)
    }

    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, InodeAttr)> {
        let entry_key = EntryInfo::generate_entry_key_str(parent, name);
        let entry_buf = self
            .operator
            .read(&entry_key)
            .await
            .context(ErrFailedToReadFromStoSnafu { key: entry_key })?;

        let entry_info =
            EntryInfo::parse_from(&entry_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        let inode = entry_info.inode;
        let inode_key = inode.generate_key_str();
        let attr_buf = self
            .operator
            .read(&inode_key)
            .await
            .context(ErrFailedToReadFromStoSnafu { key: inode_key })?;
        // TODO: juicefs also handle the attr buf empty case, wired.
        let attr: InodeAttr =
            bincode::deserialize(&attr_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        Ok((inode, attr))
    }

    fn resolve_case(&self, ctx: &MetaContext, parent: Ino, name: &str) {
        todo!()
    }

    // Readdir returns all entries for given directory, which include attributes if plus is true.
    pub async fn read_dir(&self, ctx: &MetaContext, inode: Ino, plus: bool) -> Result<Vec<Entry>> {
        trace!(dir=?inode, "readdir");
        match self.read_dir_inner(ctx, inode, plus).await {
            Ok(_) => {}
            Err(_) => {}
        }
        todo!()
    }

    async fn read_dir_inner(
        &self,
        ctx: &MetaContext,
        inode: Ino,
        plus: bool,
    ) -> Result<Vec<Entry>> {
        let inode = self.check_root(inode);
        let mut attr = self.get_attr(inode).await?;
        let mmask = if plus {
            MODE_MASK_R | MODE_MASK_X
        } else {
            MODE_MASK_X
        };

        access(ctx, inode, &attr, mmask)?;

        if inode == self.root {
            attr.parent = self.root;
        }

        let mut basic_entries = vec![
            Entry::new(inode, DOT, FileType::Directory),
            Entry::new(attr.parent, DOT_DOT, FileType::Directory),
        ];

        if let Err(e) = self.do_read_dir(inode, plus, &mut basic_entries, -1).await {
            if let MetaError::ErrFailedToReadFromSto { source, key } = e {
                if source.kind() == opendal::ErrorKind::NotFound && inode.is_trash() {
                    return Ok(basic_entries);
                }
            }
        }

        Ok(basic_entries)
    }
    async fn do_read_dir(
        &self,
        inode: Ino,
        plus: bool,
        basic_entries: &mut Vec<Entry>,
        limit: i64,
    ) -> Result<()> {
        let entry_prefix = EntryInfo::generate_entry_key_str(inode, "");

        let sto_entries = self
            .operator
            .list(&entry_prefix)
            .await
            .context(ErrOpendalListSnafu)?;
        for sto_entry in &sto_entries {
            let name = sto_entry.name();
            if name.len() == 0 {
                warn!("empty entry name under {:?}", inode);
                continue;
            }
            let entry_info_key = sto_entry.path();
            let entry_info_buf =
                self.operator
                    .read(entry_info_key)
                    .await
                    .context(ErrFailedToReadFromStoSnafu {
                        key: entry_info_key.to_string(),
                    })?;
            let entry_info =
                EntryInfo::parse_from(&entry_info_buf).context(ErrBincodeDeserializeFailedSnafu)?;
            basic_entries.push(Entry::new(entry_info.inode, name, entry_info.typ));
        }

        if plus && basic_entries.len() != 0 {
            todo!()
            // let mut entries = Vec::with_capacity(basic_entries.len());
            // for entry in basic_entries {
            //     let attr = self.get_attr(entry.inode).await?;
            //     entry.attr = attr;
            //     entries.push(entry.clone());
            // }
            // *basic_entries = entries;
        }
        Ok(())
    }

    // Change root to a directory specified by sub_dir
    pub async fn chroot<P: AsRef<Path>>(&self, ctx: &MetaContext, sub_dir: P) -> Result<()> {
        let sub_dir = sub_dir.as_ref();
        for c in sub_dir.components() {
            let name = match c {
                Component::Normal(name) => {
                    name.to_str().expect("invalid path component { sub_dir}")
                }
                _ => unreachable!("invalid path component: {:?}", c),
            };
            let (inode, attr) = match self.lookup(ctx, self.root, name, true).await {
                Ok(r) => r,
                Err(e) => {
                    if e.to_errno() == libc::ENOENT {
                        let (inode, attr) = self.mkdir(ctx, self.root, name, 0o777, 0).await?;
                        (inode, attr)
                    } else {
                        return Err(e);
                    }
                }
            };
            if attr.get_filetype() != FileType::Directory {
                return Err(MetaError::ErrNotDir { inode })?;
            }
        }
        Ok(())
    }

    // Mkdir creates a sub-directory with given name and mode.
    pub async fn mkdir(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
    ) -> Result<(Ino, InodeAttr)> {
        self.mknod(
            ctx,
            parent,
            name,
            FileType::Directory,
            mode,
            cumask,
            0,
            String::new(),
        )
        .await
        .and_then(|r| {
            self.dir_parents.insert(r.0, parent);
            Ok(r)
        })
    }

    // Mknod creates a node in a directory with given name, type and permissions.
    pub async fn mknod(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: &str,
        typ: FileType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        if parent.is_trash() || parent.is_root() && name == TRASH_INODE_NAME {
            return Err(MetaError::ErrMknod { kind: libc::EPERM });
        }
        if self.config.read_only {
            return Err(MetaError::ErrMknod { kind: libc::EROFS });
        }
        if name.len() == 0 {
            return Err(MetaError::ErrMknod { kind: libc::ENOENT });
        }

        let parent = self.check_root(parent);
        let (space, inodes) = (align4k(0), 1i64);
        self.check_quota(ctx, space, inodes, parent)?;
        self.do_mknod(ctx, parent, name, typ, mode, cumask, rdev, path)
            .await
            .and_then(|r| {
                self.update_stats(space, inodes)?;
                self.update_update_dir_stat(parent, 0, space, inodes)?;
                self.update_dir_quota(parent, space, inodes)?;
                Ok(r)
            })
    }

    async fn do_mknod(
        &self,
        ctx: &MetaContext,
        parent: Ino,
        name: &str,
        typ: FileType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        let inode = if parent.is_trash() {
            let next = Counter::NextTrash
                .increment(self.operator.clone())
                .await
                .context(ErrFailedToDoCounterSnafu)?;
            TRASH_INODE + Ino::from(next)
        } else {
            Ino::from(
                self.free_inodes
                    .next()
                    .await
                    .context(ErrFailedToDoCounterSnafu)?,
            )
        };

        let mut attr = InodeAttr::default()
            .set_perm(mode & !cumask)
            .set_kind(typ)
            .set_gid(ctx.gid)
            .set_uid(ctx.uid)
            .set_parent(parent)
            .set_full()
            .to_owned();
        if typ == FileType::Directory {
            attr.set_nlink(2).set_length(4 << 10);
        } else {
            attr.set_nlink(1);
            if typ == FileType::Symlink {
                attr.set_length(path.len() as u64);
            } else {
                attr.set_length(0).set_rdev(rdev);
            }
        };

        todo!()
    }
}

pub fn access(ctx: &MetaContext, inode: Ino, attr: &InodeAttr, perm_mask: u8) -> Result<()> {
    if ctx.uid == 0 {
        return Ok(());
    }
    if !ctx.check_permission {
        return Ok(());
    }

    let perm = attr.access_perm(ctx.uid, &ctx.gid_list);
    if perm & perm_mask != perm_mask {
        // This condition checks if all the bits set in mmask (requested permissions)
        // are also set in mode (file's permissions).
        //
        // perm = 0o644 (rw-r--r--)
        // perm_mask = 0o4 (read permission)
        // perm & perm_mask = 0o4 (read permission is granted)
        return Err(MetaError::ErrBadAccessPerm {
            inode,
            want: perm_mask,
            grant: perm,
        })?;
    }

    Ok(())
}

impl Debug for MetaEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meta")
            .field("scheme", &self.config.scheme)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn path_components() {
        let p = PathBuf::from("d1");
        for c in p.components() {
            println!("{:?}", c);
        }
    }
}
