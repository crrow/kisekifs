use crate::meta::config::{Format, MetaConfig};
use crate::meta::types::{Ino, InternalNode, OpenFiles};

use opendal::Operator;
use snafu::{ResultExt, Snafu};

use crate::common::err::ToErrno;
use crate::meta::util::*;
use crate::meta::{
    EntryInfo, InodeAttr, MetaContext, DOT, DOT_DOT, ROOT_INO, TRASH_INODE, TRASH_INODE_NAME,
};
use dashmap::DashMap;
use fuser::FileType;
use libc::c_int;
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use tokio::time::timeout;
use tracing::trace;

#[derive(Debug, Snafu)]
pub enum MetaError {
    #[snafu(display("failed to parse scheme: {}: {}", got, source))]
    FailedToParseScheme { source: opendal::Error, got: String },
    #[snafu(display("failed to open operator: {}", source))]
    FailedToOpenOperator { source: opendal::Error },
    #[snafu(display("bad access permission for inode:{inode}, want:{want}, grant:{grant}"))]
    ErrBadAccessPerm { inode: Ino, want: u8, grant: u8 },
    #[snafu(display("inode {inode} is not a directory"))]
    ErrNotDir { inode: Ino },
    #[snafu(display("look failed: {parent}-{name} doesn't exist"))]
    ErrLookupFailed { parent: Ino, name: String },
    #[snafu(display("failed to deserialize: {source}"))]
    ErrBincodeDeserializeFailed { source: bincode::Error },
    #[snafu(display("failed to read {key} from opendal: {source}"))]
    ErrOpendalRead { key: String, source: opendal::Error },
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
            MetaError::ErrLookupFailed { .. } => libc::ENOENT,
            MetaError::ErrBincodeDeserializeFailed { .. } => libc::EIO,
            MetaError::ErrOpendalRead { .. } => libc::ENOENT,
        }
    }
}

pub type Result<T> = std::result::Result<T, MetaError>;

/// MetaEngine describes a meta service for file system.
pub struct MetaEngine {
    pub config: MetaConfig,
    format: Option<Format>,
    root: Ino,
    operator: Operator,
    sub_trash: Option<InternalNode>,
    open_files: OpenFiles,
    dir_parents: DashMap<Ino, Ino>,
}

impl MetaEngine {
    pub fn open(config: MetaConfig) -> Result<MetaEngine> {
        let op = Operator::via_map(config.scheme, config.scheme_config.clone())
            .context(FailedToOpenOperatorSnafu)?;
        let m = MetaEngine {
            config: config.clone(),
            format: None,
            root: ROOT_INO,
            operator: op,
            sub_trash: None,
            open_files: OpenFiles::new(config.open_cache, config.open_cache_limit),
            dir_parents: DashMap::new(),
        };
        Ok(m)
    }
    pub fn info(&self) -> String {
        format!("meta-{}", self.config.scheme)
    }

    // Lookup returns the inode and attributes for the given entry in a directory.
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
                MetaError::ErrLookupFailed { .. } if self.config.case_insensitive => {
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
        let attr_buf = self
            .operator
            .read(&inode_key)
            .await
            .context(ErrOpendalReadSnafu {
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
            .context(ErrOpendalReadSnafu { key: entry_key })?;

        let entry_info =
            EntryInfo::parse_from(&entry_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        let inode = entry_info.inode;
        let inode_key = inode.generate_key_str();
        let attr_buf = self
            .operator
            .read(&inode_key)
            .await
            .context(ErrOpendalReadSnafu { key: inode_key })?;
        // TODO: juicefs also handle the attr buf empty case, wired.
        let attr: InodeAttr =
            bincode::deserialize(&attr_buf).context(ErrBincodeDeserializeFailedSnafu)?;
        Ok((inode, attr))
    }

    fn resolve_case(&self, ctx: &MetaContext, parent: Ino, name: &str) {
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
