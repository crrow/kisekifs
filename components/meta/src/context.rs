use std::time::Instant;

use kiseki_types::{attr::InodeAttr, ino::Ino};
use lazy_static::lazy_static;
use snafu::ensure;
use tracing::debug;

use crate::err::{LibcSnafu, Result};

lazy_static! {
    pub static ref EMPTY_CONTEXT: FuseContext = FuseContext {
        gid: 0,
        gid_list: vec![],
        uid: 0,
        pid: 0,
        check_permission: false,
        start_at: Instant::now(),
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseContext {
    pub gid: u32,
    pub gid_list: Vec<u32>,
    pub uid: u32,
    pub pid: u32,
    pub check_permission: bool,
    pub start_at: Instant,
}

impl<'a> From<&'a kiseki_types::Request<'a>> for FuseContext {
    fn from(req: &'a kiseki_types::Request) -> Self {
        Self {
            gid: req.gid(),
            gid_list: vec![],
            uid: req.uid(),
            pid: req.pid(),
            check_permission: true,
            start_at: Instant::now(),
        }
    }
}

impl FuseContext {
    pub fn check(&self, _inode: Ino, attr: &InodeAttr, perm_mask: u8) -> Result<()> {
        if self.uid == 0 {
            return Ok(());
        }
        if !self.check_permission {
            return Ok(());
        }

        let perm = attr.access_perm(self.uid, &self.gid_list);
        // This condition checks if all the bits set in mmask (requested permissions)
        // are also set in mode (file's permissions).
        //
        // perm = 0o644 (rw-r--r--)
        // perm_mask = 0o4 (read permission)
        // perm & perm_mask = 0o4 (read permission is granted)
        ensure!(
            perm & perm_mask == perm_mask,
            LibcSnafu {
                errno: libc::EACCES,
            }
        );
        return Ok(());
    }
}

impl FuseContext {
    #[allow(dead_code)]
    pub fn background() -> Self {
        Self {
            gid: 1,
            gid_list: vec![],
            uid: 1,
            pid: 1,
            check_permission: false,
            start_at: Instant::now(),
        }
    }
    pub fn contains_gid(&self, gid: u32) -> bool {
        self.gid_list.contains(&gid)
    }
}
