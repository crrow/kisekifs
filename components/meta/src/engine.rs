use std::{
    cmp::{max, min},
    fmt::{Display, Formatter},
    ops::Add,
    path::{Component, Path},
    sync::{
        atomic::{AtomicI64, Ordering, Ordering::Acquire},
        Arc,
    },
    time::{Duration, SystemTime},
};

use crossbeam::{atomic::AtomicCell, channel::at};
use dashmap::DashMap;
use futures::AsyncReadExt;
use kiseki_common::{CHUNK_SIZE, DOT, DOT_DOT, MODE_MASK_R, MODE_MASK_W, MODE_MASK_X};
use kiseki_types::{
    attr::{InodeAttr, SetAttrFlags},
    entry::{DEntry, Entry, FullEntry},
    ino::{Ino, ROOT_INO, TRASH_INODE},
    internal_nodes::{InternalNode, TRASH_INODE_NAME},
    setting::Format,
    slice::{Slice, SliceID, Slices, SLICE_BYTES},
    stat::{DirStat, FSStat},
    FileType,
};
use scopeguard::defer;
use snafu::{ensure, ResultExt};
use tokio::time::{timeout, Instant};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    backend::{key::Counter, open_backend, BackendRef},
    config::MetaConfig,
    context::FuseContext,
    err::{Error, Error::LibcError, LibcSnafu, Result, TokioJoinSnafu},
    id_table::IdTable,
    open_files::OpenFiles,
};

pub type MetaEngineRef = Arc<MetaEngine>;

pub fn open(config: MetaConfig) -> Result<MetaEngineRef> {
    let backend = open_backend(&config.dsn)?;
    let format = backend.load_format()?;

    let me = MetaEngine {
        open_files: OpenFiles::new(config.open_cache, config.open_cache_limit),
        config,
        format,
        root: ROOT_INO,
        sub_trash: None,
        dir_parents: Default::default(),
        fs_stat: Default::default(),
        dir_stats: Default::default(),
        free_inodes: IdTable::new(backend.clone(), Counter::NextInode),
        free_slices: IdTable::new(backend.clone(), Counter::NextSlice),
        backend,
    };

    debug!("open meta engine: {}", me);

    Ok(Arc::new(me))
}

// update_format is used to change the file system's setting.
pub fn update_format(dsn: String, format: Format, force: bool) -> Result<()> {
    let backend = open_backend(&dsn)?;

    let mut need_init_root = false;
    match backend.load_format() {
        Ok(old_format) => {
            debug!("found exists format, need to update");
            if !old_format.dir_stats && format.dir_stats {
                // remove dir stats as they are outdated
            }
            // TODO: update the old format
        }
        Err(e) => {
            if matches!(e, Error::UninitializedEngine { .. }) {
                // we need to initialize the engine
                debug!("cannot found format, need to initialize the engine");
                need_init_root = true;
            } else {
                debug!("cannot found format, but got error: {:?}", e);
                return Err(e);
            }
        }
    }

    let mut basic_attr = InodeAttr::default()
        .set_kind(kiseki_types::FileType::Directory)
        .set_nlink(2)
        .set_length(4 << 10)
        .set_parent(ROOT_INO)
        .to_owned();

    if format.trash_days > 0 {
        if let Err(e) = backend.get_attr(TRASH_INODE) {
            if e.is_not_found() {
                basic_attr.set_perm(0o555);
                backend.set_attr(TRASH_INODE, &basic_attr)?;
            } else {
                return Err(e);
            }
        }
    }
    backend.set_format(&format)?;
    if need_init_root {
        basic_attr.set_perm(0o777);
        backend.set_attr(ROOT_INO, &basic_attr)?;
        backend.increase_count_by(Counter::NextInode, 2)?;
    }

    Ok(())
}

pub struct MetaEngine {
    config: MetaConfig,
    format: Format,
    root: Ino,
    sub_trash: Option<InternalNode>,
    open_files: OpenFiles,
    dir_parents: DashMap<Ino, Ino>,
    fs_stat: AtomicCell<FSStat>,
    dir_stats: DashMap<Ino, DirStat>,

    free_inodes: IdTable,
    free_slices: IdTable,

    // Backend for the meta engine
    backend: BackendRef,
}

impl Display for MetaEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO
        write!(f, "MetaEngine: {}", self.format.name)
    }
}

impl MetaEngine {
    pub fn get_format(&self) -> &Format {
        &self.format
    }

    pub fn next_slice_id(&self) -> Result<SliceID> {
        self.free_slices.next()
    }

    /// StatFS returns summary statistics of a volume.
    pub fn stat_fs(&self, ctx: Arc<FuseContext>, inode: Ino) -> Result<FSStat> {
        let stat = self.fs_stat.load();

        // let inode = self.check_root(inode);
        // if inode == ROOT_INO {
        //     return Ok(state);
        // }
        //
        // let attr = self.get_attr(inode).await?;
        // if ctx.check(inode, &attr, MODE_MASK_R & MODE_MASK_X).is_err() {
        //     return Ok(state);
        // }

        // TODO: quota check
        Ok(stat)
    }

    // Verifies if the requested access mode (mmask) is permitted for the given user
    // or group based on the file's actual permissions (mode). Ensures access
    // control based on file permissions.
    pub fn check_root(&self, inode: Ino) -> Ino {
        if inode.is_zero() {
            ROOT_INO // force using Root inode
        } else if inode == ROOT_INO {
            self.root
        } else {
            inode
        }
    }

    /// Lookup returns the inode and attributes for the given entry in a
    /// directory.
    pub async fn lookup(
        &self,
        ctx: &FuseContext,
        parent: Ino,
        name: &str,
        check_perm: bool,
    ) -> Result<(Ino, InodeAttr)> {
        trace!(parent=?parent, ?name, "lookup");
        let parent = self.check_root(parent);
        if check_perm {
            let parent_attr = self.get_attr(parent).await?;
            ctx.check(parent, &parent_attr, MODE_MASK_X)?;
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
                ensure!(
                    parent_attr.get_filetype() == FileType::Directory,
                    LibcSnafu {
                        errno: libc::ENOTDIR
                    }
                );
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
        let (inode, attr) = self.do_lookup(parent, name)?;

        if attr.kind == FileType::Directory && !parent.is_trash() {
            self.dir_parents.insert(inode, parent);
        }

        Ok((inode, attr))
    }

    fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, InodeAttr)> {
        let entry_info = self.backend.get_entry_info(parent, name)?;
        let inode = entry_info.inode;
        let attr = self.backend.get_attr(inode)?;
        Ok((inode, attr))
    }

    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        let inode = self.check_root(inode);
        // check cache
        if !self.config.open_cache.is_zero() {
            if let Some(attr) = self.open_files.check(inode) {
                return Ok(attr);
            }
        }

        // TODO: add timeout here
        let mut attr = self.backend.get_attr(inode)?;

        // update cache
        self.open_files.update(inode, &mut attr);
        if attr.is_filetype(FileType::Directory) && !inode.is_root() && !attr.parent.is_trash() {
            self.dir_parents.insert(inode, attr.parent);
        }
        Ok(attr)
    }

    // Readdir returns all entries for given directory, which include attributes if
    // plus is true.
    pub async fn read_dir(&self, ctx: &FuseContext, inode: Ino, plus: bool) -> Result<Vec<Entry>> {
        debug!(dir=?inode, "readdir in plus?, {plus}");
        let inode = self.check_root(inode);
        let mut attr = self.get_attr(inode).await?;
        let mmask = if plus {
            MODE_MASK_R | MODE_MASK_X
        } else {
            MODE_MASK_X
        };

        ctx.check(inode, &attr, mmask)?;

        if inode == self.root {
            attr.parent = self.root;
        }

        let mut basic_entries = Entry::new_basic_entry_pair(inode, attr.parent);
        // let mut basic_entries = vec![];

        if let Err(e) = self.do_read_dir(inode, plus, &mut basic_entries, -1).await {
            return if e.is_not_found() && inode.is_trash() {
                Ok(basic_entries)
            } else {
                error!("readdir failed: {:?}", e);
                Err(e)
            };
        }

        debug!("find entries: {:?}", &basic_entries);

        Ok(basic_entries)
    }
    async fn do_read_dir(
        &self,
        inode: Ino,
        plus: bool,
        basic_entries: &mut Vec<Entry>,
        _limit: i64,
    ) -> Result<()> {
        let backend = self.backend.clone();
        let entries = tokio::task::spawn_blocking(move || backend.list_entry_info(inode, _limit))
            .await
            .context(TokioJoinSnafu)??;
        // let entries = self.backend.list_entry_info(inode, _limit)?;
        for de in entries {
            let entry = if plus {
                let attr = self.backend.get_attr(de.inode)?;
                Entry::Full(FullEntry {
                    inode,
                    name: de.name.clone(),
                    attr,
                })
            } else {
                Entry::DEntry(de)
            };
            basic_entries.push(entry);
        }
        Ok(())
    }

    // Change root to a directory specified by sub_dir.
    // pub async fn chroot<P: AsRef<Path>>(&self, ctx: &FuseContext, sub_dir: P) ->
    // Result<()> {     let sub_dir = sub_dir.as_ref();
    //     for c in sub_dir.components() {
    //         let name = match c {
    //             Component::Normal(name) => {
    //                 name.to_str().expect("invalid path component { sub_dir}")
    //             }
    //             _ => unreachable!("invalid path component: {:?}", c),
    //         };
    //         let (inode, attr) = match self.lookup(ctx, self.root, name,
    // true).await {             Ok(r) => r,
    //             Err(e) => {
    //                 if e.to_errno() == libc::ENOENT {
    //                     let (inode, attr) = self.mkdir(ctx, self.root, name,
    // 0o777, 0).await?;                     (inode, attr)
    //                 } else {
    //                     return Err(e);
    //                 }
    //             }
    //         };
    //         ensure!(
    //             attr.get_filetype() == FileType::Directory,
    //             LibcSnafu {
    //                 errno: libc::ENOTDIR,
    //             }
    //         );
    //     }
    //     Ok(())
    // }

    // Mkdir creates a sub-directory with given name and mode.
    pub async fn mkdir(
        &self,
        ctx: &FuseContext,
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
        .map(|r| {
            self.dir_parents.insert(r.0, parent);
            r
        })
    }

    // Mknod creates a node in a directory with given name, type and permissions.
    #[instrument(skip(self, ctx), fields(parent=?parent, name=?name, typ=?typ, mode=?mode, cumask=?cumask, rdev=?rdev, path=?path))]
    #[allow(clippy::too_many_arguments)]
    pub async fn mknod(
        &self,
        ctx: &FuseContext,
        parent: Ino,
        name: &str,
        typ: FileType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        ensure!(
            !parent.is_trash() && !(parent.is_root() && name == TRASH_INODE_NAME),
            LibcSnafu { errno: libc::EPERM }
        );
        ensure!(!self.config.read_only, LibcSnafu { errno: libc::EROFS });
        ensure!(
            !name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT
            }
        );

        let parent = self.check_root(parent);
        let (space, inodes) = (kiseki_utils::align::align4k(0), 1i64);
        // self.check_quota(ctx, space, inodes, parent)?;
        let r = self.do_mknod(ctx, parent, name, typ, mode, cumask, rdev, path)?;

        self.update_mem_dir_stat(parent, 0, space, inodes)?;

        Ok(r)
    }

    #[allow(clippy::too_many_arguments)]
    fn do_mknod(
        &self,
        ctx: &FuseContext,
        parent: Ino,
        name: &str,
        typ: FileType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        let inode = if parent.is_trash() {
            let next = self.backend.increase_count_by(Counter::NextTrash, 1)?;
            TRASH_INODE + Ino::from(next)
        } else {
            Ino::from(self.free_inodes.next()?)
        };

        let mut attr = InodeAttr::default()
            .set_perm(mode & !cumask)
            .set_kind(typ)
            .set_gid(ctx.gid)
            .set_uid(ctx.uid)
            .set_parent(parent)
            // .set_flags(flags) // TODO, maybe we don't have to
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

        // FIXME: we need transaction here
        let mut parent_attr = self.backend.get_attr(parent)?;
        ensure!(
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR
            }
        );
        // check if the parent is trash
        ensure!(
            !parent_attr.parent.is_trash(),
            LibcSnafu {
                errno: libc::ENOENT
            }
        );
        // check if the parent have the permission
        ctx.check(parent, &parent_attr, kiseki_common::MODE_MASK_W)?;

        ensure!(
            !kiseki_types::attr::Flags::from_bits(parent_attr.flags as u8)
                .unwrap()
                .contains(kiseki_types::attr::Flags::IMMUTABLE),
            LibcSnafu { errno: libc::EPERM }
        );

        // check if the entry already exists
        if let Err(e) = self.backend.get_entry_info(parent, name) {
            if !e.is_not_found() {
                return Err(e);
            }
        } else {
            LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }

        // check if we need to update the parent
        let mut update_parent_attr = false;
        if !parent.is_trash() && typ == FileType::Directory {
            parent_attr.set_nlink(parent_attr.nlink + 1);
            if self.config.skip_dir_nlink == 0 {
                let now = SystemTime::now();
                parent_attr.mtime = now;
                parent_attr.ctime = now;
                update_parent_attr = true;
            }
        };

        let now = SystemTime::now();
        attr.set_atime(now);
        attr.set_mtime(now);
        attr.set_ctime(now);

        #[cfg(target_os = "darwin")]
        {
            attr.set_gid(parent_attr.gid);
        }

        // TODO: review the logic here
        #[cfg(target_os = "linux")]
        {
            if parent_attr.perm & 0o2000 != 0 {
                attr.set_gid(parent_attr.gid);
            }
            if typ == FileType::Directory {
                attr.perm |= 0o2000;
            } else if attr.perm & 0o2010 == 0o2010
                && ctx.uid != 0
                && !ctx.gid_list.contains(&parent_attr.gid)
            {
                attr.perm &= !0o2010;
            }
        }

        self.backend.set_dentry(parent, name, inode, typ)?;
        info!("create entry info: parent: {parent}, name: {name}, ino: {inode}");
        self.backend.set_attr(inode, &attr)?;
        if update_parent_attr {
            self.backend.set_attr(parent, &parent_attr)?;
        }
        if typ == FileType::Symlink {
            self.backend.set_symlink(inode, path)?;
        } else if typ == FileType::Directory {
            self.backend
                .set_dir_stat(inode, kiseki_types::stat::DirStat::default())?;
        };
        Ok((inode, attr))
    }

    pub async fn create(
        &self,
        ctx: &FuseContext,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: i32,
    ) -> Result<(Ino, InodeAttr)> {
        debug!(
            "create with parent {:?}, name {:?}, mode {:?}, cumask {:?}, flags {:?}",
            parent, name, mode, cumask, flags
        );
        let mut x = match self
            .mknod(
                ctx,
                parent,
                name,
                FileType::RegularFile,
                mode,
                cumask,
                0,
                String::new(),
            )
            .await
        {
            Ok(r) => r,
            Err(e) if matches!(e, LibcError{errno, ..} if errno == libc::EEXIST) => {
                warn!("create failed: {:?}", e);
                let r = self.do_lookup(parent, name)?;
                r
            }
            Err(e) => return Err(e),
        };

        self.open_files.open(x.0, &mut x.1);
        Ok(x)
    }
    pub async fn check_set_attr(
        &self,
        ctx: &FuseContext,
        ino: Ino,
        flags: SetAttrFlags,
        new_attr: &mut InodeAttr,
    ) -> Result<()> {
        let inode = self.check_root(ino);
        let cur_attr = self.get_attr(inode).await?;
        self.merge_attr(ctx, flags, inode, &cur_attr, new_attr, SystemTime::now())?;

        // TODO: implement me
        Ok(())
    }
    // SetAttr updates the attributes for given node.
    pub async fn set_attr(
        &self,
        ctx: &FuseContext,
        flags: SetAttrFlags,
        ino: Ino,
        new_attr: &mut InodeAttr,
    ) -> Result<()> {
        let inode = self.check_root(ino);

        let cur_attr = self.backend.get_attr(inode)?;
        ensure!(
            !cur_attr.parent.is_trash(),
            LibcSnafu { errno: libc::EPERM }
        );
        let now = SystemTime::now();
        let mut dirty_attr = self.merge_attr(ctx, flags, ino, &cur_attr, new_attr, now)?;
        dirty_attr.ctime = now;
        self.backend.set_attr(inode, &dirty_attr)?;
        Ok(())
    }
    fn merge_attr(
        &self,
        ctx: &FuseContext,
        flags: SetAttrFlags,
        inode: Ino,
        cur: &InodeAttr,
        new_attr: &mut InodeAttr,
        now: SystemTime,
    ) -> Result<InodeAttr> {
        let mut dirty_attr = cur.clone();
        if flags.contains(SetAttrFlags::MODE)
            && flags.contains(SetAttrFlags::UID | SetAttrFlags::GID)
        {
            // This operation isolates the file permission bits representing the user's file
            // type (setuid, setgid, or sticky bit).
            dirty_attr.perm |= cur.perm & 0o6000;
        }
        let mut changed = false;
        if cur.perm & 0o6000 != 0 && flags.contains(SetAttrFlags::UID | SetAttrFlags::GID) {
            #[cfg(target_os = "linux")]
            {
                if !dirty_attr.is_dir() {
                    if ctx.uid != 0 || (dirty_attr.perm >> 3) & 1 != 0 {
                        // clear SUID and SGID
                        dirty_attr.perm &= 0o1777; // WHY ?
                        new_attr.perm &= 0o1777;
                    } else {
                        // keep SGID if the file is non-group-executable
                        dirty_attr.perm &= 0o3777;
                        new_attr.perm &= 0o3777;
                    }
                }
            }
            changed = true;
        }
        if flags.contains(SetAttrFlags::GID) {
            ensure!(
                ctx.uid == 0 || ctx.uid == cur.uid,
                LibcSnafu { errno: libc::EPERM }
            );
            // if ctx.uid != 0 && ctx.uid != cur.uid {
            //     return Err(MetaError::ErrLibc { kind: libc::EPERM });
            // }
            if cur.gid != new_attr.gid {
                if ctx.check_permission && cur.uid != 0 && !ctx.contains_gid(new_attr.gid) {
                    LibcSnafu { errno: libc::EPERM }.fail()?;
                }
                dirty_attr.gid = new_attr.gid;
                changed = true;
            }
        }
        if flags.contains(SetAttrFlags::UID) && cur.uid != new_attr.uid {
            ensure!(ctx.uid == 0, LibcSnafu { errno: libc::EPERM });
            // if ctx.uid != 0 {
            //     return Err(MetaError::ErrLibc { kind: libc::EPERM });
            // }
            dirty_attr.uid = new_attr.uid;
            changed = true;
        }
        if flags.contains(SetAttrFlags::MODE) {
            if ctx.uid != 0 && new_attr.perm & 0o2000 != 0 && ctx.uid != cur.gid {
                new_attr.perm &= 0o5777;
            }
            if cur.perm != new_attr.perm {
                if ctx.uid != 0
                    && ctx.uid != cur.uid
                    && (cur.perm & 0o1777 != new_attr.perm & 0o1777
                        || new_attr.perm & 0o2000 > cur.perm & 0o2000
                        || new_attr.perm & 0o4000 > cur.perm & 0o4000)
                {
                    LibcSnafu { errno: libc::EPERM }.fail()?;
                }

                dirty_attr.perm = new_attr.perm;
                changed = true;
            }
        }
        if flags.contains(SetAttrFlags::ATIME_NOW) {
            if ctx.check(inode, cur, MODE_MASK_W).is_err() {
                ensure!(ctx.uid == cur.uid, LibcSnafu { errno: libc::EPERM });
            }
            dirty_attr.atime = now;
            changed = true;
        } else if flags.contains(SetAttrFlags::ATIME) {
            ensure!(
                ctx.uid == cur.uid,
                LibcSnafu {
                    errno: libc::EACCES
                }
            );
            if ctx.check(inode, cur, MODE_MASK_W).is_err() {
                ensure!(
                    ctx.uid == cur.uid,
                    LibcSnafu {
                        errno: libc::EACCES
                    }
                );
            }
            dirty_attr.atime = new_attr.atime;
            changed = true;
        }
        if flags.contains(SetAttrFlags::MTIME_NOW) {
            if ctx.check(inode, cur, MODE_MASK_W).is_err() {
                ensure!(
                    ctx.uid == cur.uid,
                    LibcSnafu {
                        errno: libc::EACCES
                    }
                );
            }
            dirty_attr.mtime = now;
            changed = true;
        } else if flags.contains(SetAttrFlags::MTIME) {
            ensure!(
                ctx.uid == cur.uid,
                LibcSnafu {
                    errno: libc::EACCES
                }
            );
            if ctx.check(inode, cur, MODE_MASK_W).is_err() && ctx.uid != cur.uid {
                LibcSnafu {
                    errno: libc::EACCES,
                }
                .fail()?;
            }
            dirty_attr.mtime = new_attr.mtime;
            changed = true;
        }
        if flags.contains(SetAttrFlags::FLAG) {
            dirty_attr.flags = flags.0;
            changed = true;
        }
        if !changed {
            dirty_attr = cur.clone();
        }
        Ok(dirty_attr)
    }

    // Open checks permission on a node and track it as open.
    pub async fn open_inode(&self, ctx: &FuseContext, inode: Ino, flags: i32) -> Result<InodeAttr> {
        if self.config.read_only
            && flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_TRUNC | libc::O_APPEND) != 0
        {
            LibcSnafu { errno: libc::EROFS }.fail()?;
        }

        defer!(self.refresh_atime(ctx, inode));

        if !self.config.open_cache.is_zero() {
            if let Some(attr) = self.open_files.open_check(inode) {
                return Ok(attr);
            }
        }

        let mut attr = self.backend.get_attr(inode)?;
        let mask = match flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
            libc::O_RDONLY => MODE_MASK_R,
            libc::O_WRONLY => MODE_MASK_W,
            libc::O_RDWR => MODE_MASK_W | MODE_MASK_R,
            _ => LibcSnafu {
                errno: libc::EINVAL,
            }
            .fail()?,
        };

        ctx.check(inode, &attr, mask)?;

        let attr_flags = kiseki_types::attr::Flags::from_bits(attr.flags as u8).unwrap();
        if (attr_flags.contains(kiseki_types::attr::Flags::IMMUTABLE) || attr.parent.is_trash())
            && flags & (libc::O_WRONLY | libc::O_RDWR) != 0
        {
            LibcSnafu { errno: libc::EPERM }.fail()?;
        }
        if attr_flags.contains(kiseki_types::attr::Flags::APPEND) {
            if flags & (libc::O_WRONLY | libc::O_RDWR) != 0 && flags & libc::O_APPEND == 0 {
                LibcSnafu { errno: libc::EPERM }.fail()?;
            }
            if flags & libc::O_TRUNC != 0 {
                LibcSnafu { errno: libc::EPERM }.fail()?;
            }
        }
        self.open_files.open(inode, &mut attr);
        Ok(attr)
    }

    fn refresh_atime(&self, _ctx: &FuseContext, _inode: Ino) {}

    // Write put a slice of data on top of the given chunk.
    #[instrument(skip(self, mtime), fields(inode=?inode, chunk_idx=?chunk_idx, chunk_pos=?chunk_pos, slice=?slice))]
    pub async fn write_slice(
        &self,
        inode: Ino,
        chunk_idx: usize,
        chunk_pos: usize,
        slice: Slice, // FIXME: introduce another slice type.
        mtime: Instant,
    ) -> Result<()> {
        assert!(
            matches!(slice, Slice::Owned { .. }),
            "slice should be owned for writing slice"
        );
        debug!(
            "write-slice: with inode {:?}, chunk_idx {:?}, off {:?}, slice_id {:?}, mtime {:?}",
            inode, chunk_idx, chunk_pos, slice, mtime
        );

        // check if the inode is a file
        let mut attr = self.backend.get_attr(inode)?;
        ensure!(attr.is_file(), LibcSnafu { errno: libc::EPERM });

        let mut slices_buf = self
            .backend
            .get_raw_chunk_slices(inode, chunk_idx)?
            .unwrap_or(vec![]);

        let mut dir_stat_length: i64 = 0;
        let mut dir_stat_space: i64 = 0;
        let new_len =
            chunk_idx as u64 * CHUNK_SIZE as u64 + chunk_pos as u64 + slice.get_size() as u64;
        if new_len > attr.length {
            dir_stat_length = new_len as i64 - attr.length as i64;
            dir_stat_space = kiseki_utils::align::align4k(new_len - attr.length);
            debug!(
                "update inode: {} old_length: {} new_length: {}",
                inode, attr.length, new_len
            );
            attr.length = new_len;
        }
        let now = SystemTime::now();
        attr.mtime = now;
        attr.ctime = now;
        let val = slice.encode();
        if slices_buf.eq(&val) {
            warn!(
                "{inode} try to write the same slice {:?} at {chunk_idx}",
                slice
            );
            return Ok(());
        }
        slices_buf.extend_from_slice(&val);
        self.backend.set_attr(inode, &attr)?;
        let slice_cnt = slices_buf.len() / SLICE_BYTES; // number of slices
        self.backend
            .set_raw_chunk_slices(inode, chunk_idx, slices_buf)?;

        if slice_cnt > 350 || slice_cnt % 100 == 99 {
            // start a background task to compact these slices
            // TODO: we need to do compaction
        }
        self.update_parent_stats(inode, attr.parent, dir_stat_length, dir_stat_space)?;

        // TODO: update the cache
        if let Some(mut open_file) = self.open_files.files.get_mut(&inode) {
            // invalidate the cache.
            open_file.chunks.remove(&chunk_idx);
        }

        Ok(())
    }

    fn update_parent_stats(&self, _inode: Ino, parent: Ino, length: i64, space: i64) -> Result<()> {
        if length == 0 && space == 0 {
            return Ok(());
        }

        self.update_fs_stat(space, 0);
        if !self.format.dir_stats {
            return Ok(());
        }
        if parent.0 > 0 {
            self.update_mem_dir_stat(parent, length, space, 0)?;
        }

        // WTF

        Ok(())
    }

    fn update_fs_stat(&self, space: i64, inodes: i64) {
        let old = self.fs_stat.load();
        let mut new = old.clone();
        new.total_size += space as u64;
        new.file_count += inodes as u64;
    }

    fn update_mem_dir_stat(&self, ino: Ino, length: i64, space: i64, inodes: i64) -> Result<()> {
        if !self.format.dir_stats {
            return Ok(());
        }

        match self.dir_stats.get_mut(&ino) {
            None => {
                self.dir_stats.insert(
                    ino,
                    DirStat {
                        length,
                        space,
                        inodes,
                    },
                );
            }
            Some(mut old) => {
                old.length += length;
                old.space += space;
                old.inodes += inodes;
            }
        }

        Ok(())
    }

    /// [MetaEngine::set_lk] sets a file range lock on given file.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_lk(
        &self,
        _ctx: &FuseContext,
        inode: Ino,
        owner: u64,
        block: bool,
        ltype: libc::c_int,
        start: u64,
        end: u64,
    ) -> Result<()> {
        debug!(
            "set_lk with inode {:?}, owner {:?}, block {:?}, ltype {:?}, start {:?}, end {:?}",
            inode, owner, block, ltype, start, end
        );
        Ok(())
    }

    /// [MetaEngine::read_slice] returns the rangemap of slices on the given
    /// chunk.
    pub async fn read_slice(&self, inode: Ino, chunk_index: usize) -> Result<Option<Arc<Slices>>> {
        debug!(
            "read_slice with inode {:?}, chunk_index {:?}",
            inode, chunk_index
        );

        // TODO: update access time

        if let Some(of) = self.open_files.files.get_mut(&inode) {
            if let Some(svs) = of.chunks.get(&chunk_index) {
                debug!(
                    "read ino {} chunk {} slice info from cache, get count {}",
                    inode,
                    chunk_index,
                    svs.len(),
                );
                return Ok(Some(svs.clone()));
            }
        }

        let slices = self.backend.get_chunk_slices(inode, chunk_index)?;
        // let slice_info_buf = match slice_info_buf {
        //     Some(buf) => buf,
        //     None => {
        //         let attr = self.sto_must_get_attr(inode).await?;
        //         ensure!(attr.is_file(), LibcSnafu { errno: libc::EPERM });
        //         return Ok(None);
        //     }
        // };

        // fixme
        let slices = Arc::new(slices);
        // TODO: build cache.
        self.open_files
            .update_chunk_slices_info(inode, chunk_index, slices.clone());

        Ok(Some(slices))
    }
}
