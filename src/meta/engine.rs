use std::{
    cmp::{max, min},
    collections::HashMap,
    fmt::{Debug, Formatter},
    os::unix::ffi::OsStrExt,
    path::{Component, Path},
    sync::{
        atomic::{AtomicI64, Ordering::Acquire},
        Arc,
    },
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use dashmap::DashMap;
use fuser::FileType;
use futures::TryStream;
use lazy_static::lazy_static;
use libc::c_int;
use opendal::{ErrorKind, Operator};
use snafu::{ResultExt, Snafu};
use tokio::{
    sync::RwLock,
    time::{timeout, Duration, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    common::err::ToErrno,
    meta::{
        config::{Format, MetaConfig},
        engine_sto::generate_sto_entry_key_str,
        err::*,
        internal_nodes::{InternalNode, TRASH_INODE_NAME},
        types::{DirStat, Entry, EntryInfo, FSStates, Ino, InodeAttr, ROOT_INO, TRASH_INODE},
        util::*,
        MetaContext, DOT, DOT_DOT, MODE_MASK_R, MODE_MASK_W, MODE_MASK_X,
    },
};

// Slice is a slice of a chunk.
// Multiple slices could be combined together as a chunk.
pub struct Slice {
    id: u64,
    size: u32,
    off: u32,
    len: u32,
}

pub(crate) const INODE_BATCH: u64 = 1 << 10;

lazy_static! {
    static ref COUNTER_LOCKERS: DashMap<Counter, RwLock<()>> = {
        let mut map = DashMap::new();
        for counter in COUNTER_ENUMS.iter() {
            map.insert(counter.clone(), RwLock::new(()));
        }
        map
    };
}

// FIXME: use a better way.
const COUNTER_ENUMS: [Counter; 5] = [
    Counter::UsedSpace,
    Counter::TotalInodes,
    Counter::LegacySessions,
    Counter::NextTrash,
    Counter::NextInode,
];
const COUNTER_STRINGS: [&str; 5] = [
    "used_space",
    "total_inodes",
    "legacy_sessions",
    "next_trash",
    "next_inode",
];
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub(crate) enum Counter {
    UsedSpace,
    TotalInodes,
    LegacySessions,
    NextTrash,
    NextInode,
}

impl Counter {
    pub fn to_str(&self) -> &'static str {
        match self {
            Counter::UsedSpace => COUNTER_STRINGS[0],
            Counter::TotalInodes => COUNTER_STRINGS[1],
            Counter::LegacySessions => COUNTER_STRINGS[2],
            Counter::NextTrash => COUNTER_STRINGS[3],
            Counter::NextInode => COUNTER_STRINGS[4],
        }
    }
    // FIXME: do we actually need it ?
    async fn get_counter_with_lock(
        &self,
        operator: Arc<Operator>,
    ) -> std::result::Result<i64, opendal::Error> {
        let counter_key = self.generate_sto_key();
        let locker_ref = COUNTER_LOCKERS.get(self).unwrap();
        let locker_ref = locker_ref.value();
        let guard = locker_ref.read().await;
        let counter = match operator.read(&counter_key).await {
            Ok(ref buf) => buf
                .as_slice()
                .read_i64::<LittleEndian>()
                .expect("read counter error"),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    0
                } else {
                    return Err(e);
                }
            }
        };
        Ok(counter)
    }
    pub async fn load(&self, operator: Arc<Operator>) -> std::result::Result<u64, opendal::Error> {
        let counter_key = self.generate_sto_key();
        let counter = match operator.read(&counter_key).await {
            Ok(ref buf) => {
                let v = buf
                    .as_slice()
                    .read_u64::<LittleEndian>()
                    .expect("read counter error");
                v
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    0
                } else {
                    return Err(e);
                }
            }
        };
        Ok(counter)
    }
    pub fn generate_sto_key(&self) -> String {
        format!("C{}", self.to_str())
    }
    pub async fn increment(
        &self,
        operator: Arc<Operator>,
    ) -> std::result::Result<u64, opendal::Error> {
        self.increment_by(operator, 1).await
    }
    pub async fn increment_by(
        &self,
        operator: Arc<Operator>,
        by: u64,
    ) -> std::result::Result<u64, opendal::Error> {
        let counter_key = self.generate_sto_key();
        let locker_ref = COUNTER_LOCKERS.get(self).unwrap();
        let locker_ref = locker_ref.value();
        let guard = locker_ref.write().await;
        let counter = match operator.read(&counter_key).await {
            Ok(ref buf) => {
                let v: u64 = buf
                    .as_slice()
                    .read_u64::<LittleEndian>()
                    .expect("read counter error");
                v
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    0
                } else {
                    return Err(e);
                }
            }
        };
        let counter = counter + by;
        let mut buf = vec![0u8; 8];
        buf.as_mut_slice()
            .write_u64::<LittleEndian>(counter)
            .unwrap();
        operator.write(&counter_key, buf).await?;
        Ok(counter)
    }

    pub async fn load_counter(&self) {}
}

/// A table for allocating inode numbers.
/// It starts at 2 since the root inode is 1.
pub struct IdTable {
    next_max_pair: RwLock<(u64, u64)>,
    operator: Arc<Operator>,
    counter: Counter,
    step: u64,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new(operator: Arc<Operator>, counter: Counter, step: u64) -> Self {
        Self {
            next_max_pair: RwLock::new((0, 0)),
            operator,
            counter,
            step,
        }
    }

    /// Return the next unused ID from the table.
    pub async fn next(&self) -> std::result::Result<u64, opendal::Error> {
        let mut next_max_pair = self.next_max_pair.write().await;
        if next_max_pair.0 >= next_max_pair.1 {
            let new_max = self
                .counter
                .increment_by(self.operator.clone(), self.step)
                .await?;
            next_max_pair.0 = new_max - self.step;
            next_max_pair.1 = new_max;
        }
        let mut next = next_max_pair.0;
        next_max_pair.0 += 1;
        while next <= 1 {
            next = next_max_pair.0;
            next_max_pair.0 += 1;
        }
        Ok(next)
    }
}

pub struct OpenFile {
    pub attr: InodeAttr,
    pub reference_count: usize,
    pub last_check: std::time::Instant,
    pub chunks: HashMap<u32, Vec<Slice>>,
}

pub struct OpenFiles {
    ttl: Duration,
    limit: usize,
    files: DashMap<Ino, OpenFile>,
    // TODO: background clean up
}

impl OpenFiles {
    pub(crate) fn new(ttl: Duration, limit: usize) -> Self {
        Self {
            ttl,
            limit,
            files: Default::default(),
        }
    }

    pub(crate) fn check(&self, ino: Ino) -> Option<InodeAttr> {
        self.files.get(&ino).and_then(|f| {
            if f.last_check.elapsed() < self.ttl {
                Some(f.attr.clone())
            } else {
                None
            }
        })
    }

    pub(crate) fn update(&self, ino: Ino, attr: &mut InodeAttr) -> bool {
        self.files
            .get_mut(&ino)
            .and_then(|mut open_file| {
                if attr.mtime != open_file.attr.mtime {
                    open_file.chunks = HashMap::new();
                } else {
                    attr.keep_cache = open_file.attr.keep_cache;
                }
                open_file.attr = attr.clone();
                open_file.last_check = std::time::Instant::now();
                return Some(());
            })
            .is_some()
    }
}

#[derive(Debug, Default)]
pub(crate) struct FSStatesInner {
    pub(crate) new_space: AtomicI64,
    pub(crate) new_inodes: AtomicI64,
    pub(crate) used_space: AtomicI64,
    pub(crate) used_inodes: AtomicI64,
}

/// MetaEngine describes a meta service for file system.
pub struct MetaEngine {
    pub(crate) config: MetaConfig,
    pub(crate) format: RwLock<Format>,
    root: Ino,
    pub(crate) operator: Arc<Operator>,
    sub_trash: Option<InternalNode>,
    open_files: OpenFiles,
    dir_parents: DashMap<Ino, Ino>,
    pub(crate) fs_states: FSStatesInner,
    free_inodes: IdTable,
    pub(crate) dir_stats: DashMap<Ino, DirStat>,
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
            dir_stats: DashMap::new(),
        };
        Ok(m)
    }
    // Init is used to initialize a meta service.
    pub async fn init(&self, format: Format, force: bool) -> Result<()> {
        info!("do init ...");
        let mut need_init_root = false;
        if let Some(old_format) = self.sto_get_format().await? {
            if !old_format.dir_stats && format.dir_stats {
                // remove dir stats as they are outdated
            }

            // TODO: update the old format
        } else {
            need_init_root = true;
        }

        let mut guard = self.format.write().await;
        *guard = format.clone();

        let mut basic_attr = InodeAttr::default()
            .set_kind(FileType::Directory)
            .set_nlink(2)
            .set_length(4 << 10)
            .set_parent(ROOT_INO)
            .to_owned();

        if format.trash_days > 0 {
            if let None = self.sto_get_attr(TRASH_INODE).await? {
                basic_attr.set_perm(0o555);
                self.sto_set_attr(TRASH_INODE, basic_attr.clone()).await?;
            }
        }
        self.sto_set_format(&format).await?;
        if need_init_root {
            basic_attr.set_perm(0o777);
            tokio::try_join!(
                self.sto_set_attr(ROOT_INO, basic_attr.clone()),
                self.sto_increment_counter(Counter::NextInode, 2),
            )?;
        }

        Ok(())
    }
    pub fn dump_config(&self) -> MetaConfig {
        self.config.clone()
    }
    pub fn info(&self) -> String {
        format!("meta-{}", self.config.scheme)
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

        let total_space = if format.capacity_in_bytes > 0 {
            min(format.capacity_in_bytes, used_space as u64)
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

    /// Lookup returns the inode and attributes for the given entry in a
    /// directory.
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

    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        debug!("get_attr with inode {:?}", inode);
        let inode = self.check_root(inode);
        // check cache
        if let Some(attr) = self.open_files.check(inode) {
            return Ok(attr);
        }

        let mut attr = if inode.is_trash() || inode.is_root() {
            // call do_get_attr with timeout
            //
            // In the timeout case, we give the root and trash inodes a default hard code
            // value.
            //
            // Availability: The Root and Trash inodes are critical for filesystem
            // operations. Providing default values guarantees that they're
            // always accessible, even under slow or unreliable conditions.
            // Consistency: Ensuring consistent behavior for these inodes, even with
            // timeouts, helps maintain filesystem integrity.
            timeout(Duration::from_millis(300), self.sto_must_get_attr(inode))
                .await
                .unwrap_or(Ok(InodeAttr::hard_code_inode_attr(inode.is_trash())))?
        } else {
            self.sto_must_get_attr(inode).await?
        };

        // update cache
        self.open_files.update(inode, &mut attr);
        if attr.is_filetype(FileType::Directory) && !inode.is_root() && !attr.parent.is_trash() {
            self.dir_parents.insert(inode, attr.parent);
        }
        Ok(attr)
    }

    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, InodeAttr)> {
        let entry_info = self.sto_get_entry_info(parent, name).await?;
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

    // Readdir returns all entries for given directory, which include attributes if
    // plus is true.
    pub async fn read_dir(&self, ctx: &MetaContext, inode: Ino, plus: bool) -> Result<Vec<Entry>> {
        info!(dir=?inode, "readdir");
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
            } else {
                return Err(e);
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
        let entry_prefix = generate_sto_entry_key_str(inode, "");
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

        // TODO: optimize me
        if plus && basic_entries.len() != 0 {
            for e in basic_entries {
                let attr = self.get_attr(e.inode).await?;
                e.attr = attr;
            }
        }
        Ok(())
    }

    // Change root to a directory specified by sub_dir.
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
    #[instrument(skip(self, ctx), fields(parent=?parent, name=?name, typ=?typ, mode=?mode, cumask=?cumask, rdev=?rdev, path=?path))]
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
        let r = self
            .do_mknod(ctx, parent, name, typ, mode, cumask, rdev, path)
            .await?;

        tokio::try_join!(
            self.update_mem_dir_stat(parent, 0, space, inodes),
            self.update_dir_quota(parent, space, inodes)
        )?;

        Ok(r)
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
            let next = self.sto_increment_counter(Counter::NextTrash, 1).await?;
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

        // FIXME: we need transaction here
        let mut parent_attr = self.sto_must_get_attr(parent).await?;
        if !parent_attr.is_dir() {
            return Err(MetaError::ErrMknod {
                kind: libc::ENOTDIR,
            })?;
        }
        // check if the parent is trash
        if parent_attr.parent.is_trash() {
            return Err(MetaError::ErrMknod { kind: libc::ENOENT })?;
        }

        // check if the parent have the permission
        access(ctx, parent, &parent_attr, MODE_MASK_W)?;
        if parent_attr.juicefs_flags & Flag::Immutable as u8 != 0 {
            return Err(MetaError::ErrMknod { kind: libc::EPERM })?;
        }

        // check if the entry already exists
        match self.sto_get_entry_info(parent, name).await {
            Ok(found) => return Err(MetaError::ErrMknod { kind: libc::EEXIST }),
            Err(e) => {
                if e.to_errno() != libc::ENOENT {
                    return Err(e)?;
                }
            }
        };

        // check if we need to update the parent
        let mut update_parent_attr = false;
        if !parent.is_trash() && typ == FileType::Directory {
            parent_attr.set_nlink(parent_attr.nlink + 1);
            if self.config.skip_dir_nlink <= 0 {
                let now = std::time::SystemTime::now();
                parent_attr.mtime = now;
                parent_attr.ctime = now;
                update_parent_attr = true;
            }
        };

        let now = std::time::SystemTime::now();
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
                attr.perm |= 02000;
            } else if attr.perm & 02010 == 02010 && ctx.uid != 0 {
                if !ctx.gid_list.contains(&parent_attr.gid) {
                    attr.perm &= !02010;
                }
            }
        }

        self.sto_set_entry_info(parent, name, EntryInfo::new(inode, typ))
            .await?;
        self.sto_set_attr(inode, attr).await?;
        if update_parent_attr {
            self.sto_set_attr(parent, parent_attr).await?;
        }
        if typ == FileType::Symlink {
            self.sto_set_sym(inode, path).await?;
        } else if typ == FileType::Directory {
            self.sto_set_dir_stat(inode, DirStat::default()).await?;
        }

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
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn path_components() {
        let p = PathBuf::from("d1");
        for c in p.components() {
            println!("{:?}", c);
        }
    }
    #[tokio::test]
    async fn counter() {
        let mut builder = opendal::services::Memory::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.root(tempdir_path);

        let op = Arc::new(Operator::new(builder).unwrap().finish());
        let counter = Counter::UsedSpace;
        let v = counter.get_counter_with_lock(op.clone()).await.unwrap();
        assert_eq!(v, 0);
        let v = counter.increment(op.clone()).await.unwrap();
        assert_eq!(v, 1);

        let (first, sec) =
            tokio::join!(counter.increment(op.clone()), counter.increment(op.clone()),);
        println!("{}, {}", first.unwrap(), sec.unwrap());

        let v = counter.load(op.clone()).await.unwrap();
        assert_eq!(v, 3);
    }

    #[tokio::test]
    async fn id_table_alloc() {
        let mut builder = opendal::services::Memory::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.root(tempdir_path);

        let op = Arc::new(Operator::new(builder).unwrap().finish());

        let counter = Counter::NextInode;
        let id_table = IdTable::new(op, counter, INODE_BATCH);
        let x = id_table.next().await.unwrap();
        assert_eq!(x, 2);
    }
}
