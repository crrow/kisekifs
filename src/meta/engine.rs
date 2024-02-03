// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    cmp::{max, min},
    collections::HashMap,
    fmt::{Debug, Formatter},
    future::Future,
    ops::BitOr,
    os::unix::ffi::OsStrExt,
    path::{Component, Path},
    sync::{
        atomic::{AtomicI64, Ordering::Acquire},
        Arc,
    },
    time::SystemTime,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use dashmap::DashMap;
use fuser::FileType;
use futures::TryStream;
use lazy_static::lazy_static;
use libc::EACCES;
use opendal::{raw::oio::WriteExt, ErrorKind, Operator};
use rangemap::RangeMap;
use scopeguard::defer;
use snafu::ResultExt;
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::meta::types::{OverlookedSlicesRef, Slice, Slices};
use crate::{
    common::err::ToErrno,
    meta::{
        config::{Format, MetaConfig},
        engine_sto::generate_sto_entry_key_str,
        err::*,
        internal_nodes::{InternalNode, TRASH_INODE_NAME},
        types::{
            DirStat, Entry, EntryInfo, FSStates, Ino, InodeAttr, ROOT_INO, SLICE_BYTES, TRASH_INODE,
        },
        util::*,
        MetaContext, SetAttrFlags, DOT, DOT_DOT, MODE_MASK_R, MODE_MASK_W, MODE_MASK_X,
    },
    vfs::storage::DEFAULT_CHUNK_SIZE,
};

pub(crate) const INODE_BATCH: u64 = 1 << 10;
pub(crate) const SLICE_ID_BATCH: u64 = 4 << 10;

lazy_static! {
    static ref COUNTER_LOCKERS: DashMap<Counter, tokio::sync::RwLock<()>> = {
        let mut map = DashMap::new();
        for counter in COUNTER_ENUMS.iter() {
            map.insert(counter.clone(), tokio::sync::RwLock::new(()));
        }
        map
    };
}

// FIXME: use a better way.
const COUNTER_ENUMS: [Counter; 6] = [
    Counter::UsedSpace,
    Counter::TotalInodes,
    Counter::LegacySessions,
    Counter::NextTrash,
    Counter::NextInode,
    Counter::NextSlice,
];
const COUNTER_STRINGS: [&str; 6] = [
    "used_space",
    "total_inodes",
    "legacy_sessions",
    "next_trash",
    "next_inode",
    "next_slice",
];
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub(crate) enum Counter {
    UsedSpace,
    TotalInodes,
    LegacySessions,
    NextTrash,
    NextInode,
    NextSlice,
}

impl Counter {
    pub fn to_str(&self) -> &'static str {
        match self {
            Counter::UsedSpace => COUNTER_STRINGS[0],
            Counter::TotalInodes => COUNTER_STRINGS[1],
            Counter::LegacySessions => COUNTER_STRINGS[2],
            Counter::NextTrash => COUNTER_STRINGS[3],
            Counter::NextInode => COUNTER_STRINGS[4],
            Counter::NextSlice => COUNTER_STRINGS[5],
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
        drop(guard);
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
        defer!(drop(guard));
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
    next_max_pair: tokio::sync::RwLock<(u64, u64)>,
    operator: Arc<Operator>,
    counter: Counter,
    step: u64,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new(operator: Arc<Operator>, counter: Counter, step: u64) -> Self {
        Self {
            next_max_pair: tokio::sync::RwLock::new((0, 0)),
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
    pub chunks: HashMap<usize, Arc<Slices>>, // should we add lock on it ?
}

pub struct OpenFiles {
    ttl: Duration,
    limit: usize,
    pub(crate) files: DashMap<Ino, OpenFile>,
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
    pub(crate) fn open(&self, inode: Ino, attr: &mut InodeAttr) {
        match self.files.get_mut(&inode) {
            None => {
                self.files.insert(
                    inode,
                    OpenFile {
                        attr: attr.keep_cache().clone(),
                        reference_count: 1,
                        last_check: std::time::Instant::now(),
                        chunks: HashMap::new(),
                    },
                );
            }
            Some(mut op) => {
                let mut op = op.value_mut();
                if op.attr.mtime == attr.mtime {
                    attr.keep_cache = op.attr.keep_cache;
                }
                op.attr.keep_cache = true;
                op.reference_count += 1;
                op.last_check = std::time::Instant::now();
            }
        }
    }

    pub(crate) fn open_check(&self, ino: Ino) -> Option<InodeAttr> {
        if let Some(mut of) = self.files.get_mut(&ino) {
            let mut of = of.value_mut();
            if of.last_check.elapsed() < self.ttl {
                of.reference_count += 1;
                return Some(of.attr.clone());
            }
        }

        return None;
    }

    pub(crate) fn update_chunk_slices_info(&self, ino: Ino, chunk_idx: usize, views: Arc<Slices>) {
        if let Some(mut of) = self.files.get_mut(&ino) {
            let mut of = of.value_mut();
            of.chunks.insert(chunk_idx, views);
        }
    }

    pub(crate) fn invalidate_chunk_slice_info(&self, inode: Ino, chunk_idx: usize) {
        defer!(debug!("invalidate ino: {}, chunk: {},  slice info succeed", inode, chunk_idx););
        if let Some(mut of) = self.files.get_mut(&inode) {
            let mut of = of.value_mut();
            of.chunks.remove(&chunk_idx);
        }
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
    pub(crate) format: tokio::sync::RwLock<Format>,
    root: Ino,
    pub(crate) operator: Arc<Operator>,
    sub_trash: Option<InternalNode>,
    open_files: OpenFiles,
    dir_parents: DashMap<Ino, Ino>,
    pub(crate) fs_states: FSStatesInner,
    free_inodes: IdTable,
    free_slices: IdTable,
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
            format: tokio::sync::RwLock::new(Format::default()),
            root: ROOT_INO,
            operator: op.clone(),
            sub_trash: None,
            open_files: OpenFiles::new(config.open_cache, config.open_cache_limit),
            dir_parents: DashMap::new(),
            fs_states: Default::default(),
            free_inodes: IdTable::new(op.clone(), Counter::NextInode, INODE_BATCH),
            free_slices: IdTable::new(op.clone(), Counter::NextSlice, SLICE_ID_BATCH),
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
                self.sto_set_attr(TRASH_INODE, &basic_attr).await?;
            }
        }
        self.sto_set_format(&format).await?;
        if need_init_root {
            basic_attr.set_perm(0o777);
            tokio::try_join!(
                self.sto_set_attr(ROOT_INO, &basic_attr),
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

    pub async fn next_slice_id(&self) -> Result<usize> {
        let s = self
            .free_slices
            .next()
            .await
            .context(ErrFailedToDoCounterSnafu)?;
        Ok(s as usize)
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
        info!(dir=?inode, "readdir in plus?, {plus}");
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
        debug!("read entries from sto: { }", sto_entries.len());
        for sto_entry in &sto_entries {
            let name = sto_entry.name();
            info!("read entry in sto: {name}");
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
            info!("found entry info: ino:{}, name: {}", entry_info.inode, name);
            basic_entries.push(Entry::new(entry_info.inode, name, entry_info.typ));
        }

        // TODO: optimize me
        if plus && basic_entries.len() != 0 {
            info!("in plus mode, get dir entries' attr");
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
            // .set_flags(flags) // TODO, maybe we don't have to
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

        if Flags::from_bits(parent_attr.flags as u8)
            .unwrap()
            .contains(Flags::IMMUTABLE)
        {
            return Err(MetaError::ErrMknod { kind: libc::EPERM })?;
        }

        // check if the entry already exists
        match self.sto_get_entry_info(parent, name).await {
            Ok(_) => return Err(MetaError::ErrMknod { kind: libc::EEXIST }),
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
        info!("create entry info: parent: {parent}, name: {name}, ino: {inode}");
        self.sto_set_attr(inode, &attr).await?;
        if update_parent_attr {
            self.sto_set_attr(parent, &parent_attr).await?;
        }
        if typ == FileType::Symlink {
            self.sto_set_sym(inode, path).await?;
        } else if typ == FileType::Directory {
            self.sto_set_dir_stat(inode, DirStat::default()).await?;
        };
        Ok((inode, attr))
    }

    pub async fn create(
        &self,
        ctx: &MetaContext,
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
            Err(e) => {
                warn!("create failed: {:?}", e);
                if e.to_errno() == libc::EEXIST {
                    let rt = self.do_lookup(parent, name).await?;
                    rt
                } else {
                    return Err(e);
                }
            }
        };

        self.open_files.open(x.0, &mut x.1);
        return Ok(x);
    }
    pub async fn check_set_attr(
        &self,
        ctx: &MetaContext,
        ino: Ino,
        flags: SetAttrFlags,
        new_attr: &mut InodeAttr,
    ) -> Result<()> {
        let inode = self.check_root(ino);
        let cur_attr = self.get_attr(inode).await?;
        self.merge_attr(ctx, flags, inode, &cur_attr, new_attr, SystemTime::now())?;

        // TODO: implement me
        return Ok(());
    }
    // SetAttr updates the attributes for given node.
    pub async fn set_attr(
        &self,
        ctx: &MetaContext,
        flags: SetAttrFlags,
        ino: Ino,
        new_attr: &mut InodeAttr,
    ) -> Result<Entry> {
        let inode = self.check_root(ino);

        let cur_attr = self
            .sto_get_attr(inode)
            .await?
            .ok_or_else(|| MetaError::ErrLibc { kind: libc::ENOENT })?;
        if cur_attr.parent.is_trash() {
            return Err(MetaError::ErrLibc { kind: libc::EPERM });
        }
        let mut dirty_attr =
            self.merge_attr(ctx, flags, ino, &cur_attr, new_attr, SystemTime::now())?;
        dirty_attr.ctime = SystemTime::now();
        self.sto_set_attr(inode, &dirty_attr).await?;
        Ok(Entry::new_with_attr(inode, "", dirty_attr))
    }
    fn merge_attr(
        &self,
        ctx: &MetaContext,
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
                        dirty_attr.perm &= 01777; // WHY ?
                        new_attr.perm &= 01777;
                    } else {
                        // keep SGID if the file is non-group-executable
                        dirty_attr.perm &= 03777;
                        new_attr.perm &= 03777;
                    }
                }
            }
            changed = true;
        }
        if flags.contains(SetAttrFlags::GID) {
            if ctx.uid != 0 && ctx.uid != cur.uid {
                return Err(MetaError::ErrLibc { kind: libc::EPERM });
            }
            if cur.gid != new_attr.gid {
                if ctx.check_permission && cur.uid != 0 && !ctx.contains_gid(new_attr.gid) {
                    return Err(MetaError::ErrLibc { kind: libc::EPERM });
                }
                dirty_attr.gid = new_attr.gid;
                changed = true;
            }
        }
        if flags.contains(SetAttrFlags::UID) && cur.uid != new_attr.uid {
            if ctx.uid != 0 {
                return Err(MetaError::ErrLibc { kind: libc::EPERM });
            }
            dirty_attr.uid = new_attr.uid;
            changed = true;
        }
        if flags.contains(SetAttrFlags::MODE) {
            if ctx.uid != 0 && new_attr.perm & 0o2000 != 0 {
                if ctx.uid != cur.gid {
                    new_attr.perm &= 0o5777;
                }
            }
            if cur.perm != new_attr.perm {
                if ctx.uid != 0 && ctx.uid != cur.uid {
                    if (cur.perm & 01777 != new_attr.perm & 01777
                        || new_attr.perm & 02000 > cur.perm & 02000
                        || new_attr.perm & 04000 > cur.perm & 04000)
                    {
                        return Err(MetaError::ErrLibc { kind: libc::EPERM });
                    }
                }

                dirty_attr.perm = new_attr.perm;
                changed = true;
            }
        }
        if flags.contains(SetAttrFlags::ATIME_NOW) {
            if let Err(_) = access(ctx, inode, cur, MODE_MASK_W) {
                if ctx.uid != cur.uid {
                    return Err(MetaError::ErrLibc { kind: libc::EACCES });
                }
            }
            dirty_attr.atime = now;
            changed = true;
        } else if flags.contains(SetAttrFlags::ATIME) {
            if ctx.uid != cur.uid {
                return Err(MetaError::ErrLibc { kind: libc::EACCES });
            }
            if let Err(_) = access(ctx, inode, cur, MODE_MASK_W) {
                if ctx.uid != cur.uid {
                    return Err(MetaError::ErrLibc { kind: libc::EACCES });
                }
            }
            dirty_attr.atime = new_attr.atime;
            changed = true;
        }
        if flags.contains(SetAttrFlags::MTIME_NOW) {
            if let Err(_) = access(ctx, inode, cur, MODE_MASK_W) {
                if ctx.uid != cur.uid {
                    return Err(MetaError::ErrLibc { kind: libc::EACCES });
                }
            }
            dirty_attr.mtime = now;
            changed = true;
        } else if flags.contains(SetAttrFlags::MTIME) {
            if ctx.uid != cur.uid {
                return Err(MetaError::ErrLibc { kind: libc::EACCES });
            }
            if let Err(_) = access(ctx, inode, cur, MODE_MASK_W) {
                if ctx.uid != cur.uid {
                    return Err(MetaError::ErrLibc { kind: libc::EACCES });
                }
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
    pub async fn open_inode(&self, ctx: &MetaContext, inode: Ino, flags: i32) -> Result<InodeAttr> {
        if self.config.read_only
            && flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_TRUNC | libc::O_APPEND) != 0
        {
            return Err(MetaError::ErrLibc { kind: libc::EROFS });
        }

        defer!(self.touch_atime(ctx, inode));

        if !self.config.open_cache.is_zero() {
            if let Some(attr) = self.open_files.open_check(inode) {
                return Ok(attr);
            }
        }

        let mut attr = self.sto_must_get_attr(inode).await?;
        let mask = match flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
            libc::O_RDONLY => MODE_MASK_R,
            libc::O_WRONLY => MODE_MASK_W,
            libc::O_RDWR => MODE_MASK_W | MODE_MASK_R,
            _ => return Err(MetaError::ErrLibc { kind: libc::EINVAL }),
        };

        access(ctx, inode, &attr, mask)?;

        let attr_flags = Flags::from_bits(attr.flags as u8).unwrap();
        if attr_flags.contains(Flags::IMMUTABLE) || attr.parent.is_trash() {
            if flags & (libc::O_WRONLY | libc::O_RDWR) != 0 {
                return Err(MetaError::ErrLibc { kind: libc::EPERM });
            }
        }
        if attr_flags.contains(Flags::APPEND) {
            if flags & (libc::O_WRONLY | libc::O_RDWR) != 0 && flags & libc::O_APPEND == 0 {
                return Err(MetaError::ErrLibc { kind: libc::EPERM });
            }
            if flags & libc::O_TRUNC != 0 {
                return Err(MetaError::ErrLibc { kind: libc::EPERM });
            }
        }
        self.open_files.open(inode, &mut attr);
        Ok(attr)
    }

    fn touch_atime(&self, ctx: &MetaContext, inode: Ino) {}

    // Write put a slice of data on top of the given chunk.
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
        trace!(
            "write-slice: with inode {:?}, chunk_idx {:?}, off {:?}, slice_id {:?}, mtime {:?}",
            inode,
            chunk_idx,
            chunk_pos,
            slice,
            mtime
        );

        // TODO: juicefs lock the open file here, should we also lock it ?
        if let Some(mut open_file) = self.open_files.files.get_mut(&inode) {
            // invalidate the cache.
            open_file.chunks.remove(&chunk_idx);
        }

        let mut attr = self.sto_get_attr(inode).await?.unwrap_or(
            InodeAttr::default()
                .set_kind(FileType::RegularFile)
                .to_owned(),
        );
        if !attr.is_file() {
            return Err(MetaError::ErrLibc { kind: libc::EPERM })?;
        }

        let mut slices_buf = self
            .sto_get_chunk_info(inode, chunk_idx)
            .await?
            .unwrap_or(vec![]);
        if slices_buf.len() % SLICE_BYTES != 0 {
            error!(
                "Invalid chunk value for inode {} chunk_idx {}: {}",
                inode,
                chunk_idx,
                slices_buf.len()
            );
            return Err(MetaError::ErrLibc { kind: libc::EIO });
        }

        let mut dir_stat_length: i64 = 0;
        let mut dir_stat_space: i64 = 0;
        let new_len = chunk_idx as u64 * DEFAULT_CHUNK_SIZE as u64
            + chunk_pos as u64
            + slice.get_size() as u64;
        if new_len > attr.length {
            dir_stat_length = new_len as i64 - attr.length as i64;
            dir_stat_space = align4k(new_len - attr.length);
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
        self.sto_set_attr(inode, &attr).await?;
        let slice_cnt = slices_buf.len() / SLICE_BYTES; // number of slices
        self.sto_set_chunk_info(inode, chunk_idx, slices_buf)
            .await?;

        if slice_cnt > 350 || slice_cnt % 100 == 99 {
            // start a background task to compact these slices
            // TODO: we need to do compaction
        }
        self.update_parent_stats(inode, attr.parent, dir_stat_length, dir_stat_space)
            .await?;

        Ok(())
    }

    /// [MetaEngine::set_lk] sets a file range lock on given file.
    pub async fn set_lk(
        &self,
        ctx: &MetaContext,
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

    /// [MetaEngine::read_slice] returns the rangemap of slices on the given chunk.
    pub async fn read_slice(&self, inode: Ino, chunk_index: usize) -> Result<Option<Arc<Slices>>> {
        debug!(
            "read_slice with inode {:?}, chunk_index {:?}",
            inode, chunk_index
        );

        // TODO: update access time
        let of = self.open_files.files.get_mut(&inode);
        if let Some(of) = of {
            if let Some(svs) = of.chunks.get(&chunk_index) {
                debug!(
                    "read ino {} chunk {} slice info from cache, get count {}",
                    inode,
                    chunk_index,
                    svs.len(),
                );
                return Ok(Some(svs.clone()));
            }
        };

        let slice_info_buf = self.sto_get_chunk_info(inode, chunk_index).await?;
        let slice_info_buf = match slice_info_buf {
            Some(buf) => buf,
            None => {
                let attr = self.sto_must_get_attr(inode).await?;
                if !attr.is_file() {
                    return Err(MetaError::ErrLibc { kind: libc::EPERM })?;
                }
                return Ok(None);
            }
        };

        let slices = Arc::new(Slices::decode(&slice_info_buf)?);
        // TODO: build cache.
        self.open_files
            .update_chunk_slices_info(inode, chunk_index, slices.clone());
        Ok(Some(slices))
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
        return Err(MetaError::ErrLibc { kind: EACCES })?;
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
    use crate::meta::types::Slice;
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

    #[tokio::test]
    async fn list_entries() {
        let mut builder = opendal::services::Memory::default();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.as_ref().to_str().unwrap();
        builder.root(tempdir_path);

        let op = Arc::new(Operator::new(builder).unwrap().finish());

        let meta_engine = MetaEngine {
            config: MetaConfig::default(),
            format: tokio::sync::RwLock::new(Format::default()),
            root: ROOT_INO,
            operator: op.clone(),
            sub_trash: None,
            open_files: OpenFiles::new(Duration::from_secs(0), 0),
            dir_parents: DashMap::new(),
            fs_states: Default::default(),
            free_inodes: IdTable::new(op.clone(), Counter::NextInode, INODE_BATCH),
            free_slices: IdTable::new(op.clone(), Counter::NextSlice, SLICE_ID_BATCH),
            dir_stats: DashMap::new(),
        };

        meta_engine
            .sto_set_entry_info(Ino(1), "a", EntryInfo::new(Ino(2), FileType::RegularFile))
            .await
            .unwrap();
        meta_engine
            .sto_set_entry_info(Ino(1), "b", EntryInfo::new(Ino(3), FileType::RegularFile))
            .await
            .unwrap();

        let entry_infos = meta_engine.sto_list_entry_info(Ino(1)).await.unwrap();
        assert_eq!(entry_infos.len(), 2);
    }
}
