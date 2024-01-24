/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::atomic::Ordering;

use byteorder::{LittleEndian, WriteBytesExt};
use futures::StreamExt;
use opendal::ErrorKind::NotFound;
use snafu::ResultExt;
use tracing::{debug, instrument};

use crate::meta::{
    engine::{Counter, MetaEngine},
    err::*,
    types::{DirStat, EntryInfo, Ino, InodeAttr},
    Format,
};

// TODO: create a new type for maintain the persistent logic.

impl MetaEngine {
    pub(crate) fn update_mem_fs_stats(&self, space: i64, inodes: i64) {
        self.fs_states.new_space.fetch_add(space, Ordering::AcqRel);
        self.fs_states
            .new_inodes
            .fetch_add(inodes, Ordering::AcqRel);
    }
    pub(crate) async fn update_mem_dir_stat(
        &self,
        ino: Ino,
        length: i64,
        space: i64,
        inodes: i64,
    ) -> Result<()> {
        let guard = self.format.read().unwrap();
        if !guard.dir_stats {
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
    pub(crate) async fn sto_must_get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        let inode_key = inode.generate_key_str();
        let buf = self
            .operator
            .read(&inode_key)
            .await
            .context(ErrFailedToReadFromStoSnafu {
                key: inode_key.to_string(),
            })?;
        let attr: InodeAttr =
            bincode::deserialize(&buf).context(ErrBincodeDeserializeFailedSnafu)?;
        Ok(attr)
    }

    pub(crate) async fn sto_get_attr(&self, inode: Ino) -> Result<Option<InodeAttr>> {
        // TODO: do we need transaction ?
        let inode_key = inode.generate_key_str();
        match self.operator.read(&inode_key).await {
            Ok(buf) => {
                let attr: InodeAttr =
                    bincode::deserialize(&buf).context(ErrBincodeDeserializeFailedSnafu)?;
                Ok(Some(attr))
            }
            Err(e) => {
                if e.kind() == NotFound {
                    Ok(None)
                } else {
                    Err(e).context(ErrFailedToReadFromStoSnafu {
                        key: inode_key.to_string(),
                    })
                }
            }
        }
    }

    // pub(crate) async fn sto_batch_get_attr<I: Iterator<Item = Ino>>(
    //     &self,
    //     inodes: I,
    // ) -> impl Stream<Item = Result<(Ino, InodeAttr)>> {
    //     let f = FuturesUnordered::new();
    //
    //     for x in inodes {
    //         f.push(async { (x, self.sto_get_attr(x).await) });
    //     }
    // }

    pub(crate) async fn sto_set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()> {
        let inode_key = inode.generate_key_str();
        let attr_buf = bincode::serialize(attr).unwrap();
        self.operator
            .write(&inode_key, attr_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu {
                key: inode_key.to_string(),
            })?;
        Ok(())
    }

    pub(crate) async fn sto_get_entry_info(&self, parent: Ino, name: &str) -> Result<EntryInfo> {
        let entry_key = generate_sto_entry_key_str(parent, name);
        let entry_buf = self
            .operator
            .read(&entry_key)
            .await
            .context(ErrFailedToReadFromStoSnafu { key: entry_key })?;

        EntryInfo::parse_from(&entry_buf).context(ErrBincodeDeserializeFailedSnafu)
    }

    pub(crate) async fn sto_list_entry_info(&self, parent: Ino) -> Result<Vec<EntryInfo>> {
        let entry_key = generate_sto_entry_key_str(parent, "");
        let mut stream = self
            .operator
            .list(&entry_key)
            .await
            .context(ErrFailedToReadFromStoSnafu { key: entry_key })?;
        let mut result = vec![];
        for sto_entry in &stream {
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
            result.push(entry_info)
        }
        Ok(result)
    }

    #[instrument(level = "info", skip(self), fields(parent, name, entry_info))]
    pub(crate) async fn sto_set_entry_info(
        &self,
        parent: Ino,
        name: &str,
        entry_info: EntryInfo,
    ) -> Result<()> {
        let entry_key = generate_sto_entry_key_str(parent, name);
        let entry_buf = entry_info.encode();
        self.operator
            .write(&entry_key, entry_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: entry_key })?;
        Ok(())
    }

    pub(crate) async fn sto_set_sym(&self, inode: Ino, path: String) -> Result<()> {
        let sym_key = generate_sto_sym_key_str(inode);
        let sym_buf = path.into_bytes();
        self.operator
            .write(&sym_key, sym_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: sym_key })?;
        Ok(())
    }

    pub(crate) async fn sto_set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()> {
        let dir_stat_key = generate_sto_dir_stat_key_str(inode);
        let dir_stat_buf = dir_stat.encode();
        self.operator
            .write(&dir_stat_key, dir_stat_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu { key: dir_stat_key })?;
        Ok(())
    }

    /// Load loads the existing setting of a formatted volume from meta service.
    pub async fn load_format(&self, check_version: bool) -> Result<Format> {
        debug!("load_format");
        let format = self.sto_get_format().await?;
        let format = if let Some(format) = format {
            if check_version {
                format.check_version()?;
            }
            format
        } else {
            return Err(MetaError::ErrMetaHasNotBeenInitializedYet {});
        };
        let mut guard = self.format.write().unwrap();
        *guard = format.clone();
        Ok(format)
    }
    pub(crate) async fn sto_get_format(&self) -> Result<Option<Format>> {
        let format_key_str = Format::format_key_str();
        match self.operator.blocking().read(&format_key_str) {
            Ok(buf) => {
                let format = Format::parse_from(&buf).context(ErrBincodeDeserializeFailedSnafu)?;
                Ok(Some(format))
            }
            Err(e) => {
                if e.kind() == NotFound {
                    Ok(None)
                } else {
                    Err(e).context(ErrFailedToReadFromStoSnafu {
                        key: format_key_str,
                    })
                }
            }
        }
    }
    pub(crate) async fn sto_set_format(&self, format: &Format) -> Result<()> {
        let format_key_str = Format::format_key_str();
        let format_buf = format.encode();
        self.operator
            .write(&format_key_str, format_buf)
            .await
            .context(ErrFailedToWriteToStoSnafu {
                key: format_key_str,
            })?;
        Ok(())
    }

    pub(crate) async fn sto_increment_counter(&self, c: Counter, step: u64) -> Result<u64> {
        let v = c
            .increment_by(self.operator.clone(), step)
            .await
            .context(ErrFailedToDoCounterSnafu)?;
        Ok(v)
    }
}

pub(crate) fn generate_sto_sym_key_str(inode: Ino) -> String {
    let mut buf = vec![0u8; 10];
    buf.write_u8('A' as u8).unwrap();
    buf.write_u64::<LittleEndian>(inode.0).unwrap();
    buf.write_u8('S' as u8).unwrap();
    String::from_utf8(buf).unwrap()
}
pub(crate) fn generate_sto_dir_stat_key_str(inode: Ino) -> String {
    let mut buf = vec![0u8; 10];
    buf.write_u8('U' as u8).unwrap();
    buf.write_u64::<LittleEndian>(inode.0).unwrap();
    buf.write_u8('I' as u8).unwrap();
    String::from_utf8(buf).unwrap()
}
// key: AiiiiiiiiD/{name}
// key-len: 11 + name.len()
pub(crate) fn generate_entry_key(parent: Ino, name: &str) -> Vec<u8> {
    let mut buf = vec![0u8; 11 + name.len()];
    buf.write_u8('A' as u8).unwrap();
    buf.write_u64::<LittleEndian>(parent.0).unwrap();
    buf.write_u8('D' as u8).unwrap();
    buf.write_u8('/' as u8).unwrap();
    buf.extend_from_slice(name.as_bytes());
    buf
}
pub(crate) fn generate_sto_entry_key_str(parent: Ino, name: &str) -> String {
    let str = format!("A{:0>8}D/{}", parent.0, name);
    debug!("generate entry key str: {str}");
    str
}
