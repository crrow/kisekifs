use std::num::ParseIntError;
use std::{
    cmp::Ordering,
    collections::hash_map::DefaultHasher,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    str::FromStr,
    sync::Arc,
};

use crate::slice::Error::InvalidSliceKeyStr;
use bincode::serialize;
use lazy_static::lazy_static;
use rangemap::RangeMap;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Location, ResultExt, Snafu, Whatever};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    InvalidSliceBuf {
        len: usize,
        #[snafu(implicit)]
        location: Location,
    },
    InvalidSliceKeyStr {
        str: String,
        #[snafu(implicit)]
        location: Location,
    },
    DecodeSliceBufError {
        source: bincode::Error,
        #[snafu(implicit)]
        location: Location,
    },
    ParseSliceKeyFailed {
        str: String,
        #[snafu(implicit)]
        location: Location,
        source: ParseIntError,
    },
}

lazy_static! {
    pub static ref ID_GENERATOR: sonyflake::Sonyflake =
        sonyflake::Sonyflake::new().expect("failed to create id generator");
}

pub fn make_slice_object_key(slice_id: SliceID, block_idx: usize, block_size: usize) -> String {
    SliceKey::new(slice_id, block_idx, block_size).gen_path_for_object_sto()
}

pub const EMPTY_SLICE_ID: SliceID = 0;

pub type SliceID = u64;

pub fn random_slice_id() -> u64 {
    ID_GENERATOR.next_id().expect("failed to generate id")
}

pub type OverlookedSlices = RangeMap<usize, Slice>;
pub type OverlookedSlicesRef = Arc<OverlookedSlices>;

#[derive(Debug, Eq, PartialEq)]
pub struct Slices(pub Vec<Slice>);

impl Slices {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.0.len() * SLICE_BYTES);
        for slice in &self.0 {
            buf.extend_from_slice(&slice.encode());
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Slices, Error> {
        ensure!(
            buf.len() % SLICE_BYTES != 0,
            InvalidSliceBufSnafu { len: buf.len() }
        );
        let mut slices = Vec::new();
        let mut i = 0;
        while i < buf.len() {
            let slice = Slice::decode(&buf[i..i + SLICE_BYTES])?;
            slices.push(slice);
            i += SLICE_BYTES;
        }
        Ok(Slices(slices))
    }

    /// Look over all slices and build a RangeMap for them.
    pub fn overlook(&self) -> RangeMap<usize, Slice> {
        let mut rm = rangemap::RangeMap::new();
        self.0.iter().for_each(|s| {
            rm.insert(
                s.get_chunk_pos()..s.get_chunk_pos() + s.get_size(),
                s.clone(),
            )
        });
        rm
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum Slice {
    /// Owned means this slice is built by write operation.
    Owned {
        /// The chunk position of the slice.
        chunk_pos: u32,
        /// The unique id of the slice.
        id: SliceID,
        /// the underlying data size
        size: u32,
        _padding: u64,
    },
    /// The slice is borrowed from other slice, built by File RangeCopy.
    Borrowed {
        /// The chunk position of the slice.
        chunk_pos: u32,
        /// The unique id of the slice.
        id: SliceID,
        /// the underlying data size
        size: u32,
        /// The offset of the borrowed slice in the owned slice.
        off: u32,
        /// The length of the borrowed slice.
        len: u32,
    },
}

impl Slice {
    pub fn new_owned(chunk_pos: usize, slice_id: u64, size: usize) -> Self {
        Slice::Owned {
            chunk_pos: chunk_pos as u32,
            id: slice_id,
            size: size as u32,
            _padding: 0,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serialize(self).unwrap()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        ensure!(
            buf.len() >= SLICE_BYTES,
            InvalidSliceBufSnafu { len: buf.len() }
        );
        let x: Slice = bincode::deserialize(buf).context(DecodeSliceBufSnafu)?;
        Ok(x)
    }

    pub fn get_chunk_pos(&self) -> usize {
        (match self {
            Slice::Owned { chunk_pos, .. } => *chunk_pos,
            Slice::Borrowed { chunk_pos, off, .. } => *chunk_pos + off,
        }) as usize
    }

    pub fn get_id(&self) -> SliceID {
        (match self {
            Slice::Owned { id: slice_id, .. } => *slice_id,
            Slice::Borrowed { id: slice_id, .. } => *slice_id,
        })
    }

    pub fn get_size(&self) -> usize {
        (match self {
            Slice::Owned { size, .. } => *size,
            // for borrowed slice, the size is the length of the borrowed part.
            Slice::Borrowed { len, .. } => *len,
        }) as usize
    }

    pub fn get_underlying_size(&self) -> usize {
        (match self {
            Slice::Owned { size, .. } => *size,
            Slice::Borrowed { size, .. } => *size,
        }) as usize
    }
}

pub const SLICE_BYTES: usize = 28;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord)]
pub struct SliceKey {
    pub slice_id: SliceID,
    pub block_idx: usize,
    pub block_size: usize,
}

pub const EMPTY_SLICE_KEY: SliceKey = SliceKey {
    slice_id: 0,
    block_idx: 0,
    block_size: 0,
};

impl Display for SliceKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:08X}_{:08X}_{:08X}_{:08X}_{:08X}",
            // we can overwrite a slice, so we need to avoid the conflict.
            self.slice_id / 1000 / 1000,
            self.slice_id / 1000,
            self.slice_id,
            self.block_idx,
            self.block_size
        )
    }
}

impl PartialOrd for SliceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let sc = self.slice_id.cmp(&other.slice_id);
        if sc == Ordering::Equal {
            let bc = self.block_idx.cmp(&other.block_idx);
            Some(bc)
        } else {
            Some(sc)
        }
    }
}

impl SliceKey {
    pub fn new(slice_id: SliceID, block_idx: usize, block_size: usize) -> Self {
        Self {
            slice_id,
            block_idx,
            block_size,
        }
    }

    pub fn gen_path_for_local_sto(&self) -> String {
        format!("{}", self)
    }

    pub fn gen_path_for_object_sto(&self) -> String {
        format!("chunks{}", self)
    }

    pub fn random() -> Self {
        SliceKey {
            slice_id: ID_GENERATOR.next_id().expect("failed to generate id"),
            block_idx: 0,
            block_size: 0,
        }
    }

    pub fn cal_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl TryFrom<&str> for SliceKey {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        SliceKey::from_str(s)
    }
}

impl FromStr for SliceKey {
    type Err = Error; // Define the error type for parsing

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.strip_prefix("chunks/").unwrap_or(s) // Remove the "chunks-" prefix
            .split('_')
            .map(|part| u64::from_str(part) ) // Parse hexadecimal parts
            .collect::<Result<Vec<_>, _>>().context(ParseSliceKeyFailedSnafu { str: s.to_string()})?;
        ensure!(
            parts.len() == 5,
            InvalidSliceKeyStrSnafu { str: s.to_string() }
        );
        Ok(SliceKey {
            slice_id: parts[2],
            block_idx: parts[3] as usize,
            block_size: parts[4] as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_v2() {
        let slice = Slice::Owned {
            chunk_pos: 0,
            id: 1,
            size: 1024,
            _padding: 0,
        };
        let buf = slice.encode();
        let slice2 = Slice::decode(&buf).unwrap();
        assert_eq!(slice, slice2);
        println!("{}", buf.len());
        matches!(slice2, Slice::Owned { .. });

        let slice = Slice::Borrowed {
            chunk_pos: 0,
            id: 1,
            size: 1024,
            off: 0,
            len: 1024,
        };
        let buf = slice.encode();
        let slice2 = Slice::decode(&buf).unwrap();
        assert_eq!(slice, slice2);
        println!("{}", buf.len());
        matches!(slice2, Slice::Borrowed { .. });

        let slices = Slices(vec![slice, slice2]);
        let buf = slices.encode();
        let slices2 = Slices::decode(&buf).unwrap();
        assert_eq!(slices, slices2);
    }
}
