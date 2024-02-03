use bincode::serialize;

use std::sync::Arc;

use crate::meta::err::{ErrBincodeDeserializeFailedSnafu, ErrInvalidSliceBufSnafu, Result};

use rangemap::RangeMap;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

pub type OverlookedSlices = RangeMap<usize, Slice>;
pub type OverlookedSlicesRef = Arc<OverlookedSlices>;

#[derive(Debug, Eq, PartialEq)]
pub struct Slices(pub Vec<Slice>);

impl Slices {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.0.len() * SLICE_BYTES);
        for slice in &self.0 {
            buf.extend_from_slice(&slice.encode());
        }
        buf
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<Slices> {
        if buf.len() % SLICE_BYTES != 0 {
            return ErrInvalidSliceBufSnafu.fail();
        }
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
    pub(crate) fn overlook(&self) -> RangeMap<usize, Slice> {
        let mut rm = rangemap::RangeMap::new();
        self.0.iter().for_each(|s| {
            rm.insert(
                s.get_chunk_pos()..s.get_chunk_pos() + s.get_size(),
                s.clone(),
            )
        });
        rm
    }

    pub(crate) fn len(&self) -> usize {
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
        slice_id: u64,
        /// the underlying data size
        size: u32,
        _padding: u64,
    },
    /// The slice is borrowed from other slice, built by File RangeCopy.
    Borrowed {
        /// The chunk position of the slice.
        chunk_pos: u32,
        /// The unique id of the slice.
        slice_id: u64,
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
            slice_id,
            size: size as u32,
            _padding: 0,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serialize(self).unwrap()
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < SLICE_BYTES {
            return ErrInvalidSliceBufSnafu.fail();
        }
        let x: Slice = bincode::deserialize(buf).context(ErrBincodeDeserializeFailedSnafu)?;
        Ok(x)
    }

    pub fn get_chunk_pos(&self) -> usize {
        (match self {
            Slice::Owned { chunk_pos, .. } => *chunk_pos,
            Slice::Borrowed { chunk_pos, off, .. } => *chunk_pos + off,
        }) as usize
    }
    pub fn get_id(&self) -> usize {
        (match self {
            Slice::Owned { slice_id, .. } => *slice_id,
            Slice::Borrowed { slice_id, .. } => *slice_id,
        }) as usize
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

pub(crate) const SLICE_BYTES: usize = 28;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_v2() {
        let slice = Slice::Owned {
            chunk_pos: 0,
            slice_id: 1,
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
            slice_id: 1,
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
