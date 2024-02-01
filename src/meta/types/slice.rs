use std::io::Cursor;

use crate::meta::SliceView;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub(crate) struct Slice {
    pub(crate) pos: u32,
    pub(crate) id: u64,
    pub(crate) size: u32,
    pub(crate) off: u32,
    pub(crate) len: u32,
}

pub(crate) const SLICE_BYTES: usize = 24;

pub(crate) fn make_slice_buf(pos: u32, id: u64, size: u32, off: u32, len: u32) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(SLICE_BYTES); // Pre-allocate for efficiency
    let mut writer = Cursor::new(&mut buffer);
    writer.write_u32::<LittleEndian>(pos).unwrap(); // Handle potential errors explicitly
    writer.write_u64::<LittleEndian>(id).unwrap();
    writer.write_u32::<LittleEndian>(size).unwrap();
    writer.write_u32::<LittleEndian>(off).unwrap();
    writer.write_u32::<LittleEndian>(len).unwrap();

    buffer
}

pub(crate) fn read_slice_buf(buf: &[u8]) -> Vec<Slice> {
    let mut slices = Vec::new();
    let mut reader = Cursor::new(buf);
    while reader.position() < buf.len() as u64 {
        let pos = reader.read_u32::<LittleEndian>().unwrap();
        let id = reader.read_u64::<LittleEndian>().unwrap();
        let size = reader.read_u32::<LittleEndian>().unwrap();
        let off = reader.read_u32::<LittleEndian>().unwrap();
        let len = reader.read_u32::<LittleEndian>().unwrap();
        slices.push(Slice {
            pos,
            id,
            size,
            off,
            len,
        });
    }
    slices
}

/// project_slices will project all history slices into the latest view.
///
/// Example: if we have 3 slices write to the same chunk
///     write_slice1 ( chunk_pos: 0, slice_size: 1024)
///     write_slice2 ( chunk_pos: 128, slice_size: 8)
///     write_slice3 ( chunk_pos: 512, slice_size: 8)
/// Then the projected slices will be:
///     slice1 (chunk_pos: 0, slice_size: 128)
///     slice2 (chunk_pos: 128, slice_size: 8)
///     slice1 (chunk_pos: 128 + 8 = 136, slice_size: 512 - 136 = 376)
///     slice3 (chunk_pos: 512, slice_size: 8)
///     slice1 (chunk_pos: 520, slice_size: 1024 - 520 = 504)
pub(crate) fn project_slices(slices: &Vec<Slice>) -> Vec<SliceView> {
    let mut rm = rangemap::RangeMap::new();

    slices
        .iter()
        .for_each(|s| rm.insert(s.pos..s.pos + s.size, s.clone()));

    rm.iter()
        .map(|(r, s)| SliceView {
            id: s.id,
            size: s.size,
            off: r.start - s.pos,
            len: r.end - r.start,
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec() {
        let mut buf = vec![];
        buf.extend_from_slice(&make_slice_buf(1, 2, 3, 4, 5));
        buf.extend_from_slice(&make_slice_buf(1 + 1, 2 + 1, 3 + 1, 4 + 1, 5 + 1));
        buf.extend_from_slice(&make_slice_buf(1 + 2, 2 + 2, 3 + 2, 4 + 2, 5 + 2));

        let slices = read_slice_buf(&buf);
        assert_eq!(slices.len(), 3);

        assert_eq!(
            slices,
            vec![
                Slice {
                    pos: 1,
                    id: 2,
                    size: 3,
                    off: 4,
                    len: 5
                },
                Slice {
                    pos: 2,
                    id: 3,
                    size: 4,
                    off: 5,
                    len: 6
                },
                Slice {
                    pos: 3,
                    id: 4,
                    size: 5,
                    off: 6,
                    len: 7
                },
            ]
        );
    }

    #[test]
    fn handle_internal_slice() {
        let slices = vec![
            Slice {
                pos: 0,
                id: 1,
                size: 64 << 20,
                off: 0,
                len: 64 << 20,
            },
            Slice {
                pos: 30 << 20,
                id: 2,
                size: 8,
                off: 0,
                len: 8,
            },
            Slice {
                pos: 40 << 20,
                id: 3,
                size: 8,
                off: 0,
                len: 8,
            },
        ];

        let slice_infos = vec![
            SliceView {
                id: 1,
                size: 67108864,
                off: 0,
                len: 31457280,
            },
            SliceView {
                id: 2,
                size: 8,
                off: 0,
                len: 8,
            },
            SliceView {
                id: 1,
                size: 67108864,
                off: 31457288,
                len: 10485752,
            },
            SliceView {
                id: 3,
                size: 8,
                off: 0,
                len: 8,
            },
            SliceView {
                id: 1,
                size: 67108864,
                off: 41943048,
                len: 25165816,
            },
        ];

        let svs = project_slices(&slices);
        assert_eq!(svs, slice_infos)
    }

    #[test]
    fn build_project() {
        let slices = vec![
            Slice {
                pos: 0,
                id: 1,
                size: 1024,
                off: 0,
                len: 1024,
            },
            Slice {
                pos: 128,
                id: 2,
                size: 8,
                off: 0,
                len: 8,
            },
            Slice {
                pos: 512,
                id: 3,
                size: 8,
                off: 0,
                len: 8,
            },
        ];

        project_slices(&slices)
            .iter()
            .for_each(|sv| println!("{:?}", sv))
    }
}
