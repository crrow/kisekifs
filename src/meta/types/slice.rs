use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Deserialize, Serialize)]
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
