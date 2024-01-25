use bytes::{Buf, BufMut, Bytes, BytesMut};
use opendal::raw::oio::WriteBuf;
use std::io::{copy, Read, Write};
use std::rc::{Rc, Weak};

pub const MAX_PAGE_SIZE: usize = 1 << 20; // 1 MB

/// Page represents we hold a page of data in memory.
///
/// Its thread unsafe.
#[derive(Debug)]
pub struct UnsafePage {
    // which can only hold `MAX_BLOCK_SIZE` bytes.
    data: Rc<Vec<u8>>,
    size: usize,
}

impl Clone for UnsafePage {
    fn clone(&self) -> Self {
        Self {
            data: Rc::clone(&self.data),
            size: self.size,
        }
    }
}

impl UnsafePage {
    // The only way to allocate a page is through this method.
    //
    // If we allocate the data in advance,
    // it will affect the performance a lot.
    //
    // Otherwise, this abstraction still introduces some overhead,
    // but its close to the origin one.
    pub fn allocate(size: usize) -> Self {
        assert!(size <= MAX_PAGE_SIZE && size > 0);
        Self {
            data: Rc::new(Vec::with_capacity(size)),
            size,
        }
    }

    pub fn writer(&mut self, offset: usize, size: usize) -> UnsafePageWriter {
        assert!(offset + size <= self.data.len());

        let slice = Rc::make_mut(&mut self.data);
        let buf = &mut slice[offset..offset + size];
        UnsafePageWriter {
            buf,
            capacity: size,
        }
    }

    pub fn reader(&self, offset: usize, size: usize) -> UnsafePageView {
        assert!(offset + size <= self.data.len());
        let slice = Rc::downgrade(&self.data);
        UnsafePageView {
            slice,
            offset,
            capacity: size,
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        Rc::into_inner(self.data).expect("should not fail when take vec out of UnsafePage")
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn resize(&mut self, size: usize) {
        assert!(size <= MAX_PAGE_SIZE && size > 0);
        Rc::make_mut(&mut self.data).resize(size, 0);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl From<Vec<UnsafePage>> for UnsafePages {
    fn from(value: Vec<UnsafePage>) -> Self {
        Self(value)
    }
}

pub struct UnsafePages(pub Vec<UnsafePage>);

impl Into<Vec<u8>> for UnsafePages {
    fn into(self) -> Vec<u8> {
        let mut buf = Vec::new();
        for mut src in self.0 {
            let mut src_r = src.as_slice();
            copy(&mut src_r, &mut buf).unwrap();
        }
        buf
    }
}
impl Into<Bytes> for UnsafePages {
    fn into(self) -> Bytes {
        let mut buf = BytesMut::new();
        for mut src in self.0 {
            let mut src_r = src.as_slice();
            buf.reserve(src_r.len());
            buf.copy_from_slice(&mut src_r);
        }
        buf.freeze()
    }
}

pub struct UnsafePageWriter<'a> {
    buf: &'a mut [u8],
    capacity: usize,
}

impl<'a> Write for UnsafePageWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer too large",
            ));
        }
        self.buf.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> UnsafePageWriter<'a> {
    // How many bytes can be written to this writer.
    fn capacity(&self) -> usize {
        self.capacity
    }

    // How many bytes have been written to this writer.
    fn len(&self) -> usize {
        self.buf.len()
    }
}

/// PageView represents a view of a page,
/// its only valid when the page is not modified after the view is created.
#[derive(Debug)]
pub struct UnsafePageView {
    slice: Weak<Vec<u8>>,
    offset: usize,
    capacity: usize,
}

impl Read for UnsafePageView {
    fn read(&mut self, dst: &mut [u8]) -> std::io::Result<usize> {
        let n = std::cmp::min(self.capacity, dst.len());
        return if let Some(x) = self.slice.upgrade() {
            dst[..n].copy_from_slice(&x[self.offset..self.offset + n]);
            Ok(n)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "page has been modified or dropped",
            ))
        };
    }
}

impl UnsafePageView {
    /// How many bytes can be read from this reader.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut page = UnsafePage::allocate(2);
        let mut ps = page.writer(0, 2);
        let n = ps.write(b"ab").unwrap();
        assert_eq!(n, 2);

        let n = ps.write(b"cd");
        assert!(n.is_err(), "should not write more than 2 bytes");
        drop(ps);

        let mut pr = page.reader(0, 2);
        let buf = &mut [0; 2];
        let n = pr.read(buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf, b"ab");

        let mut ps = page.writer(0, 2);
        let n = ps.write(b"cd").unwrap();
        assert_eq!(n, 2);

        let n = pr.read(buf);
        assert!(n.is_err(), "the underlying buffer has been updated");
    }

    #[test]
    #[should_panic]
    fn bad_read_offset() {
        let mut page = UnsafePage::allocate(1);
        let mut ps = page.reader(0, 2);
    }

    #[test]
    #[should_panic]
    fn bad_write_offset() {
        let mut page = UnsafePage::allocate(1);
        let mut ps = page.writer(0, 2);
    }

    #[test]
    #[should_panic]
    fn bad_allocate() {
        let mut page = UnsafePage::allocate(0);
    }

    #[test]
    fn reader_before_writer() {
        let mut page = UnsafePage::allocate(2);
        let mut reader = page.reader(0, 2);

        let mut ps = page.writer(0, 2);
        let n = ps.write(b"ab").unwrap();
        assert_eq!(n, 2);

        let buf = &mut [0; 2];
        let r = reader.read(buf);
        assert_eq!(r.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }
}
