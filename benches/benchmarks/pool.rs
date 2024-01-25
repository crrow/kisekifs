/// Which only use vec to allocate memory.
struct RawPool {
    data: [Vec<u8>; 30],
}

impl RawPool {
    fn init() -> RawPool {
        let mut rp = RawPool {
            data: Default::default(),
        };
        for i in 0..30 {
            rp.data[i] = vec![0; 1 << i];
        }
        rp
    }
}

fn power_of_2(size: usize) -> usize {
    let mut bits = 0;
    let mut p = 1;
    while p < size {
        bits += 1;
        p *= 2;
    }
    bits
}

impl RawPool {
    fn alloc(&self, size: usize) -> *mut u8 {
        let zeros = power_of_2(size);
    }

    fn dealloc(&self, _ptr: *mut u8, _size: usize) {
        // do nothing
    }
}
