pub fn align4k(length: u64) -> i64 {
    if length == 0 {
        return 1 << 12; // 4096
    }

    // Calculate the number of 4K blocks needed to hold the data
    let blocks_needed = (length - 1) / 4096 + 1;

    // Return the aligned length (number of blocks * block size) as i64
    (blocks_needed * 4096) as i64
}
