#[derive(Debug, Default)]
pub(crate) struct FSStates {
    /// Represents the total amount of storage space in bytes allocated for the
    /// file system.
    pub total_space: u64,
    /// Represents the amount of free storage space in bytes available for new
    /// data.
    pub avail_space: u64,
    /// Represents the used of inodes.
    pub used_inodes: u64,
    /// Represents the number of available inodes that can be used for new files
    /// or directories.
    pub available_inodes: u64,
}
