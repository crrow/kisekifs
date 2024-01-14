use serde::{Deserialize, Serialize};

/// Atime (Access Time):
/// Every file has three timestamps:
/// atime (access time): The last time the file was read or accessed.
/// mtime (modification time): The last time the file's content was modified.
/// ctime (change time): The last time the file's metadata (e.g., permissions, owner) was changed.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum AccessTimeMode {
    /// Disables atime updates entirely.
    /// Reading a file doesn't update its atime timestamp.
    /// Improves performance, especially for frequently accessed files.
    /// Can make it difficult to determine when a file was last accessed.
    Never,
    /// Default atime mode on many Linux systems.
    /// Updates atime only if:
    /// The file's atime is older than its mtime or ctime.
    /// The file has been accessed more than a certain time threshold (usually 1 day).
    /// Balances performance and access time tracking.
    Relative,
    /// Always updates atime whenever a file is read.
    /// Accurately tracks file access times.
    /// Can impact performance, especially on storage systems with slow write speeds.
    Everytime,
}

impl Default for AccessTimeMode {
    fn default() -> Self {
        Self::Never
    }
}

pub type Ino = u64;
