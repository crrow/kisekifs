use std::path::PathBuf;

/// Configuration for a FUSE background session.
#[derive(Debug, Clone)]
pub struct FuseConfig {
    pub mount_point:        PathBuf,
    pub mount_options:      Vec<fuser::MountOption>,
    /// work threads count for tokio runtime.
    pub async_work_threads: usize,
}
