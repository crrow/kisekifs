use std::path::PathBuf;
use std::time::Duration;

/// Configuration for a FUSE background session.
#[derive(Debug, Clone)]
pub struct FuseConfig {
    pub mount_point: PathBuf,
    pub mount_options: Vec<fuser::MountOption>,
}

#[derive(Debug, Clone, Default)]
pub struct FsConfig {
    pub attr_timeout: Duration,
    pub dir_entry_timeout: Duration,
    pub entry_timeout: Duration,
    pub backup_meta_interval: Duration,
}
