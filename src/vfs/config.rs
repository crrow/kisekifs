use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::vfs::storage::{self, EngineConfig};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VFSConfig {
    pub attr_timeout: Duration,
    pub dir_entry_timeout: Duration,
    pub entry_timeout: Duration,
    pub backup_meta_interval: Duration,
    pub prefix_internal: bool,
    pub hide_internal: bool,

    pub engine_config: EngineConfig,
}
