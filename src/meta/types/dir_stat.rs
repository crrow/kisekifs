use serde::{Deserialize, Serialize};

use crate::meta::types::Ino;

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct DirStat {}

impl DirStat {
    pub(crate) fn generate_sto_key_str(inode: Ino) -> String {
        format!("U{}", inode)
    }
}
