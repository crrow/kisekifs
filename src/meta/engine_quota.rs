use crate::meta::{engine::MetaEngine, err::Result, types::Ino, MetaContext};
use std::sync::atomic::Ordering;

impl MetaEngine {
    pub fn check_quota(
        &self,
        ctx: &MetaContext,
        space: i64,
        inodes: i64,
        parent: Ino,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn update_dir_quota(&self, ino: Ino, space: i64, inodes: i64) -> Result<()> {
        todo!()
    }
}
