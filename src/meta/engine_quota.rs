use crate::meta::{engine::MetaEngine, err::Result, types::Ino, MetaContext};

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
    pub(crate) fn update_stats(&self, space: i64, inodes: i64) -> Result<()> {
        todo!()
    }
    pub(crate) fn update_update_dir_stat(
        &self,
        ino: Ino,
        length: i64,
        space: i64,
        inodes: i64,
    ) -> Result<()> {
        todo!()
    }
    pub(crate) fn update_dir_quota(&self, ino: Ino, space: i64, inodes: i64) -> Result<()> {
        todo!()
    }
}
