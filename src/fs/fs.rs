use crate::meta::Meta;
use fuser::{Filesystem, KernelConfig, ReplyEntry, Request};
use libc::c_int;
use std::ffi::OsStr;
use tracing::debug;

pub struct KisekiFS<M> {
    meta: M,
}

impl<M> KisekiFS<M>
    where
        M: Meta,
{
    pub fn new(meta: M) -> Self {
        Self { meta }
    }
}

impl<M> Filesystem for KisekiFS<M>
    where
        M: Meta,
{
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    /// The kernel module connection can be configured using the KernelConfig object
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("init kiseki...");
        Ok(())
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {}
}
