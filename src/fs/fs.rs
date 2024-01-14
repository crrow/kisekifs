use crate::meta::Meta;
use fuser::{Filesystem, ReplyEntry, Request};
use std::ffi::OsStr;
use tracing::warn;

pub struct KisekiFS<M: Meta> {
    meta: M,
}

impl<M> Filesystem for KisekiFS<M>
where
    M: Meta,
{
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {}
}
