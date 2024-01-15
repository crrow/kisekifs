pub mod config;

use crate::common::Ino;
use config::{Format, MetaConfig};
use opendal::Operator;
use std::fmt::{Debug, Formatter};

/// Trait `Meta` describes a meta service for file system.
pub(crate) struct Meta {
    pub(crate) config: MetaConfig,
    format: Option<Format>,
    root: Ino,
    operator: Operator,
}

impl Debug for Meta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meta")
            .field("scheme", &self.config.scheme)
            .finish()
    }
}
