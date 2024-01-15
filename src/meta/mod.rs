pub mod config;
pub mod types;

use crate::common::err::Result;
use crate::meta::types::{InternalNode, OpenFiles, PreInternalNodes};
use config::{Format, MetaConfig};

use opendal::Operator;
use snafu::{ResultExt, Snafu};

use std::fmt::{Debug, Formatter};

use types::Ino;

#[derive(Debug, Snafu)]
pub enum MetaError {
    #[snafu(display("failed to parse scheme: {}: {}", got, source))]
    FailedToParseScheme { source: opendal::Error, got: String },
    #[snafu(display("failed to open operator: {}", source))]
    FailedToOpenOperator { source: opendal::Error },
}

impl From<MetaError> for crate::common::err::Error {
    fn from(value: MetaError) -> Self {
        Self::MetaError { source: value }
    }
}

/// Trait `Meta` describes a meta service for file system.
pub(crate) struct Meta {
    pub(crate) config: MetaConfig,
    format: Option<Format>,
    root: Ino,
    operator: Operator,
    sub_trash: Option<InternalNode>,
    open_files: OpenFiles,
}

impl Meta {
    pub fn open(config: MetaConfig) -> Result<Meta> {
        let op = Operator::via_map(config.scheme, config.scheme_config.clone())
            .context(FailedToOpenOperatorSnafu)?;
        let m = Meta {
            config: config.clone(),
            format: None,
            root: 0.into(),
            operator: op,
            sub_trash: None,
            open_files: OpenFiles::new(config.open_cache, config.open_cache_limit),
        };
        Ok(m)
    }
    pub fn info(&self) -> String {
        format!("meta-{}", self.config.scheme)
    }
}

impl Debug for Meta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meta")
            .field("scheme", &self.config.scheme)
            .finish()
    }
}
