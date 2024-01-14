use super::config::Format;
use super::err::Result;
use super::Meta;
use opendal::Operator;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[derive(Debug, Snafu)]
enum KvMetaError {
    #[snafu(display("failed to build operator from {}: {}", scheme, source))]
    ErrBuildOperator {
        source: opendal::Error,
        scheme: opendal::Scheme,
    },
}

impl From<KvMetaError> for super::err::Error {
    fn from(value: KvMetaError) -> Self {
        Self::GenericError {
            component: "kv_meta".to_string(),
            source: Box::new(value),
        }
    }
}

pub struct KvMeta {
    schema: opendal::Scheme,
    sto: Operator,
}

impl Debug for KvMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvMeta")
            .field("name", &self.schema)
            .finish()
    }
}

impl Meta for KvMeta {
    fn scheme() -> opendal::Scheme {
        opendal::Scheme::Sled
    }

    fn name(&self) -> String {
        format!("kv-{ }", self.schema)
    }

    fn reset(&mut self) -> Result<()> {
        Ok(())
    }

    fn init(&mut self, format: Format, force: bool) -> Result<()> {
        todo!()
    }

    fn load(&mut self, check_version: bool) -> Result<Format> {
        todo!()
    }
}

impl KvMeta {
    pub(crate) fn new(
        scheme: opendal::Scheme,
        config_map: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            schema: scheme,
            sto: Operator::via_map(scheme, config_map).context(ErrBuildOperatorSnafu { scheme })?,
        })
    }
}
