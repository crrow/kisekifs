use std::sync::Arc;

use crate::backend::Backend;

pub type MetaEngineRef = Arc<MetaEngine>;

pub struct MetaEngine {
    backend: Arc<dyn Backend>,
}
