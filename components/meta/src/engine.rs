use std::sync::Arc;

use crate::backend::BackendRef;
use crate::id_table::IdTable;
use kiseki_types::setting::Format;

pub type MetaEngineRef = Arc<MetaEngine>;

pub struct MetaEngine {
    backend: BackendRef,
    format: Format,
    free_inodes: IdTable,
    free_slices: IdTable,
}
