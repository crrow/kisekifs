use super::request::CommitChunkRequest;
use crate::vfs::{storage::scheduler::BackgroundTaskPoolRef, FH};

/// Task to flush a slice.
pub(crate) struct FlushSliceTask {}
