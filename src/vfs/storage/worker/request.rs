// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;

use crate::meta::types::Ino;

pub(crate) enum WorkerRequest {
    FlushBlock(FlushBlockRequest),
    FlushReleaseSlice(FlushAndReleaseSliceRequest),
    /// Notify a worker to stop.
    Stop,
}

impl WorkerRequest {
    pub(crate) fn new_flush_block_request(
        ino: Ino,
        chunk_idx: usize,
        internal_slice_seq: u64,
        flush_to: usize,
    ) -> Self {
        WorkerRequest::FlushBlock(FlushBlockRequest {
            ino,
            chunk_idx,
            internal_slice_seq,
            flush_to,
        })
    }

    pub(crate) fn new_flush_and_release_slice_request(
        ino: Ino,
        chunk_idx: usize,
        internal_slice_seq: u64,
        reason: FlushAndReleaseSliceReason,
    ) -> Self {
        WorkerRequest::FlushReleaseSlice(FlushAndReleaseSliceRequest {
            ino,
            chunk_idx,
            internal_slice_seq,
            reason,
        })
    }
}

/// Try to flush some blocks in the current slice.
#[derive(Clone)]
pub(crate) struct FlushBlockRequest {
    pub(crate) ino: Ino,
    pub(crate) chunk_idx: usize,
    pub(crate) internal_slice_seq: u64,
    pub(crate) flush_to: usize,
}

impl Eq for FlushBlockRequest {}

impl PartialEq<Self> for FlushBlockRequest {
    fn eq(&self, other: &Self) -> bool {
        self.flush_to == other.flush_to
    }
}

impl PartialOrd<Self> for FlushBlockRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FlushBlockRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        self.flush_to.cmp(&other.flush_to)
    }
}

/// Flush as many as blocks in the specified slice.
/// When we release the slice, we should mark this slice as done.
///
/// Otherwise, the background [CommitChunkRequest] will continue try
/// to flush this slice.
#[derive(Debug)]
pub(crate) struct FlushAndReleaseSliceRequest {
    pub(crate) ino: Ino,
    pub(crate) chunk_idx: usize,
    pub(crate) internal_slice_seq: u64,
    pub(crate) reason: FlushAndReleaseSliceReason,
}

#[derive(Debug)]
pub(crate) enum FlushAndReleaseSliceReason {
    // The slice is full.
    Full,
    // Encounter random write.
    RandomWrite,
    // Background flush.
    Background,
    // Flush manually.
    Manually,
}

/// CommitChunkRequest is used for committing a chunk,
/// we try to flush all the slices in this chunk.
///
/// Then we should try to free this chunk is no one is writing.
#[derive(Debug)]
pub(crate) struct CommitChunkRequest {
    pub(crate) ino: Ino,
    pub(crate) chunk_idx: usize,
}
