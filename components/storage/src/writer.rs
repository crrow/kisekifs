use crate::error::Result;
use crate::slice_buffer::SliceBuffer;
use kiseki_types::{
    ino::Ino,
    slice::{Slice, EMPTY_SLICE_ID},
    BlockIndex, BlockSize,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::time::Instant;
use tracing::debug;

/// FileWriter is concurrent free.
pub struct FileWriter {}

/// FileFlusher is the background task to flush the file to the underlying storage.
struct FileFlusher {
    inode: Ino,
}

// SliceWriter control the behavior of writing data to the underlying storage.
// pub(crate) struct SliceWriter {
//     // the chunk index
//     chunk_idx: usize,
//     // The chunk offset.
//     chunk_start_offset: usize,
//
//     // did we prepare the slice id?
//     slice_id: Arc<AtomicU64>,
//     // the underlying write buffer.
//     write_buffer: RwLock<SliceBuffer>,
//     // before we flush the slice, we need to freeze it.
//     frozen: Arc<AtomicBool>,
//     // as long as we aren't done, we should try to flush this
//     // slice.
//     // since we may fail at the flush process.
//     done: Arc<AtomicBool>,
//
//     // notify the background clean task that im done.
//     done_notify: Arc<Notify>,
//     last_modified: RwLock<Option<Instant>>,
//     total_slice_counter: Arc<AtomicUsize>,
//     start_at: Instant,
// }
//
// impl SliceWriter {
//     fn key_gen(&self) -> impl Fn(BlockIndex, BlockSize) -> String {
//         let idx = self.chunk_idx;
//         let slice_id = self.slice_id.clone();
//         move |block_idx, block_size| {
//             format!(
//                 "chunk_{}_slice_{}_block_{}_{}",
//                 idx,
//                 slice_id.load(Ordering::Acquire),
//                 block_idx,
//                 block_size
//             )
//         }
//     }
//
//     // freeze this slice writer, make it cannot be written for the flushing.
//     pub(crate) fn freeze(self: &Arc<Self>) {
//         self.frozen.store(true, Ordering::Release)
//     }
//
//     // check if this slice writer can be written.
//     pub(crate) fn frozen(self: &Arc<Self>) -> bool {
//         self.frozen.load(Ordering::Acquire)
//     }
//
//     // get the underlying write buffer's released length and total write length.
//     async fn get_flushed_length_and_total_write_length(self: &Arc<Self>) -> (usize, usize) {
//         let guard = self.write_buffer.read().await;
//         let flushed_len = guard.flushed_len();
//         let write_len = guard.len();
//         (flushed_len, write_len)
//     }
//
//     async fn get_length(self: &Arc<Self>) -> usize {
//         let guard = self.write_buffer.read().await;
//         guard.len()
//     }
//
//     // return the current write len to this buffer,
//     // the buffer total length, and the flushed length.
//     async fn write(
//         self: &Arc<Self>,
//         slice_offset: usize,
//         data: &[u8],
//     ) -> Result<(usize, usize, usize)> {
//         let mut guard = self.write_buffer.write().await;
//         let write_len = guard.write_at(slice_offset, data).await?;
//         self.last_modified.write().await.replace(Instant::now());
//         Ok((write_len, guard.length(), guard.flushed_length()))
//     }
//
//     /// [SliceWriter::finish] will try to flush all data to the backend storage,
//     /// and free the undelrying buffer.
//     pub(crate) async fn finish(self: &Arc<Self>) -> Result<()> {
//         let mut guard = self.write_buffer.write().await;
//         if guard.length() == 0 {
//             return Ok(());
//         }
//         // guard.flush(|block_idx, block_size| -> String {}).await?;
//         self.done.store(true, Ordering::Release);
//         Ok(())
//     }
//
//     pub(crate) async fn do_flush_to(self: &Arc<Self>, offset: usize) -> Result<()> {
//         let mut guard = self.write_buffer.write().await;
//         guard.flush_to(offset).await?;
//         Ok(())
//     }
//
//     async fn prepare_slice_id(self: &Arc<Self>, meta_engine: Arc<MetaEngine>) -> Result<()> {
//         if !self.slice_id.load(Ordering::Acquire) {
//             let guard = self.write_buffer.read().await;
//             if guard.get_slice_id() == EMPTY_SLICE_ID {
//                 drop(guard);
//                 if self.slice_id.load(Ordering::Acquire) {
//                     // load again
//                     return Ok(());
//                 }
//                 let mut write_guard = self.write_buffer.write().await;
//                 if write_guard.get_slice_id() == EMPTY_SLICE_ID {
//                     let sid = meta_engine.next_slice_id().await?;
//                     debug!("assign slice id {} to slice {}", sid, self.internal_seq);
//                     write_guard.set_slice_id(sid);
//                 }
//                 self.slice_id.store(true, Ordering::Release);
//             }
//         }
//         Ok(())
//     }
//
//     async fn to_meta_slice(self: &Arc<Self>) -> Slice {
//         let guard = self.write_buffer.read().await;
//         Slice::new_owned(
//             self.chunk_start_offset,
//             guard.get_slice_id(),
//             guard.length(),
//         )
//     }
// }
//
// impl Drop for SliceWriter {
//     fn drop(&mut self) {
//         self.total_slice_counter.fetch_sub(1, Ordering::AcqRel);
//     }
// }
