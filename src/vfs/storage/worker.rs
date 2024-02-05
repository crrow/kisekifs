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

mod flush;
mod listener;
pub(crate) use listener::WorkerListener;
mod request;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
pub(crate) use request::{FlushAndReleaseSliceReason, WorkerRequest};
use snafu::{ensure, ResultExt};
use tokio::{
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{debug, info, warn};

use crate::{
    common,
    meta::types::Ino,
    vfs::{
        err::{JoinSnafu, Result, WorkerStoppedSnafu},
        storage::{
            engine::Config as EngineConfig,
            scheduler::BackgroundTaskPoolRef,
            worker::request::{CommitChunkRequest, FlushAndReleaseSliceRequest},
            writer::{FileWriter, FileWritersRef},
        },
        FH,
    },
};

/// Identifier for a worker.
pub(crate) type WorkerId = u32;

/// WorkerGroup is used for managing different workers.
pub(crate) struct WorkerGroup {
    // background works majorly for flush and prefetch.
    background_workers: DashMap<WorkerId, Worker>,
}

/// Worker start config.
pub(crate) struct WorkerStarter {
    pub(crate) id: WorkerId,
    pub(crate) config: Arc<EngineConfig>,
    pub(crate) task_pool_ref: BackgroundTaskPoolRef,
    pub(crate) listener: WorkerListener,
    pub(crate) file_writers: FileWritersRef,
}

impl WorkerStarter {
    /// Start a work task and its background task.
    pub(crate) fn start(self) -> Worker {
        let (sender, receiver) = mpsc::channel(self.config.worker_channel_size);
        let running = Arc::new(AtomicBool::new(true));

        let mut worker_loop_task = WorkerLoop {
            id: self.id,
            config: self.config.clone(),
            sender: sender.clone(),
            receiver,
            running: running.clone(),
            task_pool_ref: self.task_pool_ref.clone(),
            listener: self.listener,
            file_writers: self.file_writers,
            commit_chunk_handles: Default::default(),
        };

        let handle = common::runtime::spawn(async move {
            worker_loop_task.run().await;
        });

        Worker {
            id: self.id,
            sender,
            handle: Mutex::new(Some(handle)),
            running,
        }
    }
}

/// Worker to handle the request from the vfs.
/// The worker is the api for the [WorkerLoop].
pub(crate) struct Worker {
    /// Id of the worker.
    id: WorkerId,
    /// Request sender. Send to the worker loop thread.
    sender: Sender<WorkerRequest>,
    /// Handle to the worker thread.
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Whether to run the worker thread.
    running: Arc<AtomicBool>,
}

impl Worker {
    /// Submits request to background worker task.
    pub(crate) async fn submit_request(&self, request: WorkerRequest) -> Result<()> {
        ensure!(self.is_running(), WorkerStoppedSnafu { id: self.id });
        if self.sender.send(request).await.is_err() {
            warn!(
                "Worker {} is already exited but the running flag is still true",
                self.id
            );
            // Manually set the running flag to false to avoid printing more warning logs.
            self.set_running(false);
            return WorkerStoppedSnafu { id: self.id }.fail()?;
        }

        Ok(())
    }

    /// Stop the worker.
    ///
    /// This method waits until the worker thread exists.
    async fn stop(&self) -> Result<()> {
        let handle = self.handle.lock().await.take();
        if let Some(handle) = handle {
            info!("Stop region worker {}", self.id);

            self.set_running(false);
            if self.sender.send(WorkerRequest::Stop).await.is_err() {
                warn!("Worker {} is already exited before stop", self.id);
            }

            handle.await.context(JoinSnafu {})?;
        }

        Ok(())
    }

    /// Returns true if the worker is still running.
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Sets whether the worker is still running.
    fn set_running(&self, value: bool) {
        self.running.store(value, Ordering::Relaxed)
    }
}

/// Background worker loop to handle requests.
pub(crate) struct WorkerLoop {
    /// Id of the worker.
    id: WorkerId,
    /// Engine config.
    config: Arc<EngineConfig>,
    /// Request sender.
    sender: Sender<WorkerRequest>,
    /// Request receiver.
    receiver: Receiver<WorkerRequest>,
    /// Whether the worker thread is still running.
    running: Arc<AtomicBool>,
    /// Background job scheduler.
    task_pool_ref: BackgroundTaskPoolRef,
    /// Event listener
    listener: WorkerListener,
    file_writers: FileWritersRef,
    commit_chunk_handles: HashMap<(Ino, usize), JoinHandle<()>>,
}

impl WorkerLoop {
    /// Starts the worker loop.
    async fn run(&mut self) {
        info!("Start worker task {}", self.id);

        // Buffer to retrieve requests from receiver.
        let mut buffer = Vec::with_capacity(self.config.worker_request_batch_size);

        while self.running.load(Ordering::Relaxed) {
            // Clear the buffer before handling next batch of requests.
            buffer.clear();

            match self.receiver.recv().await {
                Some(request) => buffer.push(request),
                None => break,
            }

            // Try to recv more requests from the channel.
            for _ in 1..buffer.capacity() {
                // We have received one request so we start from 1.
                match self.receiver.try_recv() {
                    Ok(req) => buffer.push(req),
                    // We still need to handle remaining requests.
                    Err(_) => break,
                }
            }

            self.handle_requests(&mut buffer).await;
        }

        info!("Exit worker task {}", self.id);
    }

    /// Dispatches and processes requests.
    ///
    /// We should drain the buffer.
    async fn handle_requests(&mut self, buffer: &mut Vec<WorkerRequest>) {
        let mut fb_map: HashMap<(Ino, usize, u64), usize> = HashMap::new();
        let fr_vec = Vec::with_capacity(buffer.capacity());
        for wr in buffer.drain(..) {
            match wr {
                WorkerRequest::Stop => {
                    // We receive a stop signal, but we still want to process remaining
                    // requests. The worker thread will then check the running flag and
                    // then exit.
                    //
                    // we should not stop for multiple times.
                    debug_assert!(!self.running.load(Ordering::Relaxed));
                }
                WorkerRequest::FlushBlock(fb) => {
                    let key3 = (fb.ino, fb.chunk_idx, fb.internal_slice_seq);
                    if let Some(old) = fb_map.get(&key3) {
                        if fb.flush_to > *old {
                            fb_map.insert(key3, fb.flush_to);
                        }
                    } else {
                        fb_map.insert(key3, fb.flush_to);
                    }
                }
                WorkerRequest::FlushReleaseSlice(fl) => {
                    // remove the single block flush req, since we still need to flush the full one.
                    fb_map.remove(&(fl.ino, fl.chunk_idx, fl.internal_slice_seq));
                }
            }
        }

        self.handle_flush_block_reqs(fb_map).await;
        self.handle_flush_and_release(fr_vec.into_iter()).await;
    }

    async fn handle_flush_block_reqs(&self, fb_reqs: HashMap<(Ino, usize, u64), usize>) {
        let mut cache: HashMap<Ino, Arc<FileWriter>> = HashMap::new();

        let mut handles = Vec::new();
        for ((ino, chunk_idx, slice_seq), flush_to) in fb_reqs.into_iter() {
            let fw = if let Some(fw) = cache.get(&ino) {
                fw.clone()
            } else {
                let fw = self.file_writers.get(&ino).unwrap().clone();
                cache.insert(ino, fw.clone());
                fw
            };

            if let Some(cw) = fw.find_chunk_writer(chunk_idx) {
                if let Some(sw) = cw.find_slice_writer(slice_seq).await {
                    let handle = common::runtime::spawn(async move {
                        if let Err(e) = sw.do_flush_to(flush_to).await {
                            warn!("flush block failed: {}", e);
                        }
                    });
                    handles.push(handle);
                }
            }
        }

        futures::future::join_all(handles).await;
    }

    async fn handle_flush_and_release(
        &mut self,
        fl_reqs: impl Iterator<Item = FlushAndReleaseSliceRequest>,
    ) {
        for fl_req in fl_reqs {
            // TODO: do really flush and release buffer.
            debug!("flush and release: {:?}", fl_req);
        }
    }
}

fn flush_location(fh: FH, chunk_idx: usize, slice_seq: u64) -> String {
    format!("{}-{}-{}", fh, chunk_idx, slice_seq)
}
