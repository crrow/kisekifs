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

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, RwLock,
    },
};


use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::common;

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub(crate) enum State {
    Running = 0,
    Stop = 1,
    AwaitTermination = 2,
}
pub(crate) type Job = Pin<Box<dyn Future<Output = ()> + Send>>;
pub(crate) type BackgroundTaskPoolRef = Arc<BackgroundTaskPool>;
pub(crate) struct BackgroundTaskPool {
    /// Sends jobs to flume bounded channel
    sender: RwLock<Option<async_channel::Sender<Job>>>,
    /// Background handles
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// Token used to halt the scheduler
    cancel_token: CancellationToken,
    /// State of scheduler
    state: Arc<AtomicU8>,
}

impl BackgroundTaskPool {
    /// Start the background task pool.
    pub(crate) fn start(concurrency: usize) -> Self {
        let (tx, rx) = async_channel::unbounded();
        let token = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(State::Running as u8));

        let mut handles = Vec::with_capacity(concurrency);

        for _ in 0..concurrency {
            let child = token.child_token();
            let receiver = rx.clone();
            let state_clone = state.clone();
            let handle = common::runtime::spawn(async move {
                while state_clone.load(Ordering::Relaxed) == State::Running as u8 {
                    tokio::select! {
                        _ = child.cancelled() => {
                            break;
                        }
                        req_opt = receiver.recv() =>{
                            if let Ok(job) = req_opt {
                                job.await;
                            }
                        }
                    }
                }
                // When task scheduler is cancelled, we will wait all task finished
                if state_clone.load(Ordering::Relaxed) == State::AwaitTermination as u8 {
                    // recv_async waits until all sender's been dropped.
                    while let Ok(job) = receiver.recv().await {
                        job.await;
                    }
                    state_clone.store(State::Stop as u8, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        Self {
            sender: RwLock::new(Some(tx)),
            cancel_token: token,
            handles: Mutex::new(handles),
            state,
        }
    }
    #[inline]
    pub(crate) fn is_running(&self) -> bool {
        let v = self.state.load(Ordering::Relaxed);
        v == State::Running as u8
    }
}
