// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;

use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tracing::debug;

static GLOBAL_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    debug!("start tokio runtime");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap()
});

pub fn handle() -> tokio::runtime::Handle { GLOBAL_RUNTIME.handle().clone() }

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    GLOBAL_RUNTIME.spawn(future)
}

#[allow(dead_code)]
pub fn spawn_blocking<F, R>(func: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    GLOBAL_RUNTIME.spawn_blocking(func)
}

#[allow(dead_code)]
pub fn block_on<F: Future>(future: F) -> F::Output { GLOBAL_RUNTIME.block_on(future) }
