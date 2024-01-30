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

use std::{sync::Arc, time::Duration};

#[derive(Default)]
pub(crate) struct WorkerListener {
    #[cfg(any(test, feature = "test"))]
    listener: Option<EventListenerRef>,
}

pub(crate) type EventListenerRef = Arc<dyn EventListener>;

pub(crate) trait EventListener: Send + Sync + 'static {
    /// Notifies the listener that the event has succeed.
    fn on_success(&self);

    /// Notifies the listener that the event starts to begin.
    fn on_begin(&self);

    /// Notifies the listener that the later drop task starts running.
    /// Returns the gc interval if we want to override the default one.
    fn on_later_drop_begin(&self) -> Option<Duration> {
        None
    }

    /// Notifies the listener that the later drop task of the region is
    /// finished.
    fn on_later_drop_end(&self, removed: bool) {
        let _ = removed;
    }
}
