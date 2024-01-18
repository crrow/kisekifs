// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod data;
pub use data::*;
mod tracing_span;
use std::{thread, thread::JoinHandle, time::Duration};

use dashmap::DashMap;
use metrics::{self, Key, Recorder};
pub use tracing_span::metrics_tracing_span_layer;

use crate::sync::{
    mpsc::{channel, RecvTimeoutError, Sender},
    Arc,
};

/// How long between drains of each thread's local metrics into the global sink
const AGGREGATION_PERIOD: Duration = Duration::from_secs(5);

/// The log target to use for emitted metrics
pub const TARGET_NAME: &str = "kiseki::metrics";

/// Initialize and install the global metrics sink, and return a handle that can
/// be used to shut the sink down. The sink should only be shut down after any
/// threads that generate metrics are done with their work; metrics generated
/// after shutting down the sink will be lost.
///
/// Panics if a sink has already been installed.
pub fn install() -> MetricsSinkHandle {
    let sink = Arc::new(MetricsSink::new());

    let (tx, rx) = channel();

    let publisher_thread = {
        let inner = Arc::clone(&sink);
        thread::spawn(move || {
            loop {
                match rx.recv_timeout(AGGREGATION_PERIOD) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => inner.publish(),
                }
            }
            // Drain metrics one more time before shutting down. This has a chance of
            // missing any new metrics data after the sink shuts down, but we
            // assume a clean shutdown stops generating new metrics before
            // shutting down the sink.
            inner.publish();
        })
    };

    let handle = MetricsSinkHandle {
        shutdown: tx,
        handle: Some(publisher_thread),
    };

    let recorder = MetricsRecorder { sink };
    metrics::set_boxed_recorder(Box::new(recorder)).unwrap();

    handle
}

#[derive(Debug)]
struct MetricsSink {
    metrics: DashMap<Key, Metric>,
}

impl MetricsSink {
    fn new() -> Self {
        Self {
            metrics: DashMap::with_capacity(64),
        }
    }

    fn counter(&self, key: &Key) -> metrics::Counter {
        let entry = self
            .metrics
            .entry(key.clone())
            .or_insert_with(Metric::counter);
        entry.as_counter()
    }

    fn gauge(&self, key: &Key) -> metrics::Gauge {
        let entry = self
            .metrics
            .entry(key.clone())
            .or_insert_with(Metric::gauge);
        entry.as_gauge()
    }

    fn histogram(&self, key: &Key) -> metrics::Histogram {
        let entry = self
            .metrics
            .entry(key.clone())
            .or_insert_with(Metric::histogram);
        entry.as_histogram()
    }

    /// Publish all this sink's metrics to `tracing` log messages
    fn publish(&self) {
        // Collect the output lines so we can sort them to make reading easier
        let mut metrics = vec![];

        for mut entry in self.metrics.iter_mut() {
            let (key, metric) = entry.pair_mut();
            let Some(metric) = metric.fmt_and_reset() else {
                continue;
            };
            let labels = if key.labels().len() == 0 {
                String::new()
            } else {
                format!(
                    "[{}]",
                    key.labels()
                        .map(|label| format!("{}={}", label.key(), label.value()))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            metrics.push(format!("{}{}: {}", key.name(), labels, metric));
        }

        metrics.sort();

        for metric in metrics {
            tracing::info!(target: TARGET_NAME, "{}", metric);
        }
    }
}

/// The actual recorder that will be installed for the metrics facade. Just a
/// wrapper around a [MetricsSinkInner] that does all the real work.
struct MetricsRecorder {
    sink: Arc<MetricsSink>,
}

impl Recorder for MetricsRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // No-op -- we don't implement descriptions
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // No-op -- we don't implement descriptions
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // No-op -- we don't implement descriptions
    }

    fn register_counter(&self, key: &Key) -> metrics::Counter {
        self.sink.counter(key)
    }

    fn register_gauge(&self, key: &Key) -> metrics::Gauge {
        self.sink.gauge(key)
    }

    fn register_histogram(&self, key: &Key) -> metrics::Histogram {
        self.sink.histogram(key)
    }
}

#[derive(Debug)]
pub struct MetricsSinkHandle {
    shutdown: Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for MetricsSinkHandle {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
