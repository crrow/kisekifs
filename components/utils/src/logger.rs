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

use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::Sampler};
use opentelemetry_semantic_conventions::resource;
use serde::{Deserialize, Serialize};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    filter, fmt::Layer, layer::SubscriberExt, prelude::*, EnvFilter, Registry,
};

const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";
pub const DEFAULT_LOG_DIR: &str = "/tmp/kiseki.logs";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingOptions {
    pub dir: String,
    pub level: Option<String>,
    pub enable_otlp_tracing: bool,
    pub otlp_endpoint: Option<String>,
    pub tracing_sample_ratio: Option<f64>,
    pub append_stdout: bool,
}

impl PartialEq for LoggingOptions {
    fn eq(&self, other: &Self) -> bool {
        self.dir == other.dir
            && self.level == other.level
            && self.enable_otlp_tracing == other.enable_otlp_tracing
            && self.otlp_endpoint == other.otlp_endpoint
            && self.tracing_sample_ratio == other.tracing_sample_ratio
            && self.append_stdout == other.append_stdout
    }
}

impl Eq for LoggingOptions {}

impl Default for LoggingOptions {
    fn default() -> Self {
        Self {
            dir: DEFAULT_LOG_DIR.to_string(),
            level: None,
            enable_otlp_tracing: false,
            otlp_endpoint: None,
            tracing_sample_ratio: None,
            append_stdout: true,
        }
    }
}

impl LoggingOptions {
    pub fn with_dir(self, dir: String) -> Self {
        Self { dir, ..self }
    }

    pub fn with_enable_otlp_tracing(self, v: bool) -> Self {
        Self {
            enable_otlp_tracing: v,
            ..self
        }
    }
}

const DEFAULT_LOG_TARGETS: &str = "info";

#[allow(clippy::print_stdout)]
pub fn init_global_logging(app_name: &str, opts: &LoggingOptions) -> Vec<WorkerGuard> {
    let mut guards = vec![];
    let dir = &opts.dir;
    let level = &opts.level;
    let enable_otlp_tracing = opts.enable_otlp_tracing;

    let stdout_logging_layer = if opts.append_stdout {
        let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        guards.push(stdout_guard);

        Some(Layer::new().with_writer(stdout_writer))
    } else {
        None
    };

    // JSON log layer.
    let rolling_appender = RollingFileAppender::new(Rotation::HOURLY, dir, app_name);
    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    let file_logging_layer = Layer::new().with_writer(rolling_writer);
    guards.push(rolling_writer_guard);

    // error JSON log layer.
    let err_rolling_appender =
        RollingFileAppender::new(Rotation::HOURLY, dir, format!("{}-{}", app_name, "err"));
    let (err_rolling_writer, err_rolling_writer_guard) =
        tracing_appender::non_blocking(err_rolling_appender);
    let err_file_logging_layer = Layer::new().with_writer(err_rolling_writer);
    guards.push(err_rolling_writer_guard);

    // resolve log level settings from:
    // - options from command line or config files
    // - environment variable: RUST_LOG
    // - default settings
    let rust_log_env = std::env::var(EnvFilter::DEFAULT_ENV).ok();
    let targets_string = level
        .as_deref()
        .or(rust_log_env.as_deref())
        .unwrap_or(DEFAULT_LOG_TARGETS);
    let filter = targets_string
        .parse::<filter::Targets>()
        .expect("error parsing log level string");
    let sampler = opts
        .tracing_sample_ratio
        .map(Sampler::TraceIdRatioBased)
        .unwrap_or(Sampler::AlwaysOn);
    let fmt_layer = Layer::new()
        .with_file(true)
        .with_line_number(true)
        .with_ansi(supports_color::on(supports_color::Stream::Stdout).is_some());

    let subscriber = Registry::default()
        .with(fmt_layer)
        .with(filter)
        .with(stdout_logging_layer)
        .with(file_logging_layer)
        .with(err_file_logging_layer.with_filter(filter::LevelFilter::ERROR));

    if enable_otlp_tracing {
        global::set_text_map_propagator(TraceContextPropagator::new());
        // otlp exporter
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter().tonic().with_endpoint(
                    opts.otlp_endpoint
                        .as_ref()
                        .map(|e| format!("http://{}", e))
                        .unwrap_or(DEFAULT_OTLP_ENDPOINT.to_string()),
                ),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(sampler)
                    .with_resource(opentelemetry_sdk::Resource::new(vec![
                        KeyValue::new(resource::SERVICE_NAME, app_name.to_string()),
                        KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                        KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
                    ])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .expect("otlp tracer install failed");
        let tracing_layer = Some(tracing_opentelemetry::layer().with_tracer(tracer));
        let subscriber = subscriber.with(tracing_layer);
        tracing::subscriber::set_global_default(subscriber)
            .expect("error setting global tracing subscriber");
    } else {
        tracing::subscriber::set_global_default(subscriber)
            .expect("error setting global tracing subscriber");
    }

    guards
}

pub fn init_global_logging_without_runtime(
    app_name: &str,
    opts: &LoggingOptions,
) -> Vec<WorkerGuard> {
    // The opentelemetry batch processor and the OTLP exporter needs a Tokio
    // runtime. Create a dedicated runtime for them. One thread should be
    // enough.
    //
    // (Alternatively, instead of batching, we could use the "simple
    // processor", which doesn't need Tokio, and use "reqwest-blocking"
    // feature for the OTLP exporter, which also doesn't need Tokio.  However,
    // batching is considered best practice, and also I have the feeling that
    // the non-Tokio codepaths in the opentelemetry crate are less used and
    // might be more buggy, so better to stay on the well-beaten path.)
    //
    // We leak the runtime so that it keeps running after we exit the
    // function.
    let runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("otlp runtime thread")
            .worker_threads(1)
            .build()
            .unwrap(),
    ));
    let _guard = runtime.enter();
    init_global_logging(app_name, opts)
}

#[allow(dead_code)]
pub fn install_fmt_log() {
    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    let subscriber = Registry::default().with(stdout_log);
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");
}
