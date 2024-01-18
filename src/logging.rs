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

use std::path::PathBuf;

use snafu::Whatever;
use supports_color;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::metrics::metrics_tracing_span_layer;

/// Configuration for Mountpoint logging
#[derive(Debug)]
pub struct LoggingConfig {
    /// A directory to create log files in. If unspecified, logs will be routed
    /// to syslog.
    pub log_directory: Option<PathBuf>,
    /// Whether to duplicate logs to stdout in addition to syslog or the log
    /// directory.
    pub log_to_stdout: bool,
    /// The default filter directive (in the sense of
    /// [tracing_subscriber::filter::EnvFilter]) to use for logs. Will be
    /// overridden by the `MOUNTPOINT_LOG` environment variable if set.
    pub default_filter: String,
}

impl LoggingConfig {
    pub fn init_tracing_subscriber(self) -> Result<(), Whatever> {
        let env_filter = create_env_filter(&self.default_filter);
        // Don't create the files or subscribers if we'll never emit any logs
        if env_filter.max_level_hint() == Some(LevelFilter::OFF) {
            return Ok(());
        }
        let env_filter = create_env_filter(&self.default_filter);
        // Don't create the files or subscribers if we'll never emit any logs
        if env_filter.max_level_hint() == Some(LevelFilter::OFF) {
            return Ok(());
        }
        let console_layer = if self.log_to_stdout {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_ansi(supports_color::on(supports_color::Stream::Stdout).is_some())
                .with_filter(create_env_filter(&self.default_filter));
            Some(fmt_layer)
        } else {
            None
        };

        let registry = tracing_subscriber::registry()
            // .with(syslog_layer)
            .with(console_layer)
            // .with(file_layer)
            .with(metrics_tracing_span_layer());

        registry.init();
        Ok(())
    }
}

/// Create the logging config from the MOUNTPOINT_LOG environment variable or
/// the default config if that variable is unset. We do this in a function
/// because [EnvFilter] isn't [Clone] and we need a copy of the filter for each
/// [Layer].
fn create_env_filter(filter: &str) -> EnvFilter {
    EnvFilter::try_from_env("MOUNTPOINT_LOG").unwrap_or_else(|_| EnvFilter::new(filter))
}
