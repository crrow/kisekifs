/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::time::Instant;

use metrics::histogram;
use tracing::span::Attributes;
use tracing::{Id, Level, Subscriber};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};
use tracing_subscriber::Layer;

/// A [tracing::Layer] that publishes metrics about important [Span]s (mostly the root span of FUSE
/// requests) into the aggregate metrics.
///
/// This layer "knows about" some of our Span targets and names, and uses them to decide when and
/// how to emit metrics. If those names change, this needs to change as well.
#[derive(Debug)]
struct MetricsTracingSpanLayer;

impl MetricsTracingSpanLayer {
    fn should_instrument_request_time<'a, S: LookupSpan<'a>>(span: Option<SpanRef<'a, S>>) -> bool {
        if let Some(data) = span {
            if data.metadata().target() == "kiseki::fuse" && data.parent().is_none() {
                return true;
            }
        }
        false
    }
}

impl<S> Layer<S> for MetricsTracingSpanLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if Self::should_instrument_request_time(ctx.span(id)) {
            let data = ctx.span(id).unwrap();
            data.extensions_mut().insert(RequestTime(Instant::now()));
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if Self::should_instrument_request_time(ctx.span(&id)) {
            let data = ctx.span(&id).unwrap();
            let RequestTime(start_time) = *data.extensions().get::<RequestTime>().unwrap();
            histogram!("fuse.op_latency_us", start_time.elapsed().as_micros() as f64, "op" => data.name());
        }
    }
}

pub fn metrics_tracing_span_layer<S>() -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    MetricsTracingSpanLayer.with_filter(Targets::new().with_target("kiseki::fuse", Level::DEBUG))
}

/// The time at which a request started
#[derive(Debug, Clone, Copy)]
struct RequestTime(Instant);
