use crate::tracing_utils;

use snafu::Whatever;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Initialize logging to stderr, and OpenTelemetry tracing and exporter.
///
/// Logging is configured using either `default_log_level` or
/// `RUST_LOG` environment variable as default log level.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the destination,
/// set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`. See
/// `tracing-utils` package description.
///
pub fn init_tracing_and_logging(
    service_name: &str,
    default_log_level: &str,
) -> Result<(), Whatever> {
    // Initialize Logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(std::io::stderr);

    // Initialize OpenTelemetry
    let otlp_layer =
        tracing_utils::init_tracing_without_runtime(service_name).map(OpenTelemetryLayer::new);

    // Put it all together
    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(fmt_layer)
        .init();
    tracing::info!("logging and tracing started");

    replace_panic_hook_with_tracing_panic_hook().forget();

    Ok(())
}

/// Disable the default rust panic hook by using `set_hook`.
///
/// For neon binaries, the assumption is that tracing is configured before with [`init`], after
/// that sentry is configured (if needed). sentry will install it's own on top of this, always
/// processing the panic before we log it.
///
/// When the return value is dropped, the hook is reverted to std default hook (prints to stderr).
/// If the assumptions about the initialization order are not held, use
/// [`TracingPanicHookGuard::forget`] but keep in mind, if tracing is stopped, then panics will be
/// lost.
#[must_use]
pub fn replace_panic_hook_with_tracing_panic_hook() -> TracingPanicHookGuard {
    std::panic::set_hook(Box::new(tracing_panic_hook));
    TracingPanicHookGuard::new()
}

/// Drop guard which restores the std panic hook on drop.
///
/// Tracing should not be used when it's not configured, but we cannot really latch on to any
/// imaginary lifetime of tracing.
pub struct TracingPanicHookGuard {
    act: bool,
}

impl TracingPanicHookGuard {
    fn new() -> Self {
        TracingPanicHookGuard { act: true }
    }

    /// Make this hook guard not do anything when dropped.
    pub fn forget(&mut self) {
        self.act = false;
    }
}

impl Drop for TracingPanicHookGuard {
    fn drop(&mut self) {
        if self.act {
            let _ = std::panic::take_hook();
        }
    }
}

/// Named symbol for our panic hook, which logs the panic.
fn tracing_panic_hook(info: &std::panic::PanicInfo) {
    // following rust 1.66.1 std implementation:
    // https://github.com/rust-lang/rust/blob/90743e7298aca107ddaa0c202a4d3604e29bfeb6/library/std/src/panicking.rs#L235-L288
    let location = info.location();

    let msg = match info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<dyn Any>",
        },
    };

    let thread = std::thread::current();
    let thread = thread.name().unwrap_or("<unnamed>");
    let backtrace = std::backtrace::Backtrace::capture();

    let _entered = if let Some(location) = location {
        tracing::error_span!("panic", %thread, location = %PrettyLocation(location))
    } else {
        // very unlikely to hit here, but the guarantees of std could change
        tracing::error_span!("panic", %thread)
    }
    .entered();

    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        // this has an annoying extra '\n' in the end which anyhow doesn't do, but we cannot really
        // get rid of it as we cannot get in between of std::fmt::Formatter<'_>; we could format to
        // string, maybe even to a TLS one but tracing already does that.
        tracing::error!("{msg}\n\nStack backtrace:\n{backtrace}");
    } else {
        tracing::error!("{msg}");
    }

    // ensure that we log something on the panic if this hook is left after tracing has been
    // unconfigured. worst case when teardown is racing the panic is to log the panic twice.
    tracing::dispatcher::get_default(|d| {
        if let Some(_none) = d.downcast_ref::<tracing::subscriber::NoSubscriber>() {
            let location = location.map(PrettyLocation);
            log_panic_to_stderr(thread, msg, location, &backtrace);
        }
    });
}

#[cold]
fn log_panic_to_stderr(
    thread: &str,
    msg: &str,
    location: Option<PrettyLocation<'_, '_>>,
    backtrace: &std::backtrace::Backtrace,
) {
    eprintln!("panic while tracing is unconfigured: thread '{thread}' panicked at '{msg}', {location:?}\nStack backtrace:\n{backtrace}");
}

struct PrettyLocation<'a, 'b>(&'a std::panic::Location<'b>);

impl std::fmt::Display for PrettyLocation<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.0.file(), self.0.line(), self.0.column())
    }
}

impl std::fmt::Debug for PrettyLocation<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}
