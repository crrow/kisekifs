use crate::env::{required_var, var, var_parsed};
pub use sentry::release_name;
use sentry::types::Dsn;
use sentry::{ClientInitGuard, IntoDsn};
use snafu::{ResultExt, Whatever};

pub struct SentryConfig {
    pub dsn: Option<Dsn>,
    pub environment: Option<String>,
    pub release: Option<String>,
    pub traces_sample_rate: f32,
}

impl SentryConfig {
    pub fn from_environment() -> Result<Self, Whatever> {
        let dsn = var("SENTRY_DSN")?.into_dsn().with_whatever_context(|e| {
            format!("SENTRY_DSN_API is not a valid Sentry DSN value {}", e)
        })?;

        let environment =
            match dsn {
                None => None,
                Some(_) => Some(required_var("SENTRY_ENVIRONMENT").with_whatever_context(
                    |_| "SENTRY_ENV_API must be set when using SENTRY_DSN_API",
                )?),
            };

        Ok(Self {
            dsn,
            environment,
            release: var("SENTRY_RELEASE")?,
            traces_sample_rate: var_parsed("SENTRY_TRACES_SAMPLE_RATE")?.unwrap_or(0.0),
        })
    }
}

/// Initializes the Sentry SDK from the environment variables.
///
/// If `SENTRY_DSN_API` is not set then Sentry will not be initialized,
/// otherwise it is required to be a valid DSN string. `SENTRY_ENV_API` must
/// be set if a DSN is provided.
///
/// `HEROKU_SLUG_COMMIT`, if present, will be used as the `release` property
/// on all events.
#[must_use]
pub fn init_sentry() -> Option<ClientInitGuard> {
    let config = match SentryConfig::from_environment() {
        Ok(config) => config,
        Err(error) => {
            eprint!(
                "Failed to read Sentry configuration from environment: {}",
                error
            );
            return None;
        }
    };

    let opts = sentry::ClientOptions {
        auto_session_tracking: true,
        dsn: config.dsn,
        environment: config.environment.map(Into::into),
        release: config.release.map(Into::into),
        session_mode: sentry::SessionMode::Request,
        traces_sample_rate: config.traces_sample_rate,
        ..Default::default()
    };

    let guard = sentry::init(opts);
    Some(guard)
}
