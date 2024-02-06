use snafu::{whatever, ResultExt, Whatever};
use std::error::Error;
use std::str::FromStr;

/// Reads an environment variable for the current process.
///
/// Compared to [std::env::var] there are a couple of differences:
///
/// - [var] uses [dotenvy] which loads the `.env` file from the current or
///   parent directories before returning the value.
///
/// - [var] returns `Ok(None)` (instead of `Err`) if an environment variable
///   wasn't set.
#[track_caller]
pub fn var(key: &str) -> Result<Option<String>, Whatever> {
    match dotenvy::var(key) {
        Ok(content) => Ok(Some(content)),
        Err(dotenvy::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(None),
        Err(error) => whatever!(Err(error), "Failed to read {key} environment variable"),
    }
}

/// Reads an environment variable for the current process, and parses it if
/// it is set.
///
/// Compared to [std::env::var] there are a couple of differences:
///
/// - [var] uses [dotenvy] which loads the `.env` file from the current or
///   parent directories before returning the value.
///
/// - [var] returns `Ok(None)` (instead of `Err`) if an environment variable
///   wasn't set.
#[track_caller]
pub fn var_parsed<R>(key: &str) -> Result<Option<R>, Whatever>
where
    R: FromStr,
    R::Err: Error + Send + Sync + 'static,
{
    match var(key) {
        Ok(Some(content)) => Ok(Some(content.parse().with_whatever_context(|e| {
            format!("Failed to parse {key} environment variable; {e}")
        })?)),
        Ok(None) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Reads an environment variable for the current process, and fails if it was
/// not found.
///
/// Compared to [std::env::var] there are a couple of differences:
///
/// - [var] uses [dotenvy] which loads the `.env` file from the current or
///   parent directories before returning the value.
#[track_caller]
pub fn required_var(key: &str) -> Result<String, Whatever> {
    required(var(key), key)
}

fn required<T>(res: Result<Option<T>, Whatever>, key: &str) -> Result<T, Whatever> {
    match res {
        Ok(opt) => {
            if opt.is_none() {
                whatever!("Failed to find required {key} environment variable")
            }
            Ok(opt.unwrap())
        }
        Err(error) => Err(error),
    }
}
