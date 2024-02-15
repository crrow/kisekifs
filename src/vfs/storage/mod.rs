use std::{cmp::Ordering, str::FromStr};

use snafu::ResultExt;

mod engine;

pub use engine::Config as EngineConfig;
pub(crate) use engine::Engine;
mod reader;
mod writer;
