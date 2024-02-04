use std::{
    error::Error,
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crc32fast::Hasher;
use dashmap::DashMap;
use futures::FutureExt;
use moka::future::Cache;
use snafu::ResultExt;
use tokio::{io::AsyncWriteExt, select, sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::{
    common::readable_size::ReadableSize,
    meta::types::SliceID,
    vfs::{
        err::{CacheIOSnafu, OpenDalSnafu, Result},
        VFSError,
    },
};

pub(crate) enum CacheKind {
    Moka,
    Juice,
}
