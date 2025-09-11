//! RocksDB Performance Metrics Collection
//!
//! This module provides focused metrics collection for RocksDB database
//! performance monitoring. It tracks low-level database operations,
//! latencies, and error rates.
//!
//! ## Metric Categories:
//! - **Database Operations**: Read/write operations and their latencies
//! - **Transaction Management**: Transaction commits and durations
//! - **Error Tracking**: Database-level error monitoring
//! - **Storage Performance**: Key-value operation metrics

use std::{sync::OnceLock, time::Instant};

// OpenTelemetry imports for metrics collection
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};

/// Global RocksDB metrics collector instance
static ROCKSDB_METRICS: OnceLock<RocksDbPerformanceMetrics> = OnceLock::new();

/// RocksDB Performance Metrics Collector
///
/// Focuses exclusively on RocksDB database performance metrics,
/// following OpenTelemetry semantic conventions for database monitoring.
pub struct RocksDbPerformanceMetrics {
    // === Database Operations ===
    /// Total number of RocksDB read operations performed
    pub db_reads_total:        Counter<u64>,
    /// Total number of RocksDB write operations performed  
    pub db_writes_total:       Counter<u64>,
    /// Total number of RocksDB batch write operations
    pub db_batch_writes_total: Counter<u64>,
    /// Total number of RocksDB get operations (key lookups)
    pub db_gets_total:         Counter<u64>,
    /// Total number of RocksDB put operations (key writes)
    pub db_puts_total:         Counter<u64>,
    /// Total number of RocksDB delete operations
    pub db_deletes_total:      Counter<u64>,

    // === Performance Metrics ===
    /// Histogram of RocksDB read operation latencies in milliseconds
    pub db_read_duration_ms:  Histogram<f64>,
    /// Histogram of RocksDB write operation latencies in milliseconds
    pub db_write_duration_ms: Histogram<f64>,
    /// Histogram of RocksDB get operation latencies in milliseconds
    pub db_get_duration_ms:   Histogram<f64>,
    /// Histogram of RocksDB put operation latencies in milliseconds
    pub db_put_duration_ms:   Histogram<f64>,

    // === Transaction Metrics ===
    /// Total number of RocksDB transaction commits
    pub db_transactions_total:          Counter<u64>,
    /// Total number of RocksDB transaction rollbacks
    pub db_transaction_rollbacks_total: Counter<u64>,
    /// Histogram of RocksDB transaction durations in milliseconds
    pub db_transaction_duration_ms:     Histogram<f64>,

    // === Error Metrics ===
    /// Total number of database errors by type
    pub db_errors_total:            Counter<u64>,
    /// Total number of serialization errors
    pub serialization_errors_total: Counter<u64>,
    /// Total number of corruption errors detected
    pub corruption_errors_total:    Counter<u64>,
}

impl RocksDbPerformanceMetrics {
    /// Initialize RocksDB performance metrics
    ///
    /// Creates all metric instruments focused on RocksDB database
    /// performance. Follows OpenTelemetry semantic conventions for
    /// database monitoring.
    ///
    /// ## Metric Naming Convention:
    /// - `kiseki_rocksdb_*` - RocksDB database performance metrics
    /// - `kiseki_rocksdb_transaction_*` - Transaction-related metrics
    /// - `kiseki_rocksdb_error_*` - Database error metrics
    fn new() -> Self {
        let meter = opentelemetry::global::meter("kiseki-rocksdb-performance");

        Self {
            // Database Operations
            db_reads_total: meter
                .u64_counter("kiseki_rocksdb_reads_total")
                .with_description("Total number of RocksDB read operations")
                .build(),

            db_writes_total: meter
                .u64_counter("kiseki_rocksdb_writes_total")
                .with_description("Total number of RocksDB write operations")
                .build(),

            db_batch_writes_total: meter
                .u64_counter("kiseki_rocksdb_batch_writes_total")
                .with_description("Total number of RocksDB batch write operations")
                .build(),

            db_gets_total: meter
                .u64_counter("kiseki_rocksdb_gets_total")
                .with_description("Total number of RocksDB get operations")
                .build(),

            db_puts_total: meter
                .u64_counter("kiseki_rocksdb_puts_total")
                .with_description("Total number of RocksDB put operations")
                .build(),

            db_deletes_total: meter
                .u64_counter("kiseki_rocksdb_deletes_total")
                .with_description("Total number of RocksDB delete operations")
                .build(),

            // Performance Metrics
            db_read_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_read_duration_ms")
                .with_description("RocksDB read operation latency in milliseconds")
                .build(),

            db_write_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_write_duration_ms")
                .with_description("RocksDB write operation latency in milliseconds")
                .build(),

            db_get_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_get_duration_ms")
                .with_description("RocksDB get operation latency in milliseconds")
                .build(),

            db_put_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_put_duration_ms")
                .with_description("RocksDB put operation latency in milliseconds")
                .build(),

            // Transaction Metrics
            db_transactions_total: meter
                .u64_counter("kiseki_rocksdb_transactions_total")
                .with_description("Total number of RocksDB transaction commits")
                .build(),

            db_transaction_rollbacks_total: meter
                .u64_counter("kiseki_rocksdb_transaction_rollbacks_total")
                .with_description("Total number of RocksDB transaction rollbacks")
                .build(),

            db_transaction_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_transaction_duration_ms")
                .with_description("RocksDB transaction duration in milliseconds")
                .build(),

            // Error Metrics
            db_errors_total: meter
                .u64_counter("kiseki_rocksdb_errors_total")
                .with_description("Total number of RocksDB database errors")
                .build(),

            serialization_errors_total: meter
                .u64_counter("kiseki_rocksdb_serialization_errors_total")
                .with_description("Total number of data serialization errors")
                .build(),

            corruption_errors_total: meter
                .u64_counter("kiseki_rocksdb_corruption_errors_total")
                .with_description("Total number of data corruption errors detected")
                .build(),
        }
    }
}

/// Get the global RocksDB performance metrics instance
pub fn get_rocksdb_metrics() -> &'static RocksDbPerformanceMetrics {
    ROCKSDB_METRICS.get_or_init(RocksDbPerformanceMetrics::new)
}

/// Record RocksDB read operation with timing
pub fn record_db_read<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0; // Convert to milliseconds

    let metrics = get_rocksdb_metrics();
    metrics.db_reads_total.add(1, &[]);
    metrics.db_read_duration_ms.record(duration, &[]);

    result
}

/// Record RocksDB write operation with timing
pub fn record_db_write<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    let metrics = get_rocksdb_metrics();
    metrics.db_writes_total.add(1, &[]);
    metrics.db_write_duration_ms.record(duration, &[]);

    result
}

/// Record RocksDB batch write operation with timing
pub fn record_db_batch_write<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    let metrics = get_rocksdb_metrics();
    metrics.db_batch_writes_total.add(1, &[]);
    metrics.db_write_duration_ms.record(duration, &[]);

    result
}

/// Record RocksDB get operation with timing
pub fn record_db_get<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    let metrics = get_rocksdb_metrics();
    metrics.db_gets_total.add(1, &[]);
    metrics.db_get_duration_ms.record(duration, &[]);

    result
}

/// Record RocksDB put operation with timing
pub fn record_db_put<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    let metrics = get_rocksdb_metrics();
    metrics.db_puts_total.add(1, &[]);
    metrics.db_put_duration_ms.record(duration, &[]);

    result
}

/// Record RocksDB transaction with timing
pub fn record_db_transaction<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    let metrics = get_rocksdb_metrics();
    metrics.db_transactions_total.add(1, &[]);
    metrics.db_transaction_duration_ms.record(duration, &[]);

    result
}

/// Record RocksDB database error
pub fn record_db_error(error_type: &str) {
    let metrics = get_rocksdb_metrics();
    let labels = &[KeyValue::new("error_type", error_type.to_string())];
    metrics.db_errors_total.add(1, labels);
}

/// Record serialization error
pub fn record_serialization_error() {
    let metrics = get_rocksdb_metrics();
    metrics.serialization_errors_total.add(1, &[]);
}

/// Record data corruption error
pub fn record_corruption_error() {
    let metrics = get_rocksdb_metrics();
    metrics.corruption_errors_total.add(1, &[]);
}

// === RocksDB Performance Metrics Macros ===

/// Macro for RocksDB operation counter increments
macro_rules! rocksdb_counter {
    (db_reads_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_reads_total
            .add(1, &[]);
    }};
    (db_writes_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_writes_total
            .add(1, &[]);
    }};
    (db_batch_writes_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_batch_writes_total
            .add(1, &[]);
    }};
    (db_gets_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_gets_total
            .add(1, &[]);
    }};
    (db_puts_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_puts_total
            .add(1, &[]);
    }};
    (db_deletes_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_deletes_total
            .add(1, &[]);
    }};
    (db_transactions_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_transactions_total
            .add(1, &[]);
    }};
    (db_transaction_rollbacks_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_transaction_rollbacks_total
            .add(1, &[]);
    }};
}

/// Macro for RocksDB histogram operations
macro_rules! rocksdb_histogram {
    (db_read_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_read_duration_ms
            .record($value, &[]);
    }};
    (db_write_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_write_duration_ms
            .record($value, &[]);
    }};
    (db_get_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_get_duration_ms
            .record($value, &[]);
    }};
    (db_put_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_put_duration_ms
            .record($value, &[]);
    }};
    (db_transaction_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_transaction_duration_ms
            .record($value, &[]);
    }};
}

/// Macro for RocksDB error recording with typed error constants
macro_rules! rocksdb_error {
    ($error_type:expr) => {{
        let labels = &[opentelemetry::KeyValue::new("error_type", $error_type)];
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_errors_total
            .add(1, labels);
    }};
}

/// Combined macro for RocksDB operations with timing and counter
macro_rules! rocksdb_timed_op {
    ($counter_name:ident, $histogram_name:ident, $operation:expr) => {{
        let start = std::time::Instant::now();
        let result = $operation;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        rocksdb_counter!($counter_name);
        rocksdb_histogram!($histogram_name, duration_ms);

        result
    }};
}

/// Simple macro for recording delete operations (no timing needed for batch
/// operations)
macro_rules! rocksdb_delete {
    () => {{
        rocksdb_counter!(db_deletes_total);
    }};
}

pub(crate) use rocksdb_counter;
pub(crate) use rocksdb_delete;
pub(crate) use rocksdb_error;
pub(crate) use rocksdb_histogram;
pub(crate) use rocksdb_timed_op;
