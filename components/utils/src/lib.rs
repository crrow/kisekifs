pub mod env;
pub mod logger;
pub mod pyroscope_init;
pub mod readable_size;
pub mod runtime;
pub mod sentry_init;

lazy_static::lazy_static! {
    pub static ref RANDOM_ID_GENERATOR: sonyflake::Sonyflake =
        sonyflake::Sonyflake::new().expect("failed to create id generator");
}

pub fn random_id() -> u64 {
    RANDOM_ID_GENERATOR
        .next_id()
        .expect("failed to generate id")
}

pub mod num_cpus {
    pub use num_cpus::get;
    pub use num_cpus::get_physical;
}
