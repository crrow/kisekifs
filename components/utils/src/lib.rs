use lazy_static::lazy_static;
use users::{Groups, Users};
pub mod align;
pub mod env;
pub mod logger;
pub mod object_storage;
pub mod panic_hook;
pub mod pyroscope_init;
pub mod readable_size;
pub mod runtime;
pub mod sentry_init;

lazy_static::lazy_static! {
    pub static ref RANDOM_ID_GENERATOR: sonyflake::Sonyflake =
        sonyflake::Sonyflake::new().expect("failed to create id generator");

    /// the user ID for the user running the process.
    static ref UID: u32 = users::get_current_uid();
    /// the group ID for the user running the process.
    static ref GID: u32 = users::get_current_gid();

    /// the number of available CPUs(number of logical cores.) of the current system.
    static ref NUM_CPUS: usize = num_cpus::get();
    /// the number of physical cores of the current system.
    /// This will always return at least 1.
    static ref NUM_PHYSICAL_CPUS: usize = num_cpus::get_physical();
}

pub fn random_id() -> u64 {
    RANDOM_ID_GENERATOR
        .next_id()
        .expect("failed to generate id")
}

#[inline(always)]
pub fn num_cpus() -> usize { *NUM_CPUS }
#[inline(always)]
pub fn num_physical_cpus() -> usize { *NUM_PHYSICAL_CPUS }

#[inline(always)]
pub fn uid() -> u32 { *UID }

#[inline(always)]
pub fn gid() -> u32 { *GID }
