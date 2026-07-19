// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod align;
pub mod env;
pub mod logger;
pub mod object_storage;
pub mod panic_hook;
pub mod pyroscope_init;
pub mod readable_size;
pub mod runtime;
pub mod sentry_init;

use std::sync::LazyLock;

pub static RANDOM_ID_GENERATOR: LazyLock<sonyflake::Sonyflake> =
    LazyLock::new(|| sonyflake::Sonyflake::new().expect("failed to create id generator"));

/// the user ID for the user running the process.
static UID: LazyLock<u32> = LazyLock::new(users::get_current_uid);
/// the group ID for the user running the process.
static GID: LazyLock<u32> = LazyLock::new(users::get_current_gid);

/// the number of available CPUs(number of logical cores.) of the current
/// system.
static NUM_CPUS: LazyLock<usize> = LazyLock::new(num_cpus::get);
/// the number of physical cores of the current system.
/// This will always return at least 1.
static NUM_PHYSICAL_CPUS: LazyLock<usize> = LazyLock::new(num_cpus::get_physical);

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
