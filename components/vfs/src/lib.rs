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

// `err::Error` transitively embeds `opendal::Error` (via the meta and storage
// error types); boxing those variants is deferred to avoid touching the object
// storage facade, so the large-Err lint is silenced crate-wide for now.
#![allow(clippy::result_large_err)]

mod config;

pub use config::Config;
mod err;
mod handle;
mod kiseki;
pub use kiseki::KisekiVFS;
mod data_manager;
mod reader;
mod writer;
