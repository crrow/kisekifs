// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod built {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub use built::*;

pub const AUTHOR: &str = built::PKG_AUTHORS;

/// Valid SemVer version constructed using declared Cargo version and short
/// commit hash if needed.
pub const FULL_VERSION: &str = {
    if is_official_release() {
        built::PKG_VERSION
    } else {
        // A little hacky so we can pull out the hash as a const
        const COMMIT_HASH_STR: &str = match built::GIT_COMMIT_HASH_SHORT {
            Some(hash) => hash,
            None => "",
        };
        const COMMIT_DIRTY_STR: &str = match built::GIT_DIRTY {
            Some(true) => "-dirty",
            _ => "",
        };
        const UNOFFICIAL_SUFFIX: &str = if COMMIT_HASH_STR.is_empty() {
            "-unofficial"
        } else {
            const_format::concatcp!("-unofficial+", COMMIT_HASH_STR, COMMIT_DIRTY_STR)
        };
        const_format::concatcp!(built::PKG_VERSION, UNOFFICIAL_SUFFIX)
    }
};
const fn is_official_release() -> bool { option_env!("KISEKI_RELEASE").is_some() }
