#!/bin/bash
# Copyright 2024 kisekifs
#
# JuiceFS, Copyright 2020 Juicedata, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Exit immediately if any command fails
set -e

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update && sudo apt-get install -y \
    build-essential \
    pkg-config \
    fuse3 libfuse3-dev \
    openssl libssl-dev \
    clang \
    fio \
    curl
    

# install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
export PATH=/root/.cargo/bin/:$PATH
export PATH=$HOME/.cargo/bin/:$PATH

rustup update stable
cargo install just hawkeye
cargo install taplo-cli --locked