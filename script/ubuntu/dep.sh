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
set -eux -o pipefail
export DEBIAN_FRONTEND=noninteractive

sudo apt-get update -y && sudo apt-get install -y \
    wget curl vim \
    fuse3 libfuse3-dev \
    build-essential \
    pkg-config \
    openssl libssl-dev \
    clang \
    fio \
    curl

install_flox() {
    # Check if flox is already installed
    if command -v flox &> /dev/null; then
        echo "flox is already installed."
    else
        # Download and run the flox installation script
        wget https://downloads.flox.dev/by-env/stable/deb/flox-1.2.2.x86_64-linux.deb -o /tmp/flox.deb
        dpkg -i /tmp/flox.deb
    fi
}

# install rust
install_rust() {
    # Check if rustup is already installed
    if command -v rustup &> /dev/null; then
        echo "rustup is already installed."
    else
        # Download and run the rustup installation script
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
    fi

    # Add Cargo bin to PATH if it's not already included
    if [[ ":$PATH:" != *":/root/.cargo/bin:"* ]]; then
        export PATH="/root/.cargo/bin:$PATH"
    fi
    if [[ ":$PATH:" != *":$HOME/.cargo/bin:"* ]]; then
        export PATH="$HOME/.cargo/bin:$PATH"
    fi

    # Update Rust to the stable version
    rustup update stable

    # install some tools
    cargo install just hawkeye taplo-cli cargo-nextest cargo-release git-cliff cargo-criterion
}

install_flox
install_rust

just c

