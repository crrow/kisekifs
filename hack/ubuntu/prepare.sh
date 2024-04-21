#!/bin/bash

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