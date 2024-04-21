FROM rust:1.77.2

WORKDIR /kiseki

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y pkg-config \
    fuse3 libfuse3-dev \
    openssl libssl-dev \
    clang