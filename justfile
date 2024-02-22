# List available just recipes
@help:
    just -l

@fmt:
    cargo +nightly fmt --all

# Calculate code
@cloc:
    cloc . --exclude-dir=vendor,docs,tests,examples,build,scripts,tools,target

# Install workspace tools
@install-tools:
    cargo install cargo-nextest
    cargo install cargo-release
    cargo install git-cliff
    cargo install cargo-criterion

# Lint and automatically fix what we can fix
@lint:
    cargo clippy --fix --allow-dirty --allow-staged
    cargo fmt

alias c := check
@check:
    cargo check --all --all-features --all-targets

alias t := test
@test:
    cargo test --verbose  -- --nocapture --show-output

alias b := bench
@bench:
    #cargo bench -- --verbose --color always --nocapture --show-output
    cargo bench

@book:
    mdbook serve docs

# Applications:

@build:
    #cargo build --bin kiseki
    cargo build --package kiseki-binary

@build-release:
    cargo build --release --package kiseki-binary

alias sh := show-help
@show-help:
    cargo run --bin kiseki help
    #cargo run --bin kiseki

# ==================================================== mount

alias sh-m := help-mount
@help-mount:
    cargo run --color=always --package kiseki-binary help mount

@mount:
    just clean
    just prepare
    cargo run --color=always --package kiseki-binary mount --level debug

@release-mount:
    just clean
    just prepare
    cargo run --release --color=always --package kiseki-binary mount --no-log

@profile-mount:
    just clean
    just prepare
    cargo flamegraph --package kiseki-binary -- mount --no-log

# ==================================================== umount

alias sh-um := help-umount
@help-umount:
    cargo run --color=always --packgae kiseki-binary help umount

@umount:
    cargo run --release --color=always --package kiseki-binary umount

# ==================================================== format

@format:
    cargo run --color=always --package kiseki-binary format
    taplo format
    taplo format --check

@prepare:
    mkdir -p /tmp/kiseki /tmp/kiseki.meta/
    just format

alias sh-f := help-format
@help-format:
    cargo run --color=always --bin kiseki help format

# ==================================================== MinIO

@minio:
    docker run -p 9000:9000 -p 9001:9001 \
      quay.io/minio/minio server /data --console-address ":9001"

# ==================================================== fio test

@clean:
    - rm -r /tmp/kiseki
    echo "Done: remove mount point"
    - rm -r /tmp/kiseki.meta/
    echo "Done: remove meta dir"
    - rm -r /tmp/kiseki.cache/
    echo "Done: remove cache dir"
    - rm -r /tmp/kiseki.stage_cache/
    echo "Done: remove stage cache dir"
    - rm -r /tmp/kiseki.data/
    echo "Done: remove data dir"
    - rm -r /tmp/kiseki.log/
    echo "Done: remove log dir"
    - rm -r /home/dh/kiseki/kiseki.data


alias sw := seq-write
@seq-write:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1 --group_reporting

alias rw := random-write
@random-write:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=randwrite --bs=1m --size=512m --numjobs=4 --direct=1 --group_reporting

alias sr := seq-read
@seq-read:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1 --group_reporting

alias rr := random-read
@random-read:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1 --group_reporting