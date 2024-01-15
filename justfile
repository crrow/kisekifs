# List available just recipes
@help:
    just -l

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
    cargo check --workspace

alias t := test
@test:
    cargo test --verbose  -- --nocapture --show-output

@book:
    mdbook serve docs

# Applications:

@build-fs:
    cargo build --bin kisekifs

@run-fs-help:
    cargo run --bin kisekifs help

@mount:
    cargo run --color=always --bin kisekifs mount --target /tmp/hello

@unmount:
    cargo run --color=always --bin kisekifs unmount --target /tmp/hello