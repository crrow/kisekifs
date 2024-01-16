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
    cargo build --bin kiseki

@run-fs-help:
    cargo run --bin kiseki help
    #cargo run --bin kiseki

@help-mount:
    cargo run --color=always --bin kiseki help mount

@mount:
    cargo run --color=always --bin kiseki mount

@help-umount:
    cargo run --color=always --bin kiseki help umount

@umount:
    cargo run --color=always --bin kiseki umount

@cloc:
    cloc . --exclude-dir=vendor,docs,tests,examples,build,scripts,tools,target