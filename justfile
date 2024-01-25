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
    cargo check --workspace

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

@build-fs:
    cargo build --bin kiseki

alias sh := show-help
@show-help:
    cargo run --bin kiseki help
    #cargo run --bin kiseki

# ==================================================== mount

alias sh-m := help-mount
@help-mount:
    cargo run --color=always --bin kiseki help mount

@mount:
    cargo run --color=always --bin kiseki mount

# ==================================================== umount

alias sh-um := help-umount
@help-umount:
    cargo run --color=always --bin kiseki help umount

@umount:
    cargo run --color=always --bin kiseki umount

# ==================================================== format

@format:
    cargo run --color=always --bin kiseki format

alias sh-f := help-format
@help-format:
    cargo run --color=always --bin kiseki help format

