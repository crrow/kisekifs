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
    cargo check

alias t := test
@test:
    cargo test --verbose  -- --nocapture --show-output

@book:
    mdbook serve docs