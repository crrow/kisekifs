# KisekiFS Project Context

## What This Project Is

KisekiFS is a Rust learning project—a distributed FUSE filesystem ported from
[JuiceFS](https://github.com/juicedata/juicefs). It separates data storage
(object storage like S3) from metadata storage (RocksDB).

**Workspace Structure:**

```
components/
├── binary/      # CLI entry point (package kiseki-binary, binary `kiseki`)
├── fuse/        # FUSE layer (filesystem operations)
├── vfs/         # Virtual filesystem logic
├── meta/        # Metadata management (RocksDB backend, `meta-rocksdb` feature)
├── storage/     # Data storage layer (write buffer, caches)
├── types/       # Shared types
├── common/      # Common utilities
└── utils/       # Helper functions (incl. object_store wrapper)
tests/           # Integration tests
benches/         # Criterion benchmarks
docs/            # mdbook sources (`just book` to serve)
```

**Key Technologies:**

- **FUSE**: fuser (requires libfuse3 installed)
- **Async Runtime**: tokio
- **Object Storage**: opendal and object_store (see storage note below)
- **Metadata**: RocksDB
- **Observability**: tracing, OpenTelemetry

## Why This Architecture

This project mirrors JuiceFS's separation of concerns but with Rust-specific
improvements:

1. Uses `moka` for write-back cache (cleaner than JuiceFS's disk-eviction)
2. Uses `rangemap` for slice management (instead of linked lists)
3. Fixed-size write buffer pool (memory + mmap)

## Development Workflow

- **Direct commits to `main` are allowed.** Development happens on short-lived
  branches in git worktrees under `.worktrees/<task>`, merged back to `main`,
  then the worktree and branch are removed after merge. No PRs.
- **Storage abstraction is frozen.** Both `opendal` (components/storage, vfs)
  and `object_store` (components/utils) are present. The intended direction is
  opendal, but do not migrate anything until explicitly requested.
- **The test suite is expensive.** Do not run it as routine verification. Use
  `cargo check`, `cargo clippy`, and `cargo +nightly fmt` as the quality gate.
- **Toolchain is pinned** in `rust-toolchain.toml` (1.97.1); CI reads the
  version from that file.

**Quality gate:**

```bash
cargo check --all --all-features                                        # or: just check
cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings
cargo +nightly fmt --all
```

**Build & Run:**

```bash
just build              # cargo build --package kiseki-binary
just test               # Run tests with nextest (expensive — only when asked)
just mount              # Mount filesystem (debug mode, mounts at /tmp/kiseki)
just umount             # Unmount filesystem
just book               # Serve the mdbook docs
just lint               # clippy -D warnings + cargo doc
just fmt                # cargo +nightly fmt + taplo + hawkeye
```
