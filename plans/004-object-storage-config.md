# Plan 004: Make object storage configurable and secret-safe

> **Executor instructions**: Preserve the current object-storage facade; this
> is configuration hardening, not a provider-abstraction migration. Never put
> credential values in source, tests, logs, plan updates, or commit messages.
> Update plan 004 in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 1dc98d8..HEAD -- components/utils/src/object_storage.rs components/vfs/src/config.rs components/vfs/src/kiseki.rs components/binary/src/cmd/mount.rs components/common/src/lib.rs justfile docs/src/quick_start.md`
> Reconcile with the `object_store` version landed by plan 002.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-green-ci-baseline.md`,
  `plans/002-dependency-security.md`
- **Category**: bug / security / operations
- **Planned at**: commit `1dc98d8`, 2026-07-20

## Why this matters

`VFSConfig` contains an object-storage DSN, but `KisekiVFS::new` ignores it and
always creates the same development MinIO client with embedded development
credentials. The binary exposes no object-store option. This prevents real
deployment, risks accidental cross-volume data access, and encourages secrets
in code or command lines.

## Current state

- `components/vfs/src/config.rs:31-38,63-72` stores
  `object_storage_dsn` and defaults it to a local development path.
- `components/vfs/src/kiseki.rs:97-114` ignores the field; commented code shows
  abandoned local/memory alternatives.
- `components/utils/src/object_storage.rs:39-65` has separate local and MinIO
  constructors. The MinIO constructor embeds access and secret credentials at
  source lines 50-65; do not copy their values.
- `components/binary/src/cmd/mount.rs:190-215` builds a default VFS config and
  offers no storage option.
- Object-store errors already flow through `ObjectStorageSnafu` in
  `components/vfs/src/err.rs:29-39`.
- `AGENTS.md` freezes the `object_store`/OpenDAL split. Honor it.

## Configuration contract

- Support explicit `memory://` for tests only, `file:///absolute/path` for local
  development, and `s3://bucket[/prefix]` for production.
- S3 endpoint/region may be non-secret configuration. Credentials must come
  from the provider's standard environment/identity chain, never DSN query
  parameters or logs.
- The mount CLI must require an explicit object-store DSN or config source;
  development just recipes may pass a local value explicitly.
- Startup validates the backend and returns a contextual error before mounting.
- Debug output must redact DSN userinfo/query values and never dump all process
  arguments.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Utils tests | `cargo nextest run -p kiseki-utils object_storage` | all pass |
| VFS tests | `cargo check -p kiseki-vfs --all-targets --all-features` | exit 0 |
| CLI help | `cargo run -p kiseki-binary -- mount --help` | documents storage option without secrets |
| Gates | `just check && just lint` | both exit 0 |

## Scope

**In scope**:
- `components/utils/src/object_storage.rs`
- `components/utils/src/lib.rs` if a redacted configuration type belongs there
- `components/utils/Cargo.toml` only if URL parsing features are needed
- `components/vfs/src/config.rs`
- `components/vfs/src/kiseki.rs`
- `components/binary/src/cmd/mount.rs`
- `components/common/src/lib.rs`
- `justfile`
- `docs/src/quick_start.md`

**Out of scope**:
- Migrating between OpenDAL and `object_store`
- Persisting credentials in metadata
- Adding new metadata backends
- Supporting every cloud provider in the first implementation

## Git workflow

- Work directly on `main` after plans 001 and 002.
- Commit message: `feat: configure object storage at mount time`.
- Push after memory/local tests and workspace gates pass.

## Steps

### Step 1: Introduce one parsed storage configuration

Replace ad-hoc constructors with a typed parser/factory returning the existing
`Arc<dyn ObjectStore>`. Reject relative local paths, unknown schemes, embedded
userinfo, and credential-looking query parameters. Keep provider identity
resolution in the underlying builder/environment chain.

**Verify**: table-driven tests cover memory, absolute file, S3 bucket/prefix,
unknown scheme, relative path, malformed URL, and rejected credential-bearing
input.

### Step 2: Remove embedded credentials and argument dumping

Delete the hard-coded MinIO constructor or make it a test helper that reads
explicit test environment variables. Remove `print_args()` from mount startup;
log only structured, redacted fields such as provider, bucket, and prefix.

**Verify**:
`rg -n 'access_key|secret_access|print_args|std::env::args' components --glob '*.rs'`
→ no embedded credential setter/value or raw argument dumping in production.

### Step 3: Wire CLI to VFS

Add an object-store argument to `MountArgs`, validate it before opening FUSE,
set `VFSConfig.object_storage_dsn`, and make `KisekiVFS::new` call the factory.
Keep `VFSConfig::default` usable in tests, but do not let the production mount
path silently choose memory or a shared development bucket.

**Verify**: a unit test proves the CLI value reaches the factory; invalid DSNs
return before `fuser::mount2`.

### Step 4: Add a cheap startup probe

Perform a non-destructive capability/connectivity check appropriate to the
provider and fail startup with context. Do not create/delete a user object if a
read-only list/head capability can prove access. Respect read-only mounts.

**Verify**: fake/in-memory store tests cover success, authentication-style
failure mapping, and unreachable backend without mounting.

### Step 5: Update development commands and docs

Pass an explicit local or MinIO DSN from `just mount` and document standard
environment-based credentials without example secret values. Explain that the
same backend identity/prefix must be used on every mount of a volume.

**Verify**: `cargo run -p kiseki-binary -- mount --help` and the quick start show
the same option and schemes.

### Step 6: Run gates

**Verify**:
`cargo nextest run -p kiseki-utils object_storage && cargo check -p kiseki-vfs --all-targets --all-features && just check && just lint && cargo +nightly fmt --all -- --check`
→ all exit 0.

## Test plan

- Table-driven parser tests with no real credentials.
- Memory-store read/write round-trip.
- Local-store round-trip in `tempfile::TempDir`.
- Mock/failing store startup probe.
- CLI propagation test.
- Real S3/MinIO remains an opt-in integration test gated by environment.

## Done criteria

- [ ] Production mount explicitly selects its object store.
- [ ] `KisekiVFS::new` no longer ignores `object_storage_dsn`.
- [ ] No credential value is embedded or printed.
- [ ] Invalid/unreachable storage fails before mount.
- [ ] Memory/local tests and all quality gates pass.
- [ ] Only in-scope files and `plans/README.md` changed.

## STOP conditions

- Plan 002's `object_store` API requires an abstraction migration.
- Provider setup requires storing secrets in metadata or CLI arguments.
- A startup probe would mutate user data without an isolated namespace.
- Existing volumes depend on the hard-coded bucket/prefix with no discoverable
  migration path.

## Maintenance notes

Add providers behind the same typed factory. Never log a raw DSN. Prefix
versioning and multi-tenant isolation should be reviewed before sharing a
bucket between volumes.
