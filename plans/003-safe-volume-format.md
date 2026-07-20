# Plan 003: Make volume formatting safe and explicit

> **Executor instructions**: Implement the lifecycle contract below exactly.
> Do not reinterpret `--force` as permission to change immutable data-layout
> fields on a populated volume. Update plan 003 in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 1dc98d8..HEAD -- components/binary/src/cmd/format.rs components/meta/src/engine.rs components/meta/src/err.rs components/meta/src/backend/mod.rs components/meta/src/backend/rocksdb.rs components/types/src/setting.rs`
> Reconcile any change to format serialization or initialization first.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-green-ci-baseline.md`
- **Category**: bug / data safety
- **Planned at**: commit `1dc98d8`, 2026-07-20

## Why this matters

The CLI exposes `--force`, but always calls the metadata layer with `true`, and
the metadata layer ignores that argument. Running `kiseki format` against an
existing volume can therefore replace layout settings without explicit intent.
Changing chunk/block/page sizes after data exists makes stored slice metadata
incompatible and is a data-corruption risk.

## Current state

- `components/binary/src/cmd/format.rs:58-59` defines `force`.
- `components/binary/src/cmd/format.rs:103-112` unwraps metadata errors and
  passes a constant instead of `self.force`.
- `components/meta/src/engine.rs:87-123` names the argument `_force`, overwrites
  the stored format whenever one exists, and initializes root state through
  separate backend calls.
- `components/meta/src/backend/rocksdb.rs:532-546` performs an unconditional
  format put.
- `Format` layout fields are `chunk_size`, `block_size`, and `page_size` in
  `components/types/src/setting.rs:25-45`; quota/name fields are operationally
  mutable.
- Error handling uses Snafu enums and libc errno mapping; match
  `components/meta/src/err.rs`.

## Lifecycle contract

1. Uninitialized store + no `--force`: initialize format, root inode, and
   counters atomically.
2. Existing store + no `--force`: return a typed AlreadyInitialized error and
   leave every key unchanged.
3. Existing store + `--force`: permit name/quota updates, but preserve immutable
   chunk/block/page layout fields. If requested immutable fields differ, return
   a typed IncompatibleFormat error and leave every key unchanged.
4. Backend error: return through the CLI; never panic or print success.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Meta tests | `cargo nextest run -p kiseki-meta` | all pass |
| Binary tests | `cargo test -p kiseki-binary --all-features` | all pass |
| Check | `just check` | exit 0 |
| Lint | `just lint` | exit 0 |
| Format | `cargo +nightly fmt --all -- --check` | exit 0 |

## Scope

**In scope**:
- `components/binary/src/cmd/format.rs`
- `components/meta/src/engine.rs`
- `components/meta/src/err.rs`
- `components/meta/src/backend/mod.rs`
- `components/meta/src/backend/rocksdb.rs`
- `components/types/src/setting.rs` only for comparison/helper methods, not
  serialized field changes

**Out of scope**:
- Changing field order/types in serialized `Format`
- Object-store configuration (plan 004)
- Backup/restore tooling
- Destructive reformatting of populated volumes

## Git workflow

- Work directly on `main` after plan 001.
- Commit message: `fix: make volume formatting non-destructive`.
- Push after targeted and workspace gates pass.

## Steps

### Step 1: Add typed lifecycle errors and comparisons

Add typed metadata errors for already-initialized and incompatible immutable
format changes. Add a small helper that compares only layout fields and returns
which field differs without logging secret/config payloads.

**Verify**: unit tests cover equal layouts and each differing immutable field.

### Step 2: Make first initialization atomic

Add a backend operation that writes the format, root inode, and initial counter
in one RocksDB transaction. Do not expose partial initialization if the process
or write fails between keys.

**Verify**: backend test confirms all three keys exist after success and none
exist after an injected/pre-commit failure path.

### Step 3: Enforce `force` and immutable layout

Implement the lifecycle contract in the metadata layer. On forced mutable
updates, merge the requested name/quota fields into the existing format while
retaining stored layout values.

**Verify**: tests cover new volume, repeated format without force, forced
mutable update, and rejected forced layout change; rejected cases reload an
unchanged format.

### Step 4: Propagate CLI errors

Pass `self.force`, replace `expect`/`unwrap` with contextual `Whatever` errors,
and log success only after the metadata call succeeds. Precompile the constant
name regex or handle its construction without a user-triggerable panic.

**Verify**: CLI/unit tests assert non-zero error for an existing volume without
force and for incompatible forced layout; no panic occurs.

### Step 5: Run the repository gates

**Verify**:
`cargo nextest run -p kiseki-meta && cargo test -p kiseki-binary --all-features && just check && just lint && cargo +nightly fmt --all -- --check`
→ all exit 0.

## Test plan

- Add RocksDB temp-directory tests near existing backend format tests.
- Add engine-level lifecycle tests so backend implementation detail is not the
  only coverage.
- Add CLI argument/run tests around `FormatArgs`; use temporary metadata DSNs.

## Done criteria

- [ ] `self.force` reaches metadata unchanged.
- [ ] Existing volume without force is unchanged and returns a typed error.
- [ ] Forced layout changes are rejected and unchanged.
- [ ] Initial root/format/counter creation is atomic.
- [ ] No production `unwrap`/`expect` remains in the format command path.
- [ ] Targeted tests and all quality gates pass.
- [ ] Only in-scope files and `plans/README.md` changed.

## STOP conditions

- Existing deployed metadata uses multiple incompatible `Format` encodings.
- Atomic initialization requires a non-RocksDB backend not present in the
  current feature set.
- The requested behavior would erase dentries, attrs, slices, or counters.
- Tests reveal current data depends on changing layout fields in place.

## Maintenance notes

Future layout changes require an explicit format version and migration. `force`
must never become a generic bypass for storage-layout invariants.
