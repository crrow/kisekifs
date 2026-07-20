# Plan 006: Make write publication crash-consistent and restart-recoverable

> **Executor instructions**: Follow this plan step by step. Do not release an
> in-memory page, publish metadata, or delete a staged file before the next
> durable state is confirmed. Run every verification command. Update this
> plan's row in `plans/README.md` when complete.
>
> **Drift check (run first)**:
> `git diff --stat 2fb2c95..HEAD -- components/types/src/slice.rs components/utils/src/object_storage.rs components/storage/src/cache/file_cache.rs components/storage/src/cache/mem_cache.rs components/storage/src/slice_buffer.rs components/storage/src/err.rs components/vfs/src/data_manager.rs components/vfs/src/writer.rs components/vfs/src/kiseki.rs components/vfs/src/kiseki/file_io.rs components/meta/src/backend/mod.rs components/meta/src/backend/rocksdb.rs components/meta/src/engine.rs`
> Reconcile every changed write-state transition before proceeding. STOP if a
> new writer or metadata backend has appeared and is not covered here.

## Status

- **State**: IN PROGRESS
- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: `plans/005-fuse-boundary-hardening.md`
- **Category**: bug / correctness
- **Planned at**: commit `2fb2c95`, 2026-07-20

### Progress at 2026-07-20

Implemented the durable key parser, fsynced atomic local staging, authoritative
restart-recovered stage index, retry-safe cache migration, rollback-safe
`SliceBuffer` transitions, retryable writer failures, remote confirmation
before metadata visibility, and atomic/idempotent synced RocksDB slice commits.
The stage directory is now instance-configurable so independent mounts/tests do
not share ownership accidentally.

Still required before this plan is `DONE`: remove the fixed one-second writer
timeouts and own/drain their tasks, distinguish local-durable `flush` from
remote-durable `fsync`, add deterministic per-transition fault injection and
subprocess crash/reopen coverage, and run the mounted crash matrix.

## Why this matters

The current write-back path can acknowledge or publish data and then lose the
only readable copy after a process crash, upload failure, or one-second
background timeout. It also updates inode length and chunk slices in separate
RocksDB writes. This plan establishes one explicit durability contract and
makes every transition retryable and restart-recoverable.

## Durability contract

Use these definitions consistently in code, tests, logs, and documentation:

1. `write(2)` may buffer data in memory and does not claim persistence.
2. FUSE `flush`/close succeeds only after bytes are in a synced local stage
   file and inode length plus slice metadata are atomically published.
3. `fsync` and `fdatasync` succeed only after every published block for that
   file is present in remote object storage and metadata is durable. Treat both
   identically until metadata-only synchronization is intentionally designed.
4. A failed transition retains the previous readable/retryable state. Errors
   surface as `EIO`/the existing typed storage error; no background error is
   log-only.
5. Startup completes only after staged files have been indexed and scheduled
   for idempotent migration. A published slice must be readable from either the
   local stage or remote storage throughout migration.

The durable state order is:

`Buffered -> Staged(local file synced + atomically renamed) -> Published`
`(one RocksDB transaction) -> Remote(object upload confirmed) -> LocalDeleted`.

Replaying any completed transition must be safe.

## Current state

- `components/storage/src/cache/file_cache.rs:95` has a startup-recovery TODO.
  Its Moka eviction listener removes the in-memory index before
  `migrate_from_local_to_remote` succeeds; failure is only logged. Reads then
  miss the remaining local file and fall through to remote.
- `components/storage/src/cache/file_cache.rs:223-286` panics on local flush or
  copied-length mismatch. Release builds use `panic = abort`.
- `components/storage/src/slice_buffer.rs:351-465,483-545` uses `mem::take` and
  advances `flushed_length` before async stage/upload completion. An error drops
  the moved pages, so retry is impossible.
- `components/vfs/src/writer.rs:408-446` clears `chunk_writers` before checking
  the recorded error. The flusher at `487-580` drops full-flush futures after
  one second and `mark_flush_failed` marks the writer done while its comment
  admits buffered data can be lost.
- `components/meta/src/engine.rs:778-852` calls `set_attr` and
  `set_raw_chunk_slices` separately. A crash between them can expose a longer
  inode without the slice that contains those bytes.
- `components/meta/src/backend/rocksdb.rs` already uses
  `OptimisticTransactionDB`; volume initialization, mknod, link, truncate,
  unlink, and rename are transaction exemplars. Match their typed Snafu error
  propagation instead of introducing an ad-hoc journal.
- `components/types/src/slice.rs:236-313` writes five uppercase-hex fields with
  prefix `chunks`, but parsing strips `chunks/` and uses decimal parsing. The
  canonical filename cannot be reliably recovered after restart.
- `components/utils/src/object_storage.rs:347-355` already exposes delete and
  list. Extend the wrapper only with the minimal metadata lookup needed to
  confirm an idempotent upload; do not replace the storage abstraction.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Types tests | `cargo nextest run -p kiseki-types` | all pass |
| Storage tests | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-storage` | all pass |
| Meta tests | `cargo nextest run -p kiseki-meta` | all pass |
| VFS tests | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-vfs` | all pass |
| Workspace | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features` | all pass |
| Gates | `just check && just lint && cargo +nightly fmt --all -- --check` | all exit 0 |

## Suggested executor toolkit

- Use `rust-router`, `m06-error-handling`, `m12-lifecycle`, and
  `m13-domain-error` if available.
- Use test-driven development: every state-transition failure must be
  reproducible before its implementation changes.

## Scope

**In scope**:

- `components/types/src/slice.rs`
- `components/utils/src/object_storage.rs` only for a remote object metadata
  operation required to confirm uploads
- `components/storage/src/cache/file_cache.rs`
- `components/storage/src/cache/mem_cache.rs` only for read-fallback ordering
- `components/storage/src/slice_buffer.rs`
- `components/storage/src/err.rs`
- `components/vfs/src/data_manager.rs`
- `components/vfs/src/writer.rs`
- `components/vfs/src/kiseki.rs`
- `components/vfs/src/kiseki/file_io.rs`
- `components/meta/src/backend/mod.rs`
- `components/meta/src/backend/rocksdb.rs`
- `components/meta/src/engine.rs`
- Tests colocated with those modules
- `plans/README.md`

**Out of scope**:

- Replacing `object_store`/OpenDAL or changing canonical remote object names
- Object compaction, reference-aware deletion, or garbage collection
- Full graceful process shutdown; plan 009 owns signal handling and draining
- Metadata schema migration unrelated to atomic slice publication
- Performance tuning until the fault matrix is green

## Git workflow

- Work directly on `main`, matching the operator's standing instruction and
  current repository workflow.
- Commit message: `fix: make write publication crash-consistent`.
- Push only after all local gates and the complete fault matrix pass.

## Steps

### Step 1: Make `SliceKey` a tested durable filename contract

Add round-trip tests for zero values, numeric-only hex, values containing
`A-F`, the largest supported `slice_id`/indexes, canonical `chunks...` names,
and malformed names. Parse exactly the format emitted by `Display` using radix
16, validate all five components (including the two derived directory fields),
and use checked integer conversions. Preserve the existing emitted name so
already-uploaded objects remain addressable.

**Verify**: `cargo nextest run -p kiseki-types slice_key` -> all new round-trip
and rejection cases pass.

### Step 2: Replace eviction-as-ownership with an authoritative staged index

In `file_cache.rs`, separate expiration/flush scheduling from the authoritative
map used by reads. A TTL/capacity signal may enqueue migration, but it must not
remove the readable index entry. Stage through a unique temporary file, flush
and sync the file, atomically rename it to the canonical `SliceKey`, and sync
the directory before reporting success. Convert every panic/assert on I/O or
length into a typed error.

Migration must be idempotent: upload the immutable canonical key, finish the
remote writer, confirm the remote size, then delete the local file and index
entry. On any failure, retain both and retry with bounded exponential backoff
and observable attempt/error counters. Concurrent migration requests for the
same key must coalesce.

**Verify**: storage tests inject local write, sync/rename, remote write,
remote-finish, verification, and local-delete failures. After each failure,
`get_range` returns the original bytes and a retry reaches `Remote` without a
duplicate or corrupt object.

### Step 3: Recover the stage directory before the mount becomes ready

Add an async `FileCache` constructor/recovery phase and call it from
`KisekiVFS::new_checked` before returning. Scan canonical stage files, parse and
validate their keys and actual sizes, restore the authoritative index, and
enqueue migration. Clean only recognizable incomplete temporary files.
Malformed canonical files must fail startup with their path redacted to the
configured cache root or be moved to a documented quarantine; never silently
delete unknown data.

Recovery may find more data than the configured steady-state cache limit. Keep
it readable and block new staging until migration creates capacity; do not
discard it to satisfy the limit.

**Verify**: create a cache, stage blocks, drop it before migration, construct a
new cache over the same directory, and assert immediate local reads followed by
successful remote migration. Repeat with malformed names, size mismatch,
temporary files, and remote outage.

### Step 4: Make `SliceBuffer` transitions rollback-safe

Process pending blocks in index order. Do not increment `flushed_length` or
drop pages until the corresponding durable stage succeeds. Prefer borrowing a
block during staging; if ownership must move into a future, return that
ownership on every error and restore the exact slot. Advance
`flushed_length` only across the contiguous successful prefix. An empty buffer
flush must be a no-op rather than underflowing `length - 1`.

Apply the same rule to the direct-remote `flush_bulk_to` path or remove that
unused duplicate after proving there are no callers. Never leave a second
lossy implementation available.

**Verify**: fail staging at each block of a multi-block slice. `read_at` still
returns all original bytes, page counts remain correct, the failed block is
retryable, and the next successful flush publishes each block exactly once.

### Step 5: Keep writer state retryable until durable completion

Remove the one-second timeout branches that cancel in-flight flush futures.
Track spawned flush tasks under the owning `FileWriter` and use bounded queue
backpressure. `finish` must await all relevant tasks, inspect their results,
and clear only successfully completed chunk/slice writers. A failure returns to
a retryable dirty/staged state; `mark_flush_failed` must not claim `Done` or
discard its buffer. `close` and FUSE release must propagate the final flush
error instead of logging it and removing the only retry owner.

Make cancellation semantics explicit: plan 009 may request shutdown, but
cancellation cannot interrupt the portion between local durable stage and
state bookkeeping.

**Verify**: deterministic tests pause/fail each flusher request type
(`FlushBulk`, `FlushFull`, `CommitIdle`). `finish` waits rather than timing out,
returns the injected error, retains retry state, and succeeds after the fault
is cleared.

### Step 6: Publish inode length and slice metadata in one RocksDB transaction

Add one semantic backend operation, such as `publish_slice`, rather than
calling two generic setters. In the RocksDB implementation, read the inode attr
and chunk-slice key with conflict tracking, validate the file, append the
serialized owned slice, update mtime/length, write both keys, and commit one
transaction. Return the committed growth and slice count so `MetaEngine` can
update in-memory statistics and caches only after commit.

Keep serialization errors and transaction conflicts typed. A retry must not
append the same slice twice; define an idempotency check using the full encoded
slice at its chunk position, not `slices_buf == val`.

**Verify**: RocksDB tests run concurrent publications to the same inode/chunk,
retry an identical publication, inject an abort before commit, and reopen the
database. Every visible state contains both the correct inode length and slice
list; no duplicate slice appears.

### Step 7: Enforce the `fsync` remote-durability contract

Associate published staged keys with their file writer until remote migration
is confirmed. Add a `FileCache::flush_keys`/`flush_file` barrier and invoke it
from VFS `fsync` after local stage and atomic metadata publication. Return an
error if remote storage is unavailable; do not report success for merely
queued work. Keep ordinary `flush`/release at the local-durable contract from
this plan.

**Verify**: with remote writes paused, `flush` can complete after local durable
stage, while `fsync` remains pending and then fails/succeeds according to the
injected remote result. After successful `fsync`, delete the stage directory,
reopen metadata/VFS, and read the exact bytes from remote storage.

### Step 8: Run the crash matrix and repository gates

Use child processes where a true restart is required. Terminate immediately
after temporary write, local sync, rename, metadata transaction, remote finish,
and local delete. On restart, the result must be either the complete old state
or complete new state according to the acknowledged boundary, never a longer
zero/hole region or missing object.

**Verify**:
`cargo nextest run -p kiseki-types -p kiseki-storage -p kiseki-meta -p kiseki-vfs && KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features && just check && just lint && cargo +nightly fmt --all -- --check`
-> all exit 0.

## Test plan

- Unit-test canonical key parsing in `components/types/src/slice.rs`.
- Use a test-only fault-injecting object-store wrapper and filesystem-operation
  seam; do not put test fault switches in the production CLI.
- Test cache recovery with real temporary directories and a memory remote.
- Test atomic metadata publication against a real temporary RocksDB database.
- Test writer state with deterministic barriers/notifies, never wall-clock
  sleeps.
- Add at least one subprocess kill/reopen test for each durable transition.
- Plan 008 will repeat the key `fsync`/kill/remount scenario through FUSE.

## Done criteria

- [ ] Canonical `SliceKey` names round-trip without changing emitted names.
- [ ] No cache eviction or migration failure makes a local staged block
      unreadable.
- [ ] Startup recovers all valid staged blocks before readiness.
- [ ] `SliceBuffer` retains exact bytes and page ownership on every failure.
- [ ] Writer timeouts/cancellation cannot discard buffered data.
- [ ] Inode length and chunk slices publish in one RocksDB transaction.
- [ ] `flush` satisfies local durability; `fsync` waits for remote durability.
- [ ] No runtime `panic!`, `assert!`, or `assert_eq!` handles storage I/O errors
      in the in-scope production paths.
- [ ] Fault matrix, workspace tests, check, lint, and format all pass.
- [ ] `plans/README.md` is updated with completion evidence.

## STOP conditions

- Canonical remote keys in a deployed environment differ from the current
  `SliceKey::Display` output. Preserve compatibility and report samples without
  exposing bucket credentials.
- The selected object-store provider cannot confirm completed object size or
  overwrite the same immutable key idempotently.
- Atomic slice publication requires a persistent metadata schema change beyond
  a new backend method; write a migration plan before changing stored bytes.
- Recovery finds two different local files mapping to one canonical key or a
  size mismatch that cannot be classified safely. Preserve both and report.
- Any proposed optimization requires acknowledging `fsync` before remote
  durability. That violates this plan's contract.

## Maintenance notes

Review this change as a state machine, not as independent functions. Every
future write-path edit must identify which state owns the only readable bytes,
what acknowledgment has been made, and how restart resumes. Keep object keys
immutable; compaction and garbage collection must account for published
references before deletion.
