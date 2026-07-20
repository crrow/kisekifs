# Plan 010: Add offline, verified metadata backup and restore

> **Executor instructions**: A backup is not complete because RocksDB copied
> files. It must contain a consistent metadata checkpoint whose referenced
> blocks are remotely durable, a verified manifest, and a restore test that
> reads real file data. Never overwrite an existing destination implicitly.
> Update this plan's row in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 2fb2c95..HEAD -- components/binary/src/main.rs components/binary/src/cmd components/meta/src/backend components/meta/src/engine.rs components/meta/src/lib.rs components/storage/src/cache/file_cache.rs components/utils/src/object_storage.rs components/types/src/setting.rs docs/src justfile tests`
> This plan depends on the durable cache drain from plan 006 and instance paths
> from plan 009. Reuse those APIs; do not create a second recovery protocol.

## Status

- **State**: TODO
- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: `plans/006-crash-consistent-write-publication.md`, `plans/009-operational-mount-lifecycle.md`
- **Category**: feature / correctness
- **Planned at**: commit `2fb2c95`, 2026-07-20

## Why this matters

RocksDB holds the only namespace, inode, and slice-reference metadata, but the
CLI has no backup, verification, or restore command. `VFSConfig` currently has
an unused `backup_meta_interval`, which suggests protection that does not
exist. A production volume needs a repeatable recovery artifact and a tested
restore path before it can be entrusted with non-recreatable data.

## Current state

- `components/meta/src/backend/rocksdb.rs` uses
  `OptimisticTransactionDB` and already has one database path per RocksDB DSN,
  but exposes no checkpoint/backup operation through `Backend`.
- `components/binary/src/main.rs` exposes only `mount`, `umount`, and `format`.
- `components/vfs/src/config.rs:25,65` defines `backup_meta_interval` with a
  zero/default value; no code schedules or performs a backup.
- Object blocks are immutable and live outside RocksDB. A restored metadata
  checkpoint is useful only if every referenced object was remotely durable at
  checkpoint time. Plan 006 supplies a stage recovery/remote-drain barrier.
- Plan 009 gives each volume an explicit stage root and graceful offline state.
- There is no durable-format version/checksum manifest around the existing
  bincode/RocksDB data. `Format` is the compatibility root and plan 003 already
  prevents incompatible in-place format changes.

## Backup contract

Version 1 is intentionally offline:

1. The volume must not be mounted; opening the RocksDB DSN must acquire its
   exclusive lock. If it is in use, fail without side effects.
2. Recover the configured stage directory and migrate every published staged
   block to the configured remote object store. If any remains pending, fail
   and keep both source metadata and stage files untouched.
3. Create a RocksDB checkpoint in a temporary sibling directory.
4. Write and sync a versioned manifest containing non-secret volume identity,
   format/layout values, source schema/tool version, creation time, object-store
   provider/prefix identity without credentials, file list, sizes, and strong
   checksums.
5. Verify the artifact, sync it, then atomically rename it to the requested new
   destination. Never replace an existing backup.
6. Restore only into a new, empty metadata destination after full verification.
   Keep old metadata outside this command's destructive scope.

The artifact is a metadata backup, not a copy of remote object data. The
operator runbook must require independent durability/versioning of the object
store and retention coordination with future garbage collection.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| CLI help | `cargo run -p kiseki-binary -- backup --help` | documents offline/safety contract |
| Meta tests | `cargo nextest run -p kiseki-meta backup` | all pass |
| CLI tests | `cargo nextest run -p kiseki-binary backup` | all pass |
| Restore acceptance | `just test-backup-restore` | restored VFS reads exact fixture tree |
| Gates | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features && just check && just lint && cargo +nightly fmt --all -- --check` | all exit 0 |

## Suggested executor toolkit

- Use `rust-router`, `m12-lifecycle`, `m13-domain-error`, and `domain-cli` if
  available.
- Use RocksDB's pinned checkpoint API; inspect the local crate source/version
  before coding rather than copying an API from another release.

## Scope

**In scope**:

- `components/binary/src/main.rs`
- `components/binary/src/cmd/mod.rs`
- New `components/binary/src/cmd/backup.rs` and `restore.rs` (or one coherent
  volume-recovery module)
- `components/meta/src/backend/mod.rs`
- `components/meta/src/backend/rocksdb.rs`
- `components/meta/src/engine.rs` and `lib.rs` only for semantic checkpoint APIs
- `components/storage/src/cache/file_cache.rs` only to invoke plan 006 recovery
  and remote drain
- `components/utils/src/object_storage.rs` only for non-secret store identity or
  verification primitives
- `components/types/src/setting.rs` only for manifest compatibility validation
- `tests/`, `justfile`
- `docs/src/operator-runbook.md`, `docs/src/SUMMARY.md`
- `plans/README.md`

**Out of scope**:

- Online/hot backup while a mount accepts writes
- Copying, versioning, or garbage-collecting all remote object data
- In-place destructive restore or automatic rollback
- Cross-version bincode/schema migration; reject incompatible manifests
- Scheduled backups inside the mount process; use an external timer/supervisor
  for the offline command
- Cloud-specific backup APIs

## Git workflow

- Work directly on `main` after plan 009.
- Commit message: `feat: add verified metadata backup and restore`.
- Push only after a fresh backup and restore acceptance cycle passes from a
  separate destination.

## Steps

### Step 1: Specify and test a versioned backup manifest

Define a small serde manifest with an explicit format version. Include volume
name, chunk/block/page layout, application/schema version, UTC timestamp,
credential-free object-store identity, checkpoint file paths/sizes/checksums,
and a completion marker. Reject absolute paths, parent traversal, duplicate
entries, unknown required versions, layout mismatches, missing files, extra
unclassified files, size mismatches, and checksum failures.

Use a strong maintained checksum already available in the workspace or add one
minimal dependency. Never serialize DSNs, endpoints with userinfo, environment
credentials, or stage paths into the artifact.

**Verify**: manifest tests cover valid round-trip plus every rejection above;
debug/error strings contain no test secret marker.

### Step 2: Add a semantic RocksDB checkpoint API

Add `create_checkpoint(destination)` at the metadata layer with one RocksDB
implementation. Validate that destination is absent and its parent exists.
Create the checkpoint only inside a caller-owned temporary directory. Convert
all RocksDB/filesystem errors to existing typed Snafu errors; no `unwrap`,
`expect`, or panic in production paths.

Do not expose generic database internals through the CLI. The semantic API
returns the stored `Format` and checkpoint file inventory needed by the
manifest.

**Verify**: create a nontrivial temporary database, checkpoint it, open the
checkpoint independently, and compare format/root/counters/inodes/chunk slices.
An already-open source or existing destination fails without modifying either.

### Step 3: Implement `kiseki backup` as an atomic offline workflow

Add required arguments for source metadata DSN, object-storage DSN, stage cache
root, and new backup destination. Redact storage DSNs through the same wrapper
as mount. The command must:

- prove the mount/database is offline by acquiring the metadata store;
- recover and remotely drain staged files via plan 006;
- fail if any pending/corrupt stage entry remains;
- create a temporary checkpoint and manifest;
- fsync files/directories, verify the complete temporary artifact, then rename
  it atomically to the absent destination;
- remove only its own incomplete temporary directory on ordinary failure,
  preserving diagnostic/corrupt source data.

Emit a concise report with backup ID, volume, file/byte counts, duration, and
destination, but no credentials.

**Verify**: CLI tests cover happy path, mounted/locked source, remote outage,
stage migration failure, existing destination, unwritable parent, injected
checkpoint failure, and interrupted temporary artifact. No case changes source
metadata or deletes staged bytes.

### Step 4: Implement verify and non-destructive restore

Provide `kiseki backup verify <artifact>` (or an equivalent subcommand) that
does not open the source volume and verifies manifest, inventory, checksums,
RocksDB openability, `Format`, root inode, and basic counters.

Restore requires a verified artifact and a destination RocksDB DSN/path that
does not exist. Copy into a temporary sibling, fsync, open/validate it, then
atomically rename. Do not provide `--force`; replacement should be an explicit
operator move outside this first version. Print the exact next mount inputs,
including the original object-store identity requirement, without secrets.

**Verify**: restore rejects corrupted/truncated/extra-file artifacts,
incompatible format versions, existing/nonempty destinations, and unavailable
parents. A failed restore leaves no destination database.

### Step 5: Prove end-to-end data recovery

Create a fixture volume through normal VFS/FUSE operations containing nested
directories, links, sparse/multi-chunk files, overwrites, truncation, and
metadata changes. Ensure all writes are fsynced, unmount, run backup, restore to
a new metadata path, mount the restored metadata against the same object store,
and compare a deterministic tree manifest plus every file checksum/metadata
field in the supported matrix.

Repeat after placing recoverable staged files in the stage root: backup must
drain them remotely before checkpointing. Then delete the original metadata
directory (only the test-owned path) and prove the restored copy remains usable.

**Verify**: `just test-backup-restore` passes and leaves original/backup/restore
artifacts available on failure for diagnostics, while the success path cleans
only its test root.

### Step 6: Remove the no-op scheduling promise and document operations

Remove `backup_meta_interval` from VFS runtime config unless it was made real by
another completed plan. Document offline requirements, stage drain, command
examples using placeholders, verification, retention, restore rehearsal,
object-store dependency, monitoring, and recommended external scheduling. Add
a quarterly restore-drill checklist.

Do not claim disaster recovery covers remote object deletion, region loss, or
future garbage collection; those require separate object-store replication and
retention policies.

**Verify**: CLI `--help`, operator runbook, and code agree; `rg -n 'backup_meta_interval' components` has no matches.

### Step 7: Run repository gates

**Verify**:
`just test-backup-restore && KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features && just check && just lint && cargo +nightly fmt --all -- --check`
-> all exit 0.

## Test plan

- Unit-test manifest validation and safe relative-path handling.
- Use real temporary RocksDB checkpoints, not mocked byte copies.
- Inject failure before/after checkpoint, each fsync, manifest completion, and
  final rename.
- Verify CLI errors/debug output never expose DSNs or secret markers.
- End-to-end restore mounts against the same local object store and verifies a
  nontrivial tree byte-for-byte.
- Keep restore destructive operations confined to resolved test-owned roots.

## Done criteria

- [ ] Backup refuses a mounted/locked source and existing destination.
- [ ] All published staged blocks are remotely durable before checkpoint.
- [ ] Artifact manifest is versioned, credential-free, checksummed, and fully
      verified before completion.
- [ ] Restore is non-destructive and atomic into a new destination.
- [ ] Corruption/interruption tests leave source data intact.
- [ ] End-to-end restored mount reproduces namespace, metadata, and bytes.
- [ ] The no-op backup interval is removed; offline scheduling is documented.
- [ ] Backup/restore, workspace, check, lint, and format gates pass.
- [ ] `plans/README.md` is updated with completion evidence.

## STOP conditions

- The source database can be opened while a live mount holds it; the offline
  exclusion assumption is false and needs a real volume lock protocol.
- Plan 006 cannot prove all checkpoint-referenced objects are remote before the
  metadata checkpoint. Do not produce a portable backup.
- RocksDB checkpoint files cannot be atomically finalized on the configured
  filesystem. Require a destination on one filesystem or redesign the final
  copy protocol before proceeding.
- A restore would require overwriting/removing an existing non-test metadata
  directory. Refuse and report the resolved path.
- The manifest encounters a newer incompatible schema. Reject it; do not guess
  a migration.

## Maintenance notes

Treat backup format as a public durable interface: append compatible optional
fields, version incompatible changes, and retain old verifier fixtures. Before
object garbage collection ships, its retention design must account for objects
referenced by every retained metadata backup.
