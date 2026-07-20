# Plan 005: Make the FUSE request boundary panic-free and correct

> **Executor instructions**: Treat every FUSE request field as untrusted input.
> Return a stable errno for unsupported/invalid requests; never panic. Preserve
> `panic = abort` as a last-resort invariant policy, not normal control flow.
> Update plan 005 in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 1dc98d8..HEAD -- components/fuse/src/lib.rs components/meta/src/context.rs components/meta/src/engine.rs components/vfs/src/kiseki/file_io.rs components/vfs/src/kiseki/metadata.rs components/vfs/src/kiseki/tests.rs components/types/src/internal_nodes.rs`
> Reconcile any changed syscall signature or internal-node behavior.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-green-ci-baseline.md`
- **Category**: bug / correctness
- **Planned at**: commit `1dc98d8`, 2026-07-20

## Why this matters

Several kernel-reachable paths call `todo!()` or `expect()`. Release builds use
`panic = abort`, so an unsupported fallocate mode, special inode access, or
invalid bit flags can terminate the entire mounted filesystem. The same
boundary also swallows initialization errors, ignores primary-group access,
casts negative offsets, and returns zero-padding beyond EOF.

## Current state

- `components/fuse/src/lib.rs:147-159` always returns `Ok(())` from FUSE init,
  even when VFS initialization fails.
- `components/meta/src/context.rs:39-65` stores `req.gid` separately but passes
  an empty `gid_list` to permission checks.
- `components/vfs/src/kiseki/file_io.rs:170-180` panics for special reads and
  casts negative offsets before validation.
- `components/vfs/src/kiseki/file_io.rs:192-207` allocates the requested length,
  ignores the actual read length, and returns trailing zero bytes past EOF.
- `components/vfs/src/kiseki/file_io.rs:280-290,484-518` has unchecked offset
  arithmetic and `todo!()` in control write, fallocate, and special release.
- `components/meta/src/engine.rs:900,926,1113` expects kernel-controlled flags
  and leaves fallocate unimplemented.
- `components/vfs/src/kiseki/metadata.rs:284,345` expects flag/mtime states that
  originate at the request boundary.
- `components/types/src/internal_nodes.rs:25-60` exposes control/log/stats/config
  nodes even though their I/O semantics are incomplete.
- Existing errno conversion returns `EIO` for unmapped internal errors; follow
  `components/vfs/src/err.rs:72-89`.

## Boundary contract

- Invalid numeric ranges/flag combinations return `EINVAL` or `EFBIG`.
- Recognized but unsupported operations return `ENOTSUP`/`EOPNOTSUPP`.
- Invalid handles return `EBADF`; missing entries return `ENOENT`.
- Reads return exactly the actual byte count and no bytes beyond EOF.
- FUSE init failures prevent mount completion.
- Primary gid participates in access checks; supplementary-group limitations
  are documented and do not silently grant access.
- Synthetic inode release always frees the synthetic handle without touching
  metadata-engine open counts.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| VFS tests | `cargo nextest run -p kiseki-vfs` | all pass |
| Meta tests | `cargo nextest run -p kiseki-meta` | all pass |
| Fuse check | `cargo check -p kiseki-fuse --all-targets --all-features` | exit 0 |
| Panic scan | `rg -n 'todo!\(|unimplemented!\(|expect\(' components/fuse components/vfs/src/kiseki components/meta/src/engine.rs` | no request-reachable matches |
| Gates | `just check && just lint` | both exit 0 |

## Scope

**In scope**:
- `components/fuse/src/lib.rs`
- `components/meta/src/context.rs`
- `components/meta/src/engine.rs`
- `components/vfs/src/kiseki/file_io.rs`
- `components/vfs/src/kiseki/metadata.rs`
- `components/vfs/src/kiseki/tests.rs`
- `components/types/src/internal_nodes.rs` only for synthetic-node visibility or
  behavior required by this contract

**Out of scope**:
- Full fallocate implementation; honest `ENOTSUP` is correct for this tranche
- Full POSIX lock implementation
- Crash-consistent writeback redesign
- Broad removal of invariant-only panics inside private state machines

## Git workflow

- Work directly on `main` after plan 001.
- Commit message: `fix: harden FUSE request boundaries`.
- Push only after targeted tests and workspace gates pass.

## Steps

### Step 1: Add regression tests before changing behavior

Add direct VFS/meta tests for negative read/write offsets, checked-add overflow,
read past EOF, invalid setattr/rename/fallocate flags, unsupported fallocate,
special inode read/write/release, VFS init failure, and primary-group access.
Where the current code aborts, test the parsing/validation helper separately
first rather than crashing the test process.

**Verify**: new tests compile and fail for the intended old behavior.

### Step 2: Centralize range and flag validation

Validate signed offsets before conversion and use `checked_add` for every
offset+length calculation. Replace `from_bits(...).expect(...)` with typed
validation returning errno. Remove impossible `unwrap` states by matching the
option at the same boundary.

**Verify**: invalid/overflow regression tests return the exact boundary errno
and the process remains alive.

### Step 3: Fix read length semantics

Return `Bytes` truncated to the actual `read_guard.read` count. Avoid allocating
more than the validated request, and return an empty buffer at EOF.

**Verify**: tests cover empty file, exact EOF, partial read crossing EOF, and a
normal full read with byte-for-byte assertions.

### Step 4: Replace normal-control-flow panics

For fallocate modes not implemented, return `ENOTSUP` after validating the
combination and remove both VFS/meta `todo!()` calls. Give synthetic nodes a
coherent minimal behavior: implement `.config` reads if its content contract is
stable; otherwise hide unsupported nodes or return `ENOTSUP`. Release synthetic
handles idempotently without calling `meta.close`.

**Verify**: every formerly aborting path returns a stable errno/result and a
subsequent ordinary VFS operation succeeds in the same process.

### Step 5: Propagate initialization and permission context

Map VFS init errors to `Err(errno)` from `Filesystem::init`. Seed permission
groups with at least `req.gid`; if supplementary groups are resolved, use a
bounded/cacheable OS lookup and fail closed. Do not rely on an empty group list.

**Verify**: fake init failure returns an error; a file whose group matches the
request's primary gid uses group permission bits.

### Step 6: Run targeted and repository gates

**Verify**:
`cargo nextest run -p kiseki-vfs && cargo nextest run -p kiseki-meta && cargo check -p kiseki-fuse --all-targets --all-features && just check && just lint && cargo +nightly fmt --all -- --check`
→ all exit 0.

## Test plan

- Put VFS regression tests beside existing `kiseki/tests.rs` fixtures.
- Put flag validation tests near `FallocateMode`/rename handling.
- Add small context permission tests rather than requiring a mounted FUSE
  filesystem for primary gid.
- A later mounted acceptance suite must repeat these cases through the kernel.

## Done criteria

- [ ] No request-reachable `todo!`, `unimplemented!`, `expect`, or `unwrap` in
      the in-scope boundary paths.
- [ ] Negative/overflow ranges return errno without allocation or panic.
- [ ] Reads never return bytes past EOF.
- [ ] Unsupported fallocate and synthetic operations are honest and nonfatal.
- [ ] Init errors prevent mount completion.
- [ ] Primary gid affects group permissions.
- [ ] Targeted tests and all quality gates pass.
- [ ] Only in-scope files and `plans/README.md` changed.

## STOP conditions

- A synthetic node has an externally documented protocol not visible in the
  current repository.
- Correct release semantics require changing handle ownership outside scope.
- A test requires an actual mount and cannot be expressed below the FUSE layer;
  record it for the mounted acceptance tranche rather than guessing.
- Supplementary-group lookup would require a blocking unbounded lookup per
  request; keep primary gid correctness and report the larger design need.

## Maintenance notes

Keep syscall validation at the outermost typed boundary. Every new FUSE method
must specify supported flags, checked arithmetic, errno behavior, and a
non-panicking unsupported path before implementation is considered complete.
