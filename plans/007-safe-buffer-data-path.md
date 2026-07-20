# Plan 007: Eliminate unsafe aliasing from the buffer data path

> **Executor instructions**: Remove the need for every first-party `unsafe`
> block in the storage/VFS data path. Do not replace one raw-pointer invariant
> with another undocumented wrapper. Run Miri on the small pool tests and
> update this plan's row in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 2fb2c95..HEAD -- components/storage/src/lib.rs components/storage/src/pool/mod.rs components/storage/src/pool/memory_pool.rs components/storage/src/pool/disk_pool.rs components/storage/src/slice_buffer.rs components/vfs/src/lib.rs components/vfs/src/reader.rs .github/workflows/rust.yml`
> This plan depends on plan 006, which intentionally changes
> `slice_buffer.rs`. Re-read the live file and apply the invariants below to its
> post-plan-006 shape rather than restoring old code.

## Status

- **State**: DONE
- **Priority**: P1
- **Effort**: M
- **Risk**: HIGH
- **Depends on**: `plans/006-crash-consistent-write-publication.md`
- **Category**: bug / tech-debt
- **Planned at**: commit `2fb2c95`, 2026-07-20

### Completion at 2026-07-20

`MemoryPagePool` now moves owned `Box<[u8]>` values through its free queue.
Each acquired `Page` exclusively owns one allocation, page writes require a
mutable borrow, drop zeroes and recycles the buffer, and an outstanding page no
longer keeps its pool alive. Page-pool configuration and every page/chunk range
are validated in release builds with typed errors.

Storage and VFS parallel reads now create disjoint destination borrows with
`split_at_mut` and run borrowed futures with `try_join_all`; no spawned task or
raw pointer is needed. Both crate roots deny unsafe code. Tests cover pool
exhaustion/wakeup, zeroed reuse, pool teardown, mutable writes, invalid ranges,
logical EOF, short objects, and unaligned reads across three blocks.

The broad Tokio-based Miri selector is not runnable on macOS because Miri does
not implement `mio`'s `kqueue` syscall. The runtime-free ownership core was
therefore isolated as the narrow `miri_` test set: 2 tests pass locally and the
same command is enforced in Linux CI. Targeted storage/VFS nextest completed
94/94 tests successfully and the full workspace completed 143/143; repository
check, lint, docs, format, and Miri also pass.

## Why this matters

`MemoryPagePool` mutates memory through a shared reference and manually declares
its slot `Send + Sync`; the public-safe API does not encode the exclusivity its
SAFETY comment assumes. Parallel read helpers reconstruct multiple mutable
slices from one raw pointer to satisfy `'static` spawned tasks. These patterns
are unnecessary and make a future harmless-looking concurrency change capable
of undefined behavior.

## Current state

- `components/storage/src/pool/memory_pool.rs:40-79` stores a leaked
  `&'static mut [u8]` in `UnsafeCell`, implements `Send`/`Sync` manually, and
  creates mutable slices from `&self`.
- `components/storage/src/pool/memory_pool.rs:100` allocates the full pool with
  `Box::leak`. Every constructed pool leaks until process exit; tests construct
  multiple 300 MiB pools.
- `Page::copy_from_reader(&self, ...)` at `memory_pool.rs:211` is a safe mutating
  method callable through shared references. The same signature is propagated
  by `pool/mod.rs` and `disk_pool.rs` even though write ownership should be
  exclusive.
- `components/storage/src/slice_buffer.rs:83` and
  `components/vfs/src/reader.rs:366` repeatedly call
  `from_raw_parts_mut(dst_ptr, dst_len)` and send disjoint subslices to spawned
  tasks. The disjointness is calculated at runtime and not represented in the
  borrow graph.
- `components/storage/src/slice_buffer.rs:182,206,261` uses `get_unchecked*`
  behind `debug_assert!` range checks. Release builds remove those checks, so a
  boundary bug becomes undefined behavior instead of a typed error.
- A repository-wide scan at `2fb2c95` finds no other first-party unsafe blocks
  in `components/storage/src` or `components/vfs/src`.

## Target design

- The memory pool queue owns reusable `Box<[u8]>` buffers. Acquiring a page
  moves one buffer into `Page`; dropping the page clears and returns it. This
  represents exclusivity through ordinary Rust ownership and needs no
  self-referential slots, leak, `UnsafeCell`, or unsafe trait impl.
- Page writes require `&mut self`; page reads require `&self`.
- Parallel reads borrow disjoint destination subslices created with safe
  `split_at_mut`/chunk iteration. Run their futures concurrently with
  `try_join_all` without `tokio::spawn`, so the futures may borrow `dst`.
- All public length/offset checks are effective in release builds and return a
  typed error. Checked indexing replaces `get_unchecked*`.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Unsafe scan | `rg -n '\bunsafe\b|Box::leak|get_unchecked|from_raw_parts' components/storage/src components/vfs/src` | no code matches; comments only if intentional |
| Storage tests | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-storage` | all pass |
| VFS tests | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-vfs` | all pass |
| Miri | `KISEKI_DISABLE_DISK_POOL=1 cargo +nightly miri test -p kiseki-storage miri_` | runtime-free ownership tests pass |
| Gates | `just check && just lint && cargo +nightly fmt --all -- --check` | all exit 0 |

## Suggested executor toolkit

- Use `rust-router`, `m01-ownership`, `m03-mutability`, `m07-concurrency`, and
  `unsafe-checker` if available.
- Use `cargo expand`/LSP only for inspection; the target implementation should
  remain ordinary safe Rust.

## Scope

**In scope**:

- `components/storage/src/lib.rs`
- `components/storage/src/pool/mod.rs`
- `components/storage/src/pool/memory_pool.rs`
- `components/storage/src/pool/disk_pool.rs`
- `components/storage/src/slice_buffer.rs`
- `components/storage/src/err.rs` only for typed range/config errors
- `components/vfs/src/lib.rs`
- `components/vfs/src/reader.rs`
- `.github/workflows/rust.yml` only for a scoped Miri job
- Colocated tests and `plans/README.md`

**Out of scope**:

- Page/block size redesign or cache eviction policy
- Disk mmap replacement; its safe library API may remain
- Write-publication state transitions owned by plan 006
- General performance tuning or new cache tiers
- Unsafe code inside third-party crates

## Git workflow

- Work directly on `main` after plan 006 is complete.
- Commit message: `refactor: make buffer data path safe`.
- Push only after Miri, targeted tests, and repository gates pass.

## Steps

### Step 1: Add characterization and concurrency tests

Shrink existing pool test capacities to a few pages so sanitizers/Miri are
practical. Add tests for exhaustion/wakeup, drop-and-reuse zeroing, partial
offset read/write, many concurrent independent pages, rejected out-of-range
access, and pool drop reclaiming its allocation. Add a compile-time/API test or
caller changes demonstrating that writes require a mutable `Page`.

Record process memory only as a regression aid; do not use timing or RSS as the
sole correctness assertion.

**Verify**: new tests expose the old leak/shared-mutation design in review and
all behavior assertions pass before the structural rewrite where possible.

### Step 2: Move page buffers through the queue

Replace page-id/`Slot` storage with an `ArrayQueue<Box<[u8]>>` (or an equally
safe ownership-carrying queue). Construct exactly `capacity / page_size`
buffers. `MemoryPagePool::try_acquire_page`/`acquire_page` pop one buffer into a
`Page`; `Drop` takes it, clears it, returns it, and notifies one waiter.

Validate `page_size > 0`, `capacity > 0`, divisibility, and queue capacity in
normal code, returning the crate's typed `Result` through `PagePoolBuilder`.
Do not leave `debug_assert!` as the only protection against division by zero or
out-of-range slicing.

**Verify**: `rg -n 'UnsafeCell|Box::leak|unsafe impl|from_raw_parts' components/storage/src/pool/memory_pool.rs` -> no matches; pool tests pass and the pool allocation drops normally.

### Step 3: Encode mutable access in the Page API

Change `copy_from_reader` to take `&mut self` for memory, disk, and hybrid
pages. Keep `copy_to_writer` on `&self`. Validate `offset.checked_add(length)`
against page size before slicing/range-writer creation and return a typed error
instead of panicking.

Update all callers to bind newly acquired pages as mutable and to obtain
`&mut Page` through the containing block. Do not add a mutex just to preserve
the old shared signature; ownership already gives the correct exclusivity.

**Verify**: storage and VFS packages compile; out-of-range tests return errors;
no mutating method creates a mutable slice from a shared reference.

### Step 4: Replace raw parallel destination slicing with borrowed futures

In both `read_slice_from_object_storage` and VFS `read_slice_from_cache`, build
the sequence of block ranges with checked arithmetic. Repeatedly split the
remaining `&mut [u8]` into one disjoint destination per object block. Construct
`async move` futures that borrow those subslices and run them with
`futures::future::try_join_all`; do not use `tokio::spawn`, because spawned
tasks force the artificial `'static` requirement that caused the raw pointer.

Preserve output ordering and actual-byte-count checks as returned errors, not
runtime asserts. Handle `offset >= length`, empty destinations, final partial
blocks, and remote short/corrupt blocks explicitly.

**Verify**: multi-block, unaligned-start, short-final-block, empty, EOF, missing
block, and concurrent-read tests pass in both storage and VFS.

### Step 5: Remove unchecked indexing and release-only gaps

Replace every `get_unchecked*` in `SliceBuffer` with checked access. Clamp
`read_at` to `length - offset`, use `checked_add` for write ranges, handle empty
flushes, and return the appropriate storage error for an invalid internal
index. Keep `debug_assert!` only as redundant documentation after a real check.

**Verify**: boundary tests cover `CHUNK_SIZE - 1`, exactly `CHUNK_SIZE`, an
overflowing addition, a read crossing logical EOF, and an empty buffer. The
unsafe scan has no matches.

### Step 6: Deny regressions and add scoped Miri CI

After the scan is clean, add `#![deny(unsafe_code)]` to the storage and VFS
crate roots. Add a small Miri job for memory-pool/slice-buffer unit tests. Keep
the job bounded: disable the disk pool, use small test capacities, and do not
run RocksDB/FUSE suites under Miri.

**Verify**:
`KISEKI_DISABLE_DISK_POOL=1 cargo +nightly miri test -p kiseki-storage memory_pool`
passes locally (or on the same Linux environment used by CI), and inserting a
temporary unsafe block makes compilation fail; remove the temporary block.

### Step 7: Run repository gates

**Verify**:
`KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-storage -p kiseki-vfs && just check && just lint && cargo +nightly fmt --all -- --check`
-> all exit 0.

## Test plan

- Keep allocator tests small and deterministic.
- Use barriers/notifies for concurrent page ownership; no sleeps.
- Exercise safe parallel reads with at least three object blocks and an
  unaligned first/last range.
- Include negative/error tests for every formerly unchecked boundary.
- Run Miri only on modules that do not initialize the 1 GiB global pool.

## Done criteria

- [x] No first-party `unsafe`, unsafe trait impl, raw slice reconstruction,
      unchecked indexing, or `Box::leak` remains in storage/VFS.
- [x] Page write access requires `&mut Page`.
- [x] Pool allocations are reclaimed on drop and buffers are zeroed on reuse.
- [x] Parallel reads use safe disjoint borrowed slices.
- [x] Release builds validate all page/chunk ranges.
- [x] Storage and VFS deny new unsafe code.
- [x] Scoped Miri, targeted tests, check, lint, and format pass.
- [x] `plans/README.md` is updated with completion evidence.

## STOP conditions

- A post-plan-006 state machine requires simultaneous mutable access to one
  page. That is a design conflict; do not reintroduce shared mutation.
- A required third-party API can only accept `'static` buffers. Isolate and
  report that boundary before considering owned task buffers.
- Miri cannot execute because of a third-party unsupported syscall even after
  selecting only the small memory tests. Keep `deny(unsafe_code)`, document the
  exact blocker, and substitute the narrowest sanitizer available in CI.
- Safe slicing causes a proven, material regression in an existing benchmark.
  Preserve correctness, record the benchmark, and optimize the safe design.

## Maintenance notes

The invariant should be visible in types: one acquired `Page` owns one buffer,
and only a mutable page can write it. If future zero-copy APIs need concurrency,
split ownership into non-overlapping ranges explicitly rather than weakening
the Page API.
