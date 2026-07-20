# Plan 002: Eliminate actionable dependency vulnerabilities

> **Executor instructions**: Follow each step and verify it before continuing.
> Do not hide vulnerabilities with blanket advisory ignores. Update plan 002 in
> `plans/README.md` when complete.
>
> **Drift check (run first)**:
> `git diff --stat 1dc98d8..HEAD -- Cargo.toml Cargo.lock components/utils/Cargo.toml components/utils/src/lib.rs components/utils/src/object_storage.rs .github/workflows/rust.yml deny.toml`
> Re-audit if dependency files changed.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-green-ci-baseline.md`
- **Category**: security / migration
- **Planned at**: commit `1dc98d8`, 2026-07-20

## Why this matters

`cargo deny check advisories` currently exits non-zero. Actionable findings
include memory-safety, TLS name-constraint, denial-of-service, and object-store
credential logging advisories. A production build needs patched transitive
versions and an enforced policy so a vulnerable lockfile cannot silently ship.

## Current state

- `Cargo.toml:27` pins `bytes` below the patched release for
  `RUSTSEC-2026-0007`.
- `components/utils/Cargo.toml:27` pins `object_store` below the patched release
  for `RUSTSEC-2024-0358`.
- The lockfile contains actionable advisories:
  `RUSTSEC-2026-0007`, `RUSTSEC-2026-0204`, `RUSTSEC-2024-0358`,
  `RUSTSEC-2026-0194`, `RUSTSEC-2026-0195`, `RUSTSEC-2023-0071`,
  `RUSTSEC-2026-0098`, `RUSTSEC-2026-0099`, `RUSTSEC-2026-0104`,
  `RUSTSEC-2026-0049`, `RUSTSEC-2026-0009`, and `RUSTSEC-2025-0040`.
- `components/utils/Cargo.toml:61` directly depends on the unmaintained `users`
  crate, but `components/utils/src/lib.rs:32-35` only needs current uid/gid;
  safe `rustix` APIs can replace it.
- `bincode` is both unmaintained and part of the durable RocksDB serialization
  format (`components/meta/src/err.rs:101-112`). Replacing it without a format
  version/migration would be more dangerous than a documented temporary
  exception.
- There is no `deny.toml` and no advisory job in CI.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Audit | `cargo deny check advisories` | exit 0 after documented policy |
| Reverse deps | `cargo tree -i <crate>@<version>` | explains every remaining version |
| Check | `just check` | exit 0 |
| Lint | `just lint` | exit 0 |
| Tests | `cargo nextest run --workspace --all-features` | all pass |

## Scope

**In scope**:
- `Cargo.toml`
- `Cargo.lock`
- `components/utils/Cargo.toml`
- `components/utils/src/lib.rs`
- `components/utils/src/object_storage.rs` only for compatibility with the
  patched `object_store` API
- `deny.toml` (create)
- `.github/workflows/rust.yml`

**Out of scope**:
- Migrating from `object_store` to OpenDAL or the reverse
- Changing the on-disk bincode encoding
- Broad telemetry redesign
- Accepting a vulnerability merely because it is transitive

## Git workflow

- Work directly on `main` after plan 001 is green.
- Commit message: `security: enforce dependency advisory policy`.
- Push and verify both the advisory job and the full CI run.

## Steps

### Step 1: Establish a reviewed deny policy

Create `deny.toml` with `cargo deny init`, retain license/source defaults only
if reviewed, and configure advisories to fail on vulnerabilities and yanked
versions. Add narrowly documented exceptions only for unmaintained crates that
cannot yet be removed. Each exception must name the dependency path, why the
code path is not currently exploitable, and the plan required to remove it.

**Verify**: `cargo deny check advisories` still fails before upgrades; no
vulnerability advisory is ignored.

### Step 2: Apply compatible patched upgrades

Update direct constraints and the lockfile for `bytes`, `object_store`,
`crossbeam-epoch`, `quick-xml`, `rustls-webpki`, and `time` to the minimum
patched versions or newer compatible releases. Use `cargo tree -i` after every
blocked update to identify the actual parent. Adapt only the small
`object_storage.rs` API surface if required.

**Verify**: `cargo deny check advisories` no longer reports the patched advisory
IDs listed above.

### Step 3: Remove vulnerable or obsolete direct dependencies

Replace `users::get_current_uid/gid` with safe `rustix::process` calls and remove
`users`. Remove the legacy `opentelemetry_api` workspace dependency if `rg`
confirms no first-party use. For old OpenTelemetry dependency chains, either
upgrade them coherently or disable unused integrations; do not keep two major
telemetry stacks without an active caller.

**Verify**:
`cargo tree -i users; rg -n 'opentelemetry_api' --glob '*.rs' components` → no
reachable first-party dependency/use.

### Step 4: Vet no-safe-upgrade advisories by reachability

For bincode, document the durable-format constraint and create a follow-up
format-version migration item. For the RSA advisory and stale crates pulled by
unused OpenDAL service features, remove unused features/dependencies when
possible. If an advisory remains reachable in production and has no patch,
STOP; it cannot be accepted as production-safe.

**Verify**: `cargo deny check advisories` exits 0 with only explicit,
non-vulnerability maintenance exceptions whose rationale is in `deny.toml`.

### Step 5: Enforce the policy in CI

Install `cargo-deny` using the repository's action pattern and add a required
advisory job. Include it in the Rust aggregator established by plan 001.

**Verify**: a temporary lockfile rollback to a known vulnerable version makes
the advisory job fail; restore the lockfile and it passes.

## Test plan

- Existing object-store memory/local tests must pass after the API upgrade.
- Existing logger/telemetry tests must pass after dependency consolidation.
- Full workspace nextest is required once because lockfile changes affect every
  package.

## Done criteria

- [ ] `cargo deny check advisories` exits 0.
- [ ] No vulnerability advisory is ignored.
- [ ] `users` is absent from first-party dependencies.
- [ ] Patched versions are present in `Cargo.lock`.
- [ ] Check, lint, format, and full nextest pass.
- [ ] Required remote advisory and CI jobs are green.
- [ ] Only in-scope files and `plans/README.md` changed.

## STOP conditions

- A reachable runtime vulnerability has no patched dependency path.
- Upgrading `object_store` requires changing the storage abstraction rather
  than adapting the existing facade.
- A bincode change would alter persisted bytes without a migration/version
  strategy.
- The advisory database changed and new findings require expanding scope.

## Maintenance notes

Run the advisory job on every lockfile change and on a schedule. Revisit every
maintenance exception quarterly. Do not combine the future bincode migration
with an unrelated metadata schema change.
