# Plan 001: Restore a deterministic green CI baseline

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving on. If a
> STOP condition occurs, report it instead of weakening or skipping a gate.
> Update plan 001 in `plans/README.md` when complete.
>
> **Drift check (run first)**:
> `git diff --stat 1dc98d8..HEAD -- .github/workflows/ci.yml .github/workflows/lint.yml .github/workflows/rust.yml .github/workflows/test.yml justfile Cargo.toml components/fuse/Cargo.toml`
> If these files changed, reconcile the current run failures before editing.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests / DX
- **Planned at**: commit `1dc98d8`, 2026-07-20
- **Completed at**: commit `cf49bcc`, CI run `29710093735`, 2026-07-20

## Why this matters

The latest `main` CI run is red, so a local green build is not sufficient
evidence for production work. Run
`https://github.com/crrow/kisekifs/actions/runs/29708619783` shows Linux tests,
format, and docs passing, but Linux clippy, Linux coverage, and macOS tests
failing. Every later production change needs one deterministic required gate.

## Current state

- `.github/workflows/test.yml:43-85` runs the full workspace on Ubuntu and
  macOS, but only installs `libfuse3-dev` on Ubuntu. The macOS job fails while
  building `fuser` because neither `fuse.pc` nor `osxfuse.pc` is installed.
- Project deployment intent is Linux-first: `justfile` declares a Linux target,
  builds Ubuntu images, and `docs/src/quick_start.md` uses an Ubuntu VM.
- `.github/workflows/rust.yml:43-76` runs Linux clippy. Job `88249477689` failed
  in run `29708619783` because `libc::F_UNLCK` is already a `c_int` on Linux,
  making three `.into()` calls useless conversions under Rust 1.97.1. The same
  conversion is required on macOS because that constant has a different type.
- `.github/workflows/rust.yml:114-163` uses tarpaulin on every push to main.
  Job `88249477684` failed before instrumentation because coverage was the only
  Linux Rust job that did not install `libfuse3-dev`.
- `.github/workflows/rust.yml:165-179` does not include coverage in the
  aggregator even though a failed reusable-workflow job still makes CI red.
- The first verification push failed in an unused `arduino/setup-protoc`
  action during a GitHub API outage. The workspace has no `.proto` files or
  protobuf code-generation build dependency, so the stale setup was removed
  instead of retaining an unrelated network dependency in every Rust job.
- Repository quality commands are `just check`, `just lint`,
  `cargo +nightly fmt --all -- --check`, and expensive `just test`.
- Raw nextest runs can start many VFS test processes that all initialize the
  production `/tmp/kiseki.page_pool` mmap. The test recipe disables that shared
  cache while leaving the dedicated disk-pool tests enabled.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Local check | `just check` | exit 0 |
| Local lint | `just lint` | exit 0, no warning/error |
| Format | `cargo +nightly fmt --all -- --check` | exit 0, no diff |
| Tests | `just test` | all tests pass |
| Inspect run | `gh run view 29708619783 --repo crrow/kisekifs --log-failed` | prints the three root failures |
| Verify remote | `gh run list --repo crrow/kisekifs --branch main --limit 5` | the new CI run is `completed/success` |

## Scope

**In scope**:
- `.github/workflows/ci.yml`
- `.github/workflows/lint.yml`
- `.github/workflows/rust.yml`
- `.github/workflows/test.yml`
- `components/vfs/src/kiseki/file_io.rs` for the verified cross-platform lock
  constant normalization only
- `justfile`
- `Cargo.toml` and `components/fuse/Cargo.toml` only if feature separation is
  required to make the declared Linux target explicit

**Out of scope**:
- Product behavior and filesystem semantics
- Disabling clippy, docs, tests, or coverage with `continue-on-error`
- Adding macFUSE kernel extensions to hosted macOS runners
- Dependency-advisory remediation (plan 002)

## Git workflow

- Work directly on `main`, matching the operator's standing instruction and
  current `AGENTS.md`.
- Commit message: `ci: restore deterministic quality gates`.
- Push only after all local gates pass; then watch the resulting CI run.

## Steps

### Step 1: Capture each current CI root cause

Fetch the failed clippy, coverage, and macOS job logs. Record concise comments
next to any changed workflow command explaining platform-specific behavior.
Do not infer the Linux clippy or coverage cause solely from the job conclusion.

**Verify**:
`gh run view 29708619783 --repo crrow/kisekifs --json jobs` → identifies failed
jobs `88249477689`, `88249477684`, and `88249480136`.

### Step 2: Make the required test matrix match the supported platform

Make Ubuntu the required full-test target. Remove macOS from the required
matrix unless a compile-only configuration can build without an unavailable
kernel extension. If retaining macOS, give it a distinct feature set and name
that honestly proves only portable library compilation. Do not claim mounted
macOS support.

**Verify**: inspect `.github/workflows/test.yml` → no required job depends on an
uninstalled macFUSE/osxfuse package.

### Step 3: Fix the Linux clippy failure at its source

Reproduce the exact Linux diagnostic in an Ubuntu environment with
`libfuse3-dev`. Fix the Rust warning or the workflow environment; do not add an
allow unless the warning is a third-party macro issue with a narrow existing
precedent.

**Verify**:
`cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`
inside the Linux environment → exit 0.

### Step 4: Replace or repair the coverage runner

Prefer `cargo-llvm-cov` with nextest if tarpaulin is incompatible with the
pinned compiler. Generate an artifact in a stable format and ensure the job
fails on instrumentation/test failures, not on an absent output directory.
Keep coverage informational until a measured baseline exists; do not invent a
percentage threshold.

**Verify**: run the chosen coverage command in Linux → exit 0 and one non-empty
coverage artifact exists.

### Step 5: Remove workflow drift

Make local just recipes and CI invoke the same feature/target sets. Ensure every
failing child job is represented by its reusable-workflow result; remove stale
comments and duplicated setup only where this does not obscure the gate.

**Verify**: `just check && just lint && cargo +nightly fmt --all -- --check` →
all exit 0.

### Step 6: Prove the remote gate

Commit and push, then watch the new run instead of assuming local success maps
to GitHub runners.

**Verify**: `gh run watch <new-run-id> --repo crrow/kisekifs --exit-status` →
exit 0 and all required jobs are green.

## Test plan

- Run the complete nextest suite once through `just test` because this plan
  changes its CI platform coverage and test-process isolation.
- Validate workflow YAML via GitHub itself; local formatting is not enough.
- Preserve Ubuntu full tests, strict clippy, strict Rustdoc, and formatting.

## Done criteria

- [x] Local check, lint, format, and nextest commands exit 0.
- [x] The latest `main` CI run completes successfully.
- [x] No required job uses `continue-on-error` or an unconditional success shim.
- [x] macOS is either honestly compile-only or removed from required support.
- [x] Coverage emits a non-empty artifact without masking test failures.
- [x] Only in-scope implementation files and `plans/README.md` changed.

## Execution evidence

- Linux clippy reproduced three `clippy::useless_conversion` errors before the
  fix and completed successfully afterward under Rust 1.97.1.
- Linux tarpaulin completed with 59.73% line coverage and produced a 133,740
  byte Cobertura XML report locally.
- `just test` completed 79 tests with 79 passed and 0 skipped.
- GitHub Actions run `29710093735` completed successfully with format, clippy,
  Rustdoc, Ubuntu nextest, coverage, both aggregators, and a non-empty coverage
  artifact. The run produced no annotations.

## STOP conditions

- Linux clippy or coverage cannot be reproduced and job logs are unavailable.
- Fixing the job requires changing filesystem behavior.
- GitHub-hosted runner capabilities contradict the Linux-first support decision.
- Any gate passes only after warnings or failures are globally suppressed.

## Maintenance notes

Keep the pinned compiler in `rust-toolchain.toml` as the single version source.
When macOS becomes a supported mount target, restore it with an explicit macFUSE
installation strategy and a mounted smoke test, not just compilation.
