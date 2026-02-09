---
description: Tag and release diamond-types-extended to crates.io. Runs full test suite, code review, version bump, commit, tag, push, and publish.
---

# Release diamond-types-extended to crates.io

This command handles the full release workflow for diamond-types-extended.
Follow each phase in order. Stop and report to the user if any phase fails.

$ARGUMENTS is the new version number (e.g. `0.2.0`). If not provided, ask the user.

## Phase 0: Version Sanity Check

Before doing any work, parse the current version from `Cargo.toml` and compare
with the requested version. Flag and confirm with the user if:

- **Major bump** (e.g. `0.x.y` → `1.0.0`) — "This is a major version bump. Intentional?"
- **Minor skip** (e.g. `0.1.0` → `0.5.0`) — "This skips minor versions. Intentional?"
- **Downgrade** — "The requested version is lower than the current version. Typo?"
- **Same version** — "This version is already set. Nothing to release."

Normal increments (patch bump like `0.1.0` → `0.1.1`, or minor bump like
`0.1.0` → `0.2.0`) proceed without asking.

## Phase 1: Pre-flight Checks

Verify the repo is ready for release:

1. Confirm on `main` branch with clean working tree (`git status`)
2. Confirm up to date with remote (`git fetch origin && git log HEAD..origin/main --oneline`)
3. Run `cargo clippy --all` — must be 0 warnings
4. Run `cargo test --all` — all must pass

If any check fails, stop and report. Do not proceed.

## Phase 2: Code Review

Before releasing, get a second opinion on changes since the last release tag.

1. Find the previous release tag: `git tag --sort=-v:refname | head -1`
   - If no tags exist, this is the first release — use the initial commit instead
2. Get the diff summary: `git diff <prev-tag>..HEAD --stat`
3. Review the changes using a frontier model:
   - If `consult_gemini_pro` is available (gpal MCP server), use it
   - Otherwise, use a Task subagent with `model: opus` for the review
   - Provide: `git log <prev-tag>..HEAD --oneline` and `git diff <prev-tag>..HEAD`
   - Ask for: breaking changes, API concerns, missing tests, documentation gaps
4. Report the review summary to the user
5. Ask the user to confirm proceeding with the release

## Phase 3: Version Bump

Update the version in the root `Cargo.toml`:

```
Cargo.toml → version = "$ARGUMENTS"
```

Note: workspace member crates (`crdt-testdata`, `trace-alloc`, `bench`) are
internal-only and not published as part of this release. Only the root package
version needs updating.

After editing, run `cargo check --all` to verify everything resolves correctly.

## Phase 4: Commit and Tag

1. Stage `Cargo.toml` by name (never `git add -A`)
2. Also stage `Cargo.lock` if it changed
3. Commit with message: `chore: bump to v$ARGUMENTS`
4. Create annotated tag: `git tag -a v$ARGUMENTS -m "Release v$ARGUMENTS"`
5. Run `git status` to verify clean tree

## Phase 5: Push

1. Ask the user to confirm pushing to origin
2. Push commit and tag: `git push origin main && git push origin v$ARGUMENTS`

## Phase 6: Publish to crates.io

```
cargo publish
```

If publish fails with "crate version already exists", the version was already
published — report to the user.

If publish fails for another reason, report the full error and stop.

## Phase 7: Verify

1. Check the release is visible: `cargo search diamond-types-extended`
2. Report the published version to the user
3. Optionally create a GitHub release: `gh release create v$ARGUMENTS --generate-notes`

## Known Issues

- The `crdt-testdata`, `trace-alloc`, and `bench` crates are internal-only and
  should not be published to crates.io.
- crates.io has a propagation delay — if `cargo search` doesn't show the new
  version immediately, wait a minute and try again.
