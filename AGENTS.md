# AGENTS.md — quick-memory contributor guide

Canonical instruction file for AI coding agents working in this repository.

## What this is

Multi-machine agent memory. Authoritative data lives in S3/R2; the search
index is a derived, rebuildable layer; **any single machine can search the
full corpus** with no server, coordinator, or peer discovery.

Read [`docs/design.md`](docs/design.md) before touching storage, indexing, or
the read path.

## Non-negotiable invariants

1. **Object storage is the only shared state.** No component may assume a
   long-lived process, a stable hostname, or a peer it can reach.
2. **No singleton roles.** Anything that needs "only one at a time" is a
   lease object in the bucket plus a CAS fence — never a single node, and
   never a guarantee the operator must uphold.
3. **One commit point per scope.** A write becomes visible when
   `manifest.pb` CAS succeeds. Nothing is readable because it was uploaded.
4. **The index is derived and rebuildable.** Deleting the whole index prefix
   must be recoverable by re-running the builder over authoritative objects.
   Prove it with a test, not a comment.
5. **Every search hit is re-checked against authority** for deletion,
   supersession, and scope before it reaches a caller. The index supplies
   candidates only.
6. **ETag is the object identity.** Conditional writes require an ETag and
   fail closed without one; never compare ETag and version structurally, and
   never feed a PUT-response version id into `UpdateVersion`.
7. **Immutability by default.** Page versions, observations, WAL segments and
   split files are never overwritten in place; corrections create new objects
   and CAS a pointer.
8. **Scope filters are injected by the reader**, never accepted from a caller
   as an encoder of authority. A caller-supplied filter may only narrow.
9. **Nothing may block an agent.** Ingest is bounded, rate-limited and
   acknowledged before indexing; a probe or check that cannot reach its
   backend fails loudly instead of reporting success.

## Layout

```
crates/qm-core    pure domain types + object-key layout (no IO)
crates/qm-store   CAS primitives + conformance probe
crates/qm-search  split upload/materialise + in-process tantivy search
crates/qm-probe   S0 probe binaries
```

## Collaboration topology

- **walgit is the source of truth**: `origin` is
  `http://127.0.0.1:8081/gqf2008/quick-memory.git`. Daily work pushes there.
- **GitHub is a release mirror**: `gqf2008/quick-memory` is fed by the walgit
  mirror loop every 60s (heads + tags only; `refs/collab/*` stays on walgit).
  Never push to GitHub by hand — releases are a tag pushed to `origin`, which
  the mirror carries over, then `gh release create <tag> --generate-notes`.
- **Work is recorded in walgit collab**, not only in git: issue threads, status,
  patches, reviews and merges live in `refs/collab/*` and drive the board. See
  the `walgit` skill for the entry schema; a change with no collab entry has no
  collaboration record.
- `.walgit/board.toml` declares the board lanes and `.walgit/ci.toml` declares
  the gates the walgit CI runner executes (fmt, test, clippy).

## Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Development rules

- Small, scoped changes; keep `qm-core` free of IO and async.
- Every bug fix gets a regression test that fails before the fix. Guards that
  cannot fail are decoration: verify with a positive control (feed the check a
  deliberately broken input or backend and confirm it reports).
- A conformance check must never silently skip. Missing credentials are an
  error, and CI without credentials must not be presented as verification.
- Comments explain why, not what. No `unsafe` (workspace lint forbids it).
- Probe code and tests never run against the real bucket in CI; real-backend
  verification is an explicit, manual invocation with credentials.
