# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**diamond-types-extended** is a fork of [diamond-types](https://github.com/josephg/diamond-types) that adds
a unified Document/Transaction API layer on top of high-performance CRDTs.

The upstream internals (`OpLog`, `Branch`, `CausalGraph`) are preserved as `pub(crate)`.
The public API (`Document`, `Transaction`, `*Ref`, `*Mut`, `DocumentWriter`) is our layer.

## Build Commands

```bash
cargo build                          # Build everything
cargo test                           # Run main crate tests
cargo test -p rle -p crdt-testdata   # Run subcrate tests (CI does both)
cargo test --test document           # Single integration test file
cargo test map_set_and_get           # Single test by name
cargo bench                          # Run sync benchmarks (criterion)
cargo run --example basic            # Run an example
cargo run --example stress -- --threads 4 --duration 10  # Stress test (clap args)
```

**Features:**
- `default` = `["lz4", "storage"]`
- `serde` is always enabled — all public types derive `Serialize`/`Deserialize`
- `wchar_conversion` — UTF-16 offset support via jumprope

## Architecture

```
Document (public entry point)
  ├── transact(agent, |tx| { ... })  →  Transaction  →  MapMut / TextMut / SetMut
  ├── writer(agent)                  →  DocumentWriter (closure-free, for FFI/WASM)
  ├── root()                         →  MapRef → TextRef / SetRef / RegisterRef
  ├── ops_since(&Frontier)           →  SerializedOps  (sync out)
  ├── merge_ops(SerializedOpsOwned)  →  Result<()>     (sync in)
  └── checkout()                     →  BTreeMap<String, MaterializedValue>
```

### Three Value Tiers

| Type | Purpose | Variants |
|------|---------|----------|
| `PrimitiveValue` | **Writes** — what you pass to `set()`/`add()` | `Nil, Bool, Int, Str` |
| `Value` | **Reads** — what `get()` returns | primitives + `Map(CrdtId), Text(CrdtId), Set(CrdtId), Register(CrdtId)` |
| `MaterializedValue` | **Checkout** — fully resolved tree | primitives + `Text(String), Map(BTreeMap), Set(Vec)` |

`PrimitiveValue` exists to prevent panics from accidentally passing CRDT handles to mutation methods.

### Internal ↔ Public Boundary

- Internal: `OpLog`, `Branch`, `CRDTKind`, `Primitive` (uses `SmartString`), `CreateValue`, `DTValue`
- Public: `Document`, `Transaction`, `DocumentWriter`, `{Map,Text,Set,Register}Ref`, `{Map,Text,Set}Mut`
- `Frontier::root()` replaces `&[]` for full sync in `ops_since()`

### Sync Flow

1. Sender: `let ops = doc_a.ops_since(&peer_frontier).into();` → `SerializedOpsOwned`
2. Wire: `SerializedOpsOwned` implements `Serialize`/`Deserialize` for transport
3. Receiver: `doc_b.merge_ops(ops)?;`

### Conflict Model

- **Maps**: LWW per key, ordered by `(lamport_timestamp, agent_id)`
- **Sets**: Add-wins (concurrent add + remove → add wins)
- **Text**: Interleaving via egwalker merge algorithm
- `get_conflicted()` exposes `Conflicted<T>` with winner + losers

## Workspace Layout

```
src/              Main library — document.rs, refs.rs, muts.rs, value.rs + upstream internals
crates/rle/       Run-length encoding utilities (upstream)
crates/crdt-testdata/  Test dataset loading (upstream)
crates/trace-alloc/    Memory tracing allocator (upstream)
tests/            Integration tests (document, text, set, replication, conflicts, concurrent_ops)
examples/         basic.rs, sync.rs, wasm_style.rs, stress.rs
benches/          sync_benchmark.rs (criterion)
```

## Development Guidelines

### Code Style

- Tests use the public `Document` API exclusively; only `oplog.rs` unit tests touch internals directly
- Comments only for non-obvious "why"
- Prefer strong types and enums over primitives
- All new public API types go in the existing modules (`document.rs`, `refs.rs`, `muts.rs`, `value.rs`)

### Warnings Policy

The codebase should be **warning-free** across `cargo test`, `cargo clippy`, and `cargo doc`.

### Version Control

- Always add files by name, never `git add -A`
- Run `cargo test` before committing
- Commit messages: imperative mood, explain "why"

### Commit Attribution

```
Co-Authored-By: Claude <claude@anthropic.com>
```

## Key Files

| File | What's There |
|------|-------------|
| `src/document.rs` | `Document`, `Transaction`, `DocumentWriter` (~1260 lines) |
| `src/refs.rs` | `MapRef`, `TextRef`, `SetRef`, `RegisterRef` — read-only handles |
| `src/muts.rs` | `MapMut`, `TextMut`, `SetMut` — write handles |
| `src/value.rs` | `Value`, `PrimitiveValue`, `MaterializedValue`, `CrdtId`, `Conflicted` |
| `src/frontier.rs` | `Frontier` type with `root()` constructor |
| `src/oplog.rs` | Core `OpLog` — operations, serialization, merge (~1935 lines) |
| `src/storage/` | Incremental on-disk persistence (WIP, feature-gated) |
