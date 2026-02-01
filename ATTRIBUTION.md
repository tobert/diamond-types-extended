# Attribution

Facet is a fork of [diamond-types](https://github.com/josephg/diamond-types), created by Joseph Gentle ([@josephg](https://github.com/josephg)).

## Original Work

Diamond Types is a high-performance CRDT (Conflict-free Replicated Data Type) implementation that pioneered many of the techniques used in this library:

- Run-length encoded operation storage
- Efficient causal graph tracking
- The "egwalker" merge algorithm for text CRDTs
- Optimized order statistics tree for character positioning

The original diamond-types is licensed under the ISC license, which this fork continues to use.

## What Facet Changes

Facet provides a new public API layer over the diamond-types internals:

- Transaction-based mutation pattern (solves Rust borrow checker issues)
- Unified `Document` container with Map root
- Consistent reference types (`MapRef`/`MapMut`, `TextRef`/`TextMut`, etc.)
- Simplified `Value` enum without leaking internal types
- Clean serialization/replication API

The underlying CRDT algorithms and data structures remain largely unchanged.

## Links

- Original repository: https://github.com/josephg/diamond-types
- Blog post on performance: https://josephg.com/blog/crdts-go-brrr/
- Invisible College (original funding): https://invisible.college/
