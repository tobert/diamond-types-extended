# Facet

High-performance CRDTs (Conflict-free Replicated Data Types) for collaborative applications.

Facet is a fork of [diamond-types](https://github.com/josephg/diamond-types) with extended types and a unified API.

## Features

- **Map** - Key-value container with LWW (Last-Writer-Wins) registers per key
- **Text** - Sequence CRDT for collaborative text editing
- **Set** - OR-Set (Observed-Remove) with add-wins semantics
- **Register** - Single-value LWW container
- **Nesting** - All types can be nested within Maps
- **Replication** - Efficient sync between peers

## Quick Start

```rust
use facet::{Document, Value};

// Create a document
let mut doc = Document::new();
let alice = doc.get_or_create_agent("alice");

// All mutations happen in transactions
doc.transact(alice, |tx| {
    tx.root().set("title", "My Document");
    tx.root().set("count", 42);
    tx.root().create_text("content");
});

// Work with nested text
doc.transact(alice, |tx| {
    if let Some(mut text) = tx.get_text_mut(&["content"]) {
        text.insert(0, "Hello, world!");
    }
});

// Read values directly
assert_eq!(doc.root().get("title").unwrap().as_str(), Some("My Document"));
assert_eq!(doc.root().get_text("content").unwrap().content(), "Hello, world!");
```

## Replication

```rust
use facet::Document;

let mut doc_a = Document::new();
let mut doc_b = Document::new();

let alice = doc_a.get_or_create_agent("alice");
let bob = doc_b.get_or_create_agent("bob");

// Alice makes changes
doc_a.transact(alice, |tx| {
    tx.root().set("from_alice", "hello!");
});

// Sync to Bob
let ops = doc_a.ops_since(&[]).into();
doc_b.merge_ops(ops).unwrap();

// Bob now has Alice's changes
assert!(doc_b.root().contains_key("from_alice"));
```

## Conflict Resolution

Facet uses deterministic conflict resolution:

- **Maps**: LWW (Last-Writer-Wins) based on `(lamport_timestamp, agent_id)` ordering
- **Sets**: Add-wins semantics (concurrent add + remove = add wins)
- **Text**: Interleaving based on operation ordering

You can detect and handle conflicts explicitly:

```rust
let conflicted = doc.root().get_conflicted("key").unwrap();
if conflicted.has_conflicts() {
    println!("Winner: {:?}", conflicted.value);
    println!("Losers: {:?}", conflicted.conflicts);
}
```

## Performance

Facet inherits diamond-types' exceptional performance:
- Run-length encoded operation storage
- Optimized causal graph tracking
- The "egwalker" merge algorithm

See the original [diamond-types blog post](https://josephg.com/blog/crdts-go-brrr/) for benchmarks.

## Attribution

Facet is built on diamond-types by Joseph Gentle ([@josephg](https://github.com/josephg)).

See [ATTRIBUTION.md](ATTRIBUTION.md) for full credits.

## License

ISC License - see [LICENSE](LICENSE) for details.
