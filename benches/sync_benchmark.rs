//! Benchmarks for sync operations (serialization and deserialization).
//!
//! These benchmarks measure the performance of `ops_since` (serialization) and
//! `merge_ops` (deserialization) to detect regressions from the agent-splitting fix.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use diamond_types_extended::{Document, SerializedOpsOwned};

/// Build a document with many text operations from a single agent
fn build_single_agent_document(num_ops: usize) -> Document {
    let mut doc = Document::new();
    let agent = doc.get_or_create_agent("benchmark_agent");

    doc.transact(agent, |tx| {
        tx.root().create_text("content");
    });

    // Insert characters one at a time to create many operations
    for i in 0..num_ops {
        doc.transact(agent, |tx| {
            let c = (b'a' + (i % 26) as u8) as char;
            tx.get_text_mut(&["content"]).unwrap().push(&c.to_string());
        });
    }

    doc
}

/// Build a document with text operations from multiple alternating agents
/// This stresses the agent boundary splitting logic
fn build_multi_agent_document(num_ops: usize) -> Document {
    let mut doc = Document::new();
    let agent_a = doc.get_or_create_agent("alice");
    let agent_b = doc.get_or_create_agent("bob");

    doc.transact(agent_a, |tx| {
        tx.root().create_text("content");
    });

    // Alternate between agents (zebra pattern)
    for i in 0..num_ops {
        let (agent, c) = if i % 2 == 0 {
            (agent_a, 'A')
        } else {
            (agent_b, 'B')
        };
        doc.transact(agent, |tx| {
            tx.get_text_mut(&["content"]).unwrap().push(&c.to_string());
        });
    }

    doc
}

/// Build a document with mixed text and map operations
/// This creates non-contiguous LV patterns for text
fn build_mixed_ops_document(num_ops: usize) -> Document {
    let mut doc = Document::new();
    let agent = doc.get_or_create_agent("mixed_agent");

    doc.transact(agent, |tx| {
        tx.root().create_text("log");
    });

    for i in 0..num_ops {
        if i % 3 == 0 {
            // Text operation
            doc.transact(agent, |tx| {
                tx.get_text_mut(&["log"])
                    .unwrap()
                    .push(&format!("[{}]", i));
            });
        } else {
            // Map operation (creates LV gaps for text)
            doc.transact(agent, |tx| {
                tx.root().set(&format!("key_{}", i), i as i64);
            });
        }
    }

    doc
}

fn bench_sync_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_operations");

    // Single agent document (baseline)
    let single_agent_doc = build_single_agent_document(1000);
    let single_agent_ops: SerializedOpsOwned = single_agent_doc.ops_since(&[]).into();

    group.throughput(Throughput::Elements(1000));

    group.bench_function("serialize_single_agent_1k", |b| {
        b.iter(|| black_box(single_agent_doc.ops_since(&[])))
    });

    group.bench_function("merge_single_agent_1k", |b| {
        b.iter_with_setup(
            || Document::new(),
            |mut dest| {
                dest.merge_ops(single_agent_ops.clone()).unwrap();
                dest
            },
        )
    });

    // Multi-agent document (stresses agent boundary splitting)
    let multi_agent_doc = build_multi_agent_document(1000);
    let multi_agent_ops: SerializedOpsOwned = multi_agent_doc.ops_since(&[]).into();

    group.bench_function("serialize_multi_agent_1k", |b| {
        b.iter(|| black_box(multi_agent_doc.ops_since(&[])))
    });

    group.bench_function("merge_multi_agent_1k", |b| {
        b.iter_with_setup(
            || Document::new(),
            |mut dest| {
                dest.merge_ops(multi_agent_ops.clone()).unwrap();
                dest
            },
        )
    });

    // Mixed operations document (non-contiguous LVs)
    let mixed_doc = build_mixed_ops_document(1000);
    let mixed_ops: SerializedOpsOwned = mixed_doc.ops_since(&[]).into();

    group.bench_function("serialize_mixed_ops_1k", |b| {
        b.iter(|| black_box(mixed_doc.ops_since(&[])))
    });

    group.bench_function("merge_mixed_ops_1k", |b| {
        b.iter_with_setup(
            || Document::new(),
            |mut dest| {
                dest.merge_ops(mixed_ops.clone()).unwrap();
                dest
            },
        )
    });

    group.finish();
}

fn bench_incremental_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_sync");

    // Build a document, sync half, then benchmark syncing the other half
    let mut source = build_single_agent_document(500);
    let agent = source.get_or_create_agent("benchmark_agent");

    // Get ops for first half
    let first_half: SerializedOpsOwned = source.ops_since(&[]).into();

    // Add more ops
    for i in 500..1000 {
        source.transact(agent, |tx| {
            let c = (b'a' + (i % 26) as u8) as char;
            tx.get_text_mut(&["content"]).unwrap().push(&c.to_string());
        });
    }

    // Create a "synced" peer that has the first half
    let mut synced_peer = Document::new();
    synced_peer.merge_ops(first_half).unwrap();
    let synced_version = synced_peer.version().clone();

    group.throughput(Throughput::Elements(500));

    // Benchmark incremental serialization
    group.bench_function("serialize_incremental_500", |b| {
        b.iter(|| black_box(source.ops_since(synced_version.as_ref())))
    });

    // Benchmark incremental merge
    let incremental_ops: SerializedOpsOwned = source.ops_since(synced_version.as_ref()).into();

    group.bench_function("merge_incremental_500", |b| {
        b.iter_with_setup(
            || {
                let mut peer = Document::new();
                let first_half: SerializedOpsOwned = build_single_agent_document(500).ops_since(&[]).into();
                peer.merge_ops(first_half).unwrap();
                peer
            },
            |mut peer| {
                peer.merge_ops(incremental_ops.clone()).unwrap();
                peer
            },
        )
    });

    group.finish();
}

fn bench_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling");

    for size in [100, 500, 1000, 2000] {
        let doc = build_single_agent_document(size);
        let ops: SerializedOpsOwned = doc.ops_since(&[]).into();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(format!("serialize_{}", size), |b| {
            b.iter(|| black_box(doc.ops_since(&[])))
        });

        group.bench_function(format!("merge_{}", size), |b| {
            b.iter_with_setup(
                || Document::new(),
                |mut dest| {
                    dest.merge_ops(ops.clone()).unwrap();
                    dest
                },
            )
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sync_throughput,
    bench_incremental_sync,
    bench_scaling
);
criterion_main!(benches);
