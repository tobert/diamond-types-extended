#![allow(dead_code)]

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;
mod utils;
// mod idxtrace;

use std::env;
use criterion::{black_box, Criterion, BenchmarkId, Throughput};
use jumprope::JumpRope;
use crdt_testdata::{load_testing_data, TestData};
use facet::list::{ListCRDT, ListOpLog};
use facet::list::encoding::*;
use crate::utils::*;

/// Get path to JSON test data file
fn testing_data_path(name: &str) -> String {
    match name {
        // Papers (have .json.gz)
        "automerge-paper" | "egwalker" | "seph-blog1" =>
            format!("test_data/papers/{}.json.gz", name),
        // Collab sessions (only _flat versions have .json.gz)
        "clownschool_flat" | "friendsforever_flat" =>
            format!("test_data/collab/{}.json.gz", name),
        // Misc JSON sources
        "rustcode" | "sveltecomponent" =>
            format!("test_data/misc/{}.json.gz", name),
        _ => panic!("No JSON trace for dataset: {}", name),
    }
}

/// Get path to .dt test data file
fn testing_data_dt_path(name: &str) -> String {
    match name {
        // Papers
        "automerge-paper" | "egwalker" | "seph-blog1" =>
            format!("test_data/papers/{}.dt", name),
        // OSS projects (.dt only)
        "git-makefile" | "node_nodecc" =>
            format!("test_data/oss/{}.dt", name),
        // Collab sessions
        "clownschool" | "friendsforever" | "friendsforever_raw" =>
            format!("test_data/collab/{}.dt", name),
        // Synthetic (.dt only)
        "A1" | "A2" | "C1" | "C2" | "S1" | "S2" | "S3" =>
            format!("test_data/synthetic/{}.dt", name),
        _ => panic!("Unknown dataset: {}", name),
    }
}

fn testing_data(name: &str) -> TestData {
    load_testing_data(&testing_data_path(name))
}

fn testing_data_dt(name: &str) -> ListOpLog {
    let bytes = std::fs::read(&testing_data_dt_path(name)).unwrap();
    ListOpLog::load_from(&bytes).unwrap()
}

// const LINEAR_DATASETS: &[&str] = &["automerge-paper", "rustcode", "sveltecomponent", "seph-blog1", "friendsforever_flat"];
const LINEAR_DATASETS: &[&str] = &["automerge-paper", "seph-blog1", "clownschool_flat", "friendsforever_flat", "egwalker"];
const COMPLEX_DATASETS: &[&str] = &["automerge-paper", "seph-blog1", "egwalker", "node_nodecc", "git-makefile", "friendsforever", "clownschool"];

const PAPER_DATASETS: &[&str] = &["S1", "S2", "S3", "C1", "C2", "A1", "A2"];

fn local_benchmarks(c: &mut Criterion) {
    for name in LINEAR_DATASETS {
        let mut group = c.benchmark_group("dt");
        let test_data = testing_data(name);
        assert_eq!(test_data.start_content.len(), 0);

        group.throughput(Throughput::Elements(test_data.len() as u64));
        // group.throughput(Throughput::Elements(test_data.len_keystrokes() as u64));

        group.bench_function(BenchmarkId::new("local", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_direct(&mut doc, &test_data.txns);
                debug_assert_eq!(doc.len(), test_data.end_content.chars().count());
                black_box(doc.len());
            })
        });

        group.bench_function(BenchmarkId::new("local_push", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_push_merge(&mut doc, &test_data.txns);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        // group.bench_function(BenchmarkId::new("apply_grouped", name), |b| {
        //     b.iter(|| {
        //         let mut doc = ListCRDT::new();
        //         apply_grouped(&mut doc, &test_data.txns);
        //         // assert_eq!(doc.len(), test_data.end_content.len());
        //         black_box(doc.len());
        //     })
        // });

        // This is obnoxiously fast. Grouping operations using our RLE encoding before applying
        // drops the number of operations from ~260k -> 10k for automerge-paper, and has a
        // corresponding drop in the time taken to apply (12ms -> 0.8ms).
        let grouped_ops_rle = as_grouped_ops_rle(&test_data.txns);
        // dbg!(grouped_ops_rle.len());
        group.bench_function(BenchmarkId::new("local_rle", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_ops(&mut doc, &grouped_ops_rle);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.finish();
    }
}

// This benchmark is good but its existence drops performance of other benchmarks by 20%!!!
// fn encoding_benchmarks(c: &mut Criterion) {
//     for name in DATASETS {
//         let mut group = c.benchmark_group("encoding");
//         let test_data = testing_data(name);
//
//         let mut doc = ListCRDT::new();
//         apply_edits_direct(&mut doc, &test_data.txns);
//         assert_eq!(test_data.start_content.len(), 0);
//
//         // group.throughput(Throughput::Elements(test_data.len() as u64));
//
//         group.bench_function(BenchmarkId::new("encode", name), |b| {
//             b.iter(|| {
//                 let bytes = doc.oplog.encode(ENCODE_FULL);
//                 black_box(bytes);
//             })
//         });
//
//         let bytes = doc.oplog.encode(ENCODE_FULL);
//
//         group.bench_function(BenchmarkId::new("decode_oplog", name), |b| {
//             b.iter(|| {
//                 let doc = ListOpLog::load_from(&bytes).unwrap();
//                 black_box(doc.len());
//             })
//         });
//         group.bench_function(BenchmarkId::new("decode", name), |b| {
//             b.iter(|| {
//                 let doc = ListCRDT::load_from(&bytes).unwrap();
//                 black_box(doc.len());
//             })
//         });
//
//         group.finish();
//     }
// }

fn encoding_nodecc_benchmarks(c: &mut Criterion) {
    for name in PAPER_DATASETS {

        // for name in COMPLEX_DATASETS {
        let mut group = c.benchmark_group("dt");
        let bytes = std::fs::read(&testing_data_dt_path(name)).unwrap();
        let oplog = ListOpLog::load_from(&bytes).unwrap();
        // group.throughput(Throughput::Bytes(bytes.len() as _));
        group.throughput(Throughput::Elements(oplog.len() as _));

        // Don't care.
        group.bench_function(BenchmarkId::new("decode", name), |b| {
            b.iter(|| {
                let oplog = ListOpLog::load_from(&bytes).unwrap();
                black_box(oplog);
            });
        });

        group.bench_function(BenchmarkId::new("encode", name), |b| {
            b.iter(|| {
                let bytes = oplog.encode(&ENCODE_FULL);
                black_box(bytes);
            });
        });

        group.bench_function(BenchmarkId::new("merge", name), |b| {
            b.iter(|| {
                let branch = oplog.checkout_tip();
                black_box(branch);
            });
        });
        
        // group.bench_function(BenchmarkId::new("merge_old", name), |b| {
        //     b.iter(|| {
        //         let branch = oplog.checkout_tip_old();
        //         black_box(branch);
        //     });
        // });

        group.bench_function(BenchmarkId::new("make_plan", name), |b| {
            b.iter(|| {
                oplog.dbg_bench_make_plan();
            });
        });

        group.finish();
    }
}

// criterion_group!(benches,
//     local_benchmarks,
//     encoding_nodecc_benchmarks,
//     // encoding_benchmarks,
// );
// criterion_main!(benches);


fn paper_benchmarks(c: &mut Criterion) {
    // const PAPER_DATASETS: &[&str] = &["automerge-paperx3", "seph-blog1x3", "node_nodeccx1", "friendsforeverx25", "clownschoolx25", "egwalkerx1", "git-makefilex2"];
    for name in PAPER_DATASETS {
        let mut group = c.benchmark_group("dt");
        let oplog = testing_data_dt(name);
        group.throughput(Throughput::Elements(oplog.len() as _));

        group.bench_function(BenchmarkId::new("merge_norm", name), |b| {
            b.iter(|| {
                let branch = oplog.checkout_tip();
                black_box(branch);
            });
        });

        group.bench_function(BenchmarkId::new("ff_on", name), |b| {
            b.iter(|| {
                oplog.iter_xf_operations()
                    .for_each(|op| { black_box(op); });
            })
        });

        group.bench_function(BenchmarkId::new("ff_off", name), |b| {
            b.iter(|| {
                oplog.dbg_iter_xf_operations_no_ff()
                    .for_each(|op| { black_box(op); });
            })
        });

        group.finish();
    }
}

fn opt_load_time_benchmark(c: &mut Criterion) {
    for &name in PAPER_DATASETS {
        let mut group = c.benchmark_group("dt");

        let oplog = testing_data_dt(name);
        let doc_content = oplog.checkout_tip().content().to_string();

        let temp_dir = env::temp_dir();
        let path = temp_dir.join("content");
        // Write it.
        std::fs::write(&path, &doc_content).unwrap();

        // Then benchmark reading it back.
        group.bench_function(BenchmarkId::new("opt_load", name), |b| {
            b.iter(|| {
                let str_content = std::fs::read_to_string(&path).unwrap();
                let rope = JumpRope::from(&str_content);
                black_box(rope);
            });
        });

        group.finish();
    }

}

fn main() {
    // benches();
    let mut c = Criterion::default()
        .configure_from_args();

    // c.bench_function("count_ones", |b| {
    //     let mut n: u128 = 0;
    //     // let mut n: u128 = 0xffffffff_ffffffff_ffffffff_ffffffff;
    //     // let mut n: u128 = 0xf00ff00f_f00ff00f_f00ff00f_f00ff00f;
    //     b.iter(|| {
    //         black_box(n.count_ones());
    //         n += 1;
    //     });
    // });

    local_benchmarks(&mut c);
    encoding_nodecc_benchmarks(&mut c);
    // idxtrace_benchmarks(&mut c);
    paper_benchmarks(&mut c);
    opt_load_time_benchmark(&mut c);
    c.final_summary();
}