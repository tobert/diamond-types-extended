use std::hint::black_box;
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use facet::list::{ListCRDT, ListOpLog};

/// Get path to .dt test data file
fn dt_path(name: &str) -> String {
    match name {
        // Papers
        "automerge-paper" | "egwalker" | "seph-blog1" =>
            format!("test_data/papers/{}.dt", name),
        // OSS projects
        "git-makefile" | "node_nodecc" =>
            format!("test_data/oss/{}.dt", name),
        // Collab sessions
        "clownschool" | "friendsforever" | "friendsforever_raw" =>
            format!("test_data/collab/{}.dt", name),
        // Synthetic
        "A1" | "A2" | "C1" | "C2" | "S1" | "S2" | "S3" =>
            format!("test_data/synthetic/{}.dt", name),
        _ => panic!("Unknown dataset: {}", name),
    }
}

pub fn apply_edits_direct(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            if *del_span > 0 {
                doc.delete_without_content(id, *pos .. *pos + *del_span);
            }

            if !ins_content.is_empty() {
                doc.insert(id, *pos, ins_content);
            }
        }
    }
}

// This is a dirty addition for profiling.
#[allow(unused)]
fn profile_direct_editing() {
    let filename = "test_data/papers/automerge-paper.json.gz";
    let test_data = load_testing_data(&filename);

    for _i in 0..300 {
        let mut doc = ListCRDT::new();
        apply_edits_direct(&mut doc, &test_data.txns);
        assert_eq!(doc.len(), test_data.end_content.chars().count());
    }
}

#[allow(unused)]
fn profile_merge(name: &str, n: usize) {
    let contents = std::fs::read(&dt_path(name)).unwrap();
    let oplog = ListOpLog::load_from(&contents).unwrap();

    for _i in 0..n {
        // black_box(oplog.checkout_tip_old());
        black_box(oplog.checkout_tip());
    }
}

#[allow(unused)]
fn profile_make_plan(name: &str, n: usize) {
    let contents = std::fs::read(&dt_path(name)).unwrap();
    let oplog = ListOpLog::load_from(&contents).unwrap();

    for _i in 0..n {
        oplog.dbg_bench_make_plan();
    }
}

// RUSTFLAGS="-Cforce-frame-pointers=yes" cargo build --profile profiling --example profile
fn main() {
    profile_merge("clownschool", 500);
    // profile_make_plan("clownschool", 2);
    // profile_merge("git-makefile", 200);
    // profile_merge("git-makefile", 1);
    // profile_merge("node_nodecc", 1);
    // profile_merge("clownschool", 1);
}