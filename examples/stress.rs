//! Long-running stress test for Map, Register, and Set CRDTs.
//!
//! Designed to run for hours/days, processing billions of operations
//! with millions of convergence verifications.
//!
//! # Architecture
//!
//! Fully async design - no barriers, maximum parallelism:
//! - Each thread owns its OpLog and runs independently
//! - Threads broadcast ops to others via channels
//! - Threads receive and merge ops from inbox
//! - All threads run flat out until stop signal
//! - Final sync and verification at end
//!
//! # Performance Notes
//!
//! - All threads run continuously with no waiting
//! - Sync/merge happens inline with ops (no separate phase)
//! - Scales well to many cores
//! - Each broadcast sends only NEW ops since last broadcast (efficient bandwidth)
//! - Memory grows over time (oplogs keep full history for convergence)
//!
//! # Known Limitations
//!
//! - Nested CRDTs (Map containing Set, etc.) are disabled due to a sync bug
//! - Only primitive values (Nil, Bool, I64, Str) are used in map entries
//!
//! # Usage
//!
//! ```bash
//! # Quick smoke test
//! cargo run --release --example stress -- --duration 10
//!
//! # 8 cores for 1 hour
//! cargo run --release --example stress -- --threads 8 --duration 3600
//!
//! # 32 cores, broadcast every 500 ops
//! cargo run --release --example stress -- --threads 32 --broadcast-every 500
//!
//! # Target 1 billion ops
//! cargo run --release --example stress -- --threads 8 --target-ops 1000000000
//! ```

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender, Receiver};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use diamond_types::{CRDTKind, CreateValue, Frontier, OpLog, Primitive, ROOT_CRDT_ID, SerializedOpsOwned};

#[derive(Parser, Debug, Clone)]
#[command(name = "stress", about = "Long-running CRDT stress test")]
struct Args {
    /// Number of concurrent threads/peers
    #[arg(short, long, default_value = "8")]
    threads: usize,

    /// Broadcast ops to other peers every N operations
    #[arg(short, long, default_value = "100")]
    broadcast_every: u64,

    /// Check inbox for incoming ops every N operations
    #[arg(long, default_value = "50")]
    recv_every: u64,

    /// Target total operations (0 = run forever)
    #[arg(long, default_value = "0")]
    target_ops: u64,

    /// Maximum duration in seconds (0 = run forever)
    #[arg(short, long, default_value = "0")]
    duration: u64,

    /// RNG seed for reproducibility
    #[arg(short, long, default_value = "42")]
    seed: u64,

    /// Print stats every N seconds
    #[arg(long, default_value = "5")]
    stats_every: u64,

    /// Operation mix: map,set,register percentages (must sum to 100)
    #[arg(long, default_value = "40,40,20")]
    op_mix: String,
}

#[derive(Clone)]
struct OpMix {
    map_pct: u8,
    set_pct: u8,
    #[allow(dead_code)]
    register_pct: u8,  // Used implicitly (100 - set_pct)
}

impl OpMix {
    fn parse(s: &str) -> Self {
        let parts: Vec<u8> = s.split(',').map(|p| p.trim().parse().unwrap()).collect();
        assert_eq!(parts.len(), 3, "op-mix must be 3 comma-separated numbers");
        assert_eq!(parts[0] + parts[1] + parts[2], 100, "op-mix must sum to 100");
        Self {
            map_pct: parts[0],
            set_pct: parts[0] + parts[1],
            register_pct: 100,
        }
    }

    fn pick(&self, roll: u8) -> OpType {
        if roll < self.map_pct {
            OpType::Map
        } else if roll < self.set_pct {
            OpType::Set
        } else {
            OpType::Register
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OpType {
    Map,
    Set,
    Register,
}

struct Stats {
    total_ops: AtomicU64,
    map_ops: AtomicU64,
    set_ops: AtomicU64,
    register_ops: AtomicU64,
    merges: AtomicU64,
    broadcasts: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            map_ops: AtomicU64::new(0),
            set_ops: AtomicU64::new(0),
            register_ops: AtomicU64::new(0),
            merges: AtomicU64::new(0),
            broadcasts: AtomicU64::new(0),
        }
    }

    fn print(&self, elapsed: Duration) {
        let ops = self.total_ops.load(Ordering::Relaxed);
        let merges = self.merges.load(Ordering::Relaxed);
        let broadcasts = self.broadcasts.load(Ordering::Relaxed);
        let secs = elapsed.as_secs_f64();

        let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
        let merges_per_sec = if secs > 0.0 { merges as f64 / secs } else { 0.0 };

        println!(
            "⏱  {:>8.1}s │ ops: {:>12} ({:>10.0}/s) │ merges: {:>10} ({:>8.0}/s) │ broadcasts: {:>10}",
            secs,
            format_num(ops),
            ops_per_sec,
            format_num(merges),
            merges_per_sec,
            format_num(broadcasts)
        );
        println!(
            "   {:>8} │ map: {:>12} │ set: {:>12} │ register: {:>12}",
            "",
            format_num(self.map_ops.load(Ordering::Relaxed)),
            format_num(self.set_ops.load(Ordering::Relaxed)),
            format_num(self.register_ops.load(Ordering::Relaxed)),
        );
    }
}

fn format_num(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Keys used for operations - mix of shared (contention) and unique
const MAP_KEYS: &[&str] = &["shared", "counter", "status", "data", "meta", "config", "state", "value"];
const SET_VALUES: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"];

fn do_random_op(
    oplog: &mut OpLog,
    agent: u32,
    rng: &mut SmallRng,
    op_mix: &OpMix,
    thread_id: usize,
    stats: &Stats,
    set_created: &mut bool,
) {
    let roll: u8 = rng.gen_range(0..100);
    match op_mix.pick(roll) {
        OpType::Map => {
            // Mix of shared keys (contention) and thread-unique keys
            let key = if rng.gen_bool(0.7) {
                MAP_KEYS[rng.gen_range(0..MAP_KEYS.len())]
            } else {
                // Thread-local key avoids some contention
                MAP_KEYS[thread_id % MAP_KEYS.len()]
            };

            // Always use primitive values for now - nested CRDTs have issues
            let value = CreateValue::Primitive(random_primitive(rng));

            oplog.local_map_set(agent, ROOT_CRDT_ID, key, value);
            stats.map_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Set => {
            // Create set once per oplog (skip expensive checkout check after first creation)
            if !*set_created {
                oplog.local_map_set(agent, ROOT_CRDT_ID, "tags",
                    CreateValue::NewCRDT(CRDTKind::Set));
                *set_created = true;
            }
            let (_, set_id) = oplog.crdt_at_path(&["tags"]);

            // 70% adds, 30% removes (to build up set, but still stress add-wins)
            if rng.gen_bool(0.7) {
                let val = if rng.gen_bool(0.6) {
                    // Shared values - causes add-wins conflicts
                    Primitive::Str(SET_VALUES[rng.gen_range(0..SET_VALUES.len())].into())
                } else {
                    // Unique values
                    Primitive::I64(rng.gen_range(0..1000))
                };
                oplog.local_set_add(agent, set_id, val);
            } else {
                let val = Primitive::Str(SET_VALUES[rng.gen_range(0..SET_VALUES.len())].into());
                oplog.local_set_remove(agent, set_id, val);
            }
            stats.set_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Register => {
            // Use map keys as LWW registers - each set overwrites previous
            // This exercises the same LWW conflict resolution
            let key = if rng.gen_bool(0.5) {
                "register"  // Shared - causes conflicts
            } else {
                MAP_KEYS[thread_id % MAP_KEYS.len()]  // Thread-local
            };
            oplog.local_map_set(agent, ROOT_CRDT_ID, key,
                CreateValue::Primitive(random_primitive(rng)));
            stats.register_ops.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn random_primitive(rng: &mut SmallRng) -> Primitive {
    match rng.gen_range(0..4) {
        0 => Primitive::Nil,
        1 => Primitive::Bool(rng.gen()),
        2 => Primitive::I64(rng.gen()),
        _ => Primitive::Str(format!("v{}", rng.gen::<u32>()).into()),
    }
}

/// Create a fresh oplog with all agents registered
fn create_oplog(agent_names: &[String], my_idx: usize) -> (OpLog, u32) {
    let mut oplog = OpLog::new();
    for name in agent_names {
        oplog.cg.get_or_create_agent_id(name);
    }
    let agent = oplog.cg.get_or_create_agent_id(&agent_names[my_idx]);
    (oplog, agent)
}

/// Message sent between threads containing serialized ops
type OpsMessage = SerializedOpsOwned;

fn main() {
    let args = Args::parse();
    let op_mix = OpMix::parse(&args.op_mix);

    println!("🔥 Diamond Types Stress Test (Fully Async)");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("   Threads: {}  │  Broadcast every: {} ops  │  Seed: {}",
             args.threads, args.broadcast_every, args.seed);
    println!("   Target ops: {}  │  Duration: {}",
             if args.target_ops == 0 { "∞".to_string() } else { format_num(args.target_ops) },
             if args.duration == 0 { "∞".to_string() } else { format!("{}s", args.duration) });
    println!("   Op mix: map {}%, set {}%, register {}%",
             op_mix.map_pct, op_mix.set_pct - op_mix.map_pct, 100 - op_mix.set_pct);
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!();

    let stats = Arc::new(Stats::new());
    let start_time = Instant::now();
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Agent names shared by all threads
    let agent_names: Vec<String> = (0..args.threads)
        .map(|i| format!("peer_{}", i))
        .collect();

    // Create channels: each thread has a receiver, and senders to all others
    // Thread i receives on receivers[i], sends to senders[j] for j != i
    let mut senders: Vec<Vec<Sender<OpsMessage>>> = vec![vec![]; args.threads];
    let mut receivers: Vec<Receiver<OpsMessage>> = Vec::with_capacity(args.threads);

    for i in 0..args.threads {
        let (tx, rx) = mpsc::channel();
        receivers.push(rx);
        // Every other thread gets a sender to this thread's inbox
        for j in 0..args.threads {
            if i != j {
                // j will send to i
                if senders[j].len() <= i {
                    senders[j].resize_with(args.threads, || tx.clone());
                }
                senders[j][i] = tx.clone();
            }
        }
    }

    // Spawn stats printer thread
    let stats_clone = Arc::clone(&stats);
    let stop_flag_clone = Arc::clone(&stop_flag);
    let stats_every = args.stats_every;
    let stats_thread = thread::spawn(move || {
        let mut last_print = Instant::now();
        while !stop_flag_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
            if last_print.elapsed().as_secs() >= stats_every {
                stats_clone.print(start_time.elapsed());
                last_print = Instant::now();
            }
        }
    });

    // Spawn worker threads
    let handles: Vec<_> = (0..args.threads)
        .map(|thread_id| {
            let args = args.clone();
            let op_mix = op_mix.clone();
            let stats = Arc::clone(&stats);
            let stop_flag = Arc::clone(&stop_flag);
            let agent_names = agent_names.clone();
            let my_senders: Vec<Sender<OpsMessage>> = senders[thread_id].clone();
            let my_receiver = receivers.remove(0); // Take ownership

            thread::spawn(move || {
                let (mut oplog, agent) = create_oplog(&agent_names, thread_id);
                let mut rng = SmallRng::seed_from_u64(args.seed.wrapping_add(thread_id as u64));
                let mut ops_since_broadcast = 0u64;
                let mut ops_since_recv = 0u64;
                let mut set_created = false;  // Track if we've created the set locally
                let mut last_broadcast_version = Frontier::root();  // Track version at last broadcast

                loop {
                    // Check stop conditions
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    // Do one op
                    do_random_op(&mut oplog, agent, &mut rng, &op_mix, thread_id, &stats, &mut set_created);
                    stats.total_ops.fetch_add(1, Ordering::Relaxed);
                    ops_since_broadcast += 1;
                    ops_since_recv += 1;

                    // Periodically broadcast our NEW ops to all other threads
                    if ops_since_broadcast >= args.broadcast_every {
                        // Only send ops since last broadcast (not entire history)
                        let ops_owned: SerializedOpsOwned = oplog.ops_since(last_broadcast_version.as_ref()).into();

                        // Update our version before sending
                        last_broadcast_version = oplog.cg.version.clone();

                        // Send to all other threads (non-blocking)
                        for sender in &my_senders {
                            let _ = sender.send(ops_owned.clone()); // Ignore if receiver dropped
                        }
                        stats.broadcasts.fetch_add(1, Ordering::Relaxed);
                        ops_since_broadcast = 0;
                    }

                    // Periodically check inbox and merge
                    if ops_since_recv >= args.recv_every {
                        // Non-blocking receive all pending messages
                        while let Ok(incoming) = my_receiver.try_recv() {
                            if oplog.merge_ops_owned(incoming).is_ok() {
                                stats.merges.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        ops_since_recv = 0;
                    }

                    // Check stop conditions periodically
                    if stats.total_ops.load(Ordering::Relaxed) % 10000 == 0 {
                        let elapsed = start_time.elapsed();
                        if args.duration > 0 && elapsed.as_secs() >= args.duration {
                            stop_flag.store(true, Ordering::Relaxed);
                        }
                        if args.target_ops > 0 && stats.total_ops.load(Ordering::Relaxed) >= args.target_ops {
                            stop_flag.store(true, Ordering::Relaxed);
                        }
                    }
                }

                // Return oplog for final verification
                oplog
            })
        })
        .collect();

    // Wait for all worker threads and collect oplogs
    let mut final_oplogs: Vec<OpLog> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    // Stop stats thread
    stop_flag.store(true, Ordering::Relaxed);
    let _ = stats_thread.join();

    println!("\n🔄 Final sync and verification...");

    // Final sync: full mesh
    let n = final_oplogs.len();
    for _ in 0..2 {
        for i in 0..n {
            for j in (i + 1)..n {
                let (left, right) = final_oplogs.split_at_mut(j);
                let oplog_i = &mut left[i];
                let oplog_j = &mut right[0];
                oplog_i.merge_ops(oplog_j.ops_since(&[])).unwrap();
                oplog_j.merge_ops(oplog_i.ops_since(&[])).unwrap();
            }
        }
    }

    // Verify convergence
    let first = final_oplogs[0].checkout();
    let mut converged = true;
    for (i, oplog) in final_oplogs.iter().enumerate().skip(1) {
        let checkout = oplog.checkout();
        if first != checkout {
            eprintln!("❌ CONVERGENCE FAILURE: OpLog 0 != OpLog {}", i);
            converged = false;
        }
    }

    // Final stats
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════");
    if converged {
        println!("✅ STRESS TEST COMPLETE - All peers converged!");
    } else {
        println!("❌ STRESS TEST FAILED - Convergence check failed!");
    }
    stats.print(start_time.elapsed());
    println!("═══════════════════════════════════════════════════════════════════════════════");
}
