//! Long-running stress test for Map, Register, and Set CRDTs.
//!
//! Designed to run for hours/days, processing billions of operations
//! with bounded memory via universe rotation.
//!
//! # Architecture
//!
//! Multi-universe design for bounded memory:
//! - U universes, each with its own set of OpLogs (one per peer)
//! - Worker threads operate across all universes round-robin
//! - Compactor thread monitors universe sizes
//! - When a universe exceeds threshold, compactor:
//!   1. Locks it (threads skip to other universes)
//!   2. Syncs all peers
//!   3. Verifies convergence
//!   4. Resets to fresh state
//!   5. Unlocks
//!
//! This mirrors real-world usage where you have many documents,
//! and can compact/checkpoint each independently.
//!
//! # Performance Notes
//!
//! - Threads work on any non-locked universe
//! - One universe compacting doesn't block others
//! - Memory bounded by: U * threshold_ops * avg_op_size
//! - Convergence verified on every compaction (catches bugs early)
//!
//! # Usage
//!
//! ```bash
//! # Quick smoke test
//! cargo run --release --example stress -- --duration 10
//!
//! # 8 threads, 4 universes, compact at 50k ops each
//! cargo run --release --example stress -- --threads 8 --universes 4 --compact-at 50000
//!
//! # Long run: 32 threads, 8 universes, 1 hour
//! cargo run --release --example stress -- --threads 32 --universes 8 --duration 3600
//! ```

use std::sync::atomic::{AtomicU64, AtomicU8, AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use diamond_types_extended::{CRDTKind, CreateValue, Frontier, OpLog, Primitive, ROOT_CRDT_ID, SerializedOpsOwned};

// Universe states
const STATE_RUNNING: u8 = 0;
const STATE_COMPACTING: u8 = 1;

#[derive(Parser, Debug, Clone)]
#[command(name = "stress", about = "Long-running CRDT stress test with bounded memory")]
struct Args {
    /// Number of concurrent worker threads
    #[arg(short, long, default_value = "8")]
    threads: usize,

    /// Number of universes (more = better concurrency during compaction)
    #[arg(short, long, default_value = "4")]
    universes: usize,

    /// Compact a universe after this many operations
    #[arg(long, default_value = "50000")]
    compact_at: u64,

    /// Hard limit per universe (threads wait when exceeded) - should be 2-3x compact_at
    #[arg(long, default_value = "0")]
    max_universe_ops: u64,

    /// Broadcast ops to other peers every N operations
    #[arg(short, long, default_value = "100")]
    broadcast_every: u64,

    /// Check inbox for incoming ops every N operations
    #[arg(long, default_value = "50")]
    recv_every: u64,

    /// Channel buffer size per sender (bounds memory usage)
    #[arg(long, default_value = "100")]
    channel_size: usize,

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

    /// Operation mix: map,set,register,crash percentages (must sum to 100)
    #[arg(long, default_value = "39,39,19,3")]
    op_mix: String,

    /// Configuration preset (overrides other settings)
    /// Options: fast, chaos, endurance
    #[arg(long)]
    preset: Option<String>,
}

#[derive(Clone)]
struct OpMix {
    map_pct: u8,
    set_pct: u8,
    register_pct: u8,
    #[allow(dead_code)]
    crash_pct: u8,
}

impl OpMix {
    fn parse(s: &str) -> Self {
        let parts: Vec<u8> = s.split(',').map(|p| p.trim().parse().unwrap()).collect();
        assert_eq!(parts.len(), 4, "op-mix must be 4 comma-separated numbers (map,set,reg,crash)");
        assert_eq!(parts.iter().sum::<u8>(), 100, "op-mix must sum to 100");
        Self {
            map_pct: parts[0],
            set_pct: parts[0] + parts[1],
            register_pct: parts[0] + parts[1] + parts[2],
            crash_pct: 100,
        }
    }

    fn pick(&self, roll: u8) -> OpType {
        if roll < self.map_pct {
            OpType::Map
        } else if roll < self.set_pct {
            OpType::Set
        } else if roll < self.register_pct {
            OpType::Register
        } else {
            OpType::Crash
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OpType {
    Map,
    Set,
    Register,
    Crash,
}

/// Per-peer state within a universe
struct PeerState {
    oplog: OpLog,
    agent_idx: usize, // Index into global agent_names
    agent: u32,       // Local agent ID in the current oplog
    last_broadcast_version: Frontier,
    set_created: bool,
}

/// A universe is a self-contained set of OpLogs that can be independently compacted
struct Universe {
    /// Current state (Running or Compacting)
    state: AtomicU8,
    /// One peer state per thread, protected by RwLock
    peers: Vec<RwLock<PeerState>>,
    /// Operation count since last compaction
    op_count: AtomicU64,
}

/// Message sent between threads
struct OpsMessage {
    universe_id: usize,
    ops: SerializedOpsOwned,
}

/// Global statistics
struct Stats {
    total_ops: AtomicU64,
    map_ops: AtomicU64,
    set_ops: AtomicU64,
    register_ops: AtomicU64,
    crashes: AtomicU64,
    merges: AtomicU64,
    broadcasts: AtomicU64,
    compactions: AtomicU64,
    convergence_checks: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            map_ops: AtomicU64::new(0),
            set_ops: AtomicU64::new(0),
            register_ops: AtomicU64::new(0),
            crashes: AtomicU64::new(0),
            merges: AtomicU64::new(0),
            broadcasts: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            convergence_checks: AtomicU64::new(0),
        }
    }

    fn print(&self, elapsed: Duration) {
        let ops = self.total_ops.load(Ordering::Relaxed);
        let merges = self.merges.load(Ordering::Relaxed);
        let compactions = self.compactions.load(Ordering::Relaxed);
        let checks = self.convergence_checks.load(Ordering::Relaxed);
        let crashes = self.crashes.load(Ordering::Relaxed);
        let secs = elapsed.as_secs_f64();

        let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
        let merges_per_sec = if secs > 0.0 { merges as f64 / secs } else { 0.0 };

        println!(
            "⏱  {:>8.1}s │ ops: {:>12} ({:>10.0}/s) │ merges: {:>10} ({:>8.0}/s)",
            secs, format_num(ops), ops_per_sec, format_num(merges), merges_per_sec
        );
        println!(
            "            │ map: {:>12} │ set: {:>12} │ reg: {:>12} │ crash: {:>8}",
            format_num(self.map_ops.load(Ordering::Relaxed)),
            format_num(self.set_ops.load(Ordering::Relaxed)),
            format_num(self.register_ops.load(Ordering::Relaxed)),
            format_num(crashes),
        );
        println!(
            "            │ compactions: {:>6} │ convergence checks: {:>6} ✓",
            compactions, checks
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

/// Keys used for operations
const MAP_KEYS: &[&str] = &["shared", "counter", "status", "data", "meta", "config", "state", "value"];
const SET_VALUES: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"];
// "Nasty" strings to catch edge cases
const NASTY_STRINGS: &[&str] = &[
    "",                         // Empty
    " ",                        // Space
    "\0",                       // Null
    "\n\r\t",                   // Control
    "embedded \" quote",        // Quote
    "💾",                       // Emoji
    "你好",                      // Unicode
    "Ẓ̸̨̧̥̼͚̲̣̝̖̣͂ͪ̉̇̅̇̋ͬ̊̄̾ͨ̀̌ͬͪ̆ͮ̚͜", // Zalgo
    "مرحبا",                    // RTL
];

fn create_peer_state(agent_names: &[String], my_idx: usize) -> PeerState {
    let mut oplog = OpLog::new();
    // Pre-register all agents so IDs are somewhat stable (though not guaranteed across crashes)
    for name in agent_names {
        oplog.cg.get_or_create_agent_id(name);
    }
    let agent = oplog.cg.get_or_create_agent_id(&agent_names[my_idx]);
    PeerState {
        oplog,
        agent_idx: my_idx,
        agent,
        last_broadcast_version: Frontier::root(),
        set_created: false,
    }
}

fn do_random_op(
    peer: &mut PeerState,
    rng: &mut SmallRng,
    op_mix: &OpMix,
    agent_names: &[String],
    thread_id: usize,
    stats: &Stats,
) {
    let roll: u8 = rng.gen_range(0..100);
    match op_mix.pick(roll) {
        OpType::Map => {
            let key = if rng.gen_bool(0.7) {
                MAP_KEYS[rng.gen_range(0..MAP_KEYS.len())]
            } else {
                MAP_KEYS[thread_id % MAP_KEYS.len()]
            };
            let value = CreateValue::Primitive(random_primitive(rng));
            peer.oplog.local_map_set(peer.agent, ROOT_CRDT_ID, key, value);
            stats.map_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Set => {
            if !peer.set_created {
                peer.oplog.local_map_set(peer.agent, ROOT_CRDT_ID, "tags",
                    CreateValue::NewCRDT(CRDTKind::Set));
                peer.set_created = true;
            }
            let (_, set_id) = peer.oplog.crdt_at_path(&["tags"]);

            if rng.gen_bool(0.7) {
                let val = if rng.gen_bool(0.6) {
                    Primitive::Str(SET_VALUES[rng.gen_range(0..SET_VALUES.len())].into())
                } else {
                    Primitive::I64(rng.gen_range(0..1000))
                };
                peer.oplog.local_set_add(peer.agent, set_id, val);
            } else {
                let val = Primitive::Str(SET_VALUES[rng.gen_range(0..SET_VALUES.len())].into());
                peer.oplog.local_set_remove(peer.agent, set_id, val);
            }
            stats.set_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Register => {
            let key = if rng.gen_bool(0.5) {
                "register"
            } else {
                MAP_KEYS[thread_id % MAP_KEYS.len()]
            };
            peer.oplog.local_map_set(peer.agent, ROOT_CRDT_ID, key,
                CreateValue::Primitive(random_primitive(rng)));
            stats.register_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Crash => {
            // Simulate crash/restart:
            // 1. Serialize full state
            // 2. Create fresh OpLog
            // 3. Merge serialized state
            // 4. Re-acquire agent ID
            let full_ops: SerializedOpsOwned = peer.oplog.ops_since(&[]).into();
            
            let mut new_oplog = OpLog::new();
            // Important: Pre-register all agents again to minimize ID shuffling
            // (though merge_ops would handle it, this keeps things cleaner)
            for name in agent_names {
                new_oplog.cg.get_or_create_agent_id(name);
            }
            
            // Restore state
            new_oplog.merge_ops_owned(full_ops).expect("Crash recovery failed: unable to merge own ops");
            
            peer.oplog = new_oplog;
            peer.agent = peer.oplog.cg.get_or_create_agent_id(&agent_names[peer.agent_idx]);
            // Reset broadcast cursor since we effectively have a "new" oplog,
            // but to avoid resending everything, we set it to current version.
            // In a real system we'd persist the cursor or re-handshake.
            peer.last_broadcast_version = peer.oplog.cg.version.clone();
            
            stats.crashes.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn random_primitive(rng: &mut SmallRng) -> Primitive {
    match rng.gen_range(0..5) {
        0 => Primitive::Nil,
        1 => Primitive::Bool(rng.gen()),
        2 => Primitive::I64(rng.gen()),
        3 => {
            // Normal string
            Primitive::Str(format!("v{}", rng.gen::<u32>()).into())
        }
        _ => {
            // Nasty string
            let s = NASTY_STRINGS[rng.gen_range(0..NASTY_STRINGS.len())];
            Primitive::Str(s.into())
        }
    }
}

fn main() {
    let mut args = Args::parse();

    // Apply presets
    if let Some(preset) = &args.preset {
        match preset.as_str() {
            "fast" => {
                // Quick validation run
                args.compact_at = 10_000;
                args.max_universe_ops = 20_000;
                args.broadcast_every = 10;
                args.recv_every = 5;
                args.stats_every = 1;
                // No crashes in fast mode to ensure high throughput
                args.op_mix = "34,33,33,0".to_string(); 
            },
            "chaos" => {
                // Maximum randomization and crashes
                args.broadcast_every = 1; // Constant chatter
                args.recv_every = 1;
                args.op_mix = "30,30,20,20".to_string(); // 20% crash rate!
            },
            "endurance" => {
                // Long running, low overhead
                args.compact_at = 500_000;
                args.max_universe_ops = 1_000_000;
                args.broadcast_every = 1000;
                args.recv_every = 500;
                args.op_mix = "45,45,9,1".to_string(); // 1% crash rate
            },
            _ => panic!("Unknown preset: {}. Valid: fast, chaos, endurance", preset),
        }
    }

    let op_mix = OpMix::parse(&args.op_mix);

    // Default max_universe_ops to 2x compact_at if not set
    if args.max_universe_ops == 0 {
        args.max_universe_ops = args.compact_at * 2;
    }

    println!("🔥 Diamond Types Stress Test (Universe Rotation)");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("   Threads: {}  │  Universes: {}  │  Compact at: {} │  Max: {} ops",
             args.threads, args.universes, format_num(args.compact_at), format_num(args.max_universe_ops));
    println!("   Broadcast every: {} ops  │  Seed: {}", args.broadcast_every, args.seed);
    println!("   Target ops: {}  │  Duration: {}",
             if args.target_ops == 0 { "∞".to_string() } else { format_num(args.target_ops) },
             if args.duration == 0 { "∞".to_string() } else { format!("{}s", args.duration) });
    println!("   Op mix: map {}%, set {}%, reg {}%, crash {}%",
             op_mix.map_pct, 
             op_mix.set_pct - op_mix.map_pct, 
             op_mix.register_pct - op_mix.set_pct,
             100 - op_mix.register_pct);
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!();

    let stats = Arc::new(Stats::new());
    let start_time = Instant::now();
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Agent names shared by all
    let agent_names: Arc<Vec<String>> = Arc::new(
        (0..args.threads).map(|i| format!("peer_{}", i)).collect()
    );

    // Create universes
    let universes: Arc<Vec<Universe>> = Arc::new(
        (0..args.universes)
            .map(|_| Universe {
                state: AtomicU8::new(STATE_RUNNING),
                peers: (0..args.threads)
                    .map(|i| RwLock::new(create_peer_state(&agent_names, i)))
                    .collect(),
                op_count: AtomicU64::new(0),
            })
            .collect()
    );

    // Create bounded channels: each thread pair has one channel, messages tagged with universe
    // Bounded channels prevent memory blowup when receivers can't keep up
    let mut senders: Vec<Vec<SyncSender<OpsMessage>>> = vec![vec![]; args.threads];
    let mut receivers: Vec<Receiver<OpsMessage>> = Vec::with_capacity(args.threads);

    for i in 0..args.threads {
        let (tx, rx) = mpsc::sync_channel(args.channel_size);
        receivers.push(rx);
        for j in 0..args.threads {
            if i != j {
                if senders[j].len() <= i {
                    senders[j].resize_with(args.threads, || tx.clone());
                }
                senders[j][i] = tx.clone();
            }
        }
    }

    // Spawn stats printer thread - also handles duration stop
    let stats_clone = Arc::clone(&stats);
    let stop_flag_clone = Arc::clone(&stop_flag);
    let stats_every = args.stats_every;
    let duration_limit = args.duration;
    let target_ops = args.target_ops;
    let stats_thread = thread::spawn(move || {
        let mut last_print = Instant::now();
        while !stop_flag_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));

            // Check duration stop condition (reliable, runs every 100ms)
            let elapsed = start_time.elapsed();
            if duration_limit > 0 && elapsed.as_secs() >= duration_limit {
                stop_flag_clone.store(true, Ordering::SeqCst);
                break;
            }

            // Check target ops stop condition
            if target_ops > 0 && stats_clone.total_ops.load(Ordering::Relaxed) >= target_ops {
                stop_flag_clone.store(true, Ordering::SeqCst);
                break;
            }

            if last_print.elapsed().as_secs() >= stats_every {
                stats_clone.print(elapsed);
                last_print = Instant::now();
            }
        }
    });

    // Spawn one compactor thread per universe (parallel compaction)
    let compactor_handles: Vec<_> = (0..args.universes)
        .map(|universe_idx| {
            let universes_clone = Arc::clone(&universes);
            let stats_clone = Arc::clone(&stats);
            let stop_flag_clone = Arc::clone(&stop_flag);
            let agent_names_clone = Arc::clone(&agent_names);
            let compact_threshold = args.compact_at;

            thread::spawn(move || {
                while !stop_flag_clone.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(20));  // Check frequently

                    let universe = &universes_clone[universe_idx];

                    // Check if this universe needs compaction
                    if universe.state.load(Ordering::Relaxed) != STATE_RUNNING {
                        continue;
                    }
                    if universe.op_count.load(Ordering::Relaxed) < compact_threshold {
                        continue;
                    }

                    // Signal compaction - threads will skip this universe
                    universe.state.store(STATE_COMPACTING, Ordering::SeqCst);

                    // Brief sleep to let threads release locks
                    thread::sleep(Duration::from_millis(10));

                    // Acquire write locks on all peers
                    let mut locks: Vec<_> = universe.peers.iter()
                        .map(|p| p.write().unwrap())
                        .collect();

                    // Full mesh sync using split_at_mut
                    let n = locks.len();
                    for _ in 0..2 {
                        for i in 0..n {
                            for j in (i + 1)..n {
                                let (left, right) = locks.split_at_mut(j);
                                let peer_i = &mut left[i];
                                let peer_j = &mut right[0];
                                let ops_i: SerializedOpsOwned = peer_i.oplog.ops_since(&[]).into();
                                let ops_j: SerializedOpsOwned = peer_j.oplog.ops_since(&[]).into();
                                peer_j.oplog.merge_ops_owned(ops_i).ok();
                                peer_i.oplog.merge_ops_owned(ops_j).ok();
                            }
                        }
                    }

                    // Verify convergence
                    let first = locks[0].oplog.checkout();
                    let mut converged = true;
                    for lock in locks.iter().skip(1) {
                        if lock.oplog.checkout() != first {
                            converged = false;
                            break;
                        }
                    }

                    if converged {
                        stats_clone.convergence_checks.fetch_add(1, Ordering::Relaxed);
                    } else {
                        eprintln!("❌ CONVERGENCE FAILURE in universe {}!", universe_idx);
                    }

                    // Reset all peer states
                    for (i, lock) in locks.iter_mut().enumerate() {
                        **lock = create_peer_state(&agent_names_clone, i);
                    }

                    // Reset op count and resume
                    universe.op_count.store(0, Ordering::Relaxed);
                    universe.state.store(STATE_RUNNING, Ordering::SeqCst);

                    stats_clone.compactions.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Spawn worker threads
    let handles: Vec<_> = (0..args.threads)
        .map(|thread_id| {
            let args = args.clone();
            let op_mix = op_mix.clone();
            let stats = Arc::clone(&stats);
            let stop_flag = Arc::clone(&stop_flag);
            let universes = Arc::clone(&universes);
            let agent_names_clone = Arc::clone(&agent_names);
            let my_senders: Vec<SyncSender<OpsMessage>> = senders[thread_id].clone();
            let my_receiver = receivers.remove(0);

            thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(args.seed.wrapping_add(thread_id as u64));
                let mut universe_idx = thread_id % args.universes;  // Start on different universes
                let mut ops_since_broadcast = 0u64;
                let mut ops_since_recv = 0u64;

                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    // Find a running universe that's not at capacity (round-robin)
                    let mut attempts = 0;
                    loop {
                        // Check stop flag while searching
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }

                        let universe = &universes[universe_idx];
                        let state = universe.state.load(Ordering::Relaxed);
                        let op_count = universe.op_count.load(Ordering::Relaxed);

                        // Accept if running and under hard limit
                        if state == STATE_RUNNING && op_count < args.max_universe_ops {
                            break;
                        }

                        universe_idx = (universe_idx + 1) % args.universes;
                        attempts += 1;
                        if attempts >= args.universes * 2 {
                            // All universes full or compacting, back off
                            thread::sleep(Duration::from_millis(10));
                            attempts = 0;
                        }
                    }

                    // Re-check stop flag after exiting search loop
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    let universe = &universes[universe_idx];

                    // Try to get write lock, skip if would block (compactor might want it)
                    let lock_result = universe.peers[thread_id].try_write();
                    let mut peer = match lock_result {
                        Ok(p) => p,
                        Err(_) => {
                            universe_idx = (universe_idx + 1) % args.universes;
                            continue;
                        }
                    };

                    // Double-check state after acquiring lock
                    if universe.state.load(Ordering::Relaxed) != STATE_RUNNING {
                        drop(peer);
                        universe_idx = (universe_idx + 1) % args.universes;
                        continue;
                    }

                    // Do op
                    do_random_op(&mut peer, &mut rng, &op_mix, &agent_names_clone, thread_id, &stats);
                    stats.total_ops.fetch_add(1, Ordering::Relaxed);
                    universe.op_count.fetch_add(1, Ordering::Relaxed);
                    ops_since_broadcast += 1;
                    ops_since_recv += 1;

                    // Broadcast
                    if ops_since_broadcast >= args.broadcast_every {
                        let ops = peer.oplog.ops_since(peer.last_broadcast_version.as_ref());
                        let ops_owned: SerializedOpsOwned = ops.into();
                        peer.last_broadcast_version = peer.oplog.cg.version.clone();

                        for sender in &my_senders {
                            let _ = sender.send(OpsMessage {
                                universe_id: universe_idx,
                                ops: ops_owned.clone(),
                            });
                        }
                        stats.broadcasts.fetch_add(1, Ordering::Relaxed);
                        ops_since_broadcast = 0;
                    }

                    drop(peer);  // Release lock before receiving

                    // Receive and merge
                    if ops_since_recv >= args.recv_every {
                        while let Ok(msg) = my_receiver.try_recv() {
                            let target_universe = &universes[msg.universe_id];
                            if target_universe.state.load(Ordering::Relaxed) == STATE_RUNNING {
                                if let Ok(mut peer) = target_universe.peers[thread_id].try_write() {
                                    if peer.oplog.merge_ops_owned(msg.ops).is_ok() {
                                        stats.merges.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            // If universe is compacting or locked, message is dropped
                            // (compactor will sync everything anyway)
                        }
                        ops_since_recv = 0;
                    }

                    // Occasionally switch universes for better distribution
                    if rng.gen_bool(0.1) {
                        universe_idx = (universe_idx + 1) % args.universes;
                    }
                }
            })
        })
        .collect();

    // Wait for workers
    for handle in handles {
        handle.join().unwrap();
    }

    // Stop background threads
    stop_flag.store(true, Ordering::Relaxed);
    let _ = stats_thread.join();
    for handle in compactor_handles {
        let _ = handle.join();
    }

    println!("\n🔄 Final sync and verification across all universes...");

    // Final sync and verification for each universe
    let mut all_converged = true;
    for (universe_idx, universe) in universes.iter().enumerate() {
        let mut locks: Vec<_> = universe.peers.iter()
            .map(|p| p.write().unwrap())
            .collect();

        // Full mesh sync using split_at_mut
        let n = locks.len();
        for _ in 0..2 {
            for i in 0..n {
                for j in (i + 1)..n {
                    let (left, right) = locks.split_at_mut(j);
                    let peer_i = &mut left[i];
                    let peer_j = &mut right[0];
                    let ops_i: SerializedOpsOwned = peer_i.oplog.ops_since(&[]).into();
                    let ops_j: SerializedOpsOwned = peer_j.oplog.ops_since(&[]).into();
                    peer_j.oplog.merge_ops_owned(ops_i).ok();
                    peer_i.oplog.merge_ops_owned(ops_j).ok();
                }
            }
        }

        // Verify
        let first = locks[0].oplog.checkout();
        for (i, lock) in locks.iter().enumerate().skip(1) {
            if lock.oplog.checkout() != first {
                eprintln!("❌ Universe {} peer {} did not converge!", universe_idx, i);
                all_converged = false;
            }
        }
    }

    // Final stats
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════");
    if all_converged {
        println!("✅ STRESS TEST COMPLETE - All universes converged!");
    } else {
        println!("❌ STRESS TEST FAILED - Some universes did not converge!");
    }
    stats.print(start_time.elapsed());
    println!("═══════════════════════════════════════════════════════════════════════════════");
}
