//! Long-running stress test for Map, Text, Set, and Register CRDTs.
//!
//! Designed to run for hours/days, processing billions of operations
//! with bounded memory via universe rotation.
//!
//! # Architecture
//!
//! Multi-universe design for bounded memory:
//! - U universes, each with its own set of Documents (one per peer)
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
use rand::{RngExt, SeedableRng};

use diamond_types_extended::{Document, Frontier, PrimitiveValue, RemoteFrontierOwned, SerializedOpsOwned, Uuid};

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

    /// Operation mix: map,text,set,register,crash percentages (must sum to 100)
    #[arg(long, default_value = "30,25,25,15,5")]
    op_mix: String,

    /// Configuration preset (overrides other settings)
    /// Options: fast, chaos, endurance
    #[arg(long)]
    preset: Option<String>,
}

#[derive(Clone)]
struct OpMix {
    map_pct: u8,
    text_pct: u8,
    set_pct: u8,
    register_pct: u8,
    #[allow(dead_code)]
    crash_pct: u8,
}

impl OpMix {
    fn parse(s: &str) -> Self {
        let parts: Vec<u8> = s.split(',').map(|p| p.trim().parse().unwrap()).collect();
        assert_eq!(parts.len(), 5, "op-mix must be 5 comma-separated numbers (map,text,set,reg,crash)");
        assert_eq!(parts.iter().sum::<u8>(), 100, "op-mix must sum to 100");
        Self {
            map_pct: parts[0],
            text_pct: parts[0] + parts[1],
            set_pct: parts[0] + parts[1] + parts[2],
            register_pct: parts[0] + parts[1] + parts[2] + parts[3],
            crash_pct: 100,
        }
    }

    fn pick(&self, roll: u8) -> OpType {
        if roll < self.map_pct {
            OpType::Map
        } else if roll < self.text_pct {
            OpType::Text
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
    Text,
    Set,
    Register,
    Crash,
}

/// Per-peer state within a universe
struct PeerState {
    doc: Document,
    agent_idx: usize, // Index into global agent_names
    agent: u32,       // Local agent ID in the current document
    last_broadcast_remote: RemoteFrontierOwned,
    set_created: bool,
    text_created: bool,
}

/// A universe is a self-contained set of Documents that can be independently compacted
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
    text_ops: AtomicU64,
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
            text_ops: AtomicU64::new(0),
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
            "            │ map: {:>12} │ text: {:>11} │ set: {:>12} │ reg: {:>12} │ crash: {:>8}",
            format_num(self.map_ops.load(Ordering::Relaxed)),
            format_num(self.text_ops.load(Ordering::Relaxed)),
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
const TEXT_SNIPPETS: &[&str] = &["hello ", "world ", "foo ", "bar ", "baz ", "🔥 ", "改善 ", "テスト "];
// "Nasty" strings to catch edge cases
const NASTY_STRINGS: &[&str] = &[
    "",                         // Empty
    " ",                        // Space
    "\0",                       // Null
    "\n\r\t",                   // Control
    "embedded \" quote",        // Quote
    "💾",                       // Emoji (floppy disk)
    "你好",                     // Unicode (Chinese)
    "Ẓ̥̑̊̈̌́̀̋ͬ̊ͮ̍ͬ̊̆̕ͅ", // Zalgo
    "مرحبا",                    // RTL (Arabic)
];

fn create_peer_state(agent_uuids: &[Uuid], my_idx: usize) -> PeerState {
    let mut doc = Document::new();
    let mut my_agent = 0;
    // Pre-register all agents so IDs are stable across peers
    for (i, uuid) in agent_uuids.iter().enumerate() {
        let id = doc.create_agent(*uuid);
        if i == my_idx {
            my_agent = id;
        }
    }
    PeerState {
        doc,
        agent_idx: my_idx,
        agent: my_agent,
        last_broadcast_remote: Default::default(),
        set_created: false,
        text_created: false,
    }
}

fn do_random_op(
    peer: &mut PeerState,
    rng: &mut SmallRng,
    op_mix: &OpMix,
    agent_uuids: &[Uuid],
    thread_id: usize,
    stats: &Stats,
) {
    let roll: u8 = rng.random_range(0..100);
    match op_mix.pick(roll) {
        OpType::Map => {
            let key = if rng.random_bool(0.7) {
                MAP_KEYS[rng.random_range(0..MAP_KEYS.len())]
            } else {
                MAP_KEYS[thread_id % MAP_KEYS.len()]
            };
            let value = random_primitive(rng);
            let agent = peer.agent;
            peer.doc.transact(agent, |tx| {
                tx.root().set(key, value);
            });
            stats.map_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Text => {
            if !peer.text_created {
                let agent = peer.agent;
                peer.doc.transact(agent, |tx| {
                    tx.root().create_text("content");
                });
                peer.text_created = true;
            }

            // Read current text length before transacting
            let text_len = peer.doc.root()
                .get_text("content")
                .map(|t| t.len())
                .unwrap_or(0);

            let agent = peer.agent;
            if text_len > 10 && rng.random_bool(0.3) {
                // Delete a random range
                let start = rng.random_range(0..text_len);
                let max_del = (text_len - start).min(10);
                let end = start + rng.random_range(1..=max_del);
                peer.doc.transact(agent, |tx| {
                    if let Some(mut text) = tx.get_text_mut(&["content"]) {
                        text.delete(start..end);
                    }
                });
            } else {
                // Insert at a random position
                let pos = if text_len == 0 { 0 } else { rng.random_range(0..=text_len) };
                let snippet = TEXT_SNIPPETS[rng.random_range(0..TEXT_SNIPPETS.len())];
                peer.doc.transact(agent, |tx| {
                    if let Some(mut text) = tx.get_text_mut(&["content"]) {
                        text.insert(pos, snippet);
                    }
                });
            }
            stats.text_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Set => {
            if !peer.set_created {
                let agent = peer.agent;
                peer.doc.transact(agent, |tx| {
                    tx.root().create_set("tags");
                });
                peer.set_created = true;
            }

            let agent = peer.agent;
            if rng.random_bool(0.7) {
                let val: PrimitiveValue = if rng.random_bool(0.6) {
                    SET_VALUES[rng.random_range(0..SET_VALUES.len())].into()
                } else {
                    (rng.random_range(0i64..1000)).into()
                };
                peer.doc.transact(agent, |tx| {
                    if let Some(mut set) = tx.get_set_mut(&["tags"]) {
                        set.add(val);
                    }
                });
            } else {
                let val: String = SET_VALUES[rng.random_range(0..SET_VALUES.len())].to_string();
                peer.doc.transact(agent, |tx| {
                    if let Some(mut set) = tx.get_set_mut(&["tags"]) {
                        set.remove(val.as_str());
                    }
                });
            }
            stats.set_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Register => {
            let key = if rng.random_bool(0.5) {
                "register"
            } else {
                MAP_KEYS[thread_id % MAP_KEYS.len()]
            };
            let value = random_primitive(rng);
            let agent = peer.agent;
            peer.doc.transact(agent, |tx| {
                tx.root().set(key, value);
            });
            stats.register_ops.fetch_add(1, Ordering::Relaxed);
        }
        OpType::Crash => {
            // Simulate crash/restart:
            // 1. Serialize full state
            // 2. Create fresh Document
            // 3. Merge serialized state
            // 4. Re-acquire agent ID
            let full_ops: SerializedOpsOwned = peer.doc.ops_since(&Frontier::root()).into();

            let mut new_doc = Document::new();
            let mut my_agent = 0;
            for (i, uuid) in agent_uuids.iter().enumerate() {
                let id = new_doc.create_agent(*uuid);
                if i == peer.agent_idx {
                    my_agent = id;
                }
            }

            // Restore state
            new_doc.merge_ops(full_ops).expect("Crash recovery failed: unable to merge own ops");

            peer.doc = new_doc;
            peer.agent = my_agent;
            // Reset broadcast cursor to current version
            peer.last_broadcast_remote = peer.doc.remote_version();

            stats.crashes.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn random_primitive(rng: &mut SmallRng) -> PrimitiveValue {
    match rng.random_range(0..5) {
        0 => PrimitiveValue::Nil,
        1 => PrimitiveValue::Bool(rng.random()),
        2 => PrimitiveValue::Int(rng.random()),
        3 => PrimitiveValue::Str(format!("v{}", rng.random::<u32>())),
        _ => {
            let s = NASTY_STRINGS[rng.random_range(0..NASTY_STRINGS.len())];
            PrimitiveValue::Str(s.to_string())
        }
    }
}

fn main() {
    let mut args = Args::parse();

    // Apply presets
    if let Some(preset) = &args.preset {
        match preset.as_str() {
            "fast" => {
                args.compact_at = 10_000;
                args.max_universe_ops = 20_000;
                args.broadcast_every = 10;
                args.recv_every = 5;
                args.stats_every = 1;
                args.op_mix = "25,25,25,25,0".to_string();
            },
            "chaos" => {
                args.broadcast_every = 1;
                args.recv_every = 1;
                args.op_mix = "25,20,25,15,15".to_string();
            },
            "endurance" => {
                args.compact_at = 500_000;
                args.max_universe_ops = 1_000_000;
                args.broadcast_every = 1000;
                args.recv_every = 500;
                args.op_mix = "25,25,25,24,1".to_string();
            },
            _ => panic!("Unknown preset: {}. Valid: fast, chaos, endurance", preset),
        }
    }

    let op_mix = OpMix::parse(&args.op_mix);

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
    println!("   Op mix: map {}%, text {}%, set {}%, reg {}%, crash {}%",
             op_mix.map_pct,
             op_mix.text_pct - op_mix.map_pct,
             op_mix.set_pct - op_mix.text_pct,
             op_mix.register_pct - op_mix.set_pct,
             100 - op_mix.register_pct);
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!();

    let stats = Arc::new(Stats::new());
    let start_time = Instant::now();
    let stop_flag = Arc::new(AtomicBool::new(false));

    let agent_uuids: Arc<Vec<Uuid>> = Arc::new(
        (0..args.threads).map(|i| Uuid::from_u128(0xBE2C8 + i as u128)).collect()
    );

    // Create universes
    let universes: Arc<Vec<Universe>> = Arc::new(
        (0..args.universes)
            .map(|_| Universe {
                state: AtomicU8::new(STATE_RUNNING),
                peers: (0..args.threads)
                    .map(|i| RwLock::new(create_peer_state(&agent_uuids, i)))
                    .collect(),
                op_count: AtomicU64::new(0),
            })
            .collect()
    );

    // Create bounded channels: one (tx, rx) per thread, then each thread
    // gets senders to every *other* thread's rx.
    let mut all_senders: Vec<Vec<SyncSender<OpsMessage>>> = Vec::with_capacity(args.threads);
    let mut receivers: Vec<Receiver<OpsMessage>> = Vec::with_capacity(args.threads);
    let mut channel_txs: Vec<SyncSender<OpsMessage>> = Vec::with_capacity(args.threads);

    for _ in 0..args.threads {
        let (tx, rx) = mpsc::sync_channel(args.channel_size);
        channel_txs.push(tx);
        receivers.push(rx);
    }

    for i in 0..args.threads {
        let mut my_senders = Vec::with_capacity(args.threads - 1);
        for (j, tx) in channel_txs.iter().enumerate() {
            if i != j {
                my_senders.push(tx.clone());
            }
        }
        all_senders.push(my_senders);
    }
    drop(channel_txs);

    // Stats printer thread (also handles duration/target stop)
    let stats_clone = Arc::clone(&stats);
    let stop_flag_clone = Arc::clone(&stop_flag);
    let stats_every = args.stats_every;
    let duration_limit = args.duration;
    let target_ops = args.target_ops;
    let stats_thread = thread::spawn(move || {
        let mut last_print = Instant::now();
        while !stop_flag_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));

            let elapsed = start_time.elapsed();
            if duration_limit > 0 && elapsed.as_secs() >= duration_limit {
                stop_flag_clone.store(true, Ordering::SeqCst);
                break;
            }
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

    // Compactor threads (one per universe)
    let compactor_handles: Vec<_> = (0..args.universes)
        .map(|universe_idx| {
            let universes_clone = Arc::clone(&universes);
            let stats_clone = Arc::clone(&stats);
            let stop_flag_clone = Arc::clone(&stop_flag);
            let agent_uuids_clone = Arc::clone(&agent_uuids);
            let compact_threshold = args.compact_at;

            thread::spawn(move || {
                while !stop_flag_clone.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(20));

                    let universe = &universes_clone[universe_idx];

                    if universe.state.load(Ordering::Relaxed) != STATE_RUNNING {
                        continue;
                    }
                    if universe.op_count.load(Ordering::Relaxed) < compact_threshold {
                        continue;
                    }

                    // Signal compaction
                    universe.state.store(STATE_COMPACTING, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(10));

                    // Acquire write locks
                    let mut locks: Vec<_> = universe.peers.iter()
                        .map(|p| p.write().unwrap())
                        .collect();

                    // Full mesh sync — use full state exchange for reliable convergence
                    let n = locks.len();
                    for _ in 0..2 {
                        for i in 0..n {
                            for j in (i + 1)..n {
                                let (left, right) = locks.split_at_mut(j);
                                let peer_i = &mut left[i];
                                let peer_j = &mut right[0];

                                let ops_i: SerializedOpsOwned = peer_i.doc.ops_since(&Frontier::root()).into();
                                let ops_j: SerializedOpsOwned = peer_j.doc.ops_since(&Frontier::root()).into();

                                peer_j.doc.merge_ops(ops_i).ok();
                                peer_i.doc.merge_ops(ops_j).ok();
                            }
                        }
                    }

                    // Verify convergence
                    let first = locks[0].doc.checkout();
                    let mut converged = true;
                    for (i, lock) in locks.iter().enumerate().skip(1) {
                        let checkout = lock.doc.checkout();
                        if checkout != first {
                            eprintln!("❌ CONVERGENCE FAILURE in universe {}!", universe_idx);
                            eprintln!("  Peer 0: {:?}", first);
                            eprintln!("  Peer {}: {:?}", i, checkout);
                            converged = false;
                            break;
                        }
                    }

                    if converged {
                        stats_clone.convergence_checks.fetch_add(1, Ordering::Relaxed);
                    }

                    // Reset all peer states
                    for (i, lock) in locks.iter_mut().enumerate() {
                        **lock = create_peer_state(&agent_uuids_clone, i);
                    }

                    universe.op_count.store(0, Ordering::Relaxed);
                    universe.state.store(STATE_RUNNING, Ordering::SeqCst);
                    stats_clone.compactions.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Worker threads
    let handles: Vec<_> = (0..args.threads)
        .map(|thread_id| {
            let args = args.clone();
            let op_mix = op_mix.clone();
            let stats = Arc::clone(&stats);
            let stop_flag = Arc::clone(&stop_flag);
            let universes = Arc::clone(&universes);
            let agent_uuids_clone = Arc::clone(&agent_uuids);
            let my_senders: Vec<SyncSender<OpsMessage>> = all_senders[thread_id].clone();
            let my_receiver = receivers.remove(0);

            thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(args.seed.wrapping_add(thread_id as u64));
                let mut universe_idx = thread_id % args.universes;
                let mut ops_since_broadcast = 0u64;
                let mut ops_since_recv = 0u64;

                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    // Find a running universe under capacity
                    let mut attempts = 0;
                    loop {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }

                        let universe = &universes[universe_idx];
                        let state = universe.state.load(Ordering::Relaxed);
                        let op_count = universe.op_count.load(Ordering::Relaxed);

                        if state == STATE_RUNNING && op_count < args.max_universe_ops {
                            break;
                        }

                        universe_idx = (universe_idx + 1) % args.universes;
                        attempts += 1;
                        if attempts >= args.universes * 2 {
                            thread::sleep(Duration::from_millis(10));
                            attempts = 0;
                        }
                    }

                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    let universe = &universes[universe_idx];

                    let lock_result = universe.peers[thread_id].try_write();
                    let mut peer = match lock_result {
                        Ok(p) => p,
                        Err(_) => {
                            universe_idx = (universe_idx + 1) % args.universes;
                            continue;
                        }
                    };

                    if universe.state.load(Ordering::Relaxed) != STATE_RUNNING {
                        drop(peer);
                        universe_idx = (universe_idx + 1) % args.universes;
                        continue;
                    }

                    // Do op
                    do_random_op(&mut peer, &mut rng, &op_mix, &agent_uuids_clone, thread_id, &stats);
                    stats.total_ops.fetch_add(1, Ordering::Relaxed);
                    universe.op_count.fetch_add(1, Ordering::Relaxed);
                    ops_since_broadcast += 1;
                    ops_since_recv += 1;

                    // Broadcast using remote frontier
                    if ops_since_broadcast >= args.broadcast_every {
                        let ops_owned: SerializedOpsOwned = peer.doc.ops_since_remote(&peer.last_broadcast_remote).into();
                        peer.last_broadcast_remote = peer.doc.remote_version();

                        for sender in &my_senders {
                            let _ = sender.send(OpsMessage {
                                universe_id: universe_idx,
                                ops: ops_owned.clone(),
                            });
                        }
                        stats.broadcasts.fetch_add(1, Ordering::Relaxed);
                        ops_since_broadcast = 0;
                    }

                    drop(peer);

                    // Receive and merge
                    if ops_since_recv >= args.recv_every {
                        while let Ok(msg) = my_receiver.try_recv() {
                            let target_universe = &universes[msg.universe_id];
                            if target_universe.state.load(Ordering::Relaxed) == STATE_RUNNING
                                && let Ok(mut peer) = target_universe.peers[thread_id].try_write()
                                && peer.doc.merge_ops(msg.ops).is_ok()
                            {
                                stats.merges.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        ops_since_recv = 0;
                    }

                    if rng.random_bool(0.1) {
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

        // Full-state pairwise sync: 2 rounds should suffice
        let n = locks.len();
        for _ in 0..2 {
            for i in 0..n {
                for j in (i + 1)..n {
                    let (left, right) = locks.split_at_mut(j);
                    let peer_i = &mut left[i];
                    let peer_j = &mut right[0];

                    let ops_i: SerializedOpsOwned = peer_i.doc.ops_since(&Frontier::root()).into();
                    let ops_j: SerializedOpsOwned = peer_j.doc.ops_since(&Frontier::root()).into();

                    if let Err(e) = peer_j.doc.merge_ops(ops_i) {
                        eprintln!("❌ Final sync: merge i→j failed (universe {universe_idx}, {i}→{j}): {e:?}");
                    }
                    if let Err(e) = peer_i.doc.merge_ops(ops_j) {
                        eprintln!("❌ Final sync: merge j→i failed (universe {universe_idx}, {j}→{i}): {e:?}");
                    }
                }
            }
        }

        // CRDTs converge on content, not internal state — only compare checkout()
        let first = locks[0].doc.checkout();
        for (i, lock) in locks.iter().enumerate().skip(1) {
            let checkout = lock.doc.checkout();
            if checkout != first {
                eprintln!("❌ Universe {} peer {} did not converge!", universe_idx, i);
                eprintln!("  Peer 0: {:?}", first);
                eprintln!("  Peer {}: {:?}", i, checkout);
                all_converged = false;
            }
        }
    }

    // Final stats
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════");
    if all_converged {
        println!("✅ STRESS TEST COMPLETE — All universes converged!");
    } else {
        println!("❌ STRESS TEST FAILED — Some universes did not converge!");
    }
    stats.print(start_time.elapsed());
    println!("═══════════════════════════════════════════════════════════════════════════════");
}
