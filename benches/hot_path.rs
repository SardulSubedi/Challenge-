//! Deterministic CPU benchmarks; no network or credentials. See docs/PERFORMANCE.md.
use chrono::{DateTime, Utc};
use engineering_challenge::{
    Side,
    book::{Book, fixed},
    engine::Engine,
    exchange::{Change, Exchange, Kalshi, MarketData, Payload},
    output::Record,
};
use serde_json::json;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeSet,
    hint::black_box,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};
struct Counting;
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if COUNT.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if COUNT.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
fn measure(name: &str, n: usize, mut f: impl FnMut(usize)) {
    for i in 0..n {
        f(i);
    }
    let mut samples = Vec::new();
    for _ in 0..9 {
        let start = Instant::now();
        for i in 0..n {
            f(i);
        }
        samples.push(start.elapsed().as_nanos() as f64 / n as f64);
    }
    ALLOCS.store(0, Relaxed);
    COUNT.store(true, Relaxed);
    for i in 0..n {
        f(i);
    }
    COUNT.store(false, Relaxed);
    samples.sort_by(f64::total_cmp);
    println!(
        "{}",
        json!({"case":name,"iterations":n,"median_ns":samples[4],"min_ns":samples[0],"max_ns":samples[8],"allocations_per_op":ALLOCS.load(Relaxed) as f64/n as f64})
    );
}
fn main() {
    if cfg!(debug_assertions) {
        return;
    }
    let adapter = Kalshi::new("http://localhost".into(), "ws://localhost".into(), None).unwrap();
    let now = Instant::now();
    let at = DateTime::<Utc>::UNIX_EPOCH;
    if std::env::var_os("PERF_MEMORY_ONLY").is_some() {
        let mut engine = Engine::new(Duration::from_secs(2)).with_timing(false);
        let symbols: BTreeSet<_> = (0..150_000)
            .map(|i| format!("BENCH-MARKET-{i:06}"))
            .collect();
        let start = Instant::now();
        engine.set_symbols(&symbols);
        for symbol in &symbols {
            for source in 0..2 {
                black_box(engine.ingest(
                    source,
                    MarketData {
                        symbol: symbol.clone(),
                        clock: None,
                        exchange_at: at,
                        change: Change::Snapshot(
                            Book::from_levels(vec![(400000, 1000000)], vec![]).unwrap(),
                        ),
                    },
                    at,
                    now,
                ));
            }
        }
        let micros = start.elapsed().as_micros();
        let status = std::fs::read_to_string("/proc/self/status").ok().map(|s| {
            s.lines()
                .filter(|s| s.starts_with("VmRSS:") || s.starts_with("VmHWM:"))
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });
        println!(
            "{}",
            json!({"case":"memory_150k_two_replicas_one_level","initialize_us":micros,"status":status,"ready":engine.coverage().ready})
        );
        black_box(engine);
        return;
    }
    measure("decimal", 100_000, |i| {
        black_box(
            fixed(black_box(
                ["0.1234", "-2468.01", "0.000001", "999999.999999"][i % 4],
            ))
            .unwrap(),
        );
    });
    measure("decode_delta", 100_000, |_| {
        black_box(
            adapter
                .decode(black_box(include_str!("../tests/fixtures/delta.json")), at)
                .unwrap(),
        );
    });
    let levels: Vec<_> = (1..=100)
        .map(|i| json!([format!("0.{i:04}"), "123.45"]))
        .collect();
    let snapshot = json!({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"BENCH","yes_dollars_fp":levels,"no_dollars_fp":levels}}).to_string();
    measure("decode_snapshot_200_levels", 2000, |_| {
        black_box(adapter.decode(black_box(&snapshot), at).unwrap());
    });
    let mut book =
        Book::from_levels((0..128).map(|i| (i * 100, 1_000_000)).collect(), vec![]).unwrap();
    measure("book_update_128_levels", 100_000, |i| {
        black_box(book.apply(Side::Buy, (i % 128) as i64 * 100, 1).unwrap());
    });
    let symbols: BTreeSet<_> = (0..150_000)
        .map(|i| format!("BENCH-MARKET-{i:06}"))
        .collect();
    let hot: Vec<_> = symbols.iter().step_by(73).take(2048).cloned().collect();
    let mut engine = Engine::new(Duration::from_secs(2)).with_timing(false);
    engine.set_symbols(&symbols);
    for symbol in &hot {
        for source in 0..2 {
            engine.ingest(
                source,
                MarketData {
                    symbol: symbol.clone(),
                    clock: None,
                    exchange_at: at,
                    change: Change::Snapshot(
                        Book::from_levels(vec![(400_000, 1_000_000)], vec![]).unwrap(),
                    ),
                },
                at,
                now,
            );
        }
    }
    let mut encoded = Vec::with_capacity(256);
    measure("engine_encode_150k_universe", 100_000, |i| {
        let result = engine.ingest(
            i % 2,
            MarketData {
                symbol: hot[(i / 2) % hot.len()].clone(),
                clock: None,
                exchange_at: at,
                change: Change::Delta {
                    side: Side::Buy,
                    price: 400_000,
                    delta: 1,
                },
            },
            at,
            now,
        );
        for record in result.records {
            record.encode_into(&mut encoded).unwrap();
            black_box(&encoded);
        }
    });
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    measure("output_handoff_20k_records", 10, |_| {
        rt.block_on(async {
            let sink = engineering_challenge::output::Sink::open(None, false, None).unwrap();
            let (tx,rx) = tokio::sync::mpsc::channel(16384);
            let writer = tokio::task::spawn_blocking(move || sink.run(rx));
            for _ in 0..20_000 {
                tx.send(engineering_challenge::output::Output::Record(Record::Delta(engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain {
                    backtest_timestamp:at, exchange_timestamp:at, price_level:400000, quantity:1000000, side:Side::Buy, symbol:"BENCH".into()
                }))).await.unwrap();
            }
            drop(tx); writer.await.unwrap().unwrap();
        });
    });
    if let Some(path) = std::env::args().skip(1).find(|arg| !arg.starts_with("--")) {
        let text = std::fs::read_to_string(path).unwrap();
        let frames: Vec<_> = text.lines().collect();
        measure("decode_real_wire", frames.len() * 30, |i| {
            black_box(
                adapter
                    .decode(black_box(frames[i % frames.len()]), at)
                    .unwrap(),
            );
        });
        // A wire-equivalence fingerprint includes every decoded book level and all SBE bytes.
        let mut e = Engine::new(Duration::from_secs(2)).with_timing(false);
        let symbols = frames
            .iter()
            .filter_map(|f| match adapter.decode(f, at).unwrap().payload {
                Payload::Market(m) => Some(m.symbol),
                _ => None,
            })
            .collect();
        e.set_symbols(&symbols);
        let mut hash = 0xcbf29ce484222325u64;
        let mut records = 0;
        let mut bytes = 0;
        for f in &frames {
            if let Payload::Market(m) = adapter.decode(f, at).unwrap().payload {
                let book = match &m.change {
                    Change::Snapshot(b) => Some(b),
                    _ => None,
                };
                if let Some(b) = book {
                    for (p, q) in b.bids.iter().chain(&b.asks) {
                        for v in [*p, *q] {
                            for byte in v.to_le_bytes() {
                                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                            }
                        }
                    }
                }
                for r in e.ingest(0, m, at, now).records {
                    let data = match r {
                        Record::Delta(_) | Record::Snapshot(_) => r.encode().unwrap(),
                    };
                    records += 1;
                    bytes += data.len();
                    for byte in data {
                        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                    }
                }
            }
        }
        println!(
            "{}",
            json!({"case":"wire_equivalence","frames":frames.len(),"records":records,"bytes":bytes,"fingerprint":format!("{hash:016x}")})
        );
    }
}
