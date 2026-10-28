# Kalshi orderbook collector

Rust implementation of the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge), built on the [required template](https://github.com/voluntas-io/engineering-challenge-template). The collector discovers every live regular Kalshi market, maintains redundant orderbooks, and emits standardized SBE snapshots and price-level updates.

Measured optimizations reduce captured-frame decoding CPU time by about **62%** and initialized shallow-book test memory by **46%**. A full live run covered **151,653 currently discovered markets on both replicas**; see the [audit, methodology and limits](docs/PERFORMANCE_AUDIT.md).

## Run

Use a current stable Rust toolchain. Copy `.env.example` to `.env`, supply your Kalshi API key ID and the path to its private-key PEM. RSA-PSS and Ed25519 keys are supported; use `--demo` only for a demo key. Keep keys outside Git. `.env`, `.secrets/`, key files, and captured data are ignored.

```sh
cargo build --release --locked
# Default: ALL live regular markets, two replicas per shard, binary SBE stdout.
cargo run --release --bin collector --
# Optional aligned archive; stdout still carries the same messages without padding.
cargo run --release --bin collector -- --output data/session-01.sbe
# For an interactive terminal, explicitly suppress binary stdout:
cargo run --release --bin collector -- --output data/session-02.sbe --no-stdout
```

Do not open the default output in a text terminal or send it through a text-transforming pipeline. Logs go to stderr. Archive names must be new: existing files are never overwritten. Stop with Ctrl+C (SIGINT); SIGTERM is also handled on Unix. Sockets close, queued observations drain, and the archive's final chunk flushes before exit.

On this Windows machine, the working checkout and project-local Rust toolchain are in Ubuntu-24.04 at `/home/sardul/projects/Challenge`. Build artifacts, dependencies, and live recordings also use the Linux filesystem. Open the source from Windows at `\\wsl.localhost\Ubuntu-24.04\home\sardul\projects\Challenge`. The previous Windows folder is a backup; its launchers forward to the Linux checkout through the local, ignored `work/native-wsl-root.txt` setting. Edit the Linux checkout to avoid diverging copies.

From either the Linux checkout's Windows network path or the previous Windows folder:

```powershell
.\scripts\cargo-wsl.ps1 test --all-targets --locked
.\scripts\run-wsl.ps1
```

Run the launcher from this repository directory. It records continuously on native Linux storage and prints unique archive and coverage-report paths. See [native storage and measured latency](docs/NATIVE_LATENCY.md).

Windows PowerShell consumes a bare `--` when calling scripts. The wrapper restores it before collector arguments in `cargo run`. To preserve the boundary explicitly, quote it: `.\scripts\cargo-wsl.ps1 run --release --bin collector '--' --help`. This also distinguishes collector help from Cargo help.

When using WSL, private-key paths must use Linux syntax, for example `/mnt/c/Users/you/Documents/your-key.txt`. Native Windows builds accept normal Windows paths. The template reader has a small portability adaptation for Windows positional file reads.

## Behavior

- Paginate `/markets?status=open&mve_filter=exclude`; defensively filter MVE metadata too. Use redundant lifecycle subscriptions for targeted live-market lookups, and reconcile the complete universe every 60 seconds. Failed scans retain the previous universe rather than unsubscribing from a partial response.
- Keep existing symbol assignments stable and cap each shard at 2,000 markets by default. Each shard has two independent WebSockets and private books. New markets fill available slots; closed markets are removed. Connection starts are paced at two per second, with a default 160-socket budget and a shared cooldown after HTTP 429. Exceeding the budget fails explicitly with configuration guidance; no markets are silently skipped.
- At startup, open one connection per shard before its backups, so market coverage grows faster at the same safe connection rate. This prioritizes broad initial coverage; each shard initially has one copy until its backup starts. Discovery uses gzip responses and slows future REST page requests after throttling. A complete paginated discovery scan still precedes subscriptions.
- Print coverage percentage, unique output updates per second, cumulative record counts, source switches, and engine-queue delay every five seconds. `ready` counts markets with an initialized selected book, not two verified replicas. Startup logs estimate the paced first-copy and total connection ramps; network handshakes and snapshots can take longer. Duplicate backup updates maintain private books and intentionally do not double the output count.
- Validate per-subscription `sid`/`seq` across all symbols and sequenced command acknowledgements. A gap, regression, malformed market message, lost ACK, heartbeat failure, or disconnect invalidates that connection's books and triggers a jittered reconnect with new snapshots.
- Apply deltas as signed changes internally and output **absolute** quantities. Output zero when a level is deleted. Financial prices and quantities use exact integer millionths, without floating-point rounding. YES bids become bids; NO bids become YES asks at `1 - NO price`.
- Emit an initial snapshot, a snapshot every minute for each available book, and a snapshot at every source switch or same-source reset. Only one replica emits deltas per symbol. When the feed supplies exchange clocks, a failover candidate must have reached the last emitted clock position; otherwise output pauses until it catches up. Clockless feeds reset with a snapshot selected from an independently maintained replica; cross-connection catch-up cannot be proven in that case.
- Allow a primary to catch up for a configurable grace period before treating a backup update as evidence of a stall. This prevents ordinary duplicates after a quiet period from causing source churn. Captured event queues drain before stall decisions.
- Track multiple deltas at the same exchange clock using an ordinal. Clocks can skip values; only subscription sequences require contiguity. A clock regression causes all replicas of that market to be rebuilt.
- Monitor symbols for long silence and a fall below 10% of their historical update rate. A symbol with at least ten updates/minute of baseline activity is eligible for the rate check. Repairs unsubscribe and resubscribe with an ACK between steps, conservatively and with a cooldown. Planned symbol refreshes and socket recycling obtain permission from the book engine; with multiple replicas, they are deferred unless another caught-up book remains. Quiet-market timers are staggered across replicas.
- Send WebSocket pings, monitor transport activity, and recycle complete connections on staggered schedules (30 minutes for the first replica by default). Independent books and source-switch snapshots preserve a usable stream during recovery.
- Bound market-data and output queues. Health counters update incrementally rather than scanning the full universe. The coverage report names every missing market and missing backup; completion fails if either remains or discovery is stale. Backpressure is visible through queue-depth/processing-delay logs; sustained ingestion congestion forces a resnapshot rather than silently dropping an event.

`collector --help` documents every setting. Useful overrides:

```sh
# Discover available tickers without credentials.
cargo run --bin collector -- --list-markets
# Small, timed live test. Replace EXAMPLE with an actual ticker from discovery.
cargo run --release --bin collector -- --symbols EXAMPLE --run-secs 90 \
  --output data/smoke-01.sbe --no-stdout --latency-log data/timing-01.jsonl
# Exercise socket recycling during a longer latency comparison.
cargo run --release --bin collector -- --symbols EXAMPLE --run-secs 140 \
  --recycle-secs 60 --latency-log data/timing-02.jsonl \
  --output data/recycle-01.sbe --no-stdout
```

The run timer starts after initial discovery. The allow-list uses the REST ticker filter for fast smoke tests and is for testing only; omit `--symbols` for the complete challenge. Lifecycle notifications trigger coalesced targeted REST checks on two existing sockets. Missed notifications recover through periodic full scans, which take the discovery interval plus scan/network time. Live exchange access is needed for authentication and real data; automated tests use a local mock server.

## Output and timestamps

The upstream YAML schema and generated DTOs are retained. `SnapshotDomain` and `PriceLevelUpdateDomain` are encoded with the template's `SbeEncode` helpers. Optional archives use its `AlignedSbeWriter` with 64 MiB chunks, readable by `AlignedSbeReader<OrderbookSchema>`. Stdout concatenates complete SBE messages without file padding. Prices and quantities are scaled by **1,000,000**; `Side::Buy` is bid, `Side::Sell` is ask.

Receive time is captured immediately after reading a WebSocket frame, before JSON decoding or queueing. Delta `backtest_timestamp` is that local receive time; exchange time comes from the feed. Snapshot `received_at` is receive time for an incoming snapshot, the active worker's serialized boundary time for a scheduled snapshot, or local emission time for a failover snapshot. Scheduled boundaries follow that worker's already captured frames and precede its next captures, preserving their timestamp order under queue pressure. Such a snapshot's exchange timestamp is its last observed exchange update time. Missing exchange timestamps use Unix epoch (zero), never a fabricated local exchange time. Keep the host clock synchronized.

Archives queue bounded background writes at minute checkpoints and wait for completion at shutdown; slow writes can still cause backpressure. The required writer pads each partial chunk to 64 MiB, so a quiet archived run can use roughly 64 MiB per minute. A crash can lose the current buffered chunk; a process supervisor cannot recover historical updates the exchange does not replay. This is a collection challenge, not a durable production feed recorder. See [design and limitations](docs/DESIGN.md).

```sh
cargo run --bin inspect-sbe -- data/smoke-01.sbe
cargo run --bin latency-report -- data/timing-01.jsonl
```

The latency report prefers `(market, exchange clock, ordinal, side, price, delta)` matches. If clocks are absent, it uses clearly labeled timestamp fingerprints with weaker guarantees, excluding partial initial timestamp buckets. It reports first-arrival counts and relative delay percentiles in microseconds. It does not mistake independent subscription sequence numbers or exchange-send timestamps for comparable end-to-end latency. See [latency experiments](docs/LATENCY.md) and [validation results](docs/VALIDATION.md).

## Tests and deployment

See [final implementation, live results, and unresolved evidence](docs/FINAL_VALIDATION.md) for the latest validation.

For measured startup costs, the subscription-processing optimization, full-universe throughput, and remaining checkpoint pauses, see [performance validation](docs/PERFORMANCE.md). The latest [performance audit](docs/PERFORMANCE_AUDIT.md) includes repeated CPU/allocation comparisons, full-pipeline load tests, and full-universe memory measurements.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

The 69 automated tests (67 on Windows) cover connection-budget enforcement, shared rate-limit cooldowns, quiet-period duplicate handling, clockless timing, exact decimals, both signing algorithms, orderbook mutations, sequence gaps, redundancy, clock maintenance resets including reconnects, source-switch snapshots, discovery pagination, dynamic subscriptions, binary archive round trips, and Unix SIGINT with byte-for-byte stdout/archive comparison. Additional tests verify continuously delayed and silent primary failover, malformed snapshot rejection, scheduled snapshot ordering under backpressure, a different exchange wire protocol through the same collector, actual TCP source-port binding, maintenance transition detection, and independent reconstruction that detects deliberately incorrect exchange checkpoints. GitHub Actions runs formatting, Clippy, tests, and release builds on Linux and Windows (the Unix signal test is Unix-only).

## Independent live checks and optional experiments

`feed-lab` uses the same credentials as the collector and never places orders. Reports and archive names must be new. Substitute current live tickers:

```sh
# Compare every production-engine update with an independent signed-delta
# reconstruction, compare fresh exchange snapshots, then replay actual SBE output.
cargo run --release --bin feed-lab -- verify --symbols LIVE_BTC,LIVE_ETH \
  --seconds 90 --snapshot-every 5 --archive data/verified-01.sbe \
  --report data/verified-01.json
# Rotate each source through every start position, recording actual endpoints.
cargo run --release --bin feed-lab -- ports --symbol LIVE_BTC \
  --ports 41001,41011,41021 --rounds 4 --seconds 20 --report data/ports-01.json
# Start BEFORE an actual maintenance closure, allowing enough time for reopening.
cargo run --release --bin feed-lab -- maintenance --symbol LIVE_BTC \
  --wait-seconds 10800 --poll-ms 1000 --delays-ms 0,2000,5000 \
  --seconds 30 --report data/reopen-01.json
```

The verifier exits unsuccessfully on engine disagreement, sequence loss, reconstruction failure, snapshot mismatch, insufficient activity, or SBE replay disagreement. The maintenance experiment requires an explicit closed-to-open exchange-status transition; starting while already open or seeing HTTP errors cannot fabricate a reopening. A wait timeout produces an explicitly inconclusive report. Source-port experiments use at most eight paced connections, stop additional rounds after a failed sample, and report relative arrival times without asserting a causal port advantage. NAT can rewrite the bound local port. See [results and experiment limits](docs/LATENCY.md).

Verification archives now have an automatically saved `.wire.jsonl` sidecar containing the original public messages before decoding. Use `python scripts/audit-wire.py data/verified-01.wire.jsonl` for an independent Decimal reconstruction, or `feed-lab audit-archive --archive data/verified-01.sbe` to recover exact snapshot differences from a single-source verification archive. The [snapshot consistency investigation](docs/SNAPSHOT_CONSISTENCY.md) includes a small real exchange fixture that reproduces a disagreement without credentials. Its audit is expected to fail; the five Python regression tests assert that inconsistent data remains detectable. CI runs them with `python -m unittest discover -s tests -p test_audit_wire.py`.

`Dockerfile` provides a non-root Linux image. Mount the key read-only, pass its container path through `KALSHI_PRIVATE_KEY_PATH`, and configure the matching key ID. `docs/collector.service` is an example systemd supervisor; adjust its install paths and user before deploying. No orders or account mutations are performed by the collector.

## Template provenance

The original serialization helpers, schema, and build-time code generation are from `voluntas-io/engineering-challenge-template`, commit `97360aefacaa455289f68cbec51b9965f8b7e949`. The original example `main.rs` has been replaced by the collector. Generated SBE code is produced at build time using the pinned `ergo-sbe` dependency.
