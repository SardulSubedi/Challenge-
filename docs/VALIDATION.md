# Validation

Validated locally on October 26, 2026 using stable Rust 1.99.1 in Ubuntu-24.04/WSL on the developer's Windows machine. Tests use a local exchange simulator and do not require live credentials.

## Automated checks

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all-targets --locked`: 30 tests (25 unit tests, five integration tests).
- `cargo build --release --locked --bins`

Coverage includes exact six-place arithmetic, quantity underflow/overflow handling, both supported signing algorithms, YES/NO normalization, per-subscription continuity, redundant-book deduplication, snapshot-first failover, repeated exchange clocks, clock resets, clockless timing, quiet-market duplicate grace, stable bounded sharding, global cooldown after throttling, connection-budget rejection, REST pagination/MVE exclusion, dynamic add/remove acknowledgements, reconnect after injected sequence loss, aligned SBE round trips, overwrite protection, and Unix SIGINT with byte-for-byte stdout/archive comparison.

GitHub Actions also checks Linux and Windows. Local WSL results do not substitute for a completed Windows CI run. Container and systemd examples are supplied but have not been deployed.

## Production-feed smoke tests

Authentication succeeded with a user-supplied local key. Its contents were not logged, copied into repository files, or committed.

A 140-second run used two active 15-minute markets, one market per shard, and two replicas. Shortened 60/90-second socket lifetimes exercised six source switches. The archive validated using the template's reader: **144,715 deltas, 12 snapshots, 201,326,592 bytes**. Two books remained available at health checkpoints. This demonstrates real reconnection and SBE output; it does not prove zero historical loss.

A separate 45-second timing run validated **4,256 deltas, six snapshots, 67,108,864 bytes**. It produced **8,210 observations, 3,956 timestamp-fingerprint matches, 298 unmatched keys**. See `LATENCY.md` for per-source percentiles and interpretation limits. The contracts closed during the sample.

The production frames observed in these tests omitted the documented exchange clock. Timestamp-fingerprint matching is labeled as a heuristic and is not used as a guaranteed exchange sequence. Strict cross-source catch-up is enforced only when a comparable exchange clock is actually supplied.

## Full-universe validation

Initial scans discovered roughly 149,000 regular markets. The first configuration attempted 300 sockets and hit HTTP 429. Defaults were revised to 2,000 markets per shard, two replicas, a 160-socket budget, and two starts per second, with a shared retry cooldown. A second run also identified excessive switches after quiet periods, which were fixed with a grace period and a regression test. These preliminary runs are not claimed as complete coverage.

A three-minute collection window after discovery used 150 sockets for 148,396 markets. Reading the complete archive validated **874,722 deltas, 711,788 snapshots, 148,396 distinct initialized symbols, and 469,762,048 bytes**. Every discovered symbol appeared with an initial snapshot. This does not mean every market was continuously available: instantaneous readiness varied, and 18 connection resets plus 20 command-acknowledgement timeouts occurred during the run. No HTTP 429 occurred with this configuration. The snapshot count includes scheduled/reset snapshots and source changes; the final quiet-snapshot regression fix suppresses switches between identical clockless books, so that count is not an expected steady-state rate.

The largest reported engine-queue delay during full-load health windows was about four seconds on this local machine. Completeness/recovery behavior was exercised; this is not a low-latency production certification. Deployment hardware, subscription distribution, output storage, and venue limits need tuning for sustained operation.

A separate 75-second quiet-market run (one replica, a shortened quiet threshold) performed a real unsubscribe/resubscribe cycle and received a new snapshot. Its archive contains three snapshots and no deltas, confirming that quiet-market repair works against the live server, not only the simulator.

All raw captures and diagnostics remain local under ignored `data/` and `work/` directories.

## Limits

No exchange-maintenance timing or source-port experiment was performed. Redundant sockets cannot fill an outage during which every collector process is stopped. The template archive writer buffers a chunk; abrupt termination can lose unflushed data, and its padding affects disk use. Simulated failures and finite live runs cannot prove production completeness. These limits are described in `DESIGN.md` and `LATENCY.md`.
