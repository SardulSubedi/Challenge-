# Validation

Validated locally on October 26–27, 2026 using stable Rust 1.99.1 in Ubuntu-24.04/WSL on the developer's Windows machine. Automated tests use local servers and do not require live credentials.

For the latest implementation and live evidence, including retained failed checks, see [final validation](FINAL_VALIDATION.md).

## Automated checks

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all-targets --locked`: 57 tests (38 unit tests, eighteen network integration tests, and one generic-exchange integration test; Windows excludes the Unix signal and Linux device-error tests).
- `cargo build --release --locked --bins`

Coverage includes exact six-place arithmetic, quantity underflow/overflow handling, both supported signing algorithms, YES/NO normalization, per-subscription continuity, redundant-book deduplication, snapshot-first failover, repeated exchange clocks, clock resets, clockless timing, quiet-market duplicate grace, stable bounded sharding, global cooldown after throttling, connection-budget rejection, REST pagination/MVE exclusion, dynamic add/remove acknowledgements, reconnect after injected sequence loss, aligned SBE round trips, overwrite protection, and Unix SIGINT with byte-for-byte stdout/archive comparison.

Four later regression tests cover gzip discovery, repair acknowledgements followed by resumed streaming, recovery from REST throttling without losing a page, and initial coverage of every shard before backup connections. The six-socket startup test allows 15 seconds for shutdown to accommodate shared-host/disk contention; its ordering and command assertions remain unchanged.

The expanded suite also tests a primary that continues answering heartbeats but stops market updates: the full collector switches to its advancing backup and archives the newer state. A second adapter uses a different text protocol through the same runtime and SBE sink, proving the exchange boundary rather than only compiling a trait. Independent reference arithmetic detects intentionally wrong checkpoint snapshots, and verifies production-engine records and archive replay. Socket tests confirm the requested source port is visible at the peer. Maintenance tests require closed-to-open status and reject HTTP failures as evidence of a closure.

GitHub Actions also checks Linux and Windows. Local WSL results do not substitute for a completed Windows CI run. Container and systemd examples are supplied but have not been deployed.

## Production-feed smoke tests

Authentication succeeded with a user-supplied local key. Its contents were not logged, copied into repository files, or committed.

On October 27 at 04:52 UTC, the release `feed-lab verify` ran for 90 seconds on live BTC and ETH markets. It checked **89,810 production-engine delta records** against a separate flat-map/i128 reconstruction, compared **34 fresh in-stream exchange snapshots with zero mismatches**, and independently replayed the actual engine-produced SBE archive to exactly the same final books. Both symbols initialized successfully. The checker also exits on a subscription sequence gap or an unexpected production-engine repair. This tests numerical agreement and live checkpoint consistency, beyond simply counting readable messages. An earlier independent-reference run processed 81,820 deltas with 34 matching checkpoints; the final run additionally validates the production engine itself.

The [structured final verification report](results/live-verification-2026-10-27.json) is committed for review. The template reader separately validated its archive as **89,810 deltas, 36 snapshots, two symbols, and 67,108,864 bytes**. The binary archive remains local and ignored.

A separate final release collector run at 04:54–04:56 UTC exercised the full runtime for 75 seconds with two redundant sockets and a shortened 60-second first-replica lifetime. Both books were ready at health checkpoints; the scheduled recycle switched both markets to the backup with reset snapshots, then reconnected the first socket. Normal timed shutdown drained the queues and closed the archive. The template reader validated **61,346 deltas, six snapshots, two symbols, and 134,217,728 bytes**. Reported queue delay peaked at about 502 ms near the minute checkpoint on this machine; this is functional validation, not a latency SLA.

At 06:21 UTC, a ten-second live run launched specifically through Windows PowerShell 5.1 and `scripts/cargo-wsl.ps1` recorded **5,080 deltas and one initial snapshot** for one BTC market and shut down cleanly. This verified a wrapper fix for Windows PowerShell consuming Cargo's unquoted `--` separator. Collector help with application flags, an explicitly quoted separator, and the archive-inspection command were also checked through that same PowerShell entry point.

A 140-second run used two active 15-minute markets, one market per shard, and two replicas. Shortened 60/90-second socket lifetimes exercised six source switches. The archive validated using the template's reader: **144,715 deltas, 12 snapshots, 201,326,592 bytes**. Two books remained available at health checkpoints. This demonstrates real reconnection and SBE output; it does not prove zero historical loss.

A separate 45-second timing run validated **4,256 deltas, six snapshots, 67,108,864 bytes**. It produced **8,210 observations, 3,956 timestamp-fingerprint matches, 298 unmatched keys**. See `LATENCY.md` for per-source percentiles and interpretation limits. The contracts closed during the sample.

The production frames observed in these tests omitted the documented exchange clock. Timestamp-fingerprint matching is labeled as a heuristic and is not used as a guaranteed exchange sequence. Strict cross-source catch-up is enforced only when a comparable exchange clock is actually supplied.

## Full-universe validation

Initial scans discovered roughly 149,000 regular markets. The first configuration attempted 300 sockets and hit HTTP 429. Defaults were revised to 2,000 markets per shard, two replicas, a 160-socket budget, and two starts per second, with a shared retry cooldown. A second run also identified excessive switches after quiet periods, which were fixed with a grace period and a regression test. These preliminary runs are not claimed as complete coverage.

A three-minute collection window after discovery used 150 sockets for 148,396 markets. Reading the complete archive validated **874,722 deltas, 711,788 snapshots, 148,396 distinct initialized symbols, and 469,762,048 bytes**. Every discovered symbol appeared with an initial snapshot. This does not mean every market was continuously available: instantaneous readiness varied, and 18 connection resets plus 20 command-acknowledgement timeouts occurred during the run. No HTTP 429 occurred with this configuration. The snapshot count includes scheduled/reset snapshots and source changes; the final quiet-snapshot regression fix suppresses switches between identical clockless books, so that count is not an expected steady-state rate.

The largest reported engine-queue delay during full-load health windows was about four seconds on this local machine. Completeness/recovery behavior was exercised; this is not a low-latency production certification. Deployment hardware, subscription distribution, output storage, and venue limits need tuning for sustained operation.

A separate 75-second quiet-market run (one replica, a shortened quiet threshold) performed a real unsubscribe/resubscribe cycle and received a new snapshot. Its archive contains three snapshots and no deltas, confirming that quiet-market repair works against the live server, not only the simulator.

Raw binary captures and detailed diagnostics remain local under ignored `data/` and `work/` directories. Small JSON experiment reports without credentials are committed under `docs/results/`.

## Performance follow-up

The October 27 performance investigation removed per-frame subscription cloning, enabled compressed discovery, prioritized initial market coverage, and added five-second progress metrics. A controlled replay produced correct output roughly 30 times faster. The final three-minute full-market test validated **1,448,987 deltas, 435,085 snapshots, and 144,749 distinct symbols across lifecycle changes**. Initial coverage of 144,743 markets reached 100% about 40 seconds after the first connection, following a 35-second discovery. No socket failures occurred; a REST throttle was retried with slower future page pacing. The peak recorded output rate was 12,685 updates/second; the largest engine-queue delay was about 1.1 seconds near snapshot/checkpoint work. See [methods, raw measurements, and limits](PERFORMANCE.md).

## Limits

A real post-maintenance timing measurement is still pending an actual exchange closure/reopening. The runner and simulated transition tests are implemented, and a live already-open status check correctly returned inconclusive without starting a measurement. Source-port measurements are described in `LATENCY.md`. Redundant sockets cannot fill an outage during which every collector process is stopped. The template archive writer buffers a chunk; abrupt termination can lose unflushed data, and its padding affects disk use. Simulated failures and finite live runs cannot prove production completeness. These limits are described in `DESIGN.md` and `LATENCY.md`.
