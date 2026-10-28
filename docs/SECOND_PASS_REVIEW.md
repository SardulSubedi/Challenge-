# Second review — October 28, 2026 UTC

This pass rechecked the collector's state transitions and failure paths, exact arithmetic, venue protocol, timestamp/output boundaries, resource limits, startup/shutdown, optional diagnostics, launch/deployment files, tests and submission wording. Internal Markdown links were checked; none were missing. The required schema and serialization helpers remain in use.

## Findings repaired

1. **Lifecycle bursts were serialized into one paced REST call per ticker.** At the existing 200-ms minimum pause after each request, resolving 16 tickers required at least three seconds of pacing alone, plus network time. A regression using the actual Kalshi REST adapter timed out under the old implementation. Kalshi now resolves up to 16 tickers in one request, retaining the five-HTTP-request-per-second ceiling. That is 93.75% fewer requests for a full 16-ticker burst, not a claim of a universal 16-times latency gain. The test resolves all 16 in one call. [Kalshi documents comma-separated ticker queries](https://docs.kalshi.com/api-reference/market/get-markets).
2. **The same burst could cause repeated subscription reconciliation.** Already-ready lookup results now coalesce by symbol and newest observation into net additions/removals. A test covers open/close reversals and an older observation arriving after a newer one. Unchanged markets do not get a redundant remove/re-add. Processing and storage are bounded; generic adapters keep one-symbol pacing unless they explicitly support batches.
3. **An explicit empty smoke test could return success.** Requesting symbols with no live regular matches now fails before reserving an archive. This was tested in the local mock and against the live API: the invalid-symbol probe exited nonzero and created no file.
4. **Some wording overstated or misdescribed behavior.** CLI help no longer labels the collector “Complete”; the stop log says the output writer finished rather than claiming a successful flush when an error may follow. A sampling comment now correctly says “prefix,” matching the documented first-million cap. The README's Python count and service directory ownership instructions were corrected.

Failed lifecycle batches are retained intact, request a full scan and retry. A regression verifies an HTTP error cannot mark all requested markets closed. Retry-test synchronization explicitly gates the successful response, avoiding a scheduling-dependent assertion on a slow CI host. Existing MVE/status filtering and rejection of unexpected pagination remain; no account quota was increased.

## Checks

- **74 Linux Rust tests passed:** 50 unit, 22 network, one generic-exchange and one captured-wire replay test. Windows excludes two platform-specific tests (72 Rust tests). Five new tests cover bursts, retry preservation, net coalescing, actual two-replica subscriptions and empty explicit input.
- All six independent Python tests, formatting, all-target Clippy with warnings denied and locked release builds passed. CI repeats these on Linux and Windows.
- Captured replay retains fingerprint `5d1448d047359054`, all 2,839 records and 230,731 unpadded SBE bytes. The independently reproduced upstream quantity disagreement remains deliberately detectable.
- A fresh CPU benchmark ran nine internal timing repetitions per case for one process per variant. It confirms the allocation reductions (typical delta 19 → one; 200-level snapshot 670 → 34). This confirmation is not a replacement for the earlier three-process interleaved experiment or a new latency SLA.
- The local socket/engine/archive load test maintained both copies of all 4,000 test markets, decoded 479,000 archived deltas, and recorded monotonic send-to-capture p50 222 µs and p99 458 µs. It excludes the exchange network/TLS path. [Fresh benchmark data](results/second-pass-benchmarks-20261028.json).

## Fresh live run

The collector ran for 180 seconds after discovery and exited successfully at 2026-10-28T01:17:06.686982254+00:00. All **153,278 currently discovered markets** had initialized selected books and both replicas at completion. Missing lists were empty, discovery was fresh, and **zero WebSocket failures or invalid-book repairs** were logged. The actual lifecycle path exercised batches up to 16 tickers. Two full-discovery HTTP 429 responses and one lifecycle lookup HTTP 429 were retried; they are retained as operational evidence, not described as an error-free network.

The archive validated using the template reader: **3,914,964 deltas, 460,426 snapshots, 153,326 historical symbols and 536,870,912 bytes**. Current and historical symbol counts differ because markets enter and leave during the run.

All 12 final-minute health windows retained selected coverage. One checkpoint temporarily lacked 13 backups during protected subscription refreshes; selected books remained ready and both replicas were restored at completion. This pass does not claim uninterrupted redundancy. Final-minute receive-to-encoding window p50 upper bounds had median 127 µs, p99 bounds ranged from 2,047 to 4,095 µs, and the maximum observed sample was 5,368 µs. These are per-window log2 bounds, not aggregate quantiles, network delay or durable write completion.

[Full live evidence](results/second-pass-live-20261028.json). The live binary hash was `912f6d958198e6b0d6761edef018a8a22a854d33f4be304f17ae16027935f950`. The final normal release hash is `3a937b6deb2a3ff3bb58ecc6726055c05b6f2644d3b4f8f20d42358e24c6963e`; after the live probe only test synchronization and documentation changed. No production behavior was altered after that probe.

## Limits retained

Discovery/lifecycle lookups are asynchronous, so “all” above means the collector's current discovered universe, not an atomic exchange-wide proof. Exchange clock fields were absent, so cross-source catch-up and historical losslessness remain unprovable for that feed. The [upstream snapshot/delta disagreement](SNAPSHOT_CONSISTENCY.md), buffered-disk durability limits and stdout-consumer backpressure qualification remain explicit. No source-port or maintenance advantage is invented. Docker's actual engine remains unavailable in this WSL distribution; its declared build-context layout was checked in the earlier pass.
