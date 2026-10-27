# Native storage and latency progress — 2026-10-27

## Run from Windows

From the repository directory:

```powershell
.\scripts\run-wsl.ps1
# Optional timed run:
.\scripts\run-wsl.ps1 --run-secs 90
```

This builds release mode, records all discovered live regular markets, disables binary stdout, and writes an archive plus a JSON coverage report to native Linux storage. The default directory is `/var/tmp/voluntas-challenge/data`, accessible from Windows as `\\wsl.localhost\Ubuntu-24.04\var\tmp\voluntas-challenge\data`. The Linux environment variable `COLLECTOR_DATA_DIR` overrides it. `/var/tmp` may be cleaned by the operating system; retain important recordings in managed storage. Stop with Ctrl+C to drain and finish the archive.

## Saved changes

- Native Linux recording launcher; build artifacts were already on Linux storage.
- Per-market staggered snapshots, at most 32 scheduled snapshots per batch.
- Bounded asynchronous aligned archive writes using the template helper, with shutdown/error propagation and two chunk buffers maximum. This does not provide fsync durability.
- Reusable stdout encoding buffer, buffered coverage JSON, and incremental market/shard reconciliation.
- Monotonic queue and receive-to-sink histograms, exact missing-market lists, discovery freshness, and unsuccessful completion when selected books are missing or discovery is stale.
- Earlier repair of missing initial snapshots.

## Verification saved before pausing

Linux: 50 tests passed (34 unit, 15 network integration, one generic-exchange integration), formatting, Clippy with warnings denied, and release build passed. New checks include missing-market failure reporting, staggered snapshots, asynchronous archive round trips, write-error propagation, and timing histograms. Windows excludes two platform-specific tests; its latest pushed revision still needs CI confirmation.

The final 180-second full-universe run discovered 144,759 markets, emitted 1,633,704 deltas and 435,122 snapshots, and its 402,653,184-byte archive decoded successfully across all 144,759 symbols. Final selected coverage was 144,759/144,759 with fresh discovery. No socket failures were logged; one REST throttle was handled. Two backup books were in refresh at shutdown, so final redundant coverage was 144,757/144,759.

In the final minute, the median of five-second-window p50 upper bounds was 127 microseconds. Window p99 upper bounds ranged from 2,047 to 16,383 microseconds; the maximum receive-to-sink observation was 32,347 microseconds. These are window statistics, not an aggregate percentile. Timing runs from application frame receipt to archive encoding with stdout disabled; it excludes network transit and durable disk completion. This was a local live test, not a controlled infrastructure benchmark.

Two 64 MiB write-and-sync rounds measured about 265–267 ms on native Linux storage versus 916–986 ms on the Windows mount. Normal collection does not fsync each chunk. Detailed reports: [latency](results/native-latency-2026-10-27.json), [storage](results/native-storage-comparison-2026-10-27.json).

## Follow-up implementation

The resumed implementation replaces full-universe health scans with incremental coverage counters; routine healthy reports copy no symbol lists. Configured backups are now required for successful completion. Engine-approved planned refreshes preserve an eligible alternate, with one-replica mode explicitly accepting a temporary gap. Whole-socket recycling is postponed when it would remove a last usable copy. Timed runs stop starting elective refreshes in their final five seconds, while retaining strict completion checks. Full-universe comparisons run outside the orderbook processor and only affected shards reconcile.

Two existing sockets also carry redundant regular-market lifecycle subscriptions. A bounded resolver coalesces notifications and checks live/MVE status over REST outside the book engine. Full scans still recover missed notifications. An overlay prevents scans from reversing newer targeted observations. Live testing discovered that zero request IDs are omitted from Kalshi acknowledgements, so lifecycle uses positive IDs.

The previous commit passed both Linux and Windows CI. The resumed local suite passes 57 tests, formatting, Clippy, and release builds. See [final validation](FINAL_VALIDATION.md) for fresh full-market and independent book checks.

The observed production feed omitted exchange clocks. Cross-source catch-up and historical losslessness cannot be proven from those frames. Recovery and coverage reporting do not guarantee zero future outages. A real post-maintenance experiment remains unavailable until an actual exchange closure/reopening; its runner and simulated transition checks are implemented.
