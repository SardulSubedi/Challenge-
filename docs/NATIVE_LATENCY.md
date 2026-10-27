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

Two 256 MiB write-and-sync rounds measured about 265–267 ms on native Linux storage versus 916–986 ms on the Windows mount. Normal collection does not fsync each chunk. Detailed reports: [latency](results/native-latency-2026-10-27.json), [storage](results/native-storage-comparison-2026-10-27.json).

## Resume tomorrow

1. Confirm Linux and Windows CI for the pushed commit.
2. Profile remaining latency tails; coverage still scans the universe every five seconds, and VM scheduling/output backpressure need measurement before further changes.
3. Decide the strict redundancy policy: incomplete selected coverage fails completion, but missing backups currently appear in the report without failing completion. Investigate the two final backup refreshes.
4. Consider lifecycle streaming: discovery currently requires a complete REST scan (about 35–38 seconds here), plus the interval between scans. Newly listed markets are not subscribed instantaneously.
5. Update broader performance/design documentation after the next verification round.

The observed production feed omitted exchange clocks. Cross-source catch-up and historical losslessness cannot be proven from those frames. Recovery and coverage reporting do not guarantee zero future outages. Further latency and continuous coverage work remains unfinished.
