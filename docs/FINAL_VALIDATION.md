# Final implementation and live validation

Validated October 27, 2026 in Ubuntu-24.04/WSL. This document separates implemented behavior, finite live evidence, and limits that the exchange data cannot resolve.

## Implementation

The collector implements the base challenge and the optional redundancy, subscription monitoring, generic exchange boundary, and latency-experiment tooling. It discovers regular live markets with MVE exclusion, distributes them across bounded shards, maintains independent replica books, emits absolute-quantity SBE deltas on stdout and periodic snapshots with the required helpers, and closes sockets and archives on SIGINT. The Windows recording launcher explicitly disables binary stdout for interactive use; the collector itself emits stdout by default.

The follow-up work moves recording and build artifacts onto native Linux storage, staggers per-market snapshots, writes bounded archive chunks asynchronously, updates coverage incrementally, and compares full discovery results outside the serial book processor. Only changed shards reconcile subscriptions. Two existing sockets carry regular-market lifecycle notifications; bounded targeted REST checks accelerate lifecycle updates while full scans recover missed notifications. Targeted lookups and discovery can be throttled; failed scans never replace the previous complete universe.

The engine coordinates elective repairs and scheduled socket recycling so another eligible replica remains available. Forced failures and invalid books still invalidate immediately. A timed run stops starting elective refreshes five seconds before its deadline, allowing existing refreshes to finish. Completion still fails if selected books, configured replicas, or fresh discovery are missing; an automated test verifies that a streaming primary cannot hide a missing backup. Continuous runs have no timed cutoff.

## Automated validation

Formatting, Clippy with warnings denied, all release binaries, and **57 Linux tests** passed locally: 38 unit, 18 network integration, and one generic-exchange integration test. Windows excludes two platform-specific tests, leaving 55. The previous main commit also passed both CI platforms. GitHub Actions repeats formatting, static analysis, tests, and release builds for the submitted revision.

New regression coverage includes coordinated refresh safety, incremental coverage compared with actual books through resets/removals, missing backup detection, lifecycle add/remove before the next full poll, MVE filtering on targeted lookups, and changes confined to the affected shard. Existing tests cover failures, reconnects, snapshot-first failover, arithmetic, archive replay, and SIGINT/stdout equivalence.

## Final full-market run

A 240-second collection window after discovery finished successfully with **146,883 current markets, 146,883 ready books, and 146,883 markets with both configured replicas**. Discovery was fresh; missing and under-replicated lists were empty. The archive contains **3,797,035 deltas, 589,758 snapshots, 146,889 distinct symbols across lifecycle changes, and 536,870,912 bytes**. Every record decoded successfully with the template reader. No WebSocket failures were logged. Five discovery throttle responses were retried; three targeted lookup failures also requested full scans and retried. These are operational limitations, not omitted successes.

Initial discovery took 35.905 seconds. Full selected coverage was observed about 41 seconds after discovery; full redundancy about 81 seconds after discovery. Connection pacing intentionally spreads starts across the universe before opening backups. Many markets are quiet; output counts are unique selected-source updates, not the sum of duplicates.

In the final minute, 988,237 output delta samples had a median of window p50 upper bounds of **127 microseconds**. Five-second-window p99 upper bounds ranged from **1,023 to 4,095 microseconds**. The maximum receive-to-sink observation was **7,046 microseconds**. All final-minute health checkpoints had complete selected and redundant coverage. These are per-window histogram bounds, not aggregate percentiles or a latency SLA. Timing starts at application WebSocket receipt and ends at archive encoding with stdout disabled; network transit, kernel buffering, and durable disk completion are excluded. Separate correctness/recycle probes overlapped startup, and ended before the final-minute sample.

[Full measurements](results/final-native-validation-2026-10-27.json), [archive validation](results/final-archive-validation-2026-10-27.json).

## Independent correctness and recovery checks

A separate 180-second BTC/ETH verification checked **190,467 production-engine updates** against independent flat-map/i128 arithmetic, compared **70 fresh exchange snapshots with zero mismatches**, and replayed the actual SBE archive to identical final books. An additional Python Decimal reconstruction directly from **190,540 captured JSON frames**, without the production decoder, found zero sequence gaps and zero mismatches across the same 70 snapshot boundaries. A separate 60-second sample also passed 44,383 updates and 22 checkpoint comparisons.

An 80-second live collector run performed a scheduled source-zero recycle after 60 seconds, emitted reset snapshots for both markets, reconnected, restored both replicas, and stopped successfully. Its archive validated as **72,569 deltas and four snapshots**.

[Independent verifier](results/extended-snapshot-boundaries-2026-10-27.json), [raw-wire reconstruction](results/raw-wire-boundaries-2026-10-27.json), [shorter verification](results/snapshot-boundary-diagnostics-2026-10-27.json), [recycle run](results/final-recycle-validation-2026-10-27.json).

## Evidence that remains qualified

An earlier 90-second sample checked 145,521 updates with agreeing production arithmetic and archive replay, but **two of 34 fresh snapshot comparisons differed** (three levels at one boundary and one level at another). The verifier correctly returned failure. The cause was not established, and neither subsequent clean samples nor missing exchange clocks justify attributing it conclusively to the exchange or declaring universal snapshot consistency. The failure is retained in the [discrepancy report](results/live-snapshot-discrepancy-2026-10-27.json). Verification diagnostics now include exact differing levels and sequence/clock fields; public wire frames can be captured with `RUST_LOG=engineering_challenge::lab=trace`.

A prior timed full-market probe stopped while 21 backup refreshes were in progress and correctly failed final redundancy checks. Its selected coverage remained complete. That evidence is retained in the [reconciliation probe](results/reconciliation-latency-probe-2026-10-27.json); the timed-run maintenance cutoff avoids starting new elective refreshes at the last instant, rather than weakening completeness checks.

All observed production market frames omitted exchange clocks. Consequently, clockless failover resets snapshots but cannot prove cross-source ordering or historical losslessness. Full scans are not atomic exchange-wide snapshots; lifecycle delivery and REST lookups can lag or fail. Native buffering is not an fsync guarantee. No finite live test proves zero future outages or missed data.

The source-port experiment and findings are complete in [LATENCY.md](LATENCY.md). A real post-maintenance timing measurement remains unperformed because no actual closure/reopening was observed; the runner and simulated transition checks are implemented. No fastest-port or post-maintenance latency advantage is claimed.
