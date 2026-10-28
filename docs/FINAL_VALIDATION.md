# Final implementation and live validation

Validated October 27, 2026 in Ubuntu-24.04/WSL. This document separates implemented behavior, finite live evidence, and limits that the exchange data cannot resolve.

## Latest performance audit

The October 27–28 audit passed **69 Linux Rust tests and six independent Python tests**, formatting, all-target Clippy with warnings denied, and locked release builds. The submission also includes the native Linux source/toolchain migration and permanent raw-message consistency evidence. Historical sections below describe earlier revisions.

The exact final release completed a 180-second full-universe live run with **151,653 current markets initialized on both replicas**, no missing markets/backups, zero WebSocket failures, and zero invalid-book repairs. Its validated archive contains 2,079,335 deltas and 458,317 snapshots. Per-window timing, throttling and qualifications are in the [performance audit](PERFORMANCE_AUDIT.md) and [raw live result](results/performance-live-final.json).

Repeated benchmarks measured 62% less captured-wire decoding CPU time, about 54% less engine/encoding CPU time, and 46% less initialized shallow-book memory. Local monotonic send/capture timing improved; application-queue tail bounds remain variable. The audit also caught and fixed valid omitted-side snapshots that an overly strict pending check had incorrectly rejected. See the captured fixture and network regression.

The upstream snapshot/delta disagreement remains documented and deliberately detectable. Native Windows Rust validation is delegated to this commit's CI; it is not represented as a locally executed Windows toolchain run.

## Post-review changes

The review fixes retain per-source clock history across invalidation, so maintenance resets remain detectable after reconnects. Continuous primary activity no longer clears a lag alarm unless it catches up; clockless candidate ranking uses observed exchange time before local arrival time. The later performance audit corrected an overly strict snapshot-field check: Kalshi omits empty sides, which must be accepted. Supplied malformed levels remain rejected. Scheduled snapshots now follow timestamped boundaries from the active worker, ordered with its captured frames. Requests are bounded and coalesced instead of discarded when a worker queue is full.

Six regression tests cover reconnect resets, a legitimately lagging reconnect, continuously delayed primaries with and without clocks, successful catch-up, malformed/empty snapshots, and snapshot boundaries under one-slot backpressure. The existing periodic snapshot test also checks per-symbol timestamp order. The startup test now waits for engine-reported redundant coverage before stopping, rather than assuming accepted subscription commands imply initialized books.

The post-review Linux suite passed all 63 tests: 43 unit tests, 19 network integration tests, and one generic-exchange integration test. Backpressure ordering uses an isolated one-slot worker queue; archive cadence uses the normal collector queues so concurrent disk tests do not turn it into a sustained-congestion test. Already captured snapshot boundaries drain during shutdown alongside captured data.

Formatting verification, Clippy for all targets with warnings denied, and release builds for all binaries also passed on Ubuntu-24.04/WSL. Native Windows Rust tests have not been rerun for these changes.

The performance measurements below describe the earlier revision. A subsequent three-minute investigation reproduced seven snapshot/delta disagreements across 358 checks with zero subscription sequence gaps. Independent raw JSON/Decimal reconstruction confirms the same differences while all 97,794 Rust engine updates agree with independent arithmetic. The original SBE archive also survives and now yields both original boundaries' exact differing levels. The [snapshot consistency investigation](SNAPSHOT_CONSISTENCY.md) includes a credential-free real-wire reproduction, evidence hashes, and permanent raw-capture tooling. The reproduced inconsistency is localized to upstream input; its exact server-side mechanism and the original events' mechanism cannot be inferred without exchange clocks or original raw frames. Failed live checks remain marked as failed.

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

An earlier 90-second sample checked 145,521 updates with agreeing production arithmetic and archive replay, but **two of 34 fresh snapshot comparisons differed** (three levels at one boundary and one level at another). The verifier correctly returned failure. The original [discrepancy report](results/live-snapshot-discrepancy-2026-10-27.json) is retained, and the surviving archive now provides [exact level differences](results/original-archive-audit-20261027T165650.json). A new raw-wire capture independently reproduces the inconsistency class upstream, as documented above; neither that reproduction nor later clean samples proves a specific mechanism for the original events or universal snapshot consistency. Verification now automatically saves original public frames alongside its archive.

A prior timed full-market probe stopped while 21 backup refreshes were in progress and correctly failed final redundancy checks. Its selected coverage remained complete. That evidence is retained in the [reconciliation probe](results/reconciliation-latency-probe-2026-10-27.json); the timed-run maintenance cutoff avoids starting new elective refreshes at the last instant, rather than weakening completeness checks.

All observed production market frames omitted exchange clocks. Consequently, clockless failover resets snapshots but cannot prove cross-source ordering or historical losslessness. Full scans are not atomic exchange-wide snapshots; lifecycle delivery and REST lookups can lag or fail. Native buffering is not an fsync guarantee. No finite live test proves zero future outages or missed data.

The source-port experiment and findings are complete in [LATENCY.md](LATENCY.md). A real post-maintenance timing measurement remains unperformed because no actual closure/reopening was observed; the runner and simulated transition checks are implemented. No fastest-port or post-maintenance latency advantage is claimed.
