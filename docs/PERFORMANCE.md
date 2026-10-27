# Startup and throughput investigation

Measured October 27, 2026 on the same Windows/Ubuntu-24.04 WSL machine used for live validation. These results describe a collector challenge, not a trading latency SLA.

## What the original terminal log showed

Discovery enumerated 144,721 regular markets before subscribing. After discovery, 73 shards required 146 sockets with two replicas. At two connection starts per second, the original interleaved primary/backup ordering required approximately 73 seconds to start every shard's first copy. The user stopped about 40 seconds into this ramp, with roughly half the universe initialized. The collector had nevertheless emitted 148,735 unique price-level updates, roughly 3,700 per second over that partial-coverage interval. Thirty-second cumulative health logs made this activity difficult to judge interactively.

## Changes

- Subscription reconciliation now runs only after a desired-universe change, explicit repair, health-triggered repair, or command acknowledgement. Previously every market frame cloned and compared up to 2,000 subscription strings, even when nothing changed. Frame decoding, continuity checks, independent books, and output correctness remain in the data path.
- REST discovery negotiates gzip. Four bounded requests for the same 1,000-market page measured 2,447,613 bytes without compression versus 164,195 bytes with gzip, about 15 times less transfer. The two identity requests took 0.723/0.424 seconds; the gzip requests took 0.316/0.259 seconds. Cache, connection setup, and network variation affect those durations; the byte reduction is the stronger result. Cursor pagination still requires sequential pages, and subscriptions start only after a complete initial scan.
- Initial primaries start before backups at the same two-per-second global pace. For 73 shards, first-copy connection starts need roughly 36 seconds, while the complete two-copy connection ramp still needs roughly 73 seconds. Broad initial coverage takes priority over early redundancy on the first shards: each shard initially has one copy until its backup connects. Failed handshakes or delayed snapshots can extend these estimates.
- Discovery increases its page delay after a 429 response and retains that delay across scans. Successful compression can otherwise accelerate requests enough to expose public endpoint throttling. Failed scans still cannot replace a complete universe.
- Health logs every five seconds report coverage, unique output updates per second, cumulative snapshots/deltas, failovers, and engine-queue delay. Bulk repair reasons are grouped per connection; per-symbol source-switch details are available at debug level. An unchanged discovery result no longer triggers redundant subscription reconciliation.

## Controlled replay

The baseline release was built from commit `db85f3e`. A temporary local WebSocket simulator supplied identical data to the baseline and optimized release: 2,000 initial one-level books, followed by 20,000 signed updates across those books. An independent reader consumed every unpadded SBE stdout record, counted all snapshots/deltas, and checked every symbol's final absolute quantity was exactly 11,000,000. The timer included process startup, local discovery, subscription, receive/processing, and output consumption. No artificial exchange-rate limit was applied. Binary order was reversed in the second round to reduce ordering effects.

The baseline completed in **13.5801 / 12.9619 seconds**; the optimized release completed in **0.4475 / 0.4397 seconds**. Every run produced all 2,000 snapshots and 20,000 updates with correct final quantities. This is about **30 times faster** on this fixture, roughly 1,500 versus 45,000 updates per second including its startup. It is processing headroom, not a claim that Kalshi emits 30 times more live data or that every possible workload gains equally. The optimized replay preceded the subsequent startup-priority/logging refinements. [Structured results](results/replay-performance-2026-10-27.json).

## Full live universe

A preliminary optimized three-minute run initialized all 144,739 markets and validated 1,429,980 deltas and 410,445 snapshots. Its complete archive was 335,544,320 bytes. This run still used the original interleaved connection order and exposed REST throttling, which prompted adaptive page pacing.

The final startup-priority run discovered **144,743 markets in 34.805 seconds**, then reached **100% initial market coverage 39.679 seconds after the first socket connected**, as observed by five-second health checkpoints. Full observed startup was therefore about 75 seconds including discovery. The next completed discovery found 144,745 current markets. At the recorded health checkpoints after initialization, every current market had a selected book. Only one REST throttling response occurred, was retried, and increased later page pacing. No socket failures were logged.

The three-minute post-discovery window recorded **1,448,987 deltas and 435,085 snapshots**, with **144,749 distinct symbols across lifecycle changes**, in **335,544,320 bytes**. The archive was validated using the template reader. The peak reported five-second output rate was **12,685 updates per second**. Selected-book readiness is not proof that both replicas are initialized or that no historical message was missed. Backup duplicates intentionally do not multiply the unique output update count. [Complete health and discovery measurements](results/full-universe-performance-2026-10-27.json).

Most engine queue delays were small, but the largest observed window reached **1,075,960 microseconds** around the large scheduled snapshot/checkpoint batches. The required aligned writer, large snapshot batches, and WSL-to-Windows storage remain sources of pauses. Application receive timestamps are captured before the engine queue; that does not prove the absence of buffering before the application read. Sustained production operation needs separate latency/completeness evaluation and storage/architecture tuning.

## Watching a run

Use a new archive filename and allow initial discovery plus the connection ramp to finish. Watch `coverage_pct`, `updates_per_sec`, and `queue_depth`; cumulative `deltas` count output records since startup. `ready` counts initialized selected books. Wait for `all current markets have initialized books` before judging full-universe throughput. There is no fixed expected market-wide rate: activity changes over time, and many listed contracts are quiet.

```powershell
.\scripts\run-wsl.ps1
```

Ctrl+C closes sockets and flushes output. An archive name that already exists is refused rather than overwritten.


## Native Linux storage and later latency work

The snapshot/checkpoint pauses above describe an earlier version. Recordings now default to native Linux storage in the WSL launcher, snapshot deadlines are staggered, and aligned chunk writes run asynchronously with bounded memory and propagated failures. Health reports use incremental counters, and planned repairs coordinate through the engine to preserve an eligible alternate book. See [native measurements](NATIVE_LATENCY.md) and [final validation](FINAL_VALIDATION.md) for subsequent results and measurement boundaries. Source files remain in the Windows checkout; native build artifacts and recording writes avoid the Windows mount on the live data path.
