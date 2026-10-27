# Design and correctness

## Data path

Each transport worker owns one authenticated connection and a bounded symbol shard. The Kalshi adapter handles discovery, authentication, command encoding, and protocol decoding. The `Exchange` trait is the boundary for adding another venue; the generic workers, sharding, health monitoring, redundant state machine, and SBE output can be reused. A new adapter must normalize its feed into complete snapshots and signed price-level deltas, and map its subscription acknowledgements and continuity information into `Frame`.

All workers send timestamped events into one bounded queue. A single engine serializes state transitions and keeps independent books for every `(source, symbol)`. One selected source per symbol feeds a second bounded queue, consumed by a dedicated blocking writer. This keeps filesystem writes out of WebSocket tasks. Diagnostics are stderr-only; binary stdout cannot be polluted by application logs.

## Why independent books

Applying the first duplicate delta from any connection into one shared book is unsafe: transport sequences are local, one exchange action can produce several deltas, and a reconnect starts with its own snapshot. Instead, every connection maintains a complete separate representation. Backup deltas only maintain that backup. A selection change emits a full snapshot of the chosen replica before any later deltas.

The engine remembers the last emitted `(clock, ordinal)` position. A candidate behind that position cannot replace it. A snapshot covers every delta through its clock, represented with maximal ordinal. After a snapshot, the next delta must have a higher clock. Each following clock's first delta has ordinal one. Several deltas at the same clock increment the ordinal without treating a legitimate repeated clock as a duplicate.

A transport sequence gap invalidates *all* books on that subscription. A locally impossible update (negative quantity/overflow) invalidates that symbol's replica and requests an unsubscribe/resubscribe. A decreasing exchange clock triggers a full per-market rebuild across replicas because pre-maintenance clocks are no longer comparable. No deltas are applied to a missing or uninitialized book.

A backup update starts a grace period rather than immediately replacing a quiet primary. Normal duplicate arrival clears that pending suspicion. A periodic check switches only if the primary fails to catch up, and waits for the collector queue to drain first. Timing logs are optional; when disabled, backup updates avoid allocating output DTOs and latency observations.

## Subscription state

Workers send at most one subscription command at a time. They wait for its corresponding request ID acknowledgement before updating the confirmed market set or issuing another command. Repairs delete the affected symbols, wait for confirmation, then add them and expect fresh snapshots. New desired universes can arrive while a command is pending; the next reconciliation accounts for the latest set.

Assignments remain stable across discoveries to avoid unnecessary full-feed reconnects. All symbols are removed from the engine immediately when absent from a completed discovery scan, so late frames from a closing market are ignored. A partial or failed scan never replaces the live universe.

## Time and liveness

Local receive time is captured at the application WebSocket boundary, not at the kernel NIC. It includes TLS and runtime scheduling delays; this is a deliberate, testable measurement boundary. Health timers use monotonic time. Exchange timestamps are separate and may be unknown (zero) or skewed. Scheduled snapshots describe the latest *observed* state, which can be quiet for minutes without being invalid.

Pings distinguish a dead transport from a quiet market. Symbol-rate monitoring is a heuristic, not proof that a quiet subscription is broken. Silence and sharp activity drops trigger conservative refreshes with cooldowns. Quiet thresholds and scheduled connection recycling are staggered by replica so repairs usually preserve another copy. Rate-drop checks can still coincide; redundancy reduces risk rather than guaranteeing uninterrupted delivery.

## Resource and failure boundaries

- Data queues are bounded; control repairs are low-volume channels coalesced into a set by each worker. Book count is proportional to live markets times configured replicas. Per-book size is proportional to actual price levels.
- New connection attempts share a start limiter and use jittered exponential backoff after errors. A subscription command or transport that stalls has a timeout. A peer cannot hold shutdown indefinitely during its WebSocket close handshake.
- A failed writer cancels the collector. Output failures are fatal, never silently ignored. Captured events are drained on normal shutdown before closing the file. A consumer that stops reading stdout can still block the operating system write; use a continuously draining consumer or `--no-stdout` for an archive-only run.
- Buffered aligned chunks are not a write-ahead log or fsync guarantee. Abrupt process/host failure can lose the last unflushed chunk. Restarting emits new snapshots but cannot reconstruct historical messages missed while every process was down. A production implementation would need durable journaling, independent processes/hosts, replay/backfill, and explicit gap metadata.
- The supplied SBE schema has no sequence, source, freshness, gap, or nullable timestamp fields. Diagnostics record failovers/outages; the output cannot express every operational fact. It is therefore not sufficient by itself for audit-grade historical completeness.
- When the exchange omits `clock`, initial collection still works but cross-source catch-up cannot be proven against an already clocked source. Kalshi documents clocks, but availability must be verified against the actual feed. The production frames observed in the live test omitted clocks, so this limitation applies in that tested environment. Clockless failover uses a fresh snapshot reset from the latest independently maintained replica, without claiming a provable ordering relation to the prior source. Timestamp fingerprints are used only for approximate latency matching, not promoted to a guaranteed exchange sequence. A new venue should supply its own meaningful continuity mapping.
- Polling discovers lifecycle changes within a scan interval plus scan/network time. It is not an atomic exchange-wide snapshot. Connection/subscription quotas depend on the account; tune shard size, replicas, connection-start pacing, and the socket budget. The defaults use 2,000 symbols per shard, two replicas, at most 160 sockets, and two connection starts per second. HTTP 429 pauses connection starts globally and honors a numeric Retry-After header, with a 60-second fallback.

## Operational recovery

Run under a supervisor (example in `collector.service`). On crash, use a new archive filename; refuse to truncate previous captures. Retain logs with the data so consumers can identify outages. Validate files with `inspect-sbe`; it rejects partial chunks. Missing data across a full collector outage remains a gap and must not be described as recovered simply because a new snapshot arrived.
