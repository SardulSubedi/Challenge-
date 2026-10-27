# Latency experiments

## Method

Collect the same active markets over redundant authenticated WebSockets with `--latency-log`. Each observation records local application receive microseconds, its timing basis, an event key in the `clock` field (exchange clock or exchange timestamp microseconds), an ordinal, side, price, and signed quantity change. When exchange clocks exist, the analyzer matches that event identity. The tested production feed omitted clocks: the fallback uses exchange event timestamp, per-timestamp ordinal, side, price, and quantity change. The first partial timestamp bucket after each snapshot and buckets following timestamp regressions are excluded until the previous high-water mark is exceeded. These are explicitly labeled TimestampFingerprint matches: useful measurements, with weaker uniqueness guarantees than exchange IDs. Each source is deduplicated before comparing it with the earliest observation.

Run repeated small experiments with new archive/log filenames, equal market sets, equal shard sizes, and the same machine/network. Compare one initial connection pair with a pair after a staggered recycle. The CLI exposes connection lifetime, shard capacity, and replica count. TCP_NODELAY is enabled. Connection starts are rate limited, not spammed.

```sh
cargo run --release --bin collector -- --symbols REPLACE_WITH_LIVE_TICKER \
  --replicas 2 --recycle-secs 60 --run-secs 140 \
  --latency-log data/experiment-01.jsonl --output data/experiment-01.sbe --no-stdout
cargo run --release --bin latency-report -- data/experiment-01.jsonl
```

The report prints matched/unmatched counts, sample counts, first-arrival counts (ties included), and relative delay median/p95/p99/max for each source. Only matched samples support a comparison; unmatched counts also matter because a fast feed missing many events is not an improvement. With more than two replicas, some updates can match only a subset of sources; per-source sample counts make that visible.

## Interpretation and limits

This is **relative application receive latency**, not one-way exchange-to-NIC latency. It includes runtime scheduling, parsing, and connection buffering. It does not measure unknown network propagation time or prove clock synchronization with the exchange. Equal-clock deltas can be distinct; matching an ordinal prevents collapsing multiple changes caused by one action.

The pending match window defaults to 100,000 distinct events. Events delayed beyond that window become unmatched. Per-source percentiles use at most the first 1,000,000 matched observations; this bounds memory and means long-run percentiles describe that prefix, not an unbiased reservoir. A maintenance reset or very long run can reuse an exchange clock, so analyze bounded sessions and split logs at maintenance boundaries. Never rank connections solely by their own subscription sequence or by independent deltas' timestamps.

Connecting immediately after scheduled maintenance, selecting a source port, changing geographic location, and aggressive reconnection all need controlled measurements. This implementation does not claim those techniques are faster. Repeated connection attempts can worsen throttling and completeness. Correct books, continuity validation, and redundant resnapshot recovery take priority over small timing wins.

## Observations

On October 26, 2026 (America/Toronto), a 45-second production sample of BTC and ETH 15-minute markets recorded 8,210 timing observations, with 3,956 timestamp-fingerprint matches and 298 unmatched event keys. None used exchange clocks because the sampled production frames omitted that field. The contracts closed during the sample; most updates arrived before expiry. The sample is exploratory, not a controlled infrastructure benchmark.

For BTC, sources 0 and 1 shared 1,359 matches. Source 1 arrived first for 1,270 (93.5%); source 0 had a 3,940 microsecond median relative delay and an 11,058 microsecond p95. For ETH, sources 2 and 3 shared 2,597 matches. Source 2 arrived first for 2,499 (96.2%, ties possible); source 3 had a 3,091 microsecond median, 12,302 microsecond p95, and 50,928 microsecond p99. Relative delays were large enough to justify redundant connections, but this small sample cannot establish a permanent best socket or prove a source-port/maintenance-timing effect.

A separate 140-second run captured 144,715 deltas and exercised six scheduled source switches across four sockets. Every switch emitted a fresh snapshot, and both markets remained available at health checkpoints. That run preceded timestamp-fingerprint support, so it validates reconnection behavior rather than cross-connection latency percentiles.

A full-market scan found approximately 149,000 live regular markets. Starting about 300 connections at four per second eventually received HTTP 429 responses. The implementation now defaults to about 150 connections for that universe, two starts per second, a socket budget, and global cooldown on throttling. Large-universe operation also exposed unnecessary source switches after quiet periods; a grace period plus queue-aware stall checks fixed that behavior, with a regression test. Independent local receive time and exchange event time also showed clock skew, so one-way latency is not inferred by subtracting them.

No experiment was performed immediately after scheduled exchange maintenance, and no source-port manipulation or connection spamming was used. The supported conclusion is that redundant sockets can have materially different relative arrival times, and that aggressive connection creation can harm coverage.

## References

- [Kalshi orderbook feed and cross-connection exchange clocks](https://docs.kalshi.com/websockets/orderbook-updates)
- [Kalshi WebSocket authentication and keepalive](https://docs.kalshi.com/getting_started/quick_start_websockets)
- [API key algorithms and request signing](https://docs.kalshi.com/getting_started/api_keys)
