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

## Source-port and reopening experiments (October 27)

`feed-lab ports` explicitly binds TCP source ports and records local address, remote IP, handshake duration, first-snapshot delay, deltas, errors, and per-source timing distributions. The production collector uses the same transport with an OS-selected ephemeral port. TLS hostname verification uses the original URI in both cases. Every round rotates connection order to reduce start-order confounding; use four rounds for three fixed ports plus the ephemeral control. Connections are paced at two starts per second. No aggressive connection spamming is needed to obtain repeated fresh-connection samples.

An initial three-round, 20-second-per-round production run on one BTC market successfully bound ports 41001, 41011, and 41021 alongside an ephemeral control. All 12 connections received snapshots and deltas without sample errors. The rounds matched 14,083, 15,259, and 15,425 timestamp-fingerprint event keys. First-arrival leadership changed between connections across rounds. Because the production feed omitted exchange clocks, these remain heuristic event matches. Multiple remote IPs were selected, and this machine uses WSL/NAT, which can rewrite ports beyond the local socket. These observations do not isolate a causal source-port effect or identify a permanently faster port.

A subsequent release-build run at 04:53–04:54 UTC used four rounds, placing each condition in every start position. All **16 connections** succeeded. The rounds matched **16,434 / 16,331 / 19,803 / 22,170** event keys, with **323 / 845 / 541 / 447** unmatched keys. Port 41011 led first arrivals in round one, port 41001 in round two, and the ephemeral control in round three; round four was nearly tied between 41001 (7,697 first arrivals) and 41021 (7,666). Median relative delays across the individual source/round combinations ranged from 0 to 3,410 microseconds. Those changing winners argue against hard-coding a supposedly fastest local port. The [complete report](results/source-ports-2026-10-27.json) includes per-source sample counts, p95/p99/max, requested ports, and actual local/remote endpoints. One round overlapped the end of the lightweight live verifier; scheduling and network variability remain confounders.

`feed-lab maintenance` polls the official exchange-status endpoint until it observes an explicit inactive-to-active transition, then measures connections started immediately and after configured delays. HTTP errors never count as closure. A live five-second check while the exchange remained active correctly emitted `inconclusive_no_observed_reopen` and did not start any experimental sockets. Simulated tests exercise both a genuine status transition and HTTP failures.

The [live status report](results/maintenance-status-2026-10-27.json) preserves the observations and explicit inconclusive outcome.

No real post-maintenance measurement has been completed. [Kalshi's published regular window](https://help.kalshi.com/en/articles/13823807-what-are-trading-hours) is Thursday 03:00–05:00 Eastern; the next regular window after these Tuesday tests is October 29, beyond the October 27 challenge deadline. A status transition alone identifies reopening, not its cause; correlate it with the official maintenance schedule before labeling results post-maintenance. Run the command before closure, retain the status observations, and compare sufficient matched events across repeated sessions. One reopening also cannot prove a universal advantage.

The supported conclusion is that redundant sockets can have materially different relative arrival times, while aggressive connection creation can harm coverage. Neither port selection nor immediate post-maintenance connection is claimed as a proven optimization.

## References

- [Kalshi orderbook feed and cross-connection exchange clocks](https://docs.kalshi.com/websockets/orderbook-updates)
- [Kalshi WebSocket authentication and keepalive](https://docs.kalshi.com/getting_started/quick_start_websockets)
- [API key algorithms and request signing](https://docs.kalshi.com/getting_started/api_keys)
