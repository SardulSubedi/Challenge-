# Performance audit — October 27, 2026

This audit covers discovery/startup, authentication and transport, decoding, exact financial arithmetic, independent replica books, source selection, health/discovery work, snapshots, queue handoffs, SBE encoding, archive/stdout output, shutdown, and packaging. Correctness and complete market coverage remain requirements. This is a client-side latency audit; application receive-to-encoding excludes the exchange/network/TLS receive path and durable disk completion.

## Accepted changes

- Deserialize normal market frames once into typed, borrowed fields. Only the ticker crosses the queue as an owned string; numeric strings borrow their input. Snapshot arrays use borrowed `RawValue` slices and validated typed pairs. Escaped strings retain serde's owned fallback. Control messages keep flexible decoding, and malformed market messages still fail rather than becoming ignored controls. See [serde's borrowing documentation](https://docs.rs/serde_json/latest/serde_json/value/struct.RawValue.html).
- Parse exact decimal millionths with checked unsigned arithmetic and a fixed scaling table, preserving both i64 boundaries, excess-precision rejection, and trailing-zero compatibility. Financial values never pass through floating point.
- Update/remove price levels through one B-tree entry lookup. Validate overflow/negative quantities before mutation. Validate and sort initial snapshot levels, reject duplicate nonzero prices, then bulk-build compact trees.
- Use a standard randomized hash map for market lookup; reserve initial capacity and avoid cloning the complete initial symbol set just to compute its difference from an empty engine. Keep sorted diagnostic sets and price trees where order matters.
- Store the usual two replicas inline, with spill capacity for other replica counts. This replaces a separate multi-slot tree node per market; source identifiers and explicit failover ranking are preserved.
- Store the single possible ingest output inline and move the already-owned ticker into a selected delta instead of cloning it. Allocate a lag-tracking key only when entering the lagging state. Reuse the engine's outgoing vector and avoid a redundant wall-clock read on every market event.
- Remove the extra stdout buffer while retaining immediate per-record flushes and the required SBE helpers. Keep the original blocking receive loop after batching/draining experiments failed to show a reliable isolated gain.
- Drain already-captured data and snapshot boundaries through a full input queue during normal cancellation. A regression test verifies cancellation cannot discard these events. Bounded five-second send failures still fail explicitly.
- Copy declared benchmark targets into the Docker build stage so adding the harness does not leave unresolved manifest paths in that context.

## Measurement method

The original source was saved before editing, including the pending correctness investigation. Original and candidate run with identical harnesses, default release optimization, the same Rust toolchain, and Linux filesystem storage. The baseline has a distinct Cargo package identity and disables automatic binary targets. This prevents shared-target crate reuse and stops comparison builds from replacing the launch executables. Source behavior is unchanged by that identity; both explicitly expose the same library name. The intended release binary was frozen and its SHA-256 checked before live validation.

CPU cases run on logical CPU 2, with warmup, nine timed repetitions, and three interleaved process runs per variant. Allocation counting happens in a separate untimed pass. Reported timings are medians of the three per-process medians. There were no builds, correctness tests, or live collectors running during these comparisons. WSL host scheduling, power state and clocks were not controlled; small differences deserve less confidence than the large decoding/engine reductions.

The queue-only case sends 20,000 records through the real writer thread with I/O disabled to isolate handoff costs. Its nanoseconds are **per batch of 20,000**, not per record. It complements the full-pipeline benchmark, which enables actual SBE archival.

Raw repetitions, allocations, replay fingerprints and memory observations: [CPU/memory results](results/performance-audit-20261027.json). Full-pipeline results: [monotonic-clock load results](results/performance-pipeline-20261027.json). The [earlier queue-only load experiment](results/performance-pipeline-initial-20261027.json) is retained separately; it did not establish a consistent queue-latency win.

## Results

- Exact decimal conversion: **27.6% less CPU time**, 54.4 → 39.4 ns/operation.
- Typical delta decoding: **54.3% less CPU time**, 2214.8 → 1011.7 ns/operation.
- 200-level snapshot decoding: **57.0% less CPU time**, 81222.9 → 34952.6 ns/operation.
- Update in a 128-level book: **42.9% less CPU time**, 36.5 → 20.8 ns/operation.
- Engine plus SBE encoding in a 150,000-market universe: **54.4% less CPU time**, 643.6 → 293.4 ns/operation.
- Captured-wire decoding: **62.0% less CPU time**, 3196.9 → 1213.6 ns/operation.

Typical delta allocations fell from **19 to one**; 200-level snapshot allocations fell from **670 to 34**. The captured-wire replay remains identical in every process run.

The 150,000-market/two-replica/one-level workload fell from **365,904 to 198,224 KiB RSS** (45.8% less). Initialization, including symbol registration and both replicas, fell from 1.194 to 0.637 seconds. These are shallow-book process measurements, not a bound for arbitrary live depth; inline slots also consume space before initialization.

The repeated four-worker loopback test improved **local send-to-capture median from 422 to 340 µs**, and **p99 from 818 to 641 µs**. This includes backlog before decoding. All input deltas after a five-second warmup use one shared monotonic clock, encoded as a synthetic timestamp in this benchmark only. A diagnostic histogram shared by both variants records 1-µs bins through 32 ms with an explicit overflow bin; none of these runs overflowed. Each produced archive was replayed and decoded.

Production receive-to-encoding is measured separately using log2 histogram bounds. Final-window p50 bounds were 63 µs baseline and 127 µs candidate; p99 bounds were 1023 and 1023 µs. Windows overlap considerably: **a universal application-queue tail-latency reduction was not established**. A separate 16-worker pair is retained without claiming enough evidence to change thread defaults. Do not add percentiles from different boundaries or treat CPU gains as an exchange-network latency guarantee.

The queue-only case remained noisy (final median 365.3 versus 377.9 ns/record, a 3.5% regression). Batching and direct-draining experiments were removed because they did not show a reliable isolated gain. The final receiver keeps the original blocking loop. The material gains are decoding, engine work, initialization, memory and local capture timing.

## Reproduction

Run from the repository on native Linux storage. `scripts/cargo-wsl.sh` selects the local toolchain/cache on this machine; ordinary `cargo` works with a normal Rust installation.

```sh
cargo bench --bench hot_path
PERF_MEMORY_ONLY=1 cargo bench --bench hot_path
cargo bench --bench pipeline
PIPELINE_THREADS=16 cargo bench --bench pipeline
```

To include the captured-wire CPU case, export the compressed fixture's `frame` values to a temporary JSONL text file with Python's standard `gzip` and `json` modules, then pass that path after the benchmark separator: `cargo bench --bench hot_path -- /tmp/frames.jsonl`. The fixture contains 2,840 public messages; CPU benchmarking repeatedly decodes them. The independent replay regression test reads the compressed fixture directly and compares all SBE bytes with the original implementation's frozen fingerprint.

The pipeline harness uses 4,000 markets, two replicas, eight local WebSockets, 100 delta frames per socket every 10 ms, and three-second snapshots. The offer rate is approximately 80,000 input deltas/s and 40,000 selected output deltas/s. It runs real production workers, engine, queues, metrics, and aligned archive writes. It uses no credentials or external exchange traffic. Four-worker comparisons repeat three times per variant; a separate 16-worker pair checks that setting. Local loopback, fixed bursts and shallow books are a controlled workload, not a claim about arbitrary live exchange conditions. A diagnostic atomic histogram, present in both variants, records the shared-monotonic send-to-capture measurements just before decoding. A preliminary wall-clock-subtraction timing method produced a negative sample and was rejected; it is not used for the reported timing gains.

## Audited constraints retained

- Native Linux source, toolchain, caches, build artifacts and recordings remain in use. The launcher still checks for source changes before running a release build. Initial compilation and network-wide discovery are startup costs, separate from steady-state processing.
- TCP_NODELAY was already enabled. TLS verification, authentication, timeouts, global connection-start pacing, shared HTTP 429 cooldowns, bounded queues, exact sequence checks and strict completion coverage are retained. Increasing quotas or skipping validation is not an acceptable latency optimization.
- Cursor-based discovery still completes before replacing the live universe. Compression, paced REST pagination, targeted lifecycle resolution and stable shards remain. Server/network pagination time and account connection quotas limit startup; reducing client allocations does not remove those limits.
- Independent books, eligibility checks, source-switch snapshots, quiet/rate monitoring, and last-copy protection remain. No speculative cross-source delta merging or timestamp-based deduplication was introduced for clockless data.
- Snapshot work remains staggered and bounded; its timestamped worker boundary stays ordered with captured updates. Health work remains incremental. Output queues apply visible backpressure rather than silently dropping frames.
- The required generated schema/encoding helpers and aligned reader/writer remain. Archive encoding and stdout can both run; the straightforward double encoding in that optional combination is retained rather than adding another buffer/copy path without an isolated win. No unsafe production fast path or lossy decimal conversion was added.
- The existing two-buffer asynchronous archive writer keeps disk calls off socket/engine tasks. It still checks write errors and waits at clean shutdown. Per-record flushing of binary stdout remains deliberate for timely consumers. Neither path claims fsync durability.
- Latency matching and raw-message logging are optional diagnostics outside the default output path. Their additional work should be included when benchmarking configurations that enable them.

## Experiments rejected or deferred

A temporary-vector `blocking_recv_many` writer reduced some loaded pipeline delays but made the isolated handoff case slower in repeated comparisons. A direct `try_recv` drain also varied between small gains and regressions. Both experiments were removed from production; the original blocking receive loop is retained. Their earlier measurements are not attributed to the final receive loop.

A thin-LTO/single-codegen-unit experiment at the intermediate typed-parser stage took about two minutes to build and had mixed results across decoding, snapshots and engine cases. It did not establish a consistent general win, so release defaults remain portable. See [Cargo profile guidance](https://doc.rust-lang.org/cargo/reference/profiles.html). CPU-specific instructions, profile-guided optimization and CPU pinning are not imposed on the submitted collector from one laptop workload. They need representative deployment measurements.

Socket-buffer changes, aggressive connection racing, higher request rates, and disabling telemetry/validation were not applied without evidence of an improvement that preserves coverage. Source-port and maintenance experiments remain available; a consistently faster exchange route or a maintenance reopening was not manufactured for this audit.

## Full-universe regression found and repaired

The first whole-universe live run failed correctly: valid one-sided snapshots were being rejected by a pending defensive check that required both sides. It repeatedly invalidated whole shards, so completion failed with missing markets. A captured example is [GOVPARTYME-26-RBEN](../tests/fixtures/kalshi-omitted-empty-side.json), which supplies NO levels and omits an empty YES side.

[Kalshi's authoritative AsyncAPI schema](https://docs.kalshi.com/asyncapi.yaml) explicitly makes both level arrays optional when that side has no offers. The production decoder and independent Decimal auditor now treat omitted sides as empty. Malformed supplied levels still fail. The fixture, empty-book/unit cases, a two-replica network test, and an independent-auditor regression cover this behavior. The exact previously failing live market then passed with both replicas and no missing coverage. This corrects the assumption in the earlier snapshot-field check; it does not waive sequence validation or hide upstream snapshot/delta disagreement.

## Final live validation

The exact release (`e278eb5feeb152fe88c7beaf1bdd02d4cd6c6a55f7a16f715f103783ca99b641`) ran for 180 seconds after discovery and exited successfully at 2026-10-28T00:03:17.534700146+00:00. All **151,653 currently discovered regular markets** had selected books and both configured replicas. Missing and under-replicated lists were empty; discovery met the configured freshness threshold. All 12 final-minute checkpoints also had full coverage. There were **zero socket failures and zero invalid-replica repairs**. 101 planned quiet/rate-refresh batches were coordinated through redundancy protection; these can produce intentional source switches. 3 discovery throttle responses were retried. Discovery and lifecycle resolution are asynchronous: this is the known universe, not an atomic exchange-wide completeness proof.

The archive decoded successfully: **2,079,335 deltas, 458,317 snapshots, 154,662 distinct historical symbols and 402,653,184 bytes**. Historical symbol count exceeds final current markets because markets enter and leave during collection.

The final minute's 894,709 delta samples had median window p50 upper bound **127 µs**, p99 window bounds **511–8,191 µs**, and exact observed maximum **6,570 µs**. This measures application WebSocket receipt to archive encoding with stdout disabled. It excludes network/kernel/TLS receive backlog and durable write completion; these are per-window bounds, not aggregate quantiles or an SLA. No compiler or test workload overlapped this final run.

[Coverage, archive counts and health windows](results/performance-live-final.json).

## Correctness boundary

The captured-wire compatibility test preserves exactly 2,839 records, 230,731 unpadded SBE bytes, and fingerprint `5d1448d047359054`, including the original decoded snapshot levels. This is a regression check, not a cryptographic integrity proof; the fixture itself has its documented SHA-256. New boundary/malformed-input/shutdown tests complement the existing recovery, generic-exchange, stdout/archive and network tests.

The independently reproduced upstream snapshot/delta disagreement remains a failure finding in [snapshot consistency](SNAPSHOT_CONSISTENCY.md). Performance changes preserve its evidence. No client-side optimization proves recovery of exchange updates that were not observable, and this audit does not assert zero future outages or complete historical reconstruction.

Docker Desktop's engine is not exposed to this WSL distribution, so an actual image build/run was unavailable here. The declared build-context sources and normal locked release build are checked; this is not reported as a successful container execution.
