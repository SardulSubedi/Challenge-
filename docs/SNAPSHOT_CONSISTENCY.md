# Snapshot consistency investigation

## Finding

Snapshot/delta disagreement is now reproducible in the received Kalshi messages independently of the collector. The three-minute October 27 capture contains **97,794 deltas, 358 checkpoint comparisons, seven disagreements, and zero subscription sequence gaps**. The Rust engine agreed with independent i128 arithmetic on every delta, and its SBE archive replay agreed with that reconstruction. A separate Python Decimal implementation, which does not import the production decoder or book code, finds the same seven disagreements directly in the raw JSON.

An archive-boundary audit also matches all seven raw-message cases and all **15 differing price levels** exactly, including fixed-point scaling and the NO-to-YES-ask complement. This rules out SBE serialization as the source of those observed differences.

This localizes the reproduced problem to the data supplied at the snapshot/delta boundary. It does not establish whether the exchange snapshot is stale, internally non-atomic, or inconsistent with an omitted server-side update. No captured market frame supplies `clock`, so exact exchange-clock alignment is unavailable. A search backwards through each preceding checkpoint interval did not find an earlier complete book matching the differing snapshot; a simple whole-book lag was not demonstrated.

The collector's scheduled snapshots use its maintained book and serialized worker boundaries. The extra `get_snapshot` requests are verification probes, not part of that production periodic-snapshot path. Changing local arithmetic or silently treating inconsistent input as a passing verification would not establish correctness.

## Credential-free reproduction

The compressed fixture contains the actual first **2,840 received public messages**, through the first disagreement. It preserves the original frame text and contiguous subscription sequences. It contains no connection authentication headers.

```sh
python scripts/audit-wire.py tests/fixtures/kalshi-inconsistent-snapshot.wire.jsonl.gz
```

The auditor intentionally exits 1: at sequence **2839**, BTC's NO bid at **0.0100** has reconstructed quantity **52,478.34**, while the supplied snapshot contains **52,475.61**, a difference of **-2.73 contracts**. The test suite checks that this exact inconsistency remains detectable; it does not suppress it.

Fixture SHA-256: `31d640b225a59befea28bec92763b36acc212a78a55a64b8b7dc5c3df7bd147d`.

```sh
python -m unittest discover -s tests -p test_audit_wire.py
```

Those five tests also cover consistent input, sequence gaps, synthetic disagreement, and exact six-place quantities beyond floating-point precision.

## Original failed run

The original binary archive survived. `feed-lab audit-archive` recovers both reported boundaries and all four exact price-level differences: three levels at 16:57:00.355351 UTC and one at 16:57:25.355417 UTC. The archive contains 145,521 deltas and 34 checkpoint comparisons, matching the original report.

```sh
cargo run --release --bin feed-lab -- audit-archive \
  --archive /var/tmp/voluntas-challenge/data/reference-20261027T165650.sbe \
  --report data/original-archive-audit.json
```

That command intentionally fails on the recovered disagreements. Audit only single-source **verification archives**: source-switch snapshots in normal collector archives are deliberate resets and cannot be judged by this equality test. SBE does not retain raw frames, subscription sequences, or exchange clocks. Consequently, the original two events cannot be assigned a specific server-side mechanism, even though the discrepancy class is independently reproduced now.

## Evidence retention and results

Every `feed-lab verify --archive capture.sbe` run now also reserves `capture.wire.jsonl`. Each row records local receive time and the original message text **before decoding or sequence checking**. Evidence flushes on normal completion and handled verification failures. As with other buffered output, an abrupt process or host crash can lose a buffered tail. Existing files are never overwritten. Reports include the wire-log path. The standalone auditor accepts these JSONL files, compressed JSONL fixtures, and the earlier trace-log format.

- [Fresh live verification](results/snapshot-investigation-20261027T192238.json)
- [Independent raw JSON audit and capture hash](results/independent-wire-audit-20261027T192238.json)
- [Matching encoded archive audit](results/investigation-archive-audit-20261027T192238.json)
- [Recovered original archive differences](results/original-archive-audit-20261027T165650.json)
- [Bounded earlier-state alignment search](results/snapshot-alignment-20261027T192238.json)

The live verification remains correctly marked as failed. The client-side evidence gap is repaired, and the reproduced inconsistency is localized to upstream data. Repairing the exchange's internal consistency or reconstructing unpublished historical events is outside this repository's control. Redundant sockets, resets, and clean later samples cannot prove recovery of data the exchange did not make observable.

Validation after these changes: all 63 Linux Rust tests and five native Windows Python tests pass; formatting, Clippy with warnings denied, and all release binaries pass. CI now also runs the independent Python tests on both platforms. No exchange orders or account mutations were performed.

The subsequent [performance audit](PERFORMANCE_AUDIT.md) adds an omitted-empty-side regression according to Kalshi's documented schema. The current suite has 69 Linux Rust tests and six independent Python tests. This valid optional-field behavior is separate from the reproduced quantity disagreements above.
