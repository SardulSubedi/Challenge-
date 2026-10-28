//! Freeze pre-optimization SBE output for an actual captured production feed.
use chrono::{DateTime, Utc};
use engineering_challenge::{
    engine::Engine,
    exchange::{Exchange, Kalshi, Payload},
};
use std::{
    io::{BufRead, BufReader},
    time::{Duration, Instant},
};
#[test]
fn optimized_decoder_and_engine_preserve_captured_wire_and_sbe_bytes() {
    let input = include_bytes!("fixtures/kalshi-inconsistent-snapshot.wire.jsonl.gz");
    let rows: Vec<serde_json::Value> = BufReader::new(flate2::read::GzDecoder::new(&input[..]))
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    let adapter = Kalshi::new("http://localhost".into(), "ws://localhost".into(), None).unwrap();
    let at = DateTime::<Utc>::UNIX_EPOCH;
    let now = Instant::now();
    let mut engine = Engine::new(Duration::from_secs(2)).with_timing(false);
    let symbols = rows
        .iter()
        .filter_map(|row| {
            match adapter
                .decode(row["frame"].as_str().unwrap(), at)
                .unwrap()
                .payload
            {
                Payload::Market(m) => Some(m.symbol),
                _ => None,
            }
        })
        .collect();
    engine.set_symbols(&symbols);
    let mut hash = 0xcbf29ce484222325u64;
    let mut records = 0;
    let mut bytes = 0;
    let mut add = |byte: u8| hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    for row in &rows {
        if let Payload::Market(m) = adapter
            .decode(row["frame"].as_str().unwrap(), at)
            .unwrap()
            .payload
        {
            if let engineering_challenge::exchange::Change::Snapshot(b) = &m.change {
                for (p, q) in b.bids.iter().chain(&b.asks) {
                    for v in [*p, *q] {
                        for byte in v.to_le_bytes() {
                            add(byte);
                        }
                    }
                }
            }
            for record in engine.ingest(0, m, at, now).records {
                let data = record.encode().unwrap();
                records += 1;
                bytes += data.len();
                for byte in data {
                    add(byte);
                }
            }
        }
    }
    assert_eq!(rows.len(), 2840);
    assert_eq!(records, 2839);
    assert_eq!(bytes, 230731);
    assert_eq!(hash, 0x5d1448d047359054);
    assert_eq!(engine.coverage().missing.len(), 0);
}
