//! Independent live verification and bounded connection experiments.
//! Nothing in this module places orders or changes account state.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::json;
use tokio::time::{interval, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::{
    Side,
    book::Book,
    engine::Engine,
    exchange::{Change, Command, Exchange, Payload, Sequences},
    output::{CHUNK_SIZE, Record},
    transport,
    util::{
        AlignedSbeReader, AlignedSbeWriter, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};

/// Separate reference representation: signed exchange deltas are accumulated in
/// one flat map using i128. This does not call production Book::apply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReferenceBook(BTreeMap<(u8, i64), i64>);

impl ReferenceBook {
    pub fn from_book(book: &Book) -> Self {
        Self(
            book.bids
                .iter()
                .map(|(&p, &q)| ((0, p), q))
                .chain(book.asks.iter().map(|(&p, &q)| ((1, p), q)))
                .collect(),
        )
    }
    pub fn set(&mut self, side: Side, price: i64, quantity: i64) -> Result<()> {
        ensure!(
            (0..=1_000_000).contains(&price) && quantity >= 0,
            "invalid replay level"
        );
        let key = (side as u8, price);
        if quantity == 0 {
            self.0.remove(&key);
        } else {
            self.0.insert(key, quantity);
        }
        Ok(())
    }
    pub fn delta(&mut self, side: Side, price: i64, delta: i64) -> Result<i64> {
        let quantity =
            i128::from(self.0.get(&(side as u8, price)).copied().unwrap_or(0)) + i128::from(delta);
        ensure!(
            (0..=i128::from(i64::MAX)).contains(&quantity),
            "reference reconstruction underflow/overflow"
        );
        let quantity = quantity as i64;
        self.set(side, price, quantity)?;
        Ok(quantity)
    }
    pub fn from_snapshot(snapshot: &SnapshotDomain) -> Result<Self> {
        let mut result = Self::default();
        for level in &snapshot.bids {
            result.set(Side::Buy, level.price_level, level.quantity)?;
        }
        for level in &snapshot.asks {
            result.set(Side::Sell, level.price_level, level.quantity)?;
        }
        Ok(result)
    }
    pub fn differences(&self, other: &Self) -> usize {
        self.0
            .keys()
            .chain(other.0.keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| self.0.get(*key) != other.0.get(*key))
            .count()
    }
}

#[derive(Default, Debug, Serialize)]
pub struct VerifyReport {
    pub started_at: String,
    pub duration_ms: u64,
    pub connection: Option<transport::ConnectionInfo>,
    pub deltas: u64,
    pub engine_updates_checked: u64,
    pub initial_snapshots: u64,
    pub snapshot_checks: u64,
    pub snapshot_mismatches: u64,
    pub mismatches: Vec<Mismatch>,
    pub archive_roundtrip: bool,
    pub symbols: usize,
}

#[derive(Debug, Serialize)]
pub struct Mismatch {
    pub symbol: String,
    pub different_levels: usize,
    pub at: String,
    pub seq: Option<u64>,
    pub clock: Option<u64>,
    pub levels: Vec<LevelDifference>,
}

#[derive(Debug, Serialize)]
pub struct LevelDifference {
    pub side: u8,
    pub price: i64,
    pub reconstructed_quantity: Option<i64>,
    pub snapshot_quantity: Option<i64>,
}

fn level_differences(expected: &ReferenceBook, actual: &ReferenceBook) -> Vec<LevelDifference> {
    expected
        .0
        .keys()
        .chain(actual.0.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| expected.0.get(*key) != actual.0.get(*key))
        .take(20)
        .map(|&(side, price)| LevelDifference {
            side,
            price,
            reconstructed_quantity: expected.0.get(&(side, price)).copied(),
            snapshot_quantity: actual.0.get(&(side, price)).copied(),
        })
        .collect()
}

pub struct VerifyOptions {
    pub symbols: Vec<String>,
    pub duration: Duration,
    pub snapshot_every: Duration,
}

pub async fn verify(
    exchange: Arc<dyn Exchange>,
    options: VerifyOptions,
    archive_path: Option<&Path>,
) -> Result<VerifyReport> {
    ensure!(
        !options.symbols.is_empty()
            && !options.duration.is_zero()
            && !options.snapshot_every.is_zero(),
        "positive duration, snapshot interval, and symbols required"
    );
    let (mut socket, connection) = timeout(
        Duration::from_secs(20),
        transport::connect(exchange.request()?, None),
    )
    .await??;
    let mut archive = archive_path
        .map(|path| {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            AlignedSbeWriter::create(path, CHUNK_SIZE)
        })
        .transpose()?;
    let mut report = VerifyReport {
        started_at: Utc::now().to_rfc3339(),
        connection: Some(connection),
        ..Default::default()
    };
    let mut reference = BTreeMap::<String, ReferenceBook>::new();
    let mut engine = Engine::new(Duration::from_secs(2)).with_timing(false);
    engine.set_symbols(&options.symbols.iter().cloned().collect());
    let result: Result<()> = async {
        socket.send(Message::Text(exchange.command(1, &Command::Subscribe(options.symbols.clone()))?.into())).await?;
        let started = Instant::now();
        let deadline = tokio::time::sleep(options.duration);
        tokio::pin!(deadline);
        let mut checks = interval(options.snapshot_every);
        checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat = interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sid = None;
        let mut next_id = 2u64;
        let mut sequences = Sequences::default();
        let mut pending = BTreeSet::new();
        let mut pending_at = Instant::now();
        loop {
            tokio::select! {
                _ = &mut deadline => break,
                _ = checks.tick() => {
                    ensure!(pending.is_empty() || pending_at.elapsed() < Duration::from_secs(20), "snapshot checkpoint response timed out");
                    if let Some(sid) = sid && pending.is_empty() && reference.len() == options.symbols.len() {
                        let command = json!({"id":next_id,"cmd":"update_subscription","params":{"sids":[sid],"market_tickers":options.symbols,"action":"get_snapshot"}});
                        timeout(Duration::from_secs(5), socket.send(Message::Text(command.to_string().into()))).await??;
                        pending.extend(options.symbols.iter().cloned()); pending_at = Instant::now(); next_id += 1;
                    }
                }
                _ = heartbeat.tick() => { timeout(Duration::from_secs(5), socket.send(Message::Ping(Vec::new().into()))).await??; }
                incoming = socket.next() => {
                    let received_at = Utc::now();
                    match incoming.context("verification socket ended")?? {
                        Message::Text(text) => {
                            tracing::trace!(frame = text.as_str(), "verification exchange frame");
                            let frame = exchange.decode(text.as_str(), received_at)?;
                            sequences.check(frame.sid, frame.seq)?;
                            match frame.payload {
                                Payload::Subscribed { sid: new_sid } => sid = Some(new_sid),
                                Payload::Market(data) => {
                                  let outcome = engine.ingest(0, data.clone(), received_at, Instant::now());
                                  ensure!(outcome.repairs.is_empty(), "production engine requested repair during verification");
                                  ensure!(outcome.records.len() == 1, "production engine did not emit exactly one record");
                                  match data.change {
                                    Change::Snapshot(book) => {
                                        let actual = ReferenceBook::from_book(&book);
                                        if let Some(expected) = reference.get(&data.symbol) {
                                            report.snapshot_checks += 1;
                                            if *expected != actual {
                                                report.snapshot_mismatches += 1;
                                                if report.mismatches.len() < 10 { report.mismatches.push(Mismatch { symbol: data.symbol.clone(), different_levels: expected.differences(&actual), at: received_at.to_rfc3339(), seq: frame.seq, clock: data.clock, levels: level_differences(expected, &actual) }); }
                                            }
                                        } else { report.initial_snapshots += 1; }
                                        pending.remove(&data.symbol);
                                        reference.insert(data.symbol.clone(), actual);
                                        let Record::Snapshot(snapshot) = &outcome.records[0] else { anyhow::bail!("engine emitted delta for snapshot"); };
                                        ensure!(ReferenceBook::from_snapshot(snapshot)? == reference[&data.symbol], "production engine snapshot differs from reference");
                                    }
                                    Change::Delta { side, price, delta } => {
                                        let book = reference.get_mut(&data.symbol).context("delta arrived before initial snapshot")?;
                                        let quantity = book.delta(side, price, delta)?;
                                        let Record::Delta(update) = &outcome.records[0] else { anyhow::bail!("engine emitted snapshot for delta"); };
                                        ensure!(update.symbol == data.symbol && update.side == side && update.price_level == price && update.quantity == quantity, "production engine update differs from independent reconstruction");
                                        report.engine_updates_checked += 1;
                                        report.deltas += 1;
                                    }
                                  }
                                  if let Some(writer) = &mut archive {
                                    match &outcome.records[0] {
                                      Record::Snapshot(snapshot) => writer.write_msg(snapshot)?,
                                      Record::Delta(update) => writer.write_msg(update)?,
                                    }
                                  }
                                },
                                _ => {},
                            }
                        }
                        Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("verification peer closed before sample completed"),
                        Message::Binary(_) => anyhow::bail!("unexpected binary verification frame"),
                        _ => {},
                    }
                }
            }
        }
        report.duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        Ok(())
    }.await;
    let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
    if let Some(writer) = &mut archive {
        writer.flush()?;
    }
    drop(archive);
    result?;
    report.symbols = reference.len();
    if let Some(path) = archive_path {
        let replay = replay_archive(path)?;
        ensure!(
            replay == reference,
            "SBE archive replay differs from independently reconstructed final books"
        );
        report.archive_roundtrip = true;
    }
    Ok(report)
}

/// Reconstruct absolute-quantity SBE output without using production mutation code.
pub fn replay_archive(path: &Path) -> Result<BTreeMap<String, ReferenceBook>> {
    ensure!(
        std::fs::metadata(path)?
            .len()
            .is_multiple_of(CHUNK_SIZE as u64),
        "truncated archive"
    );
    let mut result = BTreeMap::<String, ReferenceBook>::new();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                result.insert(
                    snapshot.symbol.clone(),
                    ReferenceBook::from_snapshot(&snapshot)?,
                );
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                result
                    .get_mut(&update.symbol)
                    .context("SBE delta before snapshot")?
                    .set(update.side, update.price_level, update.quantity)?;
            }
            _ => anyhow::bail!("unexpected SBE message"),
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reference_replay_detects_divergence_and_removes_zero_levels() {
        let mut reference = ReferenceBook::default();
        reference.delta(Side::Buy, 400000, 1500000).unwrap();
        let snapshot = Book::from_levels(vec![(400000, 1000000)], vec![]).unwrap();
        assert_eq!(
            reference.differences(&ReferenceBook::from_book(&snapshot)),
            1
        );
        reference.delta(Side::Buy, 400000, -1500000).unwrap();
        assert_eq!(reference, ReferenceBook::default());
        assert!(reference.delta(Side::Buy, 400000, -1).is_err());
    }
}
