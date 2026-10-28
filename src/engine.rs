use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};

use crate::{
    book::Book,
    coverage::{Index, State},
    exchange::{Change, MarketData},
    latency::{Observation, TimestampOrdinals, TimingBasis},
    output::Record,
    util::dtos::challenge_orderbook::PriceLevelUpdateDomain,
};

pub type Source = usize;

#[derive(Default, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Coverage {
    pub markets: usize,
    pub ready: usize,
    pub redundant: usize,
    pub caught_up_redundant: usize,
    pub clocked_markets: usize,
    pub missing: Vec<String>,
    pub under_replicated: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position {
    pub clock: u64,
    pub ordinal: u32,
}

struct Replica {
    book: Book,
    position: Option<Position>,
    received: Instant,
    exchange_at: DateTime<Utc>,
    timestamp_ordinals: TimestampOrdinals,
}

// There are normally two replicas, at most eight by configuration. An inline
// list avoids a separately allocated 11-slot B-tree node for every market.
#[derive(Default)]
struct Replicas(smallvec::SmallVec<[(Source, Replica); 2]>);
impl Replicas {
    fn get(&self, source: &Source) -> Option<&Replica> {
        self.0.iter().find(|(s, _)| s == source).map(|(_, r)| r)
    }
    fn get_mut(&mut self, source: &Source) -> Option<&mut Replica> {
        self.0.iter_mut().find(|(s, _)| s == source).map(|(_, r)| r)
    }
    fn contains_key(&self, source: &Source) -> bool {
        self.get(source).is_some()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn insert(&mut self, source: Source, replica: Replica) {
        if let Some(old) = self.get_mut(&source) {
            *old = replica;
        } else {
            self.0.push((source, replica));
        }
    }
    fn remove(&mut self, source: &Source) -> Option<Replica> {
        let index = self.0.iter().position(|(s, _)| s == source)?;
        Some(self.0.swap_remove(index).1)
    }
    fn iter(&self) -> impl Iterator<Item = (&Source, &Replica)> {
        self.0.iter().map(|(s, r)| (s, r))
    }
    fn keys(&self) -> impl Iterator<Item = &Source> {
        self.0.iter().map(|(s, _)| s)
    }
    fn values(&self) -> impl Iterator<Item = &Replica> {
        self.0.iter().map(|(_, r)| r)
    }
}
impl std::ops::Index<&Source> for Replicas {
    type Output = Replica;
    fn index(&self, source: &Source) -> &Replica {
        self.get(source).expect("initialized replica")
    }
}

#[derive(Default)]
struct Market {
    replicas: Replicas,
    // A reconnect must not erase the evidence needed to detect maintenance resets.
    retired_clocks: BTreeMap<Source, u64>,
    active: Option<Source>,
    floor: Option<Position>,
    lagging_since: Option<Instant>,
}

impl Market {
    fn remove_replica(&mut self, source: Source) {
        if let Some(replica) = self.replicas.remove(&source)
            && let Some(position) = replica.position
        {
            self.retired_clocks.insert(source, position.clock);
        }
    }

    fn coverage(&self, replicas: usize) -> State {
        State {
            ready: self.active.is_some(),
            redundant: self.replicas.len() >= replicas,
            caught_up: self
                .replicas
                .values()
                .filter(|r| caught_up(r.position, self.floor))
                .count()
                >= replicas,
            clocked: self
                .active
                .is_some_and(|s| self.replicas[&s].position.is_some()),
        }
    }
}

#[derive(Default)]
pub struct Outcome {
    pub records: smallvec::SmallVec<[Record; 1]>,
    pub repairs: Vec<(Source, String)>,
    pub observation: Option<Observation>,
}

pub struct Engine {
    markets: HashMap<String, Market>,
    coverage: Index,
    replicas: usize,
    failover: Duration,
    pending_lag: BTreeSet<String>,
    capture_timing: bool,
    pub deltas_emitted: u64,
    pub snapshots_emitted: u64,
    pub failovers: u64,
}

impl Engine {
    pub fn new(failover: Duration) -> Self {
        Self {
            markets: HashMap::new(),
            coverage: Index::default(),
            replicas: 2,
            failover,
            pending_lag: BTreeSet::new(),
            capture_timing: true,
            deltas_emitted: 0,
            snapshots_emitted: 0,
            failovers: 0,
        }
    }

    pub fn with_timing(mut self, enabled: bool) -> Self {
        self.capture_timing = enabled;
        self
    }

    pub fn with_replicas(mut self, replicas: usize) -> Self {
        assert!(replicas > 0 && self.markets.is_empty());
        self.replicas = replicas;
        self
    }

    pub fn set_symbols(&mut self, symbols: &BTreeSet<String>) {
        if self.markets.is_empty() {
            self.change_symbols(symbols, &BTreeSet::new());
            return;
        }
        let old: BTreeSet<_> = self.markets.keys().cloned().collect();
        self.change_symbols(
            &symbols.difference(&old).cloned().collect(),
            &old.difference(symbols).cloned().collect(),
        );
    }

    pub fn ready_markets(&self) -> usize {
        self.markets.values().filter(|m| m.active.is_some()).count()
    }
    pub fn change_symbols(&mut self, added: &BTreeSet<String>, removed: &BTreeSet<String>) {
        for symbol in removed {
            if let Some(market) = self.markets.remove(symbol) {
                self.coverage
                    .update(symbol, Some(market.coverage(self.replicas)), None);
            }
            self.pending_lag.remove(symbol);
        }
        self.markets.reserve(added.len());
        for symbol in added {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                self.markets.entry(symbol.clone())
            {
                entry.insert(Market::default());
                self.coverage.update(
                    symbol,
                    None,
                    Some(Market::default().coverage(self.replicas)),
                );
            }
        }
    }
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn coverage(&self) -> Coverage {
        self.coverage.report()
    }

    #[cfg(test)]
    fn scanned_coverage(&self) -> Coverage {
        let replicas = self.replicas;
        let mut result = Coverage {
            markets: self.markets.len(),
            ..Default::default()
        };
        for (symbol, market) in &self.markets {
            let copies = market
                .replicas
                .values()
                .filter(|r| caught_up(r.position, market.floor))
                .count();
            if let Some(active) = market.active {
                result.ready += 1;
                if market.replicas[&active].position.is_some() {
                    result.clocked_markets += 1;
                }
            } else {
                result.missing.push(symbol.clone());
            }
            if copies >= replicas {
                result.caught_up_redundant += 1;
            }
            if market.replicas.len() >= replicas {
                result.redundant += 1;
            } else {
                result.under_replicated.push(symbol.clone());
            }
        }
        result.missing.sort_unstable();
        result.under_replicated.sort_unstable();
        result
    }

    pub fn snapshot_for(&mut self, symbol: &str, timestamp: DateTime<Utc>) -> Option<Record> {
        let market = self.markets.get(symbol)?;
        let replica = market.replicas.get(&market.active?)?;
        self.snapshots_emitted += 1;
        Some(Record::Snapshot(replica.book.snapshot(
            symbol,
            timestamp,
            replica.exchange_at,
        )))
    }

    pub fn active_source(&self, symbol: &str) -> Option<Source> {
        self.markets.get(symbol)?.active
    }

    /// Reserve planned refreshes by invalidating approved books before granting
    /// permission. Concurrent requests therefore cannot both remove the last copy.
    /// Forced disconnects and corrupt books still invalidate unconditionally.
    pub fn prepare_refresh(
        &mut self,
        source: Source,
        symbols: &BTreeSet<String>,
        all: bool,
        timestamp: DateTime<Utc>,
        now: Instant,
    ) -> (BTreeSet<String>, Vec<Record>) {
        let approved: BTreeSet<_> = symbols
            .iter()
            .filter(|symbol| {
                self.replicas == 1
                    || self.markets.get(*symbol).is_none_or(|m| {
                        !m.replicas.contains_key(&source)
                            || m.replicas
                                .iter()
                                .any(|(&s, r)| s != source && caught_up(r.position, m.floor))
                    })
            })
            .cloned()
            .collect();
        if all && approved.len() != symbols.len() {
            return (BTreeSet::new(), Vec::new());
        }
        let records = self.invalidate(source, Some(&approved), timestamp, now);
        (approved, records)
    }

    pub fn ingest(
        &mut self,
        source: Source,
        data: MarketData,
        received_at: DateTime<Utc>,
        now: Instant,
    ) -> Outcome {
        let mut outcome = Outcome::default();
        let Some(market) = self.markets.get_mut(&data.symbol) else {
            return outcome;
        };
        let before = market.coverage(self.replicas);
        let old_position = market.replicas.get(&source).and_then(|r| r.position);
        let previous_clock = old_position
            .map(|position| position.clock)
            .or_else(|| market.retired_clocks.get(&source).copied());
        // Exchange maintenance can reset clocks. Discard *all* pre-maintenance books for this market.
        if let (Some(old), Some(clock)) = (previous_clock, data.clock)
            && clock < old
        {
            tracing::warn!(symbol = %data.symbol, source, "exchange clock regressed; rebuilding all replicas");
            let sources: BTreeSet<_> = market
                .replicas
                .keys()
                .chain(market.retired_clocks.keys())
                .copied()
                .chain(std::iter::once(source))
                .collect();
            outcome
                .repairs
                .extend(sources.into_iter().map(|s| (s, data.symbol.clone())));
            *market = Market::default();
            self.coverage.update(
                &data.symbol,
                Some(before),
                Some(market.coverage(self.replicas)),
            );
            self.pending_lag.remove(&data.symbol);
            return outcome;
        }
        let mut delta_output = None;
        let is_snapshot = matches!(data.change, Change::Snapshot(_));
        match data.change {
            Change::Snapshot(book) => {
                market.retired_clocks.remove(&source);
                market.replicas.insert(
                    source,
                    Replica {
                        book,
                        position: data.clock.map(|clock| Position {
                            clock,
                            ordinal: u32::MAX,
                        }),
                        received: now,
                        exchange_at: data.exchange_at,
                        timestamp_ordinals: TimestampOrdinals::default(),
                    },
                );
            }
            Change::Delta { side, price, delta } => {
                let Some(replica) = market.replicas.get_mut(&source) else {
                    // Deltas cannot initialize a book. Transport will request a snapshot if none arrives.
                    return outcome;
                };
                let position = data.clock.map(|clock| Position {
                    clock,
                    ordinal: old_position
                        .filter(|p| p.clock == clock)
                        .map_or(1, |p| p.ordinal.saturating_add(1)),
                });
                let invalid_position =
                    matches!((old_position, position), (Some(old), Some(new)) if new <= old);
                let applied = if invalid_position {
                    Err(anyhow::anyhow!(
                        "delta does not advance exchange clock position"
                    ))
                } else {
                    replica.book.apply(side, price, delta)
                };
                match applied {
                    Ok(quantity) => {
                        replica.position = position;
                        replica.received = now;
                        replica.exchange_at = data.exchange_at;
                        if market.active == Some(source) {
                            delta_output = Some((side, price, quantity));
                        }
                        let timing = if !self.capture_timing {
                            None
                        } else if let Some(position) = position {
                            Some((TimingBasis::ExchangeClock, position.clock, position.ordinal))
                        } else {
                            replica
                                .timestamp_ordinals
                                .observe(data.exchange_at.timestamp_micros())
                                .map(|(bucket, ordinal)| {
                                    (TimingBasis::TimestampFingerprint, bucket, ordinal)
                                })
                        };
                        if let Some((basis, clock, ordinal)) = timing {
                            outcome.observation = Some(Observation {
                                symbol: data.symbol.clone(),
                                source,
                                basis,
                                clock,
                                ordinal,
                                side,
                                price,
                                delta,
                                received_us: received_at.timestamp_micros(),
                            });
                        }
                    }
                    Err(error) => {
                        tracing::warn!(source, symbol = %data.symbol, %error, "invalid replica; requesting fresh snapshot");
                        market.remove_replica(source);
                        if market.active == Some(source) {
                            market.active = None;
                        }
                        outcome.repairs.push((source, data.symbol.clone()));
                    }
                }
            }
        }
        let mut emit_delta = None;
        let previous_active = market.active;
        // Start a grace period when a backup advances. A single update after a quiet
        // interval is not proof of a stalled primary: its duplicate may arrive milliseconds later.
        if let Some(active) = market.active {
            let primary = &market.replicas[&active];
            let backup_ahead = market.replicas.iter().any(|(&candidate, replica)| {
                candidate != active
                    && match (replica.position, primary.position) {
                        (Some(backup), Some(primary)) => backup > primary,
                        (None, None) => {
                            replica.exchange_at > primary.exchange_at
                                && ((candidate == source && !is_snapshot)
                                    || replica.book != primary.book)
                        }
                        _ => false,
                    }
            });
            if backup_ahead {
                if market.lagging_since.is_none() {
                    market.lagging_since = Some(now);
                    self.pending_lag.insert(data.symbol.clone());
                }
            } else {
                market.lagging_since = None;
                self.pending_lag.remove(&data.symbol);
            }
        }
        if market.active.is_none() {
            market.active = best_source(market, now, self.failover);
            if let Some(active) = market.active {
                let replica = &market.replicas[&active];
                outcome.records.push(Record::Snapshot(replica.book.snapshot(
                    &data.symbol,
                    received_at,
                    replica.exchange_at,
                )));
                market.floor = replica.position;
                self.snapshots_emitted += 1;
                if previous_active.is_some() {
                    self.failovers += 1;
                    tracing::debug!(symbol = %data.symbol, source = active, "switched source; emitted snapshot");
                }
            }
        } else if market.active == Some(source) {
            let replica = &market.replicas[&source];
            if is_snapshot {
                // A snapshot response is an explicit reset, including on same-source repair.
                if caught_up(replica.position, market.floor) {
                    outcome.records.push(Record::Snapshot(replica.book.snapshot(
                        &data.symbol,
                        received_at,
                        replica.exchange_at,
                    )));
                    self.snapshots_emitted += 1;
                    market.floor = replica.position;
                } else {
                    market.active = None;
                }
            } else if let Some(delta) = delta_output {
                emit_delta = Some(delta);
                market.floor = replica.position;
                self.deltas_emitted += 1;
            }
        }
        self.coverage.update(
            &data.symbol,
            Some(before),
            Some(market.coverage(self.replicas)),
        );
        if let Some((side, price_level, quantity)) = emit_delta {
            outcome.records.push(Record::Delta(PriceLevelUpdateDomain {
                backtest_timestamp: received_at,
                exchange_timestamp: data.exchange_at,
                side,
                price_level,
                quantity,
                symbol: data.symbol,
            }));
        }
        outcome
    }

    pub fn invalidate(
        &mut self,
        source: Source,
        symbols: Option<&BTreeSet<String>>,
        received_at: DateTime<Utc>,
        now: Instant,
    ) -> Vec<Record> {
        let mut records = Vec::new();
        let mut unavailable = 0usize;
        let affected: Vec<_> = match symbols {
            Some(symbols) => symbols.iter().cloned().collect(),
            None => self
                .markets
                .iter()
                .filter(|(_, m)| m.replicas.contains_key(&source))
                .map(|(s, _)| s.clone())
                .collect(),
        };
        for symbol in &affected {
            let Some(market) = self.markets.get_mut(symbol) else {
                continue;
            };
            let before = market.coverage(self.replicas);
            market.remove_replica(source);
            if market.active == Some(source) {
                market.lagging_since = None;
                self.pending_lag.remove(symbol);
                market.active = best_source(market, now, self.failover);
                self.failovers += 1;
                if let Some(active) = market.active {
                    let replica = &market.replicas[&active];
                    records.push(Record::Snapshot(replica.book.snapshot(
                        symbol,
                        received_at,
                        replica.exchange_at,
                    )));
                    market.floor = replica.position;
                    self.snapshots_emitted += 1;
                    tracing::debug!(
                        symbol,
                        source = active,
                        "failed over to ready replica; emitted snapshot"
                    );
                } else {
                    unavailable += 1;
                    tracing::debug!(
                        symbol,
                        "no caught-up healthy replica; suppressing output until recovered"
                    );
                }
            }
            self.coverage
                .update(symbol, Some(before), Some(market.coverage(self.replicas)));
        }
        if unavailable > 0 {
            tracing::warn!(
                source,
                markets = unavailable,
                "markets without a caught-up backup; suppressing output until recovered"
            );
        }
        records
    }

    pub fn check_lagging(&mut self, timestamp: DateTime<Utc>, now: Instant) -> Vec<Record> {
        let mut records = Vec::new();
        let pending: Vec<_> = self.pending_lag.iter().cloned().collect();
        for symbol in pending {
            let Some(market) = self.markets.get_mut(&symbol) else {
                continue;
            };
            let before = market.coverage(self.replicas);
            if !market
                .lagging_since
                .is_some_and(|since| now.duration_since(since) >= self.failover)
            {
                continue;
            }
            if let Some(source) = best_source(market, now, self.failover)
                && market.active != Some(source)
            {
                let replica = &market.replicas[&source];
                records.push(Record::Snapshot(replica.book.snapshot(
                    &symbol,
                    timestamp,
                    replica.exchange_at,
                )));
                market.active = Some(source);
                market.floor = replica.position;
                self.failovers += 1;
                self.snapshots_emitted += 1;
                tracing::debug!(
                    symbol,
                    source,
                    "primary did not catch up during grace period; switched source with snapshot"
                );
            }
            market.lagging_since = None;
            self.coverage
                .update(&symbol, Some(before), Some(market.coverage(self.replicas)));
            self.pending_lag.remove(&symbol);
        }
        records
    }

    pub fn snapshots(&mut self, timestamp: DateTime<Utc>) -> Vec<Record> {
        let records: Vec<_> = self
            .markets
            .iter()
            .filter_map(|(symbol, market)| {
                let replica = market.replicas.get(&market.active?)?;
                Some(Record::Snapshot(replica.book.snapshot(
                    symbol,
                    timestamp,
                    replica.exchange_at,
                )))
            })
            .collect();
        self.snapshots_emitted += records.len() as u64;
        records
    }
}

fn caught_up(position: Option<Position>, floor: Option<Position>) -> bool {
    match (position, floor) {
        (Some(p), Some(f)) => p >= f,
        (_, None) => true,
        (None, Some(_)) => false,
    }
}

fn best_source(market: &Market, now: Instant, freshness: Duration) -> Option<Source> {
    // A quiet market can legitimately have an old last update. Freshness affects ranking,
    // not eligibility, when both replicas are at exactly the same position.
    market
        .replicas
        .iter()
        .filter(|(_, r)| caught_up(r.position, market.floor))
        .max_by_key(|(source, r)| {
            (
                r.position,
                // A delayed source can receive old events continuously. For a
                // clockless feed, rank observed exchange time before arrival time.
                r.position.is_none().then_some(r.exchange_at),
                now.duration_since(r.received) < freshness,
                r.received,
                std::cmp::Reverse(**source),
            )
        })
        .map(|(&source, _)| source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    #[test]
    fn incremental_coverage_matches_books_through_resets_and_lifecycle_changes() {
        for replicas in 1..=3 {
            let mut e = Engine::new(Duration::from_millis(20)).with_replicas(replicas);
            e.set_symbols(&["A".into(), "B".into()].into());
            let now = Instant::now();
            let t = Utc::now();
            for i in 0..120 {
                let source = i % 3;
                let mut data = if i % 7 == 0 {
                    snapshot((i / 7) as u64)
                } else {
                    delta((i / 7 + 1) as u64, if i % 11 == 0 { -9_000_000 } else { 1 })
                };
                if i % 5 == 0 {
                    data.symbol = "B".into();
                }
                if i % 9 == 0 {
                    data.clock = None;
                }
                e.ingest(source, data, t, now);
                if i % 13 == 0 {
                    e.invalidate(source, None, t, now);
                }
                e.check_lagging(t, now + Duration::from_secs(1));
                assert_eq!(e.coverage(), e.scanned_coverage());
            }
            e.change_symbols(&["C".into()].into(), &["B".into()].into());
            assert_eq!(e.coverage(), e.scanned_coverage());
            e.set_symbols(&BTreeSet::new());
            assert_eq!(e.coverage(), Coverage::default());
        }
    }

    #[test]
    fn planned_refreshes_cannot_remove_both_replicas() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(1, snapshot(1), t, n);
        let symbols = ["A".into()].into();
        assert_eq!(e.prepare_refresh(0, &symbols, false, t, n).0, symbols);
        assert!(e.prepare_refresh(1, &symbols, true, t, n).0.is_empty());
        assert_eq!(e.coverage().ready, 1);
        e.ingest(0, snapshot(2), t, n);
        assert_eq!(e.prepare_refresh(1, &symbols, true, t, n).0, symbols);
        assert_eq!(e.coverage(), e.scanned_coverage());
    }
    #[test]
    fn clockless_feed_records_timestamp_fingerprints_and_can_fail_over() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        let mut initial = snapshot(1);
        initial.clock = None;
        initial.exchange_at = DateTime::from_timestamp_millis(999).unwrap();
        e.ingest(0, initial.clone(), t, n);
        e.ingest(1, initial, t, n);
        let mut update = delta(2, 1);
        update.clock = None;
        update.exchange_at = DateTime::from_timestamp_millis(1000).unwrap();
        assert!(e.ingest(1, update.clone(), t, n).observation.is_none());
        update.exchange_at = DateTime::from_timestamp_millis(1001).unwrap();
        let o = e.ingest(1, update, t, n + Duration::from_secs(3));
        assert_eq!(
            o.observation.unwrap().basis,
            TimingBasis::TimestampFingerprint
        );
        assert!(o.records.is_empty());
        assert!(matches!(
            &e.check_lagging(t, n + Duration::from_secs(5))[0],
            Record::Snapshot(_)
        ));
    }
    fn data(clock: u64, change: Change) -> MarketData {
        MarketData {
            symbol: "A".into(),
            exchange_at: Utc::now(),
            clock: Some(clock),
            change,
        }
    }
    fn snapshot(clock: u64) -> MarketData {
        data(
            clock,
            Change::Snapshot(Book::from_levels(vec![(400000, 1000000)], vec![]).unwrap()),
        )
    }
    fn delta(clock: u64, amount: i64) -> MarketData {
        data(
            clock,
            Change::Delta {
                side: Side::Buy,
                price: 400000,
                delta: amount,
            },
        )
    }
    fn engine() -> Engine {
        let mut e = Engine::new(Duration::from_secs(2));
        e.set_symbols(&BTreeSet::from(["A".into()]));
        e
    }
    #[test]
    fn redundant_updates_are_deduplicated_and_deletion_is_zero() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        assert_eq!(e.ingest(0, snapshot(1), t, n).records.len(), 1);
        assert!(e.ingest(1, snapshot(1), t, n).records.is_empty());
        let o = e.ingest(0, delta(2, -1000000), t, n);
        assert!(matches!(&o.records[0], Record::Delta(d) if d.quantity == 0));
        assert!(e.ingest(1, delta(2, -1000000), t, n).records.is_empty());
    }
    #[test]
    fn failover_emits_snapshot_and_will_not_roll_back() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(1, snapshot(1), t, n);
        e.ingest(0, delta(2, 1), t, n);
        assert!(e.invalidate(0, None, t, n).is_empty());
        assert_eq!(e.ready_markets(), 0);
        let o = e.ingest(1, delta(2, 1), t, n);
        assert!(matches!(&o.records[0], Record::Snapshot(s) if s.bids[0].quantity == 1000001));
        let o = e.ingest(1, delta(3, 1), t, n);
        assert!(matches!(&o.records[0], Record::Delta(_)));
    }
    #[test]
    fn a_clock_can_contain_multiple_deltas() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(0, delta(2, 1), t, n);
        let o = e.ingest(0, delta(2, 1), t, n);
        assert!(matches!(&o.records[0], Record::Delta(d) if d.quantity == 1000002));
        assert_eq!(o.observation.unwrap().ordinal, 2);
    }
    #[test]
    fn negative_quantity_repairs_and_redundancy_recovers() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(1, snapshot(2), t, n);
        let o = e.ingest(0, delta(3, -2000000), t, n);
        assert_eq!(o.repairs, vec![(0, "A".into())]);
        assert!(matches!(&o.records[0], Record::Snapshot(_)));
    }
    #[test]
    fn clock_reset_requires_all_replicas_to_resnapshot() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(100), t, n);
        e.ingest(1, snapshot(100), t, n);
        let o = e.ingest(0, delta(1, 1), t, n);
        assert_eq!(o.repairs.len(), 2);
        assert_eq!(e.ready_markets(), 0);
        assert!(e.ingest(1, delta(101, 1), t, n).records.is_empty());
        assert_eq!(e.ingest(0, snapshot(2), t, n).records.len(), 1);
    }

    #[test]
    fn maintenance_reset_is_detected_after_both_sockets_disconnect() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(100), t, n);
        e.ingest(1, snapshot(100), t, n);
        e.invalidate(0, None, t, n);
        e.invalidate(1, None, t, n);
        let reset = e.ingest(0, snapshot(1), t, n);
        assert_eq!(reset.repairs, [(0, "A".into()), (1, "A".into())]);
        assert!(reset.records.is_empty());
        assert_eq!(e.ready_markets(), 0);
        assert_eq!(e.ingest(0, snapshot(2), t, n).records.len(), 1);
        e.ingest(1, snapshot(2), t, n);
        assert_eq!(e.coverage().caught_up_redundant, 1);
        assert!(matches!(
            &e.ingest(0, delta(3, 1), t, n).records[0],
            Record::Delta(_)
        ));
    }

    #[test]
    fn lagged_reconnect_does_not_reset_an_advanced_primary() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(100), t, n);
        e.ingest(1, snapshot(90), t, n);
        e.invalidate(1, None, t, n);
        let reconnect = e.ingest(1, snapshot(95), t, n);
        assert!(reconnect.repairs.is_empty());
        assert!(reconnect.records.is_empty());
        assert_eq!(e.active_source("A"), Some(0));
        assert_eq!(e.markets["A"].floor.unwrap().clock, 100);
    }

    #[test]
    fn continuously_delayed_primary_fails_over_with_and_without_clocks() {
        for clocked in [true, false] {
            let mut e = engine();
            let n = Instant::now();
            let t = Utc::now();
            let mut initial = snapshot(1);
            initial.exchange_at = t;
            if !clocked {
                initial.clock = None;
            }
            e.ingest(0, initial.clone(), t, n);
            e.ingest(1, initial, t, n);
            let mut switched = false;
            for i in 1..=5 {
                let now = n + Duration::from_secs(i);
                let mut backup = delta(i + 10, 2);
                backup.exchange_at = t + chrono::Duration::seconds((i + 10) as i64);
                let mut primary = delta(i + 1, 1);
                primary.exchange_at = t + chrono::Duration::seconds((i + 1) as i64);
                if !clocked {
                    backup.clock = None;
                    primary.clock = None;
                }
                e.ingest(1, backup, t, now);
                e.ingest(0, primary, t, now + Duration::from_millis(100));
                let records = e.check_lagging(t, now + Duration::from_millis(200));
                switched |= records.iter().any(|r| matches!(r, Record::Snapshot(_)));
                if switched {
                    break;
                }
            }
            assert!(switched, "clocked={clocked}");
            assert_eq!(e.active_source("A"), Some(1));
        }
    }

    #[test]
    fn primary_catching_up_cancels_the_lag_timer() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(1, snapshot(1), t, n);
        e.ingest(1, delta(2, 1), t, n + Duration::from_secs(1));
        e.ingest(0, delta(2, 1), t, n + Duration::from_secs(2));
        assert!(e.check_lagging(t, n + Duration::from_secs(4)).is_empty());
        assert_eq!(e.active_source("A"), Some(0));
    }
    #[test]
    fn lagging_primary_switches_to_advanced_backup_with_snapshot() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(1, snapshot(1), t, n);
        let o = e.ingest(1, delta(2, 1), t, n + Duration::from_secs(3));
        assert!(o.records.is_empty());
        assert!(matches!(
            &e.check_lagging(t, n + Duration::from_secs(5))[0],
            Record::Snapshot(_)
        ));
    }
    #[test]
    fn ordinary_duplicate_after_quiet_period_does_not_trigger_failover() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        e.ingest(0, snapshot(1), t, n);
        e.ingest(1, snapshot(1), t, n);
        assert!(
            e.ingest(1, delta(2, 1), t, n + Duration::from_secs(30))
                .records
                .is_empty()
        );
        assert!(matches!(
            &e.ingest(0, delta(2, 1), t, n + Duration::from_millis(30010))
                .records[0],
            Record::Delta(_)
        ));
        assert!(e.check_lagging(t, n + Duration::from_secs(33)).is_empty());
        assert_eq!(e.failovers, 0);
    }
    #[test]
    fn identical_clockless_backup_snapshot_is_not_evidence_of_a_stall() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        let mut initial = snapshot(1);
        initial.clock = None;
        initial.exchange_at = t;
        e.ingest(0, initial.clone(), t, n);
        initial.exchange_at = t + chrono::Duration::seconds(1);
        e.ingest(1, initial, t, n + Duration::from_secs(30));
        assert!(e.check_lagging(t, n + Duration::from_secs(33)).is_empty());
        assert_eq!(e.failovers, 0);
    }
    #[test]
    fn removed_markets_and_uninitialized_deltas_are_ignored() {
        let mut e = engine();
        let n = Instant::now();
        let t = Utc::now();
        assert!(e.ingest(0, delta(1, 1), t, n).records.is_empty());
        e.set_symbols(&BTreeSet::new());
        assert!(e.ingest(0, snapshot(1), t, n).records.is_empty());
    }
}
