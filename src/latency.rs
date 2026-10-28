use std::collections::{BTreeMap, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::{Side, engine::Source};

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimingBasis {
    #[default]
    ExchangeClock,
    TimestampFingerprint,
}

/// A connection can start partway through a timestamp bucket. Skip that first
/// bucket so its ordinal cannot be mistaken for another connection's full prefix.
#[derive(Default)]
pub struct TimestampOrdinals {
    bucket: Option<i64>,
    high_water: i64,
    ordinal: u32,
    warmed: bool,
}
impl TimestampOrdinals {
    pub fn observe(&mut self, timestamp: i64) -> Option<(u64, u32)> {
        if timestamp <= 0 {
            return None;
        }
        match self.bucket {
            Some(previous) if previous == timestamp => {
                self.ordinal = self.ordinal.checked_add(1)?;
            }
            Some(_) => {
                self.warmed = timestamp > self.high_water;
                self.bucket = Some(timestamp);
                self.ordinal = 1;
            }
            None => {
                self.bucket = Some(timestamp);
                self.ordinal = 1;
            }
        }
        self.high_water = self.high_water.max(timestamp);
        self.warmed.then_some((timestamp as u64, self.ordinal))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub symbol: String,
    pub source: Source,
    #[serde(default)]
    pub basis: TimingBasis,
    pub clock: u64,
    pub ordinal: u32,
    pub side: Side,
    pub price: i64,
    pub delta: i64,
    pub received_us: i64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Key {
    symbol: String,
    basis: TimingBasis,
    clock: u64,
    ordinal: u32,
    side: Side,
    price: i64,
    delta: i64,
}

#[derive(Default, Debug, Serialize)]
pub struct Summary {
    pub observations: u64,
    pub matched_updates: u64,
    pub exchange_clock_matches: u64,
    pub timestamp_fingerprint_matches: u64,
    pub unmatched_updates: u64,
    pub sources: BTreeMap<Source, SourceSummary>,
}

#[derive(Default, Debug, Serialize)]
pub struct SourceSummary {
    pub matched_samples: usize,
    pub first_arrivals: usize,
    pub median_delay_us: i64,
    pub p95_delay_us: i64,
    pub p99_delay_us: i64,
    pub max_delay_us: i64,
}

/// Bounded matching window. Compare identical exchange events, never unrelated seq numbers.
pub struct Analyzer {
    pending: HashMap<Key, BTreeMap<Source, i64>>,
    order: VecDeque<Key>,
    samples: BTreeMap<Source, Vec<i64>>,
    summary: Summary,
    capacity: usize,
}

impl Analyzer {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            pending: HashMap::new(),
            order: VecDeque::new(),
            samples: BTreeMap::new(),
            summary: Summary::default(),
            capacity,
        }
    }
    pub fn push(&mut self, o: Observation) {
        self.summary.observations += 1;
        let key = Key {
            symbol: o.symbol,
            basis: o.basis,
            clock: o.clock,
            ordinal: o.ordinal,
            side: o.side,
            price: o.price,
            delta: o.delta,
        };
        if !self.pending.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.pending
            .entry(key)
            .or_default()
            .entry(o.source)
            .or_insert(o.received_us);
        while self.order.len() > self.capacity {
            if let Some(key) = self.order.pop_front() {
                self.finish_key(&key);
            }
        }
    }
    fn finish_key(&mut self, key: &Key) {
        let Some(times) = self.pending.remove(key) else {
            return;
        };
        if times.len() < 2 {
            self.summary.unmatched_updates += 1;
            return;
        }
        self.summary.matched_updates += 1;
        match key.basis {
            TimingBasis::ExchangeClock => self.summary.exchange_clock_matches += 1,
            TimingBasis::TimestampFingerprint => self.summary.timestamp_fingerprint_matches += 1,
        }
        let first = *times.values().min().unwrap();
        for (source, received) in times {
            // Bounded prefix: cap memory for multi-hour runs; report this limit in docs.
            let samples = self.samples.entry(source).or_default();
            if samples.len() < 1_000_000 {
                samples.push(received.saturating_sub(first));
            }
        }
    }
    pub fn finish(mut self) -> Summary {
        while let Some(key) = self.order.pop_front() {
            self.finish_key(&key);
        }
        for (source, mut samples) in self.samples {
            samples.sort_unstable();
            let percentile = |p: usize| samples[((samples.len() - 1) * p) / 100];
            self.summary.sources.insert(
                source,
                SourceSummary {
                    matched_samples: samples.len(),
                    first_arrivals: samples.iter().filter(|&&s| s == 0).count(),
                    median_delay_us: percentile(50),
                    p95_delay_us: percentile(95),
                    p99_delay_us: percentile(99),
                    max_delay_us: *samples.last().unwrap(),
                },
            );
        }
        self.summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compares_same_event_and_deduplicates_per_source() {
        let mut a = Analyzer::new(10);
        let o = Observation {
            symbol: "A".into(),
            source: 0,
            basis: TimingBasis::ExchangeClock,
            clock: 1,
            ordinal: 1,
            side: Side::Buy,
            price: 4,
            delta: 1,
            received_us: 100,
        };
        a.push(o.clone());
        a.push(o.clone());
        a.push(Observation {
            source: 1,
            received_us: 118,
            ..o.clone()
        });
        a.push(Observation { clock: 2, ..o });
        let s = a.finish();
        assert_eq!(s.matched_updates, 1);
        assert_eq!(s.unmatched_updates, 1);
        assert_eq!(s.sources[&1].median_delay_us, 18);
    }

    #[test]
    fn timestamp_matching_skips_partial_initial_bucket_and_regressions() {
        let mut t = TimestampOrdinals::default();
        assert_eq!(t.observe(1000), None);
        assert_eq!(t.observe(1000), None);
        assert_eq!(t.observe(1001), Some((1001, 1)));
        assert_eq!(t.observe(1001), Some((1001, 2)));
        assert_eq!(t.observe(999), None);
        assert_eq!(t.observe(1000), None);
        assert_eq!(t.observe(1002), Some((1002, 1)));
    }
}
