use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// One deadline per published book. Initial snapshots naturally stagger the
/// minute schedule; bounded batches prevent a universe-wide allocation pause.
pub struct Snapshots {
    period: Duration,
    deadlines: BTreeMap<String, Instant>,
    due: BTreeSet<(Instant, String)>,
}
impl Snapshots {
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            deadlines: BTreeMap::new(),
            due: BTreeSet::new(),
        }
    }
    pub fn published(&mut self, symbol: &str, now: Instant) {
        self.at(symbol, now + self.period);
    }
    pub fn retry(&mut self, symbol: &str, now: Instant) {
        self.at(symbol, now + self.period.min(Duration::from_secs(1)));
    }
    fn at(&mut self, symbol: &str, at: Instant) {
        if let Some(old) = self.deadlines.insert(symbol.into(), at) {
            self.due.remove(&(old, symbol.into()));
        }
        self.due.insert((at, symbol.into()));
    }
    pub fn retain(&mut self, symbols: &BTreeSet<String>) {
        self.deadlines.retain(|symbol, _| symbols.contains(symbol));
        self.due.retain(|(_, symbol)| symbols.contains(symbol));
    }
    pub fn remove(&mut self, symbol: &str) {
        if let Some(old) = self.deadlines.remove(symbol) {
            self.due.remove(&(old, symbol.into()));
        }
    }
    pub fn take_due(&mut self, now: Instant, limit: usize) -> Vec<String> {
        let mut symbols = Vec::new();
        while symbols.len() < limit && self.due.first().is_some_and(|(at, _)| *at <= now) {
            let (_, symbol) = self.due.pop_first().unwrap();
            self.deadlines.remove(&symbol);
            symbols.push(symbol);
        }
        symbols
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deadlines_are_staggered_bounded_and_reset_without_duplicates() {
        let now = Instant::now();
        let mut s = Snapshots::new(Duration::from_secs(60));
        for i in 0..1000 {
            s.published(&format!("S{i:04}"), now);
        }
        s.published("S0000", now + Duration::from_secs(10));
        s.retain(&(0..999).map(|i| format!("S{i:04}")).collect());
        assert!(s.take_due(now + Duration::from_secs(59), 32).is_empty());
        let mut due = BTreeSet::new();
        loop {
            let batch = s.take_due(now + Duration::from_secs(60), 32);
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= 32);
            for symbol in batch {
                assert!(due.insert(symbol));
            }
        }
        assert_eq!(due.len(), 998);
        assert_eq!(s.take_due(now + Duration::from_secs(70), 32), ["S0000"]);
    }
}
