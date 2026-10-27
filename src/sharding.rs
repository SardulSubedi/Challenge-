use std::collections::BTreeSet;

/// Stable, capacity-bounded placement: existing symbols stay on their shard.
pub struct Shards {
    capacity: usize,
    pub assignments: Vec<BTreeSet<String>>,
}

impl Shards {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            assignments: Vec::new(),
        }
    }
    pub fn reconcile(&mut self, symbols: &BTreeSet<String>) {
        for shard in &mut self.assignments {
            shard.retain(|s| symbols.contains(s));
        }
        let assigned: BTreeSet<_> = self
            .assignments
            .iter()
            .flat_map(|s| s.iter().cloned())
            .collect();
        for symbol in symbols.difference(&assigned) {
            let shard = self
                .assignments
                .iter()
                .enumerate()
                .filter(|(_, s)| s.len() < self.capacity)
                .min_by_key(|(index, s)| (s.len(), *index))
                .map(|(index, _)| index)
                .unwrap_or_else(|| {
                    self.assignments.push(BTreeSet::new());
                    self.assignments.len() - 1
                });
            self.assignments[shard].insert(symbol.clone());
        }
    }
    pub fn change_symbols(&mut self, added: &BTreeSet<String>, removed: &BTreeSet<String>) {
        for symbol in removed {
            if let Some(shard) = self.assignments.iter_mut().find(|s| s.contains(symbol)) {
                shard.remove(symbol);
            }
        }
        for symbol in added {
            let shard = self
                .assignments
                .iter()
                .enumerate()
                .filter(|(_, s)| s.len() < self.capacity)
                .min_by_key(|(i, s)| (s.len(), *i))
                .map(|(i, _)| i)
                .unwrap_or_else(|| {
                    self.assignments.push(BTreeSet::new());
                    self.assignments.len() - 1
                });
            self.assignments[shard].insert(symbol.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_placement_removal_and_capacity() {
        let mut s = Shards::new(2);
        s.reconcile(&["A", "B", "C", "D", "E"].map(String::from).into());
        let original = s.assignments.clone();
        assert_eq!(original.len(), 3);
        s.reconcile(&["A", "B", "C", "D", "E", "F"].map(String::from).into());
        for (old, new) in original.iter().zip(&s.assignments) {
            assert!(old.is_subset(new));
        }
        assert!(s.assignments.iter().all(|s| s.len() <= 2));
        s.reconcile(&["A", "F"].map(String::from).into());
        assert_eq!(s.assignments.iter().map(BTreeSet::len).sum::<usize>(), 2);
    }
}
