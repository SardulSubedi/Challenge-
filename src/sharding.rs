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
    pub fn change_symbols(
        &mut self,
        added: &BTreeSet<String>,
        removed: &BTreeSet<String>,
    ) -> BTreeSet<usize> {
        let mut changed = BTreeSet::new();
        for symbol in removed {
            if let Some((index, shard)) = self
                .assignments
                .iter_mut()
                .enumerate()
                .find(|(_, s)| s.contains(symbol))
            {
                shard.remove(symbol);
                changed.insert(index);
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
            changed.insert(shard);
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn incremental_changes_touch_only_the_affected_shard() {
        let mut s = Shards::new(2);
        s.reconcile(&["A", "B", "C", "D"].map(String::from).into());
        let old = s.assignments.clone();
        assert_eq!(
            s.change_symbols(&["E".into()].into(), &["A".into()].into()),
            [0].into()
        );
        assert_eq!(s.assignments[1], old[1]);
        assert_eq!(s.assignments[0], ["B".into(), "E".into()].into());
        assert_eq!(
            s.change_symbols(&["F".into()].into(), &BTreeSet::new()),
            [2].into()
        );
        assert!(s.assignments.iter().all(|s| s.len() <= 2));
    }
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
