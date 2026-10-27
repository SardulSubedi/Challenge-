use std::collections::BTreeSet;

use crate::engine::Coverage;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct State {
    pub ready: bool,
    pub redundant: bool,
    pub caught_up: bool,
    pub clocked: bool,
}

/// Update counters when one market changes; health reporting never walks books.
#[derive(Default)]
pub(crate) struct Index {
    counts: [usize; 5],
    missing: BTreeSet<String>,
    under_replicated: BTreeSet<String>,
}

impl Index {
    pub fn update(&mut self, symbol: &str, before: Option<State>, after: Option<State>) {
        if before == after {
            return;
        }
        let values = |state: Option<State>| {
            let s = state.unwrap_or_default();
            [
                state.is_some(),
                s.ready,
                s.redundant,
                s.caught_up,
                s.clocked,
            ]
        };
        for (count, (old, new)) in self
            .counts
            .iter_mut()
            .zip(values(before).into_iter().zip(values(after)))
        {
            *count = *count - usize::from(old) + usize::from(new);
        }
        for (set, old, new) in [
            (
                &mut self.missing,
                before.is_some_and(|s| !s.ready),
                after.is_some_and(|s| !s.ready),
            ),
            (
                &mut self.under_replicated,
                before.is_some_and(|s| !s.redundant),
                after.is_some_and(|s| !s.redundant),
            ),
        ] {
            if old && !new {
                set.remove(symbol);
            }
            if new && !old {
                set.insert(symbol.to_owned());
            }
        }
    }

    pub fn report(&self) -> Coverage {
        Coverage {
            markets: self.counts[0],
            ready: self.counts[1],
            redundant: self.counts[2],
            caught_up_redundant: self.counts[3],
            clocked_markets: self.counts[4],
            missing: self.missing.iter().cloned().collect(),
            under_replicated: self.under_replicated.iter().cloned().collect(),
        }
    }
}
