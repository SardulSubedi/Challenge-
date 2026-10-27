use crate::exchange::Exchange;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

pub(crate) struct Resolved {
    pub symbol: String,
    pub live: bool,
    pub observed: Instant,
}

pub(crate) struct Update {
    pub added: BTreeSet<String>,
    pub removed: BTreeSet<String>,
    pub discovery_completed: Option<Instant>,
}

/// Comparing and dropping complete REST universes can touch hundreds of
/// thousands of strings. Keep that work off the serial orderbook engine.
pub(crate) async fn reconcile(
    mut known: BTreeSet<String>,
    mut scans: mpsc::Receiver<(BTreeSet<String>, Instant, Instant)>,
    mut resolved: mpsc::Receiver<Resolved>,
    updates: mpsc::Sender<Update>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut overlay = Overlay::default();
    loop {
        let update = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            scan = scans.recv() => {
                if cancel.is_cancelled() { return Ok(()); }
                let (mut symbols, started, completed) = scan.ok_or_else(|| anyhow::anyhow!("discovery task stopped"))?;
                overlay.apply(&mut symbols, started);
                let update = Update {
                    added: symbols.difference(&known).cloned().collect(),
                    removed: known.difference(&symbols).cloned().collect(),
                    discovery_completed: Some(completed),
                };
                known = symbols;
                update
            }
            resolution = resolved.recv() => {
                if cancel.is_cancelled() { return Ok(()); }
                let r = resolution.ok_or_else(|| anyhow::anyhow!("lifecycle resolver stopped"))?;
                if !overlay.resolved(r.symbol.clone(), r.live, r.observed) || known.contains(&r.symbol) == r.live { continue; }
                tracing::info!(symbol = %r.symbol, live = r.live, "lifecycle market change resolved");
                if r.live { known.insert(r.symbol.clone()); } else { known.remove(&r.symbol); }
                Update {
                    added: if r.live { [r.symbol.clone()].into() } else { BTreeSet::new() },
                    removed: if r.live { BTreeSet::new() } else { [r.symbol].into() },
                    discovery_completed: None,
                }
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = updates.send(update) => if result.is_err() { return Ok(()); },
        }
    }
}

/// Bounded, coalesced REST work stays outside the orderbook engine. A complete
/// scan remains the authority after a missed hint, lookup failure, or reconnect.
pub(crate) async fn resolve(
    exchange: Arc<dyn Exchange>,
    mut hints: mpsc::Receiver<String>,
    results: mpsc::Sender<Resolved>,
    rescan: Arc<Notify>,
    cancel: CancellationToken,
) {
    let mut pending = VecDeque::new();
    let mut queued = HashSet::new();
    loop {
        if pending.is_empty() {
            let symbol = tokio::select! { _ = cancel.cancelled() => break, s = hints.recv() => match s { Some(s) => s, None => break } };
            queued.insert(symbol.clone());
            pending.push_back(symbol);
        }
        while let Ok(symbol) = hints.try_recv() {
            if queued.contains(&symbol) {
                continue;
            }
            if queued.len() >= 16_384 {
                tracing::warn!("lifecycle lookup backlog full; requesting complete discovery");
                rescan.notify_one();
                break;
            }
            queued.insert(symbol.clone());
            pending.push_back(symbol);
        }
        let symbol = pending.pop_front().unwrap();
        queued.remove(&symbol);
        let observed = Instant::now();
        let result = tokio::select! { _ = cancel.cancelled() => break, result = exchange.resolve_symbol(&symbol) => result };
        let delay = match result {
            Ok(live) => {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = results.send(Resolved { symbol, live, observed }) => if result.is_err() { break; },
                }
                Duration::from_millis(200)
            }
            Err(error) => {
                tracing::warn!(%symbol, %error, "lifecycle lookup failed; requesting full discovery and retrying");
                rescan.notify_one();
                queued.insert(symbol.clone());
                pending.push_back(symbol);
                if error
                    .downcast_ref::<reqwest::Error>()
                    .and_then(|e| e.status())
                    .is_some_and(|s| s.as_u16() == 429)
                {
                    Duration::from_secs(60)
                } else {
                    Duration::from_secs(1)
                }
            }
        };
        tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
    }
}

/// Reapply targeted observations newer than a full scan's start. A slow scan
/// must not undo an activation/deactivation resolved while its pages arrived.
#[derive(Default)]
pub(crate) struct Overlay {
    changes: BTreeMap<String, (bool, Instant)>,
    last_scan_started: Option<Instant>,
}

impl Overlay {
    pub fn resolved(&mut self, symbol: String, live: bool, observed: Instant) -> bool {
        if self.last_scan_started.is_some_and(|start| observed < start)
            || self
                .changes
                .get(&symbol)
                .is_some_and(|(_, previous)| *previous > observed)
        {
            return false;
        }
        self.changes.insert(symbol, (live, observed));
        true
    }

    pub fn apply(&mut self, symbols: &mut BTreeSet<String>, started: Instant) {
        self.last_scan_started = Some(started);
        self.changes.retain(|_, (_, observed)| *observed >= started);
        for (symbol, (live, _)) in &self.changes {
            if *live {
                symbols.insert(symbol.clone());
            } else {
                symbols.remove(symbol);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn slow_scans_and_old_responses_cannot_undo_newer_lifecycle_observations() {
        let start = Instant::now();
        let mut overlay = Overlay::default();
        assert!(overlay.resolved("NEW".into(), true, start + Duration::from_secs(1)));
        assert!(overlay.resolved("CLOSED".into(), false, start + Duration::from_secs(2)));
        let mut symbols = ["CLOSED".into(), "OTHER".into()].into();
        overlay.apply(&mut symbols, start);
        assert_eq!(symbols, ["NEW".into(), "OTHER".into()].into());
        assert!(!overlay.resolved("CLOSED".into(), true, start + Duration::from_secs(1)));
        overlay.apply(&mut symbols, start + Duration::from_secs(3));
        assert!(overlay.changes.is_empty());
        assert!(!overlay.resolved("NEW".into(), false, start));
    }
}
