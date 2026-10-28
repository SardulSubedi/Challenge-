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
                let first = resolution.ok_or_else(|| anyhow::anyhow!("lifecycle resolver stopped"))?;
                let mut latest = BTreeMap::new();
                coalesce(&mut latest, first);
                for _ in 0..127 {
                    match resolved.try_recv() {Ok(r)=>coalesce(&mut latest,r),Err(_)=>break}
                }
                let mut added=BTreeSet::new(); let mut removed=BTreeSet::new();
                for (_,r) in latest {
                    if !overlay.resolved(r.symbol.clone(),r.live,r.observed) || known.contains(&r.symbol)==r.live {continue;}
                    tracing::info!(symbol=%r.symbol,live=r.live,"lifecycle market change resolved");
                    if r.live {known.insert(r.symbol.clone());added.insert(r.symbol);}
                    else {known.remove(&r.symbol);removed.insert(r.symbol);}
                }
                if added.is_empty() && removed.is_empty() {continue;}
                Update {added,removed,discovery_completed:None}
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = updates.send(update) => if result.is_err() { return Ok(()); },
        }
    }
}

fn coalesce(latest: &mut BTreeMap<String, Resolved>, r: Resolved) {
    if latest
        .get(&r.symbol)
        .is_none_or(|old| old.observed <= r.observed)
    {
        latest.insert(r.symbol.clone(), r);
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
        // Bound work as well as queue size, including duplicate-only bursts.
        for _ in 0..16_384 {
            let Ok(symbol) = hints.try_recv() else {
                break;
            };
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
        let size = exchange.resolution_batch_size().clamp(1, 16);
        let batch: Vec<_> = (0..size).filter_map(|_| pending.pop_front()).collect();
        for symbol in &batch {
            queued.remove(symbol);
        }
        let observed = Instant::now();
        let result = tokio::select! { _ = cancel.cancelled() => break, result = exchange.resolve_symbols(&batch) => result };
        let delay = match result {
            Ok(live) => {
                tracing::debug!(
                    markets = batch.len(),
                    live = live.len(),
                    elapsed_ms = observed.elapsed().as_millis() as u64,
                    "lifecycle lookup batch resolved"
                );
                for symbol in batch {
                    let live = live.contains(&symbol);
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        result = results.send(Resolved {symbol,live,observed}) => if result.is_err() {return;},
                    }
                }
                Duration::from_millis(200)
            }
            Err(error) => {
                tracing::warn!(markets=batch.len(), %error,"lifecycle lookup failed; requesting full discovery and retrying");
                rescan.notify_one();
                for symbol in batch {
                    if queued.insert(symbol.clone()) {
                        pending.push_back(symbol);
                    }
                }
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
    #[tokio::test]
    async fn lifecycle_burst_resolves_without_one_request_per_market() {
        use crate::exchange::Kalshi;
        use axum::{
            Json, Router,
            extract::{Query, State},
            routing::get,
        };
        use std::{
            future::IntoFuture,
            sync::atomic::{AtomicUsize, Ordering},
        };
        async fn markets(
            State(calls): State<Arc<AtomicUsize>>,
            Query(query): Query<BTreeMap<String, String>>,
        ) -> Json<serde_json::Value> {
            calls.fetch_add(1, Ordering::SeqCst);
            let rows: Vec<_> = query["tickers"]
                .split(',')
                .map(|ticker| serde_json::json!({"ticker":ticker,"status":"active"}))
                .collect();
            Json(serde_json::json!({"markets":rows,"cursor":""}))
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/markets", get(markets))
                    .with_state(calls.clone()),
            )
            .into_future(),
        );
        let adapter = Arc::new(
            Kalshi::new(format!("http://{address}"), "ws://localhost".into(), None).unwrap(),
        );
        let (hints, rx) = mpsc::channel(64);
        let (results, mut resolved) = mpsc::channel(64);
        for i in 0..16 {
            hints.send(format!("NEW-{i:02}")).await.unwrap();
        }
        let cancel = CancellationToken::new();
        let task = tokio::spawn(resolve(
            adapter,
            rx,
            results,
            Arc::new(Notify::new()),
            cancel.clone(),
        ));
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            let mut symbols = BTreeSet::new();
            while symbols.len() < 16 {
                let r = resolved.recv().await.unwrap();
                assert!(r.live);
                symbols.insert(r.symbol);
            }
            symbols
        })
        .await;
        cancel.cancel();
        task.await.unwrap();
        server.abort();
        assert_eq!(
            received
                .expect("a lifecycle burst must not wait for 16 sequential paced requests")
                .len(),
            16
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ready_lifecycle_results_preserve_net_changes_and_newest_observation() {
        let now = Instant::now();
        let (_scan_tx, scan_rx) = mpsc::channel(1);
        let (tx, rx) = mpsc::channel(16);
        let (updates, mut out) = mpsc::channel(4);
        for (symbol, live, age) in [
            ("A", false, 1),
            ("A", true, 2),
            ("B", true, 4),
            ("B", false, 3),
            ("C", false, 5),
        ] {
            tx.send(Resolved {
                symbol: symbol.into(),
                live,
                observed: now + Duration::from_millis(age),
            })
            .await
            .unwrap();
        }
        let cancel = CancellationToken::new();
        let task = tokio::spawn(reconcile(
            ["A".into(), "C".into()].into(),
            scan_rx,
            rx,
            updates,
            cancel.clone(),
        ));
        let update = tokio::time::timeout(Duration::from_secs(1), out.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update.added, ["B".into()].into());
        assert_eq!(update.removed, ["C".into()].into());
        assert!(update.discovery_completed.is_none());
        tokio::task::yield_now().await;
        assert!(out.try_recv().is_err());
        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_lifecycle_batch_is_retried_intact_and_requests_a_scan() {
        use crate::exchange::Kalshi;
        use axum::{
            Json, Router,
            extract::{Query, State},
            http::StatusCode,
            response::IntoResponse,
            routing::get,
        };
        use std::{
            future::IntoFuture,
            sync::atomic::{AtomicUsize, Ordering},
        };
        async fn markets(
            State((calls, release)): State<(Arc<AtomicUsize>, Arc<Notify>)>,
            Query(query): Query<BTreeMap<String, String>>,
        ) -> axum::response::Response {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            release.notified().await;
            let markets: Vec<_> = query["tickers"]
                .split(',')
                .map(|ticker| serde_json::json!({"ticker":ticker,"status":"active"}))
                .collect();
            Json(serde_json::json!({"markets":markets,"cursor":""})).into_response()
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/markets", get(markets))
                    .with_state((calls.clone(), release.clone())),
            )
            .into_future(),
        );
        let adapter = Arc::new(
            Kalshi::new(format!("http://{address}"), "ws://localhost".into(), None).unwrap(),
        );
        let (tx, rx) = mpsc::channel(16);
        let (results, mut out) = mpsc::channel(16);
        for i in 0..16 {
            tx.send(format!("RETRY-{i:02}")).await.unwrap();
        }
        let rescan = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(resolve(
            adapter,
            rx,
            results,
            rescan.clone(),
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(2), rescan.notified())
            .await
            .unwrap();
        assert!(
            out.try_recv().is_err(),
            "failed requests must not mark markets closed"
        );
        release.notify_one();
        let seen = tokio::time::timeout(Duration::from_secs(4), async {
            let mut seen = BTreeSet::new();
            while seen.len() < 16 {
                let r = out.recv().await.unwrap();
                assert!(r.live);
                seen.insert(r.symbol);
            }
            seen
        })
        .await
        .unwrap();
        assert_eq!(seen.len(), 16);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        cancel.cancel();
        task.await.unwrap();
        server.abort();
    }

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
