use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::{interval, timeout},
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    engine::Source,
    exchange::{Change, Command, Exchange, MarketData, Payload, Sequences},
    monitor::Activity,
    transport::{self, Socket},
};

pub enum Event {
    Snapshot {
        source: Source,
        symbols: Vec<String>,
        received_at: DateTime<Utc>,
    },
    Lifecycle(String),
    Rescan,
    Refresh {
        source: Source,
        symbols: BTreeSet<String>,
        all: bool,
        reply: oneshot::Sender<BTreeSet<String>>,
    },
    Data {
        source: Source,
        data: MarketData,
        received_at: DateTime<Utc>,
        now: Instant,
    },
    Invalid {
        source: Source,
        symbols: Option<BTreeSet<String>>,
    },
}

/// Shared connection pacing and an account-wide cooldown after HTTP 429.
/// The pacing lock is released before starting the network handshake.
pub struct ConnectGate {
    next: Mutex<tokio::time::Instant>,
    blocked_until: std::sync::Mutex<tokio::time::Instant>,
    interval: Duration,
}

impl Default for ConnectGate {
    fn default() -> Self {
        Self::new(Duration::from_millis(500))
    }
}
impl ConnectGate {
    pub fn new(interval: Duration) -> Self {
        Self {
            next: Mutex::new(tokio::time::Instant::now()),
            blocked_until: std::sync::Mutex::new(tokio::time::Instant::now()),
            interval,
        }
    }
    pub fn penalize(&self, delay: Duration) {
        let mut blocked = self.blocked_until.lock().unwrap();
        *blocked = (*blocked).max(tokio::time::Instant::now() + delay);
    }
    pub(crate) async fn wait(&self, cancel: &CancellationToken) -> Result<()> {
        let mut next = tokio::select! { _ = cancel.cancelled() => bail!("shutdown"), next = self.next.lock() => next };
        loop {
            let slot = (*next).max(*self.blocked_until.lock().unwrap());
            tokio::select! { _ = cancel.cancelled() => bail!("shutdown"), _ = tokio::time::sleep_until(slot) => {} }
            if tokio::time::Instant::now() < *self.blocked_until.lock().unwrap() {
                continue;
            }
            *next = tokio::time::Instant::now() + self.interval;
            return Ok(());
        }
    }
}

pub struct Worker {
    pub source: Source,
    pub config: Arc<Config>,
    pub exchange: Arc<dyn Exchange>,
    pub desired: watch::Receiver<BTreeSet<String>>,
    pub repairs: mpsc::UnboundedReceiver<String>,
    pub snapshots: mpsc::Receiver<Vec<String>>,
    pub events: mpsc::Sender<Event>,
    pub cancel: CancellationToken,
    pub gate: Arc<ConnectGate>,
}

struct Pending {
    id: u64,
    command: Command,
    sent: Instant,
}

impl Worker {
    pub async fn run_after(self, delay: Duration) -> Result<()> {
        tokio::select! {
            _ = self.cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {},
        }
        self.run().await
    }

    pub async fn run(mut self) -> Result<()> {
        let mut failures = 0u32;
        let mut reconnect = false;
        loop {
            while self.desired.borrow().is_empty() {
                tokio::select! {
                    _ = self.cancel.cancelled() => return Ok(()),
                    changed = self.desired.changed() => if changed.is_err() { return Ok(()); },
                }
            }
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            if self.gate.wait(&self.cancel).await.is_err() {
                return Ok(());
            }
            let started = Instant::now();
            let result = self.connect_and_run(reconnect).await;
            reconnect = true;
            // All books on this transport become invalid before any new connection can send data.
            if self
                .events
                .send(Event::Invalid {
                    source: self.source,
                    symbols: None,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            match result {
                Ok(()) => failures = 0,
                Err(error) => {
                    if let Some(tokio_tungstenite::tungstenite::Error::Http(response)) =
                        error.downcast_ref::<tokio_tungstenite::tungstenite::Error>()
                    {
                        match response.status().as_u16() {
                            401 | 403 => return Err(error.context("Kalshi authentication rejected; check key ID, private key, and production/demo environment")),
                            429 => {
                                let delay = response.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(60).clamp(1, 300);
                                self.gate.penalize(Duration::from_secs(delay));
                                tracing::warn!(delay, "exchange rate-limited handshake; pausing ALL connection attempts");
                            }
                            _ => {},
                        }
                    }
                    tracing::warn!(source = self.source, %error, "socket failed; books invalidated; reconnecting");
                    failures = if started.elapsed() > Duration::from_secs(60) {
                        1
                    } else {
                        failures.saturating_add(1)
                    };
                }
            }
            let cap = (1u64 << failures.min(6)).min(60);
            let delay = Duration::from_millis(rand::random::<u64>() % (cap * 1000) + 250);
            tokio::select! { _ = self.cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(delay) => {} }
        }
    }

    async fn connect_and_run(&mut self, reconnect: bool) -> Result<()> {
        let request = self.exchange.request()?;
        let cancel = self.cancel.clone();
        let (mut socket, connection) = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = timeout(Duration::from_secs(20), transport::connect(request, None)) => result??,
        };
        tracing::info!(
            source = self.source,
            symbols = self.desired.borrow().len(),
            local_addr = %connection.local_addr,
            peer_addr = %connection.peer_addr,
            handshake_us = connection.handshake_us,
            "socket connected"
        );
        let result = self.session(&mut socket, reconnect).await;
        // Bound close: an unresponsive peer must not prevent shutdown/recovery.
        let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
        result
    }

    async fn session(&mut self, socket: &mut Socket, reconnect: bool) -> Result<()> {
        // Two existing orderbook sockets carry redundant global lifecycle feeds;
        // this consumes no additional connections from the account budget.
        let mut lifecycle_pending = if self.source < self.config.replicas {
            if let Some(command) = self.exchange.lifecycle_subscription(1) {
                timeout(
                    Duration::from_secs(5),
                    socket.send(Message::Text(command.into())),
                )
                .await??;
                Some(Instant::now())
            } else {
                None
            }
        } else {
            None
        };
        let mut sid = None;
        let mut confirmed = BTreeSet::new();
        let mut repair = BTreeSet::new();
        let mut activities = BTreeMap::<String, Activity>::new();
        let mut seq = Sequences::default();
        // Kalshi omits zero IDs in acknowledgements. Reserve positive ID 1 for
        // lifecycle and start orderbook commands at 2 on those sockets.
        let mut next_id = if lifecycle_pending.is_some() {
            2u64
        } else {
            1u64
        };
        let mut pending: Option<Pending> = None;
        let mut reconcile = true;
        let mut last_frame = Instant::now();
        let mut health = interval(Duration::from_secs(1));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat = interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut recycle_at = Instant::now()
            + Duration::from_secs(
                self.config.recycle_secs
                    + (self.source % self.config.replicas) as u64
                        * (self.config.recycle_secs / self.config.replicas as u64),
            );

        loop {
            // One in-flight command. Reconcile on control changes/ACKs, not on
            // every market frame: cloning and diffing thousands of symbols here
            // turns a constant-size update into work proportional to shard size.
            if pending.is_none() && reconcile {
                reconcile = false;
                let desired = self.desired.borrow().clone();
                if desired.is_empty() {
                    return Ok(());
                }
                let remove: Vec<_> = confirmed
                    .iter()
                    .filter(|s| !desired.contains(*s) || repair.contains(*s))
                    .cloned()
                    .collect();
                let add: Vec<_> = desired.difference(&confirmed).cloned().collect();
                let command = match sid {
                    None => Some(Command::Subscribe(desired.iter().cloned().collect())),
                    Some(sid) if !remove.is_empty() => Some(Command::Delete {
                        sid,
                        symbols: remove,
                    }),
                    Some(sid) if !add.is_empty() => Some(Command::Add { sid, symbols: add }),
                    _ => None,
                };
                if let Some(command) = command {
                    let now = Instant::now();
                    match &command {
                        Command::Subscribe(symbols) | Command::Add { symbols, .. } => {
                            for symbol in symbols {
                                repair.remove(symbol);
                                activities.insert(symbol.clone(), Activity::new(now));
                            }
                        }
                        Command::Delete { symbols, .. } => {
                            self.emit(Event::Invalid {
                                source: self.source,
                                symbols: Some(symbols.iter().cloned().collect()),
                            })
                            .await?;
                            for symbol in symbols {
                                activities.remove(symbol);
                            }
                        }
                    }
                    let text = self.exchange.command(next_id, &command)?;
                    timeout(
                        Duration::from_secs(5),
                        socket.send(Message::Text(text.into())),
                    )
                    .await??;
                    pending = Some(Pending {
                        id: next_id,
                        command,
                        sent: now,
                    });
                    next_id = next_id.checked_add(1).context("command id overflow")?;
                }
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                changed = self.desired.changed() => {
                    if changed.is_err() { return Ok(()); }
                    reconcile = true;
                },
                Some(symbol) = self.repairs.recv() => { repair.insert(symbol); reconcile = true; },
                Some(symbols) = self.snapshots.recv() => {
                    // This marker follows every frame already captured by this worker.
                    // Later frames receive timestamps after the marker, even under backpressure.
                    self.emit(Event::Snapshot {
                        source: self.source,
                        symbols,
                        received_at: Utc::now(),
                    }).await?;
                },
                incoming = socket.next() => {
                    let received_at = Utc::now();
                    let now = Instant::now();
                    let message = incoming.context("socket stream ended")??;
                    last_frame = now;
                    match message {
                        Message::Text(text) => {
                            tracing::trace!(source = self.source, frame = text.as_str(), "exchange frame");
                            let frame = self.exchange.decode(text.as_str(), received_at)?;
                            seq.check(frame.sid, frame.seq)?;
                            match &frame.payload {
                                Payload::Subscribed { sid: new_sid } => {
                                    if frame.id == Some(1) && lifecycle_pending.take().is_some() {
                                        tracing::info!(source = self.source, "lifecycle subscription ready");
                                        if reconnect { self.emit(Event::Rescan).await?; }
                                    }
                                    if let Some(p) = pending.as_ref().filter(|p| frame.id == Some(p.id))
                                        && let Command::Subscribe(symbols) = &p.command {
                                            sid = Some(*new_sid); confirmed.extend(symbols.iter().cloned()); pending = None; reconcile = true;
                                        }
                                }
                                Payload::Ack => {
                                    if let Some(p) = pending.as_ref().filter(|p| frame.id == Some(p.id)) {
                                        match &p.command {
                                            Command::Add { symbols, .. } => confirmed.extend(symbols.iter().cloned()),
                                            Command::Delete { symbols, .. } => for symbol in symbols { confirmed.remove(symbol); },
                                            Command::Subscribe(_) => bail!("expected subscribed acknowledgement"),
                                        }
                                        pending = None;
                                        reconcile = true;
                                    }
                                }
                                _ => {}
                            }
                            if let Payload::Lifecycle(symbol) = &frame.payload {
                                self.emit(Event::Lifecycle(symbol.clone())).await?;
                            }
                            if let Payload::Market(data) = frame.payload {
                                if !self.desired.borrow().contains(&data.symbol) || repair.contains(&data.symbol) { continue; }
                                if let Some(activity) = activities.get_mut(&data.symbol) {
                                    match data.change { Change::Snapshot(_) => activity.snapshot(now), Change::Delta { .. } => activity.delta(now) }
                                    self.emit(Event::Data { source: self.source, data, received_at, now }).await?;
                                }
                            }
                        }
                        Message::Ping(payload) => timeout(Duration::from_secs(5), socket.send(Message::Pong(payload))).await??,
                        Message::Pong(_) => {},
                        Message::Close(_) => bail!("peer closed websocket"),
                        Message::Binary(_) => bail!("unexpected binary exchange message"),
                        Message::Frame(_) => {},
                    }
                }
                _ = health.tick() => {
                    let now = Instant::now();
                    if now >= recycle_at {
                        let desired = self.desired.borrow().clone();
                        let approved = self.request_refresh(desired.clone(), true).await?;
                        if approved == desired {
                            tracing::info!(source = self.source, "scheduled staggered socket recycle");
                            return Ok(());
                        }
                        tracing::warn!(source = self.source, "postponing planned recycle until alternate books are ready");
                        recycle_at = now + Duration::from_secs(20);
                    }
                    if now.duration_since(last_frame) > Duration::from_secs(self.config.heartbeat_timeout_secs) { bail!("websocket heartbeat timeout"); }
                    if pending.as_ref().is_some_and(|p| p.sent.elapsed() >= Duration::from_secs(20)) { bail!("subscription command acknowledgement timeout"); }
                    if lifecycle_pending.is_some_and(|sent| sent.elapsed() >= Duration::from_secs(20)) { bail!("lifecycle subscription acknowledgement timeout"); }
                    // Offset replicas' quiet thresholds to preserve a healthy copy during repairs.
                    let quiet = Duration::from_secs(self.config.quiet_secs + (self.source % self.config.replicas) as u64 * 15);
                    let mut reasons = BTreeMap::new();
                    let mut requested = BTreeSet::new();
                    for (symbol, activity) in &mut activities {
                        if let Some(reason) = activity.repair_reason(now, quiet) {
                            tracing::debug!(source = self.source, symbol, reason, "resubscribing symbol");
                            *reasons.entry(reason).or_insert(0usize) += 1;
                            requested.insert(symbol.clone());
                        }
                    }
                    if !reasons.is_empty() {
                        let approved = self.request_refresh(requested, false).await?;
                        tracing::warn!(source = self.source, ?reasons, approved = approved.len(), "refreshing subscriptions after quiet/rate checks");
                        reconcile |= !approved.is_empty();
                        repair.extend(approved);
                    }
                }
                _ = heartbeat.tick() => { timeout(Duration::from_secs(5), socket.send(Message::Ping(Vec::new().into()))).await??; }
            }
        }
    }

    async fn emit(&self, event: Event) -> Result<()> {
        if matches!(&event, Event::Data { .. } | Event::Snapshot { .. }) {
            // A normal stop must drain a frame whose receive timestamp was
            // already captured, even when the bounded engine queue is full.
            return timeout(Duration::from_secs(5), self.events.send(event))
                .await?
                .context("collector stopped");
        }
        tokio::select! {
            _ = self.cancel.cancelled() => bail!("shutdown"),
            result = timeout(Duration::from_secs(5), self.events.send(event)) => result?.context("collector stopped"),
        }
    }

    async fn request_refresh(
        &self,
        symbols: BTreeSet<String>,
        all: bool,
    ) -> Result<BTreeSet<String>> {
        let (reply, response) = oneshot::channel();
        self.emit(Event::Refresh {
            source: self.source,
            symbols,
            all,
            reply,
        })
        .await?;
        tokio::select! {
            _ = self.cancel.cancelled() => Ok(BTreeSet::new()),
            result = response => result.context("collector stopped before refresh decision"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn shutdown_drains_captured_data_and_boundaries_under_backpressure() {
        use crate::{book::Book, exchange::Kalshi};
        use clap::Parser;
        let (events, mut rx) = mpsc::channel(1);
        let (_desired, desired) = watch::channel(BTreeSet::new());
        let (_repairs, repairs) = mpsc::unbounded_channel();
        let (_snapshots, snapshots) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let worker = Worker {
            source: 0,
            config: Arc::new(Config::parse_from(["collector"])),
            exchange: Arc::new(
                Kalshi::new("http://localhost".into(), "ws://localhost".into(), None).unwrap(),
            ),
            desired,
            repairs,
            snapshots,
            events: events.clone(),
            cancel: cancel.clone(),
            gate: Arc::new(ConnectGate::default()),
        };
        events.send(Event::Rescan).await.unwrap();
        let task = tokio::spawn(async move {
            worker
                .emit(Event::Data {
                    source: 0,
                    data: MarketData {
                        symbol: "A".into(),
                        clock: None,
                        exchange_at: Utc::now(),
                        change: Change::Snapshot(Book::default()),
                    },
                    received_at: Utc::now(),
                    now: Instant::now(),
                })
                .await
                .unwrap();
            worker
                .emit(Event::Snapshot {
                    source: 0,
                    symbols: vec!["A".into()],
                    received_at: Utc::now(),
                })
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        cancel.cancel();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert!(matches!(rx.recv().await, Some(Event::Rescan)));
        assert!(matches!(rx.recv().await, Some(Event::Data { .. })));
        assert!(matches!(rx.recv().await, Some(Event::Snapshot { .. })));
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn global_cooldown_holds_new_connection_attempts() {
        let gate = Arc::new(ConnectGate::default());
        let cancel = CancellationToken::new();
        gate.wait(&cancel).await.unwrap();
        gate.penalize(Duration::from_secs(30));
        let task = tokio::spawn({
            let gate = gate.clone();
            let cancel = cancel.clone();
            async move { gate.wait(&cancel).await }
        });
        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        task.await.unwrap().unwrap();
    }
}
