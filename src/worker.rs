use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc, watch},
    time::{interval, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    engine::Source,
    exchange::{Change, Command, Exchange, MarketData, Payload, Sequences},
    monitor::Activity,
};

pub enum Event {
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
    async fn wait(&self, cancel: &CancellationToken) -> Result<()> {
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
    pub events: mpsc::Sender<Event>,
    pub cancel: CancellationToken,
    pub gate: Arc<ConnectGate>,
}

struct Pending {
    id: u64,
    command: Command,
    sent: Instant,
}
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut failures = 0u32;
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
            let result = self.connect_and_run().await;
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

    async fn connect_and_run(&mut self) -> Result<()> {
        let request = self.exchange.request()?;
        // TCP_NODELAY is set by connect_async's implementation when disable_nagle is requested.
        let cancel = self.cancel.clone();
        let (mut socket, _) = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = timeout(Duration::from_secs(20), tokio_tungstenite::connect_async_with_config(request, None, true)) => result??,
        };
        tracing::info!(
            source = self.source,
            symbols = self.desired.borrow().len(),
            "socket connected"
        );
        let result = self.session(&mut socket).await;
        // Bound close: an unresponsive peer must not prevent shutdown/recovery.
        let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
        result
    }

    async fn session(&mut self, socket: &mut Socket) -> Result<()> {
        let mut sid = None;
        let mut confirmed = BTreeSet::new();
        let mut repair = BTreeSet::new();
        let mut activities = BTreeMap::<String, Activity>::new();
        let mut seq = Sequences::default();
        let mut next_id = 1u64;
        let mut pending: Option<Pending> = None;
        let mut last_frame = Instant::now();
        let mut health = interval(Duration::from_secs(1));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat = interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let recycle_at = Instant::now()
            + Duration::from_secs(
                self.config.recycle_secs
                    + (self.source % self.config.replicas) as u64
                        * (self.config.recycle_secs / self.config.replicas as u64),
            );

        loop {
            // One in-flight subscription command. Reconcile only after its ACK.
            if pending.is_none() {
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
                changed = self.desired.changed() => if changed.is_err() { return Ok(()); },
                Some(symbol) = self.repairs.recv() => { repair.insert(symbol); },
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
                                    if let Some(p) = pending.as_ref().filter(|p| frame.id == Some(p.id))
                                        && let Command::Subscribe(symbols) = &p.command {
                                            sid = Some(*new_sid); confirmed.extend(symbols.iter().cloned()); pending = None;
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
                                    }
                                }
                                _ => {}
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
                    if now >= recycle_at { tracing::info!(source = self.source, "scheduled staggered socket recycle"); return Ok(()); }
                    if now.duration_since(last_frame) > Duration::from_secs(self.config.heartbeat_timeout_secs) { bail!("websocket heartbeat timeout"); }
                    if pending.as_ref().is_some_and(|p| p.sent.elapsed() >= Duration::from_secs(20)) { bail!("subscription command acknowledgement timeout"); }
                    // Offset replicas' quiet thresholds to preserve a healthy copy during repairs.
                    let quiet = Duration::from_secs(self.config.quiet_secs + (self.source % self.config.replicas) as u64 * 15);
                    for (symbol, activity) in &mut activities {
                        if let Some(reason) = activity.repair_reason(now, quiet) {
                            tracing::warn!(source = self.source, symbol, reason, "resubscribing symbol");
                            repair.insert(symbol.clone());
                        }
                    }
                }
                _ = heartbeat.tick() => { timeout(Duration::from_secs(5), socket.send(Message::Ping(Vec::new().into()))).await??; }
            }
        }
    }

    async fn emit(&self, event: Event) -> Result<()> {
        tokio::select! {
            _ = self.cancel.cancelled() => bail!("shutdown"),
            result = timeout(Duration::from_secs(5), self.events.send(event)) => result?.context("collector stopped"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
