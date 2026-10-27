use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{interval, timeout},
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    exchange::{Change, Command, Exchange, Payload, Sequences},
    latency::{Analyzer, Observation, Summary, TimestampOrdinals, TimingBasis},
    transport::{self, ConnectionInfo},
    worker::ConnectGate,
};

#[derive(Debug, Serialize)]
pub struct ConnectionSample {
    pub source: usize,
    pub requested_port: Option<u16>,
    pub start_delay_ms: u64,
    pub connection: Option<ConnectionInfo>,
    pub first_snapshot_us: Option<u64>,
    pub deltas: u64,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WaveReport {
    pub started_at: String,
    pub sample_seconds: u64,
    pub samples: Vec<ConnectionSample>,
    pub timing: Summary,
}

/// A small number of paced, simultaneous connections. Ports and start delays
/// are recorded rather than treated as proven causes of latency differences.
pub async fn sample_wave(
    exchange: Arc<dyn Exchange>,
    symbol: String,
    conditions: Vec<(usize, Option<u16>, Duration)>,
    duration: Duration,
) -> Result<WaveReport> {
    ensure!(
        !conditions.is_empty() && conditions.len() <= 8 && !duration.is_zero(),
        "one to eight connections and a positive sample duration required"
    );
    let started_at = Utc::now().to_rfc3339();
    let gate = Arc::new(ConnectGate::default());
    let (tx, mut rx) = mpsc::channel(8192);
    let mut tasks = JoinSet::new();
    for (source, port, delay) in conditions {
        let exchange = exchange.clone();
        let symbol = symbol.clone();
        let tx = tx.clone();
        let gate = gate.clone();
        tasks.spawn(async move {
            capture(exchange, symbol, source, port, delay, duration, gate, tx).await
        });
    }
    drop(tx);
    let mut analyzer = Analyzer::new(100000);
    let mut samples = Vec::new();
    while !tasks.is_empty() || !rx.is_empty() {
        tokio::select! {
            Some(observation) = rx.recv() => analyzer.push(observation),
            Some(result) = tasks.join_next(), if !tasks.is_empty() => samples.push(result.context("experiment task panicked")?),
        }
    }
    samples.sort_by_key(|s| s.source);
    Ok(WaveReport {
        started_at,
        sample_seconds: duration.as_secs(),
        samples,
        timing: analyzer.finish(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn capture(
    exchange: Arc<dyn Exchange>,
    symbol: String,
    source: usize,
    port: Option<u16>,
    delay: Duration,
    duration: Duration,
    gate: Arc<ConnectGate>,
    tx: mpsc::Sender<Observation>,
) -> ConnectionSample {
    let mut sample = ConnectionSample {
        source,
        requested_port: port,
        start_delay_ms: delay.as_millis() as u64,
        connection: None,
        first_snapshot_us: None,
        deltas: 0,
        error: None,
    };
    let result: Result<()> = async {
        tokio::time::sleep(delay).await;
        gate.wait(&CancellationToken::new()).await?;
        let (mut socket, connection) = timeout(Duration::from_secs(20), transport::connect(exchange.request()?, port)).await??;
        sample.connection = Some(connection);
        let result: Result<()> = async {
            let connected = Instant::now();
            socket.send(Message::Text(exchange.command(1, &Command::Subscribe(vec![symbol.clone()]))?.into())).await?;
            let deadline = tokio::time::sleep(duration); tokio::pin!(deadline);
            let mut heartbeat = interval(Duration::from_secs(10));
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut sequences = Sequences::default();
            let mut last_clock: Option<(u64, u32)> = None;
            let mut timestamps = TimestampOrdinals::default();
            let mut initialized = false;
            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    _ = heartbeat.tick() => { timeout(Duration::from_secs(5), socket.send(Message::Ping(Vec::new().into()))).await??; }
                    incoming = socket.next() => {
                        let received = Utc::now();
                        match incoming.context("sample socket ended")?? {
                            Message::Text(text) => {
                                let frame = exchange.decode(text.as_str(), received)?;
                                sequences.check(frame.sid, frame.seq)?;
                                if let Payload::Market(data) = frame.payload {
                                    ensure!(data.symbol == symbol, "unexpected sample symbol");
                                    match data.change {
                                        Change::Snapshot(_) => {
                                            sample.first_snapshot_us.get_or_insert(connected.elapsed().as_micros() as u64);
                                            initialized = true;
                                            timestamps = TimestampOrdinals::default();
                                            last_clock = data.clock.map(|clock| (clock, u32::MAX));
                                        }
                                        Change::Delta { side, price, delta } => {
                                            ensure!(initialized, "sample delta before initial snapshot");
                                            sample.deltas += 1;
                                            let key = if let Some(clock) = data.clock {
                                                let ordinal = if let Some((old, ordinal)) = last_clock {
                                                    ensure!(clock >= old, "exchange clock regressed during sample");
                                                    if clock == old { ordinal.checked_add(1).context("delta at snapshot clock")? } else { 1 }
                                                } else { 1 };
                                                last_clock = Some((clock, ordinal));
                                                Some((TimingBasis::ExchangeClock, clock, ordinal))
                                            } else { timestamps.observe(data.exchange_at.timestamp_micros()).map(|(key, ordinal)| (TimingBasis::TimestampFingerprint, key, ordinal)) };
                                            if let Some((basis, clock, ordinal)) = key {
                                                timeout(Duration::from_secs(5), tx.send(Observation { symbol: symbol.clone(), source, basis, clock, ordinal, side, price, delta, received_us: received.timestamp_micros() })).await??;
                                            }
                                        }
                                    }
                                }
                            }
                            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                            Message::Close(_) => anyhow::bail!("sample peer closed"),
                            Message::Binary(_) => anyhow::bail!("unexpected binary sample frame"),
                            _ => {},
                        }
                    }
                }
            }
            ensure!(initialized, "sample received no initial snapshot");
            Ok(())
        }.await;
        let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
        result
    }.await;
    if let Err(error) = result {
        sample.error = Some(error.to_string());
    }
    sample
}

#[derive(Debug, Deserialize)]
struct ExchangeStatus {
    exchange_active: bool,
    trading_active: bool,
}

#[derive(Debug, Serialize)]
pub struct StatusObservation {
    pub at: String,
    pub exchange_active: Option<bool>,
    pub trading_active: Option<bool>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReopenReport {
    pub started_at: String,
    pub poll_ms: u64,
    pub observations: u64,
    pub errors: u64,
    pub observed_closed_at: Option<String>,
    pub observed_reopen_at: Option<String>,
    pub recent_status: VecDeque<StatusObservation>,
}

/// Require an explicit closed -> open status transition. HTTP failures, starting
/// while already open, and a wait timeout are never labeled post-maintenance.
pub async fn wait_for_reopen(
    status_url: &str,
    wait: Duration,
    poll: Duration,
) -> Result<ReopenReport> {
    ensure!(
        !wait.is_zero() && !poll.is_zero(),
        "positive wait and poll intervals required"
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut report = ReopenReport {
        started_at: Utc::now().to_rfc3339(),
        poll_ms: poll.as_millis() as u64,
        observations: 0,
        errors: 0,
        observed_closed_at: None,
        observed_reopen_at: None,
        recent_status: VecDeque::new(),
    };
    let end = tokio::time::sleep(wait);
    tokio::pin!(end);
    loop {
        let response = tokio::select! {
            _ = &mut end => break,
            result = async { client.get(status_url).send().await?.error_for_status()?.json::<ExchangeStatus>().await } => result,
        };
        let at = Utc::now().to_rfc3339();
        report.observations += 1;
        let observation = match response {
            Ok(status) => {
                let active = status.exchange_active && status.trading_active;
                if !active && report.observed_closed_at.is_none() {
                    report.observed_closed_at = Some(at.clone());
                }
                if active && report.observed_closed_at.is_some() {
                    report.observed_reopen_at = Some(at.clone());
                }
                StatusObservation {
                    at,
                    exchange_active: Some(status.exchange_active),
                    trading_active: Some(status.trading_active),
                    error: None,
                }
            }
            Err(error) => {
                report.errors += 1;
                StatusObservation {
                    at,
                    exchange_active: None,
                    trading_active: None,
                    error: Some(error.to_string()),
                }
            }
        };
        report.recent_status.push_back(observation);
        if report.recent_status.len() > 1000 {
            report.recent_status.pop_front();
        }
        if report.observed_reopen_at.is_some() {
            break;
        }
        tokio::select! { _ = &mut end => break, _ = tokio::time::sleep(poll) => {} }
    }
    Ok(report)
}

#[derive(Serialize)]
pub struct PortExperiment {
    pub symbol: String,
    pub rounds: Vec<WaveReport>,
}

pub async fn compare_ports(
    exchange: Arc<dyn Exchange>,
    symbol: String,
    ports: Vec<u16>,
    rounds: usize,
    duration: Duration,
) -> Result<PortExperiment> {
    ensure!(
        (1..=7).contains(&ports.len()) && (1..=10).contains(&rounds),
        "one to seven ports and one to ten rounds required"
    );
    ensure!(
        ports.iter().all(|p| *p >= 1024),
        "experiment ports must be unprivileged"
    );
    ensure!(
        ports
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == ports.len(),
        "ports must be unique"
    );
    let mut variants = vec![None];
    variants.extend(ports.into_iter().map(Some));
    let mut report = PortExperiment {
        symbol: symbol.clone(),
        rounds: Vec::new(),
    };
    for round in 0..rounds {
        let mut order: Vec<_> = (0..variants.len()).collect();
        order.rotate_left(round % variants.len());
        let conditions = order
            .into_iter()
            .enumerate()
            .map(|(position, source)| {
                (
                    source,
                    variants[source],
                    Duration::from_millis(position as u64 * 500),
                )
            })
            .collect();
        tracing::info!(
            round = round + 1,
            "sampling source-port conditions with rotated start order"
        );
        let wave = sample_wave(exchange.clone(), symbol.clone(), conditions, duration).await?;
        let failed = wave.samples.iter().any(|s| s.error.is_some());
        report.rounds.push(wave);
        if failed {
            break;
        } // Never hammer a rate-limited venue to finish a benchmark.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn reopening_requires_observed_closed_then_open() {
        async fn status(State(count): State<Arc<AtomicUsize>>) -> Json<serde_json::Value> {
            let active = count.fetch_add(1, Ordering::SeqCst) >= 2;
            Json(serde_json::json!({"exchange_active":active,"trading_active":active}))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/status", get(status))
                    .with_state(Arc::new(AtomicUsize::new(0))),
            )
            .await
            .unwrap();
        });
        let r = wait_for_reopen(
            &format!("http://{address}/status"),
            Duration::from_secs(2),
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        assert!(r.observed_closed_at.is_some() && r.observed_reopen_at.is_some());
        assert_eq!(r.observations, 3);
        server.abort();
    }

    #[tokio::test]
    async fn http_failure_is_not_fabricated_as_maintenance() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/status", get(|| async { StatusCode::SERVICE_UNAVAILABLE })),
            )
            .await
            .unwrap();
        });
        let r = wait_for_reopen(
            &format!("http://{address}/status"),
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        assert!(r.errors > 0);
        assert!(r.observed_closed_at.is_none() && r.observed_reopen_at.is_none());
        server.abort();
    }
}
