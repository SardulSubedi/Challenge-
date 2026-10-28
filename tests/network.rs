use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use clap::Parser;
use engineering_challenge::{
    config::Config,
    exchange::{Command, Exchange, Frame, Kalshi},
    runtime,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
    worker::{ConnectGate, Event, Worker},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    time::{sleep, timeout},
};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};
use tokio_util::sync::CancellationToken;

struct MockState {
    universe: Mutex<BTreeSet<String>>,
    commands: Mutex<Vec<Value>>,
    connections: AtomicUsize,
    closed: AtomicUsize,
    injected_gap: AtomicBool,
    inject_gap: bool,
    corrupt_checkpoint: AtomicBool,
    stall_first: AtomicBool,
    omit_snapshot: Mutex<Option<String>>,
    omit_backup: AtomicBool,
    omit_empty_side: AtomicBool,
    lifecycle: AtomicBool,
    started: Instant,
}

impl MockState {
    fn new(inject_gap: bool) -> Arc<Self> {
        Arc::new(Self {
            universe: Mutex::new(["A".into()].into()),
            commands: Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
            closed: AtomicUsize::new(0),
            injected_gap: AtomicBool::new(false),
            inject_gap,
            corrupt_checkpoint: AtomicBool::new(false),
            stall_first: AtomicBool::new(false),
            omit_snapshot: Mutex::new(None),
            omit_backup: AtomicBool::new(false),
            omit_empty_side: AtomicBool::new(false),
            lifecycle: AtomicBool::new(false),
            started: Instant::now(),
        })
    }
}

async fn upgrade(State(state): State<Arc<MockState>>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(mut socket: WebSocket, state: Arc<MockState>) {
    let connection = state.connections.fetch_add(1, Ordering::SeqCst);
    let mut seq = 0u64;
    let mut clocks = BTreeMap::<String, u64>::new();
    let mut lifecycle_symbols = None;
    let mut lifecycle_seq = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let result: Result<()> = async {
        loop {
            tokio::select! {
                incoming = socket.next() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            let command: Value = serde_json::from_str(text.as_str())?;
                            state.commands.lock().unwrap().push(command.clone());
                            let id = command["id"].as_u64().unwrap();
                            if command["params"]["channels"][0] == "market_lifecycle_v2" {
                                lifecycle_symbols = Some(state.universe.lock().unwrap().clone());
                                socket.send(Message::Text(json!({"id":id,"type":"subscribed","msg":{"sid":2,"channel":"market_lifecycle_v2"}}).to_string().into())).await?;
                                continue;
                            }
                            let action = command["params"]["action"].as_str().unwrap_or("subscribe");
                            let symbols = command["params"]["market_tickers"].as_array().unwrap();
                            if action == "subscribe" {
                                socket.send(Message::Text(json!({"id":id,"type":"subscribed","msg":{"sid":1,"channel":"orderbook_delta"}}).to_string().into())).await?;
                            } else {
                                seq += 1;
                                socket.send(Message::Text(json!({"id":id,"type":"ok","sid":1,"seq":seq,"msg":{}}).to_string().into())).await?;
                            }
                            for symbol in symbols {
                                let symbol = symbol.as_str().unwrap();
                                if action == "delete_markets" { clocks.remove(symbol); continue; }
                                if state.omit_snapshot.lock().unwrap().as_deref() == Some(symbol) { continue; }
                                if connection > 0 && state.omit_backup.load(Ordering::SeqCst) { continue; }
                                let clock = state.started.elapsed().as_millis() as u64 / 50 + 1;
                                if action == "get_snapshot" && let Some(&old) = clocks.get(symbol) && clock > old {
                                    seq += 1;
                                    socket.send(Message::Text(json!({"type":"orderbook_delta","sid":1,"seq":seq,"msg":{"market_ticker":symbol,"clock":clock,"price_dollars":"0.4000","delta_fp":(clock-old).to_string(),"side":"yes","ts_ms":chrono::Utc::now().timestamp_millis()}}).to_string().into())).await?;
                                }
                                clocks.insert(symbol.to_owned(), clock);
                                seq += 1;
                                let quantity = clock + u64::from(action == "get_snapshot" && state.corrupt_checkpoint.load(Ordering::SeqCst));
                                let mut snapshot = json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":chrono::Utc::now().timestamp_millis(),"msg":{"market_ticker":symbol,"clock":clock,"yes_dollars_fp":[["0.4000",quantity.to_string()]],"no_dollars_fp":[["0.5000","3.00"]]}});
                                if state.omit_empty_side.load(Ordering::SeqCst) { snapshot["msg"].as_object_mut().unwrap().remove("no_dollars_fp"); }
                                socket.send(Message::Text(snapshot.to_string().into())).await?;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
                        Some(Ok(Message::Pong(_))) => {},
                        Some(Ok(Message::Close(_))) | None => { state.closed.fetch_add(1, Ordering::SeqCst); return Ok(()); }
                        Some(Err(_)) => return Ok(()),
                        _ => {},
                    }
                }
                _ = tick.tick() => {
                    if let Some(previous) = &mut lifecycle_symbols {
                        let current = state.universe.lock().unwrap().clone();
                        for (symbols, kind) in [(current.difference(previous).cloned().collect::<Vec<_>>(), "activated"), (previous.difference(&current).cloned().collect(), "determined")] {
                            for symbol in symbols {
                                lifecycle_seq += 1;
                                socket.send(Message::Text(json!({"type":"market_lifecycle_v2","sid":2,"seq":lifecycle_seq,"msg":{"market_ticker":symbol,"event_type":kind}}).to_string().into())).await?;
                            }
                        }
                        *previous = current;
                    }
                    for (symbol, old) in &mut clocks {
                        let clock = state.started.elapsed().as_millis() as u64 / 50 + 1;
                        if connection == 0 && clock >= 20 && state.stall_first.load(Ordering::SeqCst) { continue; }
                        if clock <= *old { continue; }
                        seq += 1;
                        if state.inject_gap && connection == 0 && clock >= 4 && !state.injected_gap.swap(true, Ordering::SeqCst) { seq += 1; }
                        socket.send(Message::Text(json!({"type":"orderbook_delta","sid":1,"seq":seq,"msg":{"market_ticker":symbol,"clock":clock,"price_dollars":"0.4000","delta_fp":(clock-*old).to_string(),"side":"yes","ts_ms":chrono::Utc::now().timestamp_millis()}}).to_string().into())).await?;
                        *old = clock;
                    }
                }
            }
        }
    }.await;
    let _ = result;
}

async fn markets(State(state): State<Arc<MockState>>) -> Json<Value> {
    Json(
        json!({"markets":state.universe.lock().unwrap().iter().map(|s| json!({"ticker":s,"status":"active"})).collect::<Vec<_>>(),"cursor":""}),
    )
}

async fn start_server(state: Arc<MockState>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/ws", get(upgrade))
        .route("/markets", get(markets))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("127.0.0.1:{}", address.port()), task)
}

struct MockExchange {
    state: Arc<MockState>,
    address: String,
    codec: Kalshi,
}
#[async_trait]
impl Exchange for MockExchange {
    fn lifecycle_subscription(&self, id: u64) -> Option<String> {
        if self.state.lifecycle.load(Ordering::SeqCst) {
            self.codec.lifecycle_subscription(id)
        } else {
            None
        }
    }
    async fn resolve_symbol(&self, symbol: &str) -> Result<bool> {
        Ok(self.state.universe.lock().unwrap().contains(symbol))
    }
    async fn discover(&self) -> Result<BTreeSet<String>> {
        Ok(self.state.universe.lock().unwrap().clone())
    }
    fn request(&self) -> Result<Request> {
        Ok(format!("ws://{}/ws", self.address).into_client_request()?)
    }
    fn command(&self, id: u64, command: &Command) -> Result<String> {
        self.codec.command(id, command)
    }
    fn decode(&self, data: &str, received_at: chrono::DateTime<chrono::Utc>) -> Result<Frame> {
        self.codec.decode(data, received_at)
    }
}
fn exchange(state: Arc<MockState>, address: String) -> Arc<dyn Exchange> {
    Arc::new(MockExchange {
        state,
        address,
        codec: Kalshi::new("http://localhost".into(), "ws://localhost".into(), None).unwrap(),
    })
}
fn config() -> Config {
    Config::parse_from([
        "collector",
        "--symbols-per-socket",
        "1",
        "--discovery-secs",
        "1",
        "--snapshot-secs",
        "1",
    ])
}

#[tokio::test]
async fn omitted_empty_snapshot_side_keeps_both_connections_streaming() {
    let state = MockState::new(false);
    state.omit_empty_side.store(true, Ordering::SeqCst);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("one-sided.sbe");
    let mut config = config();
    config.no_stdout = true;
    config.output = Some(path.clone());
    config.run_secs = Some(2);
    config.connect_interval_ms = 5;
    runtime::run(
        config,
        exchange(state.clone(), address),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut snapshots = 0;
    let mut deltas = 0;
    while let Some(record) = reader.read_msg().unwrap() {
        match record {
            AnyMessage::Snapshot(s) => {
                assert!(SnapshotDomain::try_from_decoder(s).unwrap().asks.is_empty());
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(_) => deltas += 1,
            _ => panic!("unexpected output"),
        }
    }
    assert!(snapshots >= 1 && deltas > 10);
    server.abort();
}

#[tokio::test]
async fn connection_budget_is_explicit_instead_of_silently_skipping_markets() {
    let state = MockState::new(false);
    *state.universe.lock().unwrap() = ["A".into(), "B".into()].into();
    let mut config = config();
    config.max_sockets = 2;
    let result = runtime::run(
        config,
        exchange(state, "127.0.0.1:1".into()),
        CancellationToken::new(),
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("universe needs 4 sockets")
    );
}

#[tokio::test]
async fn partial_coverage_returns_error_and_names_the_missing_market() {
    let state = MockState::new(false);
    *state.universe.lock().unwrap() = ["A".into(), "B".into()].into();
    *state.omit_snapshot.lock().unwrap() = Some("B".into());
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let status = dir.path().join("coverage.json");
    let mut config = config();
    config.symbols_per_socket = 2000;
    config.no_stdout = true;
    config.run_secs = Some(2);
    config.output = Some(dir.path().join("partial.sbe"));
    config.coverage_report = Some(status.clone());
    let error = runtime::run(config, exchange(state, address), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("1 current markets lack a ready book")
    );
    let report: Value = serde_json::from_slice(&std::fs::read(status).unwrap()).unwrap();
    assert_eq!(report["coverage"]["missing"], json!(["B"]));
    assert_eq!(report["coverage"]["ready"], 1);
    assert_eq!(report["coverage"]["redundant"], 1);
    assert_eq!(report["stopping"], true);
    server.abort();
}

#[tokio::test]
async fn periodic_snapshot_batches_continue_for_each_available_market() {
    let state = MockState::new(false);
    *state.universe.lock().unwrap() = ["A".into(), "B".into()].into();
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("periodic.sbe");
    let mut config = config();
    config.symbols_per_socket = 2000;
    config.replicas = 1;
    config.discovery_secs = 60;
    config.no_stdout = true;
    config.run_secs = Some(3);
    config.output = Some(path.clone());
    runtime::run(config, exchange(state, address), CancellationToken::new())
        .await
        .unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut counts = BTreeMap::new();
    let mut timestamps = BTreeMap::new();
    while let Some(message) = reader.read_msg().unwrap() {
        let (symbol, timestamp) = match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded).unwrap();
                *counts.entry(snapshot.symbol.clone()).or_insert(0) += 1;
                (snapshot.symbol, snapshot.received_at)
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                (update.symbol, update.backtest_timestamp)
            }
            _ => panic!("unexpected record"),
        };
        if let Some(previous) = timestamps.insert(symbol, timestamp) {
            assert!(timestamp >= previous, "snapshot overtook a captured delta");
        }
    }
    assert!(
        counts["A"] >= 3 && counts["B"] >= 3,
        "snapshot counts: {counts:?}"
    );
    server.abort();
}

#[tokio::test]
async fn snapshot_boundary_follows_captured_frames_under_backpressure() {
    let state = MockState::new(false);
    let (address, server) = start_server(state.clone()).await;
    let cancel = CancellationToken::new();
    let (_desired_tx, desired) = watch::channel(["A".into()].into());
    let (_repair_tx, repairs) = mpsc::unbounded_channel();
    let (snapshot_tx, snapshots) = mpsc::channel(1);
    let (events, mut rx) = mpsc::channel(1);
    let worker = Worker {
        source: 0,
        config: Arc::new(config()),
        exchange: exchange(state, address),
        desired,
        repairs,
        snapshots,
        events,
        cancel: cancel.clone(),
        gate: Arc::new(ConnectGate::default()),
    };
    let task = tokio::spawn(worker.run());
    timeout(Duration::from_secs(5), async {
        let mut last_received = loop {
            if let Event::Data { received_at, .. } = rx.recv().await.unwrap() {
                break received_at;
            }
        };
        // Fill the one-slot event queue and leave another frame blocked in emit().
        sleep(Duration::from_millis(200)).await;
        snapshot_tx.send(vec!["A".into()]).await.unwrap();
        let boundary = loop {
            match rx.recv().await.unwrap() {
                Event::Data { received_at, .. } => last_received = received_at,
                Event::Snapshot {
                    received_at,
                    symbols,
                    ..
                } => {
                    assert_eq!(symbols, ["A"]);
                    assert!(received_at >= last_received);
                    break received_at;
                }
                _ => {}
            }
        };
        loop {
            if let Event::Data { received_at, .. } = rx.recv().await.unwrap() {
                assert!(
                    received_at >= boundary,
                    "captured frame followed a newer snapshot marker"
                );
                break;
            }
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    drop(rx);
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn missing_backup_is_reported_as_incomplete_even_when_primary_streams() {
    let state = MockState::new(false);
    state.omit_backup.store(true, Ordering::SeqCst);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let status = dir.path().join("coverage.json");
    let mut config = config();
    config.no_stdout = true;
    config.run_secs = Some(2);
    config.output = Some(dir.path().join("backup.sbe"));
    config.coverage_report = Some(status.clone());
    let error = runtime::run(config, exchange(state, address), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("1 lack configured replicas"));
    let report: Value = serde_json::from_slice(&std::fs::read(status).unwrap()).unwrap();
    assert_eq!(report["coverage"]["missing"], json!([]));
    assert_eq!(report["coverage"]["under_replicated"], json!(["A"]));
    server.abort();
}

#[tokio::test]
async fn lifecycle_feed_adds_and_removes_markets_before_the_next_poll() {
    let state = MockState::new(false);
    state.lifecycle.store(true, Ordering::SeqCst);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let status = dir.path().join("coverage.json");
    let mut config = config();
    config.discovery_secs = 60;
    config.symbols_per_socket = 2000;
    config.connect_interval_ms = 20;
    config.no_stdout = true;
    config.output = Some(dir.path().join("lifecycle.sbe"));
    config.coverage_report = Some(status.clone());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(runtime::run(
        config,
        exchange(state.clone(), address),
        cancel.clone(),
    ));
    timeout(Duration::from_secs(5), async {
        while state.commands.lock().unwrap().len() < 4 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(100)).await;
    let changed = Instant::now();
    *state.universe.lock().unwrap() = ["B".into()].into();
    timeout(Duration::from_secs(5), async {
        loop {
            let adds = state
                .commands
                .lock()
                .unwrap()
                .iter()
                .filter(|c| {
                    c["params"]["action"] == "add_markets"
                        && c["params"]["market_tickers"] == json!(["B"])
                })
                .count();
            let deletes = state
                .commands
                .lock()
                .unwrap()
                .iter()
                .filter(|c| {
                    c["params"]["action"] == "delete_markets"
                        && c["params"]["market_tickers"] == json!(["A"])
                })
                .count();
            if adds == 2 && deletes == 2 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(changed.elapsed() < Duration::from_secs(5));
    sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let report: Value = serde_json::from_slice(&std::fs::read(status).unwrap()).unwrap();
    assert_eq!(report["coverage"]["markets"], 1);
    assert_eq!(report["coverage"]["ready"], 1);
    assert_eq!(report["coverage"]["redundant"], 1);
    assert!(
        state
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|c| c["params"]["action"] == "delete_markets"
                && c["params"]["market_tickers"] == json!(["A"]))
    );
    server.abort();
}

#[tokio::test]
async fn lifecycle_lookup_rechecks_status_and_excludes_multivariate_markets() {
    async fn page(Query(q): Query<HashMap<String, String>>) -> Json<Value> {
        assert_eq!(q["status"], "open");
        assert_eq!(q["mve_filter"], "exclude");
        Json(json!({"markets":[
            {"ticker":"LIVE","status":"active"},
            {"ticker":"CLOSED","status":"closed"},
            {"ticker":"MVE","status":"active","mve_selected_legs":[{}]}
        ],"cursor":""}))
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/markets", get(page)))
            .await
            .unwrap();
    });
    let adapter = Kalshi::new(format!("http://{address}"), "ws://localhost".into(), None).unwrap();
    assert!(adapter.resolve_symbol("LIVE").await.unwrap());
    for symbol in ["CLOSED", "MVE", "MISSING"] {
        assert!(!adapter.resolve_symbol(symbol).await.unwrap());
    }
    server.abort();
}

#[tokio::test]
async fn sequence_gap_invalidates_then_reconnects_with_new_snapshot() {
    let state = MockState::new(true);
    let (address, server) = start_server(state.clone()).await;
    let cancel = CancellationToken::new();
    let (_desired_tx, desired) = watch::channel(["A".into()].into());
    let (_repair_tx, repairs) = mpsc::unbounded_channel();
    let (events, mut rx) = mpsc::channel(128);
    let worker = Worker {
        source: 0,
        config: Arc::new(config()),
        exchange: exchange(state.clone(), address),
        desired,
        repairs,
        snapshots: mpsc::channel(1).1,
        events,
        cancel: cancel.clone(),
        gate: Arc::new(ConnectGate::default()),
    };
    let task = tokio::spawn(worker.run());
    timeout(Duration::from_secs(10), async {
        let mut invalid = false;
        loop {
            match rx.recv().await.unwrap() {
                Event::Invalid { .. } => invalid = true,
                Event::Data { data, .. }
                    if invalid
                        && matches!(
                            data.change,
                            engineering_challenge::exchange::Change::Snapshot(_)
                        ) =>
                {
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(state.injected_gap.load(Ordering::SeqCst));
    assert!(state.connections.load(Ordering::SeqCst) >= 2);
    cancel.cancel();
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn collector_tracks_added_removed_markets_and_flushes_readable_sbe() {
    let state = MockState::new(false);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capture.sbe");
    let mut config = config();
    config.no_stdout = true;
    config.output = Some(path.clone());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(runtime::run(
        config,
        exchange(state.clone(), address),
        cancel.clone(),
    ));
    sleep(Duration::from_millis(550)).await;
    *state.universe.lock().unwrap() = ["B".into(), "C".into()].into();
    sleep(Duration::from_millis(2100)).await;
    cancel.cancel();
    timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut seen = BTreeSet::new();
    let mut deltas = 0;
    let mut snapshots = 0;
    while let Some(message) = reader.read_msg().unwrap() {
        match message {
            AnyMessage::Snapshot(m) => {
                let s = SnapshotDomain::try_from_decoder(m).unwrap();
                assert_eq!(s.asks[0].price_level, 500000);
                seen.insert(s.symbol);
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(m) => {
                let d = PriceLevelUpdateDomain::try_from_decoder(m).unwrap();
                assert!(seen.contains(&d.symbol));
                assert!(d.quantity >= 1000000);
                deltas += 1;
            }
            _ => panic!("unexpected SBE message type"),
        }
    }
    assert_eq!(seen, ["A".into(), "B".into(), "C".into()].into());
    assert!(deltas > 5 && snapshots >= 3);
    let commands = state.commands.lock().unwrap();
    assert!(
        commands
            .iter()
            .any(|v| v["params"]["action"] == "delete_markets")
    );
    assert!(
        commands
            .iter()
            .any(|v| v["params"]["action"] == "add_markets")
    );
    server.abort();
}

#[tokio::test]
async fn discovery_paginates_and_excludes_mves_even_if_server_returns_them() {
    async fn page(Query(query): Query<HashMap<String, String>>) -> Json<Value> {
        assert_eq!(query["status"], "open");
        assert_eq!(query["mve_filter"], "exclude");
        Json(if query["cursor"].is_empty() {
            json!({"markets":[{"ticker":"A","status":"active"},{"ticker":"MVE","status":"active","mve_collection_ticker":"COMBO"}],"cursor":"next"})
        } else {
            json!({"markets":[{"ticker":"B","status":"open"},{"ticker":"C","status":"closed"}],"cursor":""})
        })
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/markets", get(page)))
            .await
            .unwrap();
    });
    let adapter = Kalshi::new(format!("http://{address}"), "ws://localhost".into(), None).unwrap();
    assert_eq!(
        adapter.discover().await.unwrap(),
        ["A".into(), "B".into()].into()
    );
    server.abort();
}

#[tokio::test]
async fn discovery_decodes_compressed_exchange_responses() {
    use base64::Engine as _;
    let compressed = base64::engine::general_purpose::STANDARD.decode("H4sIAAAAAAACA6tWyk0syk4tKVayiq5WKslMzk4tUrJSclTSUSouSSwpBYorJSaXZJalKtXG6igllxYV54MUKNUCALSpHB86AAAA").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/markets",
                get(move |headers: axum::http::HeaderMap| {
                    let body = compressed.clone();
                    async move {
                        assert!(
                            headers["accept-encoding"]
                                .to_str()
                                .unwrap()
                                .contains("gzip")
                        );
                        (
                            [
                                ("content-encoding", "gzip"),
                                ("content-type", "application/json"),
                            ],
                            body,
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let adapter = Kalshi::new(format!("http://{address}"), "ws://localhost".into(), None).unwrap();
    assert_eq!(adapter.discover().await.unwrap(), ["A".into()].into());
    server.abort();
}

#[tokio::test]
async fn explicit_repair_reconciles_after_ack_and_streaming_resumes() {
    let state = MockState::new(false);
    let (address, server) = start_server(state.clone()).await;
    let cancel = CancellationToken::new();
    let (_desired_tx, desired) = watch::channel(["A".into()].into());
    let (repair_tx, repairs) = mpsc::unbounded_channel();
    let (events, mut rx) = mpsc::channel(128);
    let task = tokio::spawn(
        Worker {
            source: 0,
            config: Arc::new(config()),
            exchange: exchange(state.clone(), address),
            desired,
            repairs,
            snapshots: mpsc::channel(1).1,
            events,
            cancel: cancel.clone(),
            gate: Arc::new(ConnectGate::default()),
        }
        .run(),
    );
    timeout(Duration::from_secs(5), async {
        let mut snapshots = 0;
        loop {
            if let Event::Data { data, .. } = rx.recv().await.unwrap() {
                match data.change {
                    engineering_challenge::exchange::Change::Snapshot(_) => {
                        snapshots += 1;
                        if snapshots == 1 {
                            repair_tx.send("A".into()).unwrap();
                        }
                    }
                    engineering_challenge::exchange::Change::Delta { .. } if snapshots >= 2 => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let commands = state.commands.lock().unwrap();
    assert_eq!(commands.len(), 3);
    assert_eq!(commands[1]["params"]["action"], "delete_markets");
    assert_eq!(commands[2]["params"]["action"], "add_markets");
    server.abort();
}

#[tokio::test]
async fn discovery_recovers_from_throttling_without_losing_the_page() {
    async fn throttled(
        State(count): State<Arc<AtomicUsize>>,
    ) -> (axum::http::StatusCode, Json<Value>) {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error":"too many requests"})),
            )
        } else {
            (
                axum::http::StatusCode::OK,
                Json(json!({"markets":[{"ticker":"A","status":"active"}],"cursor":""})),
            )
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/markets", get(throttled))
        .with_state(count.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let adapter = Kalshi::new(format!("http://{address}"), "ws://localhost".into(), None).unwrap();
    assert_eq!(adapter.discover().await.unwrap(), ["A".into()].into());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn startup_covers_each_shard_before_opening_backups() {
    let state = MockState::new(false);
    *state.universe.lock().unwrap() = ["A".into(), "B".into(), "C".into()].into();
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = config();
    config.connect_interval_ms = 80;
    config.discovery_secs = 60;
    config.no_stdout = true;
    config.output = Some(dir.path().join("startup.sbe"));
    let coverage = dir.path().join("startup.coverage.json");
    config.coverage_report = Some(coverage.clone());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(runtime::run(
        config,
        exchange(state.clone(), address),
        cancel.clone(),
    ));
    timeout(Duration::from_secs(12), async {
        loop {
            // A server accepting six commands does not mean the engine has
            // consumed all six snapshots. Wait for actual redundant coverage.
            let status = std::fs::read(&coverage)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
            if status.is_some_and(|s| s["coverage"]["redundant"] == 3) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let commands = state.commands.lock().unwrap();
    let first: BTreeSet<_> = commands
        .iter()
        .take(3)
        .map(|c| c["params"]["market_tickers"][0].as_str().unwrap())
        .collect();
    assert_eq!(first, ["A", "B", "C"].into());
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn sigint_closes_sockets_and_stdout_matches_sbe_archive() {
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use engineering_challenge::output::CHUNK_SIZE;
    use engineering_challenge::util::dtos::SbeEncode;
    let state = MockState::new(false);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let pem = ed25519_dalek::SigningKey::from_bytes(&[42; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let key_path = dir.path().join("test.key");
    std::fs::write(&key_path, pem.as_bytes()).unwrap();
    let archive = dir.path().join("test.sbe");
    let raw = dir.path().join("stdout.raw");
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_collector"))
        .args([
            "--rest-url",
            &format!("http://{address}"),
            "--ws-url",
            &format!("ws://{address}/ws"),
            "--api-key-id",
            "test-key",
            "--private-key-path",
        ])
        .arg(key_path)
        .arg("--output")
        .arg(&archive)
        .stdout(std::fs::File::create(&raw).unwrap())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    timeout(Duration::from_secs(4), async {
        while state.connections.load(Ordering::SeqCst) < 2 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(250)).await;
    assert!(
        std::process::Command::new("kill")
            .args(["-INT", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert_eq!(
        std::fs::metadata(&archive).unwrap().len(),
        CHUNK_SIZE as u64
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive).unwrap();
    let mut bytes = Vec::new();
    while let Some(message) = reader.read_msg().unwrap() {
        bytes.extend(match message {
            AnyMessage::Snapshot(m) => SnapshotDomain::try_from_decoder(m)
                .unwrap()
                .to_bytes()
                .unwrap(),
            AnyMessage::PriceLevelUpdate(m) => PriceLevelUpdateDomain::try_from_decoder(m)
                .unwrap()
                .to_bytes()
                .unwrap(),
            _ => panic!("unexpected SBE message type"),
        });
    }
    assert!(!bytes.is_empty());
    assert_eq!(std::fs::read(raw).unwrap(), bytes);
    assert!(state.closed.load(Ordering::SeqCst) >= 2);
    server.abort();
}

#[tokio::test]
async fn independent_live_verifier_matches_checkpoints_and_sbe_replay() {
    let state = MockState::new(false);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("verified.sbe");
    let report = engineering_challenge::lab::verify(
        exchange(state, address),
        engineering_challenge::lab::VerifyOptions {
            symbols: vec!["A".into()],
            duration: Duration::from_millis(800),
            snapshot_every: Duration::from_millis(100),
        },
        Some(&archive),
    )
    .await
    .unwrap();
    assert!(report.deltas > 2 && report.snapshot_checks >= 2);
    assert_eq!(report.snapshot_mismatches, 0);
    assert!(report.archive_roundtrip);
    let wire = std::fs::read_to_string(archive.with_extension("wire.jsonl")).unwrap();
    let frames: Vec<Value> = wire
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        frames.len() as u64 >= report.deltas + report.initial_snapshots + report.snapshot_checks
    );
    for frame in frames {
        assert!(frame["received_at"].is_string());
        assert!(serde_json::from_str::<Value>(frame["frame"].as_str().unwrap()).is_ok());
    }
    assert_eq!(
        engineering_challenge::lab::audit_archive(&archive)
            .unwrap()
            .snapshot_mismatches,
        0
    );
    server.abort();
}

#[tokio::test]
async fn independent_verifier_reports_incorrect_exchange_checkpoint() {
    let state = MockState::new(false);
    state.corrupt_checkpoint.store(true, Ordering::SeqCst);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("inconsistent.sbe");
    let report = engineering_challenge::lab::verify(
        exchange(state, address),
        engineering_challenge::lab::VerifyOptions {
            symbols: vec!["A".into()],
            duration: Duration::from_millis(500),
            snapshot_every: Duration::from_millis(100),
        },
        Some(&archive),
    )
    .await
    .unwrap();
    assert!(report.snapshot_mismatches > 0);
    assert!(report.mismatches[0].different_levels > 0);
    assert!(
        archive
            .with_extension("wire.jsonl")
            .metadata()
            .unwrap()
            .len()
            > 0
    );
    let audit = engineering_challenge::lab::audit_archive(&archive).unwrap();
    assert_eq!(audit.snapshot_mismatches, report.snapshot_mismatches);
    assert_eq!(
        audit.mismatches[0].levels[0].snapshot_quantity,
        report.mismatches[0].levels[0].snapshot_quantity
    );
    server.abort();
}

#[tokio::test]
async fn connection_experiment_matches_events_and_records_real_endpoints() {
    let state = MockState::new(false);
    let (address, server) = start_server(state.clone()).await;
    let report = engineering_challenge::experiments::sample_wave(
        exchange(state, address),
        "A".into(),
        vec![(0, None, Duration::ZERO), (1, None, Duration::ZERO)],
        Duration::from_millis(800),
    )
    .await
    .unwrap();
    assert_eq!(report.samples.len(), 2);
    assert!(
        report
            .samples
            .iter()
            .all(|s| s.error.is_none() && s.first_snapshot_us.is_some())
    );
    assert!(report.timing.matched_updates > 0);
    assert_ne!(
        report.samples[0].connection.as_ref().unwrap().local_addr,
        report.samples[1].connection.as_ref().unwrap().local_addr
    );
    server.abort();
}

#[tokio::test]
async fn silent_primary_with_healthy_heartbeats_fails_over_through_complete_pipeline() {
    let state = MockState::new(false);
    state.stall_first.store(true, Ordering::SeqCst);
    let (address, server) = start_server(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silent-primary.sbe");
    let mut config = config();
    config.no_stdout = true;
    config.output = Some(path.clone());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(runtime::run(
        config,
        exchange(state.clone(), address),
        cancel.clone(),
    ));
    sleep(Duration::from_millis(4200)).await;
    cancel.cancel();
    timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut last_quantity = 0;
    let mut snapshots = 0;
    while let Some(message) = reader.read_msg().unwrap() {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let s = SnapshotDomain::try_from_decoder(decoded).unwrap();
                last_quantity = s.bids[0].quantity;
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                last_quantity = PriceLevelUpdateDomain::try_from_decoder(decoded)
                    .unwrap()
                    .quantity;
            }
            _ => panic!("unexpected SBE message"),
        }
    }
    assert!(snapshots >= 2);
    assert!(
        last_quantity >= 70_000_000,
        "backup should advance the output after silent primary stalls at 20 contracts: {last_quantity}"
    );
    server.abort();
}
