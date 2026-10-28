//! Controlled, credential-free loopback load through the real collection path.
use anyhow::{Result, ensure};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use engineering_challenge::{
    config::Config,
    exchange::{Command, Exchange, Frame, Kalshi},
    runtime,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain},
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, handshake::client::Request,
};
use tokio_util::sync::CancellationToken;
struct Loopback {
    adapter: Kalshi,
    symbols: BTreeSet<String>,
    url: String,
    clock: Instant,
    timing: Arc<LocalTiming>,
}
#[async_trait]
impl Exchange for Loopback {
    async fn discover(&self) -> Result<BTreeSet<String>> {
        Ok(self.symbols.clone())
    }
    fn request(&self) -> Result<Request> {
        Ok(self.url.clone().into_client_request()?)
    }
    fn command(&self, id: u64, c: &Command) -> Result<String> {
        self.adapter.command(id, c)
    }
    fn decode(&self, text: &str, at: DateTime<Utc>) -> Result<Frame> {
        let captured = self.clock.elapsed().as_micros() as u64;
        let frame = self.adapter.decode(text, at)?;
        if captured >= 5_000_000
            && let engineering_challenge::exchange::Payload::Market(m) = &frame.payload
            && matches!(
                m.change,
                engineering_challenge::exchange::Change::Delta { .. }
            )
        {
            let sent = m.exchange_at.timestamp_micros();
            ensure!(
                sent >= 0 && captured >= sent as u64,
                "invalid monotonic send tag"
            );
            self.timing.record(captured - sent as u64);
        }
        Ok(frame)
    }
}
async fn serve(stream: tokio::net::TcpStream, clock: Instant) -> Result<()> {
    stream.set_nodelay(true)?;
    let mut socket = tokio_tungstenite::accept_async(stream).await?;
    let Some(Ok(Message::Text(request))) = socket.next().await else {
        return Ok(());
    };
    let req: Value = serde_json::from_str(&request)?;
    let symbols: Vec<&str> = req["params"]["market_tickers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    socket
        .send(Message::Text(
            json!({"type":"subscribed","id":req["id"],"msg":{"sid":1}})
                .to_string()
                .into(),
        ))
        .await?;
    let mut seq = 1u64;
    for symbol in &symbols {
        socket.send(Message::Text(json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"msg":{"market_ticker":symbol,"yes_dollars_fp":[["0.4000","100000.00"]],"no_dollars_fp":[]}}).to_string().into())).await?;
        seq += 1;
    }
    let templates: Vec<_> = symbols.iter().map(|s|format!(r#", "msg":{{"market_ticker":"{s}","side":"yes","price_dollars":"0.4000","delta_fp":"1.00","ts":""#)).collect();
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = 0;
    loop {
        tokio::select! {
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Ping(p))) => socket.send(Message::Pong(p)).await?,
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                _ => {},
            },
            _ = tick.tick() => {
                // One send timestamp per burst, on the same host clock as receive.
                let sent = DateTime::<Utc>::from_timestamp_micros(clock.elapsed().as_micros() as i64).unwrap().to_rfc3339_opts(SecondsFormat::Micros, true);
                for _ in 0..100 {
                    let text = format!(r#"{{"type":"orderbook_delta","sid":1,"seq":{seq}{}{}"}}}}"#, templates[cursor % templates.len()], sent);
                    socket.feed(Message::Text(text.into())).await?;
                    cursor += 1; seq += 1;
                }
                socket.flush().await?;
            }
        }
    }
}
// Exact one-microsecond bins through 32 ms, with an explicit overflow bin.
// The histogram is diagnostic-only and shared by both benchmark variants.
struct LocalTiming {
    bins: Vec<AtomicU64>,
    max: AtomicU64,
}
impl LocalTiming {
    fn new() -> Self {
        Self {
            bins: (0..32769).map(|_| AtomicU64::new(0)).collect(),
            max: AtomicU64::new(0),
        }
    }
    fn record(&self, delay: u64) {
        self.bins[delay.min(32768) as usize].fetch_add(1, Relaxed);
        self.max.fetch_max(delay, Relaxed);
    }
    fn report(&self) -> Value {
        let samples: u64 = self.bins.iter().map(|b| b.load(Relaxed)).sum();
        let percentile = |percent: u64| {
            let target = (samples * percent).div_ceil(100);
            let mut count = 0;
            for (i, b) in self.bins.iter().enumerate() {
                count += b.load(Relaxed);
                if count >= target {
                    return i as u64;
                }
            }
            0
        };
        json!({"clock":"one shared monotonic Instant; capture boundary immediately before production decode","samples":samples,"p50_us":percentile(50),"p99_us":percentile(99),"max_us":self.max.load(Relaxed),"overflow_32768us":self.bins[32768].load(Relaxed)})
    }
}
fn replay_count(archive: &std::path::Path) -> Result<u64> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive)?;
    let mut count = 0;
    while let Some(message) = reader.read_msg()? {
        if let AnyMessage::PriceLevelUpdate(decoded) = message {
            PriceLevelUpdateDomain::try_from_decoder(decoded)?;
            count += 1;
        }
    }
    ensure!(count > 0, "no archived deltas");
    Ok(count)
}
fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        return Ok(());
    }
    let threads = std::env::var("PIPELINE_THREADS")
        .unwrap_or_else(|_| "4".into())
        .parse()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter("info")
        .init();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let clock=Instant::now();
        let timing=Arc::new(LocalTiming::new());
        let server = tokio::spawn(async move {
            while let Ok((s,_)) = listener.accept().await {
                tokio::spawn(async move { let _ = serve(s,clock).await; });
            }
        });
        let dir = tempfile::tempdir()?;
        let archive = dir.path().join("loopback.sbe");
        let coverage = dir.path().join("coverage.json");
        let config = Config::parse_from(["pipeline","--run-secs","12","--symbols-per-socket","1000","--connect-interval-ms","5","--snapshot-secs","3","--no-stdout","--output",archive.to_str().unwrap(),"--coverage-report",coverage.to_str().unwrap()]);
        config.validate()?;
        let exchange = Loopback {
            adapter: Kalshi::new("http://localhost".into(),format!("ws://{addr}"),None)?,
            symbols: (0..4000).map(|i|format!("BENCH-MARKET-{i:06}")).collect(),
            url: format!("ws://{addr}"),
            clock, timing:timing.clone(),
        };
        let result = runtime::run(config,Arc::new(exchange),CancellationToken::new()).await;
        server.abort(); result?;
        let status: Value = serde_json::from_slice(&std::fs::read(coverage)?)?;
        let replayed_deltas = replay_count(&archive)?;
        let timing = timing.report();
        ensure!(timing["samples"].as_u64().unwrap()>1000,"insufficient monotonic timing samples");
        println!("{}",json!({"kind":"loopback_pipeline","threads":threads,"coverage":status,"archive_bytes":std::fs::metadata(archive)?.len(),"replayed_deltas":replayed_deltas,"local_send_to_capture":timing}));
        Ok(())
    })
}
