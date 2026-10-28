use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::{
    Side,
    auth::Credentials,
    book::{Book, SCALE, fixed},
};

#[derive(Clone, Debug)]
pub enum Change {
    Snapshot(Book),
    Delta { side: Side, price: i64, delta: i64 },
}

#[derive(Clone, Debug)]
pub struct MarketData {
    pub symbol: String,
    pub exchange_at: DateTime<Utc>,
    pub clock: Option<u64>,
    pub change: Change,
}

#[derive(Debug)]
pub enum Payload {
    Market(MarketData),
    Lifecycle(String),
    Subscribed { sid: u64 },
    Ack,
    Other,
}

#[derive(Debug)]
pub struct Frame {
    pub id: Option<u64>,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub payload: Payload,
}

#[derive(Clone, Debug)]
pub enum Command {
    Subscribe(Vec<String>),
    Add { sid: u64, symbols: Vec<String> },
    Delete { sid: u64, symbols: Vec<String> },
}

/// Exchange-specific discovery/authentication/encoding lives behind this boundary.
/// Workers, supervision, book maintenance, health checks, and output are exchange-independent.
#[async_trait]
pub trait Exchange: Send + Sync {
    async fn discover(&self) -> Result<BTreeSet<String>>;
    fn request(&self) -> Result<Request>;
    fn command(&self, id: u64, command: &Command) -> Result<String>;
    fn decode(&self, message: &str, received_at: DateTime<Utc>) -> Result<Frame>;
    /// Optional fast discovery path. Other adapters can retain periodic discovery.
    fn lifecycle_subscription(&self, _id: u64) -> Option<String> {
        None
    }
    async fn resolve_symbol(&self, _symbol: &str) -> Result<bool> {
        bail!("adapter does not support lifecycle resolution")
    }
}

pub struct Kalshi {
    pub rest_url: String,
    pub ws_url: String,
    pub credentials: Option<Arc<Credentials>>,
    client: reqwest::Client,
    symbols: Vec<String>,
    discovery_delay_ms: AtomicU64,
}

impl Kalshi {
    pub fn new(
        rest_url: String,
        ws_url: String,
        credentials: Option<Arc<Credentials>>,
    ) -> Result<Self> {
        Ok(Self {
            rest_url,
            ws_url,
            credentials,
            symbols: Vec::new(),
            discovery_delay_ms: AtomicU64::new(120),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("voluntas-challenge-collector/0.1")
                .build()?,
        })
    }

    pub fn with_symbols(mut self, symbols: Vec<String>) -> Self {
        self.symbols = symbols;
        self
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: String,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
    status: String,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
    #[serde(default)]
    mve_selected_legs: Option<Vec<Value>>,
}

#[async_trait]
impl Exchange for Kalshi {
    fn lifecycle_subscription(&self, id: u64) -> Option<String> {
        Some(
            json!({"id":id,"cmd":"subscribe","params":{"channels":["market_lifecycle_v2"]}})
                .to_string(),
        )
    }

    async fn resolve_symbol(&self, symbol: &str) -> Result<bool> {
        let response = self
            .client
            .get(format!("{}/markets", self.rest_url.trim_end_matches('/')))
            .query(&[
                ("tickers", symbol),
                ("status", "open"),
                ("mve_filter", "exclude"),
                ("limit", "1000"),
            ])
            .send()
            .await?
            .error_for_status()?;
        let page: MarketsPage = response.json().await?;
        ensure!(
            page.cursor.is_empty(),
            "targeted market lookup unexpectedly paginated"
        );
        Ok(page
            .markets
            .into_iter()
            .any(|m| m.ticker == symbol && m.is_live_regular()))
    }

    async fn discover(&self) -> Result<BTreeSet<String>> {
        let mut symbols = BTreeSet::new();
        let mut cursor = String::new();
        let mut cursors = HashSet::new();
        let tickers = self.symbols.join(",");
        let mut pages = 0usize;
        let started = std::time::Instant::now();
        loop {
            let mut response = None;
            for retry in 0..5u32 {
                let mut request = self
                    .client
                    .get(format!("{}/markets", self.rest_url.trim_end_matches('/')))
                    .query(&[
                        ("status", "open"),
                        ("mve_filter", "exclude"),
                        ("limit", "1000"),
                        ("cursor", cursor.as_str()),
                    ]);
                if !tickers.is_empty() {
                    request = request.query(&[("tickers", tickers.as_str())]);
                }
                let result = request.send().await;
                match result {
                    Ok(r) if r.status().is_success() => {
                        response = Some(r);
                        break;
                    }
                    Ok(r) if r.status().as_u16() == 429 || r.status().is_server_error() => {
                        if r.status().as_u16() == 429 {
                            // Compression can make pages arrive fast enough to hit
                            // public REST quotas. Retain slower pacing across scans.
                            let delay = self.discovery_delay_ms.load(Ordering::Relaxed);
                            self.discovery_delay_ms
                                .fetch_max((delay * 3 / 2).min(2000), Ordering::Relaxed);
                        }
                        let delay = r
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(1 << retry)
                            .min(60);
                        tracing::warn!(status = %r.status(), delay, page_delay_ms = self.discovery_delay_ms.load(Ordering::Relaxed), "discovery throttled; retrying page");
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                    Ok(r) => bail!("market discovery returned HTTP {}", r.status()),
                    Err(error) if retry < 4 => {
                        tracing::warn!(%error, "market discovery request failed; retrying page");
                        tokio::time::sleep(Duration::from_secs(1 << retry)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let page: MarketsPage = response
                .context("market discovery exhausted retries")?
                .json()
                .await?;
            pages += 1;
            for market in page.markets {
                if market.is_live_regular() {
                    symbols.insert(market.ticker);
                }
            }
            if page.cursor.is_empty() {
                break;
            }
            if pages.is_multiple_of(25) {
                tracing::info!(
                    pages,
                    markets = symbols.len(),
                    elapsed_secs = started.elapsed().as_secs(),
                    "market discovery in progress"
                );
            }
            ensure!(
                cursors.insert(page.cursor.clone()),
                "discovery cursor repeated; refusing partial market universe"
            );
            cursor = page.cursor;
            // Stay below common REST rate limits, even for a large universe.
            tokio::time::sleep(Duration::from_millis(
                self.discovery_delay_ms.load(Ordering::Relaxed),
            ))
            .await;
        }
        tracing::info!(
            pages,
            markets = symbols.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "market discovery completed"
        );
        Ok(symbols)
    }

    fn request(&self) -> Result<Request> {
        self.credentials
            .as_ref()
            .context("Kalshi credentials required")?
            .request(&self.ws_url, Utc::now().timestamp_millis())
    }

    fn command(&self, id: u64, command: &Command) -> Result<String> {
        Ok(match command {
            Command::Subscribe(symbols) => json!({"id": id, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": symbols}}),
            Command::Add { sid, symbols } => json!({"id": id, "cmd": "update_subscription", "params": {"sids": [sid], "action": "add_markets", "market_tickers": symbols}}),
            Command::Delete { sid, symbols } => json!({"id": id, "cmd": "update_subscription", "params": {"sids": [sid], "action": "delete_markets", "market_tickers": symbols}}),
        }.to_string())
    }

    fn decode(&self, message: &str, _received_at: DateTime<Utc>) -> Result<Frame> {
        // Parse market messages once. RawValue is confined to snapshot arrays;
        // deltas borrow directly from the frame without scanning msg twice.
        if let Ok(frame) = serde_json::from_str::<MarketFrame<'_>>(message)
            && matches!(
                frame.kind.as_ref(),
                "orderbook_snapshot" | "orderbook_delta"
            )
        {
            let mut sid = frame.sid;
            let msg = frame.msg;
            sid = sid.or(msg.sid);
            ensure!(
                sid.is_some() && frame.seq.is_some(),
                "orderbook message lacks sid/seq"
            );
            report_clockless(msg.clock);
            let exchange_at = if let Some(ms) = msg.ts_ms.or(frame.sending_ts_ms) {
                DateTime::from_timestamp_millis(ms).context("invalid exchange timestamp")?
            } else if let Some(ts) = msg.ts {
                DateTime::parse_from_rfc3339(&ts.0)?.with_timezone(&Utc)
            } else {
                DateTime::UNIX_EPOCH
            };
            let change = if frame.kind == "orderbook_snapshot" {
                Change::Snapshot(Book::from_levels(
                    wire_levels(msg.yes_dollars_fp, msg.yes, false)?,
                    wire_levels(msg.no_dollars_fp, msg.no, true)?,
                )?)
            } else {
                let mut price = match msg.price_dollars {
                    Some(p) => fixed(&p.0)?,
                    None => msg
                        .price
                        .context("missing delta price")?
                        .checked_mul(10000)
                        .context("price overflow")?,
                };
                let side = match msg.side.as_ref().map(|s| s.0.as_ref()) {
                    Some("yes") => Side::Buy,
                    Some("no") => {
                        price = SCALE.checked_sub(price).context("price overflow")?;
                        Side::Sell
                    }
                    _ => bail!("invalid orderbook side"),
                };
                let delta = match msg.delta_fp {
                    Some(q) => fixed(&q.0)?,
                    None => msg
                        .delta
                        .context("missing delta quantity")?
                        .checked_mul(SCALE)
                        .context("quantity overflow")?,
                };
                ensure!((0..=SCALE).contains(&price), "price outside [0,1]");
                Change::Delta { side, price, delta }
            };
            let payload = Payload::Market(MarketData {
                symbol: msg.market_ticker.into_owned(),
                exchange_at,
                clock: msg.clock,
                change,
            });
            return Ok(Frame {
                id: frame.id,
                sid,
                seq: frame.seq,
                payload,
            });
        }
        let frame: WireFrame<'_> =
            serde_json::from_str(message).context("invalid exchange JSON")?;
        // Never treat a malformed financial message as an ignorable control frame.
        ensure!(
            !matches!(
                frame.kind.as_ref(),
                "orderbook_snapshot" | "orderbook_delta"
            ),
            "invalid market payload"
        );
        let mut sid = frame.sid;
        let payload = {
            let msg: Value = serde_json::from_str(frame.msg.get())?;
            sid = sid.or_else(|| msg["sid"].as_u64());
            match frame.kind.as_ref() {
                "error" => bail!(
                    "exchange command error code {}: {}",
                    msg["code"],
                    msg["msg"]
                ),
                "subscribed" => Payload::Subscribed {
                    sid: sid.context("subscription ack has no sid")?,
                },
                "ok" => Payload::Ack,
                "unsubscribed" => bail!("exchange unexpectedly ended subscription"),
                "market_lifecycle_v2" => {
                    ensure!(
                        sid.is_some() && frame.seq.is_some(),
                        "lifecycle message lacks sid/seq"
                    );
                    if matches!(
                        msg["event_type"].as_str(),
                        Some(
                            "created"
                                | "activated"
                                | "deactivated"
                                | "determined"
                                | "settled"
                                | "close_date_updated"
                        )
                    ) {
                        Payload::Lifecycle(
                            msg["market_ticker"]
                                .as_str()
                                .context("lifecycle message lacks ticker")?
                                .to_owned(),
                        )
                    } else {
                        Payload::Other
                    }
                }
                _ => Payload::Other,
            }
        };
        Ok(Frame {
            id: frame.id,
            sid,
            seq: frame.seq,
            payload,
        })
    }
}

impl Market {
    fn is_live_regular(&self) -> bool {
        matches!(self.status.as_str(), "active" | "open")
            && self
                .mve_collection_ticker
                .as_deref()
                .unwrap_or("")
                .is_empty()
            && self.mve_selected_legs.as_ref().is_none_or(Vec::is_empty)
    }
}

// A wrapper lets serde borrow Cow strings even inside Option/tuple containers.
// Escaped JSON strings still use serde's validated owned fallback.
#[derive(Deserialize)]
struct WireText<'a>(#[serde(borrow)] Cow<'a, str>);

// Borrow strings and raw level arrays from the incoming frame. Only the ticker
// must outlive decoding; numeric fields never allocate a general-purpose JSON tree.
#[derive(Deserialize)]
struct MarketFrame<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: WireMarket<'a>,
}

#[derive(Deserialize)]
struct WireFrame<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    #[serde(borrow, default = "null_message")]
    msg: &'a RawValue,
}
fn null_message() -> &'static RawValue {
    RawValue::NULL
}

#[derive(Deserialize)]
struct WireMarket<'a> {
    sid: Option<u64>,
    #[serde(borrow)]
    market_ticker: Cow<'a, str>,
    clock: Option<u64>,
    ts_ms: Option<i64>,
    #[serde(borrow)]
    ts: Option<WireText<'a>>,
    #[serde(borrow)]
    side: Option<WireText<'a>>,
    #[serde(borrow, default, deserialize_with = "present_text")]
    price_dollars: Option<WireText<'a>>,
    #[serde(borrow, default, deserialize_with = "present_text")]
    delta_fp: Option<WireText<'a>>,
    price: Option<i64>,
    delta: Option<i64>,
    // Preserve explicit null for the legacy fallback; omitted sides are empty.
    #[serde(borrow, default, deserialize_with = "present_raw")]
    yes_dollars_fp: Option<&'a RawValue>,
    #[serde(borrow, default, deserialize_with = "present_raw")]
    no_dollars_fp: Option<&'a RawValue>,
    #[serde(borrow, default, deserialize_with = "present_raw")]
    yes: Option<&'a RawValue>,
    #[serde(borrow, default, deserialize_with = "present_raw")]
    no: Option<&'a RawValue>,
}
fn present_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<WireText<'de>>, D::Error> {
    WireText::deserialize(deserializer).map(Some)
}
fn present_raw<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}
fn report_clockless(clock: Option<u64>) {
    if clock.is_none() {
        static REPORTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !REPORTED.load(Ordering::Relaxed) && !REPORTED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "feed omits exchange clock; source switches reset snapshots, and latency matches use timestamp fingerprints with weaker guarantees"
            );
        }
    }
}
fn wire_levels(
    modern: Option<&RawValue>,
    legacy: Option<&RawValue>,
    complement: bool,
) -> Result<Vec<(i64, i64)>> {
    // Kalshi explicitly omits a side when it has no offers. Absence means
    // an empty side in a complete snapshot, not a broken subscription.
    if let Some(value) = modern.filter(|v| v.get() != "null") {
        // Tuple deserialization also rejects missing/extra elements.
        let pairs: Vec<(WireText<'_>, WireText<'_>)> = serde_json::from_str(value.get())?;
        pairs
            .into_iter()
            .map(|(p, q)| {
                let price = fixed(&p.0)?;
                Ok((
                    if complement {
                        SCALE.checked_sub(price).context("price overflow")?
                    } else {
                        price
                    },
                    fixed(&q.0)?,
                ))
            })
            .collect()
    } else if let Some(value) = legacy.filter(|v| v.get() != "null") {
        let pairs: Vec<(i64, i64)> = serde_json::from_str(value.get())?;
        pairs
            .into_iter()
            .map(|(p, q)| {
                let price = p.checked_mul(10000).context("price overflow")?;
                Ok((
                    if complement {
                        SCALE.checked_sub(price).context("price overflow")?
                    } else {
                        price
                    },
                    q.checked_mul(SCALE).context("quantity overflow")?,
                ))
            })
            .collect()
    } else {
        Ok(Vec::new())
    }
}

/// Sequence numbers belong to subscriptions, never to individual markets.
#[derive(Default)]
pub struct Sequences(HashMap<u64, u64>);

impl Sequences {
    pub fn check(&mut self, sid: Option<u64>, seq: Option<u64>) -> Result<()> {
        if let (Some(sid), Some(seq)) = (sid, seq) {
            if let Some(previous) = self.0.get(&sid) {
                ensure!(
                    previous.checked_add(1) == Some(seq),
                    "subscription {sid} sequence gap/regression: {previous} -> {seq}"
                );
            }
            self.0.insert(sid, seq);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn adapter() -> Kalshi {
        Kalshi::new("http://localhost".into(), "ws://localhost".into(), None).unwrap()
    }
    #[test]
    fn modern_fractional_prices_and_no_complement() {
        let f = adapter()
            .decode(include_str!("../tests/fixtures/snapshot.json"), Utc::now())
            .unwrap();
        let Payload::Market(MarketData {
            change: Change::Snapshot(book),
            ..
        }) = f.payload
        else {
            panic!()
        };
        assert_eq!(book.bids[&220000], 333000000);
        assert_eq!(book.asks[&440000], 146000000);
        let f = adapter()
            .decode(include_str!("../tests/fixtures/delta.json"), Utc::now())
            .unwrap();
        let Payload::Market(MarketData {
            change: Change::Delta { side, price, delta },
            ..
        }) = f.payload
        else {
            panic!()
        };
        assert_eq!((side, price, delta), (Side::Sell, 440000, -500000));
    }
    #[test]
    fn legacy_and_unknown_timestamp() {
        let f = adapter().decode(r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"A","yes":[[40,2]],"no":[[50,3]]}}"#, Utc::now()).unwrap();
        let Payload::Market(data) = f.payload else {
            panic!()
        };
        assert_eq!(data.exchange_at, DateTime::UNIX_EPOCH);
        let Change::Snapshot(book) = data.change else {
            panic!()
        };
        assert_eq!(book.asks[&500000], 3000000);
    }
    #[test]
    fn subscription_sequence_includes_ack_and_multiple_symbols() {
        let mut s = Sequences::default();
        s.check(Some(1), Some(4)).unwrap();
        s.check(Some(1), Some(5)).unwrap();
        s.check(Some(2), Some(42)).unwrap();
        assert!(s.check(Some(1), Some(7)).is_err());
        assert!(s.check(Some(1), Some(5)).is_err());
    }
    #[test]
    fn malformed_financial_message_is_not_ignored() {
        assert!(adapter().decode(r#"{"type":"orderbook_delta","sid":1,"seq":3,"msg":{"market_ticker":"A","side":"yes","price_dollars":"0.3","delta_fp":"bad"}}"#, Utc::now()).is_err());
    }

    #[test]
    fn borrowed_decoder_preserves_escapes_fallbacks_and_rejects_invalid_modern_fields() {
        let frame = adapter().decode(r#"{"type":"orderbook_delta","seq":1,"msg":{"sid":3,"market_ticker":"A\u002dB","side":"y\u0065s","price_dollars":"0.\u0034","delta_fp":"1.00","ts":"2026-10-27T00:00:00Z"}}"#, Utc::now()).unwrap();
        assert_eq!(frame.sid, Some(3));
        let Payload::Market(data) = frame.payload else {
            panic!()
        };
        assert_eq!(data.symbol, "A-B");
        assert!(matches!(
            data.change,
            Change::Delta {
                side: Side::Buy,
                price: 400000,
                delta: 1000000
            }
        ));
        assert_eq!(data.exchange_at.timestamp(), 1793059200);
        for field in ["price_dollars", "delta_fp"] {
            for value in [json!(null), json!(0.5), json!(true), json!("NaN")] {
                let mut msg = json!({"market_ticker":"A","side":"yes","price":40,"delta":1,"price_dollars":"0.4","delta_fp":"1.00"});
                msg[field] = value;
                assert!(
                    adapter()
                        .decode(
                            &json!({"type":"orderbook_delta","sid":1,"seq":1,"msg":msg})
                                .to_string(),
                            Utc::now()
                        )
                        .is_err()
                );
            }
        }
        for levels in [
            json!([["0.4"]]),
            json!([["0.4", "2", "extra"]]),
            json!([["0.4", "2"], ["0.4", "3"]]),
            json!([["0.4", "-1"]]),
        ] {
            assert!(adapter().decode(&json!({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"A","yes_dollars_fp":levels,"no_dollars_fp":[]}}).to_string(),Utc::now()).is_err());
        }
    }

    #[test]
    fn snapshots_accept_documented_omitted_empty_sides() {
        for msg in [
            json!({"market_ticker":"A"}),
            json!({"market_ticker":"A","yes_dollars_fp":[]}),
            json!({"market_ticker":"A","no":null}),
            json!({"market_ticker":"A","yes_dollars_fp":[],"no_dollars_fp":[]}),
            json!({"market_ticker":"A","yes":null,"no":null}),
        ] {
            let frame = json!({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":msg});
            let decoded = adapter().decode(&frame.to_string(), Utc::now()).unwrap();
            assert!(
                matches!(decoded.payload, Payload::Market(MarketData { change: Change::Snapshot(book), .. }) if book == Book::default())
            );
        }
        let decoded = adapter()
            .decode(
                include_str!("../tests/fixtures/kalshi-omitted-empty-side.json"),
                Utc::now(),
            )
            .unwrap();
        let Payload::Market(MarketData {
            change: Change::Snapshot(book),
            ..
        }) = decoded.payload
        else {
            panic!()
        };
        assert!(book.bids.is_empty());
        assert_eq!(book.asks.len(), 10);
        assert_eq!(book.asks[&10000], 5_436_080_000);
    }
}
