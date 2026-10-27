use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
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
}

pub struct Kalshi {
    pub rest_url: String,
    pub ws_url: String,
    pub credentials: Option<Arc<Credentials>>,
    client: reqwest::Client,
    symbols: Vec<String>,
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
    async fn discover(&self) -> Result<BTreeSet<String>> {
        let mut symbols = BTreeSet::new();
        let mut cursor = String::new();
        let mut cursors = HashSet::new();
        let tickers = self.symbols.join(",");
        let mut pages = 0usize;
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
                        let delay = r
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(1 << retry)
                            .min(60);
                        tracing::warn!(status = %r.status(), delay, "discovery throttled; retrying page");
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
                if matches!(market.status.as_str(), "active" | "open")
                    && market
                        .mve_collection_ticker
                        .as_deref()
                        .unwrap_or("")
                        .is_empty()
                    && market.mve_selected_legs.as_ref().is_none_or(Vec::is_empty)
                {
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
                    "market discovery in progress"
                );
            }
            ensure!(
                cursors.insert(page.cursor.clone()),
                "discovery cursor repeated; refusing partial market universe"
            );
            cursor = page.cursor;
            // Stay below common REST rate limits, even for a large universe.
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
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

    fn decode(&self, message: &str, received_at: DateTime<Utc>) -> Result<Frame> {
        let v: Value = serde_json::from_str(message).context("invalid exchange JSON")?;
        let msg = &v["msg"];
        let sid = v["sid"].as_u64().or_else(|| msg["sid"].as_u64());
        let payload = match v["type"].as_str().context("message has no type")? {
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
            "orderbook_snapshot" | "orderbook_delta" => {
                ensure!(
                    sid.is_some() && v["seq"].as_u64().is_some(),
                    "orderbook message lacks sid/seq"
                );
                let symbol = msg["market_ticker"]
                    .as_str()
                    .context("missing market ticker")?
                    .to_owned();
                if msg["clock"].as_u64().is_none() {
                    static CLOCKLESS_REPORTED: std::sync::atomic::AtomicBool =
                        std::sync::atomic::AtomicBool::new(false);
                    if !CLOCKLESS_REPORTED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        tracing::warn!(
                            "feed omits exchange clock; source switches reset snapshots, and latency matches use timestamp fingerprints with weaker guarantees"
                        );
                    }
                }
                let exchange_at = if let Some(ms) = msg["ts_ms"]
                    .as_i64()
                    .or_else(|| v["sending_ts_ms"].as_i64())
                {
                    DateTime::from_timestamp_millis(ms).context("invalid exchange timestamp")?
                } else if let Some(ts) = msg["ts"].as_str() {
                    DateTime::parse_from_rfc3339(ts)?.with_timezone(&Utc)
                } else {
                    // The schema has no nullable timestamp. Zero explicitly means unknown.
                    DateTime::UNIX_EPOCH
                };
                let change = if v["type"] == "orderbook_snapshot" {
                    let bids = levels(msg, "yes_dollars_fp", "yes", false)?;
                    let asks = levels(msg, "no_dollars_fp", "no", true)?;
                    Change::Snapshot(Book::from_levels(bids, asks)?)
                } else {
                    let mut price = match msg.get("price_dollars") {
                        Some(p) => fixed(
                            p.as_str()
                                .context("price_dollars must be a decimal string")?,
                        )?,
                        None => msg["price"]
                            .as_i64()
                            .context("missing delta price")?
                            .checked_mul(10000)
                            .context("price overflow")?,
                    };
                    let side = match msg["side"].as_str() {
                        Some("yes") => Side::Buy,
                        Some("no") => {
                            price = SCALE.checked_sub(price).context("price overflow")?;
                            Side::Sell
                        }
                        _ => bail!("invalid orderbook side"),
                    };
                    let delta = match msg.get("delta_fp") {
                        Some(q) => fixed(q.as_str().context("delta_fp must be a decimal string")?)?,
                        None => msg["delta"]
                            .as_i64()
                            .context("missing delta quantity")?
                            .checked_mul(SCALE)
                            .context("quantity overflow")?,
                    };
                    ensure!((0..=SCALE).contains(&price), "price outside [0,1]");
                    Change::Delta { side, price, delta }
                };
                let _ = received_at; // Receive time is carried by the transport, never substituted for exchange time.
                Payload::Market(MarketData {
                    symbol,
                    exchange_at,
                    clock: msg["clock"].as_u64(),
                    change,
                })
            }
            _ => Payload::Other,
        };
        Ok(Frame {
            id: v["id"].as_u64(),
            sid,
            seq: v["seq"].as_u64(),
            payload,
        })
    }
}

fn levels(msg: &Value, current: &str, legacy: &str, complement: bool) -> Result<Vec<(i64, i64)>> {
    let modern = msg.get(current).filter(|v| !v.is_null());
    let value = modern.or_else(|| msg.get(legacy).filter(|v| !v.is_null()));
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .context("snapshot levels must be arrays")?
        .iter()
        .map(|level| {
            let pair = level.as_array().context("invalid snapshot level")?;
            ensure!(
                pair.len() == 2,
                "snapshot level must contain price and quantity"
            );
            let (price, quantity) = if modern.is_some() {
                (
                    fixed(
                        pair[0]
                            .as_str()
                            .context("snapshot price must be a string")?,
                    )?,
                    fixed(
                        pair[1]
                            .as_str()
                            .context("snapshot quantity must be a string")?,
                    )?,
                )
            } else {
                (
                    pair[0]
                        .as_i64()
                        .context("invalid legacy price")?
                        .checked_mul(10000)
                        .context("price overflow")?,
                    pair[1]
                        .as_i64()
                        .context("invalid legacy quantity")?
                        .checked_mul(SCALE)
                        .context("quantity overflow")?,
                )
            };
            Ok((
                if complement {
                    SCALE.checked_sub(price).context("price overflow")?
                } else {
                    price
                },
                quantity,
            ))
        })
        .collect()
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
}
