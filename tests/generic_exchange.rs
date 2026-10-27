//! A second wire protocol proves the collector is not tied to Kalshi JSON.
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use clap::Parser;
use engineering_challenge::{
    Side,
    book::Book,
    config::Config,
    exchange::{Change, Command, Exchange, Frame, MarketData, Payload},
    runtime,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain},
    },
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, handshake::client::Request,
};
use tokio_util::sync::CancellationToken;

struct PipeExchange {
    endpoint: String,
}
#[async_trait]
impl Exchange for PipeExchange {
    async fn discover(&self) -> Result<BTreeSet<String>> {
        Ok(["TOY".into()].into())
    }
    fn request(&self) -> Result<Request> {
        Ok(self.endpoint.as_str().into_client_request()?)
    }
    fn command(&self, id: u64, command: &Command) -> Result<String> {
        match command {
            Command::Subscribe(symbols) => Ok(format!("WATCH|{id}|{}", symbols.join(","))),
            _ => bail!("unused command in toy exchange"),
        }
    }
    fn decode(&self, text: &str, received_at: chrono::DateTime<chrono::Utc>) -> Result<Frame> {
        let parts: Vec<_> = text.split('|').collect();
        let kind = *parts.first().context("missing toy kind")?;
        let (id, seq, payload) = match kind {
            "READY" => (
                Some(parts[1].parse()?),
                None,
                Payload::Subscribed { sid: 1 },
            ),
            "BOOK" => (
                None,
                Some(parts[1].parse()?),
                Payload::Market(MarketData {
                    symbol: parts[2].into(),
                    exchange_at: received_at,
                    clock: None,
                    change: Change::Snapshot(Book::from_levels(
                        vec![(parts[3].parse()?, parts[4].parse()?)],
                        vec![],
                    )?),
                }),
            ),
            "CHANGE" => (
                None,
                Some(parts[1].parse()?),
                Payload::Market(MarketData {
                    symbol: parts[2].into(),
                    exchange_at: received_at,
                    clock: None,
                    change: Change::Delta {
                        side: Side::Buy,
                        price: parts[3].parse()?,
                        delta: parts[4].parse()?,
                    },
                }),
            ),
            _ => bail!("unknown toy message"),
        };
        Ok(Frame {
            id,
            sid: Some(1),
            seq,
            payload,
        })
    }
}

#[tokio::test]
async fn generic_collector_accepts_non_kalshi_wire_protocol_without_core_changes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
                Message::Text(command) => {
                    assert_eq!(command.as_str(), "WATCH|1|TOY");
                    break;
                }
                _ => {}
            }
        }
        for frame in [
            "READY|1",
            "BOOK|1|TOY|400000|2000000",
            "CHANGE|2|TOY|400000|500000",
        ] {
            socket.send(Message::Text(frame.into())).await.unwrap();
        }
        sent_tx.send(()).unwrap();
        while let Some(message) = socket.next().await {
            match message.unwrap() {
                Message::Ping(p) => socket.send(Message::Pong(p)).await.unwrap(),
                Message::Close(_) => break,
                _ => {}
            }
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("toy.sbe");
    let mut config = Config::parse_from(["collector", "--replicas", "1", "--no-stdout"]);
    config.output = Some(output.clone());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(runtime::run(
        config,
        Arc::new(PipeExchange { endpoint }),
        cancel.clone(),
    ));
    sent_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(output).unwrap();
    assert!(matches!(
        reader.read_msg().unwrap(),
        Some(AnyMessage::Snapshot(_))
    ));
    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected normalized delta")
    };
    let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
    assert_eq!(
        (update.symbol.as_str(), update.quantity),
        ("TOY", 2_500_000)
    );
    assert!(reader.read_msg().unwrap().is_none());
    server.await.unwrap();
}
