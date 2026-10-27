use anyhow::Result;
use clap::Parser;
use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};
use std::{collections::BTreeSet, path::PathBuf};

#[derive(Parser)]
#[command(about = "Validate a 64 MiB aligned SBE archive and count snapshots/deltas.")]
struct Args {
    input: PathBuf,
}
fn main() -> Result<()> {
    let args = Args::parse();
    let bytes = std::fs::metadata(&args.input)?.len();
    anyhow::ensure!(
        bytes.is_multiple_of(64 << 20),
        "archive is truncated (not a multiple of 64 MiB)"
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(args.input)?;
    let (mut snapshots, mut deltas) = (0u64, 0u64);
    let mut symbols = BTreeSet::new();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                engineering_challenge::book::Book::from_levels(
                    snapshot
                        .bids
                        .iter()
                        .map(|l| (l.price_level, l.quantity))
                        .collect(),
                    snapshot
                        .asks
                        .iter()
                        .map(|l| (l.price_level, l.quantity))
                        .collect(),
                )?;
                symbols.insert(snapshot.symbol);
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let delta = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                anyhow::ensure!(
                    symbols.contains(&delta.symbol),
                    "delta precedes initial snapshot for {}",
                    delta.symbol
                );
                anyhow::ensure!(
                    delta.quantity >= 0
                        && (0..=engineering_challenge::book::SCALE).contains(&delta.price_level),
                    "invalid normalized price or quantity"
                );
                deltas += 1;
            }
            _ => anyhow::bail!("unexpected SBE message type"),
        }
    }
    println!(
        "{}",
        serde_json::json!({"snapshots": snapshots, "deltas": deltas, "symbols": symbols.len(), "bytes": bytes})
    );
    Ok(())
}
