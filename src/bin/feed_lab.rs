use std::{fs::OpenOptions, io::Write, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::{Args, Parser, Subcommand};
use engineering_challenge::{
    auth::{Credentials, PrivateKey},
    exchange::{Exchange, Kalshi},
    experiments, lab,
};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(about = "Read-only Kalshi correctness checks and bounded latency experiments.")]
struct Cli {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long, global = true)]
    report: Option<PathBuf>,
    #[command(subcommand)]
    command: Action,
}

#[derive(Args)]
struct ConnectionArgs {
    #[arg(long, env = "KALSHI_API_KEY_ID", hide_env_values = true, global = true)]
    api_key_id: Option<String>,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH", global = true)]
    private_key_path: Option<PathBuf>,
    #[arg(long, env = "KALSHI_DEMO", global = true)]
    demo: bool,
    #[arg(long, env = "KALSHI_REST_URL", global = true)]
    rest_url: Option<String>,
    #[arg(long, env = "KALSHI_WS_URL", global = true)]
    ws_url: Option<String>,
}

impl ConnectionArgs {
    fn rest(&self) -> &str {
        self.rest_url.as_deref().unwrap_or(if self.demo {
            "https://external-api.demo.kalshi.co/trade-api/v2"
        } else {
            "https://external-api.kalshi.com/trade-api/v2"
        })
    }
    fn exchange(&self) -> Result<Arc<dyn Exchange>> {
        let id = self.api_key_id.clone().context("set KALSHI_API_KEY_ID")?;
        let key = PrivateKey::load(
            self.private_key_path
                .as_ref()
                .context("set KALSHI_PRIVATE_KEY_PATH")?,
        )?;
        let ws = self.ws_url.as_deref().unwrap_or(if self.demo {
            "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
        } else {
            "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
        });
        Ok(Arc::new(Kalshi::new(
            self.rest().into(),
            ws.into(),
            Some(Arc::new(Credentials { id, key })),
        )?))
    }
}

#[derive(Subcommand)]
enum Action {
    /// Reconstruct signed deltas independently; compare with fresh exchange snapshots and replay SBE.
    Verify {
        #[arg(long, value_delimiter = ',')]
        symbols: Vec<String>,
        #[arg(long, default_value_t = 90)]
        seconds: u64,
        #[arg(long, default_value_t = 5)]
        snapshot_every: u64,
        #[arg(long, default_value_t = 100)]
        min_deltas: u64,
        #[arg(long)]
        archive: PathBuf,
    },
    /// Compare explicit TCP source ports against ephemeral ports over repeated connection rounds.
    Ports {
        #[arg(long)]
        symbol: String,
        #[arg(long, value_delimiter = ',', default_value = "41001,41011,41021")]
        ports: Vec<u16>,
        #[arg(long, default_value_t = 3)]
        rounds: usize,
        #[arg(long, default_value_t = 20)]
        seconds: u64,
    },
    /// Observe a real closed->open transition, then compare immediate and delayed connection starts.
    Maintenance {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value_t = 600)]
        wait_seconds: u64,
        #[arg(long, default_value_t = 1000)]
        poll_ms: u64,
        #[arg(long, value_delimiter = ',', default_value = "0,2000,5000")]
        delays_ms: Vec<u64>,
        #[arg(long, default_value_t = 30)]
        seconds: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter("info")
        .init();
    let cli = Cli::parse();
    let mut output = cli
        .report
        .as_ref()
        .map(|path| {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            OpenOptions::new().write(true).create_new(true).open(path)
        })
        .transpose()?;
    let result: Result<(Value, bool)> = async {
        Ok(match cli.command {
            Action::Verify { symbols, seconds, snapshot_every, min_deltas, archive } => {
                ensure!((1..=600).contains(&seconds) && snapshot_every > 0 && snapshot_every < seconds, "verify requires 1..=600 seconds and a shorter positive snapshot interval");
                let count = symbols.len();
                let report = lab::verify(cli.connection.exchange()?, lab::VerifyOptions { symbols, duration: Duration::from_secs(seconds), snapshot_every: Duration::from_secs(snapshot_every) }, Some(&archive)).await?;
                let passed = report.symbols == count && report.snapshot_checks >= count as u64 && report.snapshot_mismatches == 0 && report.deltas >= min_deltas && report.archive_roundtrip;
                (json!({"kind":"live_verification","passed":passed,"minimum_deltas":min_deltas,"report":report}), passed)
            }
            Action::Ports { symbol, ports, rounds, seconds } => {
                ensure!((1..=120).contains(&seconds), "sample seconds must be 1..=120");
                let report = experiments::compare_ports(cli.connection.exchange()?, symbol, ports, rounds, Duration::from_secs(seconds)).await?;
                let passed = report.rounds.len() == rounds && report.rounds.iter().all(|r| r.timing.matched_updates > 0 && r.samples.iter().all(|s| s.error.is_none()));
                (json!({"kind":"source_ports","passed":passed,"causal_port_advantage_proven":false,"report":report}), passed)
            }
            Action::Maintenance { symbol, wait_seconds, poll_ms, delays_ms, seconds } => {
                ensure!(wait_seconds > 0 && poll_ms >= 250 && (1..=120).contains(&seconds), "positive wait, poll >= 250ms, and 1..=120 sample seconds required");
                ensure!(!delays_ms.is_empty() && delays_ms.len() <= 8 && delays_ms.iter().all(|d| *d <= 60000), "one to eight delays of at most 60 seconds required");
                let status = experiments::wait_for_reopen(&format!("{}/exchange/status", cli.connection.rest().trim_end_matches('/')), Duration::from_secs(wait_seconds), Duration::from_millis(poll_ms)).await?;
                if status.observed_reopen_at.is_none() {
                    (json!({"kind":"maintenance","outcome":"inconclusive_no_observed_reopen","measurement_started":false,"status":status}), true)
                } else {
                    let conditions = delays_ms.into_iter().enumerate().map(|(source, delay)| (source, None, Duration::from_millis(delay))).collect();
                    let wave = experiments::sample_wave(cli.connection.exchange()?, symbol, conditions, Duration::from_secs(seconds)).await?;
                    let passed = wave.timing.matched_updates > 0 && wave.samples.iter().all(|s| s.error.is_none());
                    (json!({"kind":"maintenance","outcome":"observed_reopen","measurement_started":true,"passed":passed,"polling_precision_ms":poll_ms,"status":status,"report":wave}), passed)
                }
            }
        })
    }.await;
    let (value, passed) = match result {
        Ok(result) => result,
        Err(error) => (json!({"passed":false,"error":format!("{error:#}")}), false),
    };
    let text = serde_json::to_string_pretty(&value)?;
    if let Some(file) = &mut output {
        file.write_all(text.as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()?;
    }
    println!("{text}");
    ensure!(
        passed,
        "experiment failed or lacked sufficient evidence; inspect the JSON report"
    );
    Ok(())
}
