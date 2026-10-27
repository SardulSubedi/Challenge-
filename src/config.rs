use std::{path::PathBuf, time::Duration};

use anyhow::{Result, ensure};
use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Complete, redundant Kalshi orderbook collector. Binary SBE on stdout; diagnostics on stderr."
)]
pub struct Config {
    #[arg(long, env = "KALSHI_API_KEY_ID", hide_env_values = true)]
    pub api_key_id: Option<String>,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    pub private_key_path: Option<PathBuf>,
    /// Use demo endpoints (requires a demo key).
    #[arg(long, env = "KALSHI_DEMO", default_value_t = false)]
    pub demo: bool,
    #[arg(long, env = "KALSHI_REST_URL")]
    pub rest_url: Option<String>,
    #[arg(long, env = "KALSHI_WS_URL")]
    pub ws_url: Option<String>,
    #[arg(long, default_value_t = 2000)]
    pub symbols_per_socket: usize,
    /// Connection budget. Fails explicitly if the universe needs more sockets.
    #[arg(long, default_value_t = 160)]
    pub max_sockets: usize,
    #[arg(long, default_value_t = 500)]
    pub connect_interval_ms: u64,
    #[arg(long, default_value_t = 2)]
    pub replicas: usize,
    #[arg(long, default_value_t = 60)]
    pub discovery_secs: u64,
    #[arg(long, default_value_t = 60)]
    pub snapshot_secs: u64,
    #[arg(long, default_value_t = 300)]
    pub quiet_secs: u64,
    #[arg(long, default_value_t = 1800)]
    pub recycle_secs: u64,
    #[arg(long, default_value_t = 30)]
    pub heartbeat_timeout_secs: u64,
    /// Switch to a caught-up replica if the active symbol falls this far behind.
    #[arg(long, default_value_t = 2000)]
    pub failover_ms: u64,
    #[arg(long, default_value_t = 16384)]
    pub queue_capacity: usize,
    /// Optional aligned SBE archive. Refuses to overwrite existing files.
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Disable binary stdout (use with --output).
    #[arg(long)]
    pub no_stdout: bool,
    /// Receive timing observations for cross-connection latency analysis.
    #[arg(long)]
    pub latency_log: Option<PathBuf>,
    /// Atomically replaced JSON status with exact missing/under-replicated symbols.
    #[arg(long)]
    pub coverage_report: Option<PathBuf>,
    /// Optional explicit market allow-list for smoke tests. Default: all live regular markets.
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,
    /// Stop cleanly after this many seconds. Default: run until Ctrl+C/SIGTERM.
    #[arg(long)]
    pub run_secs: Option<u64>,
    /// List live regular market tickers and exit; no private key required.
    #[arg(long)]
    pub list_markets: bool,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.symbols_per_socket > 0 && self.symbols_per_socket <= 10000,
            "symbols-per-socket must be 1..=10000"
        );
        ensure!(
            self.replicas > 0 && self.replicas <= 8,
            "replicas must be 1..=8"
        );
        ensure!(
            self.max_sockets >= self.replicas && self.connect_interval_ms > 0,
            "max-sockets must allow at least one shard and connect-interval-ms must be positive"
        );
        ensure!(
            self.discovery_secs > 0 && self.snapshot_secs > 0,
            "intervals must be positive"
        );
        ensure!(
            self.quiet_secs >= 30 && self.recycle_secs >= 60,
            "quiet-secs >= 30 and recycle-secs >= 60 required"
        );
        ensure!(
            self.heartbeat_timeout_secs >= 15 && self.failover_ms > 0,
            "heartbeat timeout >= 15 and positive failover-ms required"
        );
        ensure!(self.queue_capacity > 0, "queue-capacity must be positive");
        ensure!(
            self.list_markets || !self.no_stdout || self.output.is_some(),
            "--no-stdout requires --output"
        );
        Ok(())
    }

    pub fn rest_endpoint(&self) -> &str {
        self.rest_url.as_deref().unwrap_or(if self.demo {
            "https://external-api.demo.kalshi.co/trade-api/v2"
        } else {
            "https://external-api.kalshi.com/trade-api/v2"
        })
    }

    pub fn ws_endpoint(&self) -> &str {
        self.ws_url.as_deref().unwrap_or(if self.demo {
            "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
        } else {
            "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
        })
    }

    pub fn failover(&self) -> Duration {
        Duration::from_millis(self.failover_ms)
    }
}
