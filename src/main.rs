use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use engineering_challenge::{
    auth::{Credentials, PrivateKey},
    config::Config,
    exchange::{Exchange, Kalshi},
    runtime,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_ansi(false)
        .init();
    let config = Config::parse();
    config.validate()?;
    let credentials = if config.list_markets {
        None
    } else {
        let id = config
            .api_key_id
            .clone()
            .context("set KALSHI_API_KEY_ID in .env or environment")?;
        let path = config
            .private_key_path
            .as_ref()
            .context("set KALSHI_PRIVATE_KEY_PATH to your local private-key file")?;
        Some(Arc::new(Credentials {
            id,
            key: PrivateKey::load(path)?,
        }))
    };
    let exchange = Arc::new(
        Kalshi::new(
            config.rest_endpoint().into(),
            config.ws_endpoint().into(),
            credentials,
        )?
        .with_symbols(config.symbols.clone()),
    );
    if config.list_markets {
        for symbol in exchange.discover().await? {
            println!("{symbol}");
        }
        return Ok(());
    }
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await?;
        }
        signal_cancel.cancel();
        Ok::<(), std::io::Error>(())
    });
    let result = runtime::run(config, exchange, cancel).await;
    signal.abort();
    result
}
