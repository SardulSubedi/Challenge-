use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{interval, interval_at},
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    engine::{Engine, Outcome},
    exchange::Exchange,
    output::{Output, Sink},
    sharding::Shards,
    worker::{ConnectGate, Event, Worker},
};

pub async fn run(
    config: Config,
    exchange: Arc<dyn Exchange>,
    cancel: CancellationToken,
) -> Result<()> {
    let config = Arc::new(config);
    let initial = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        result = exchange.discover() => filter_symbols(result?, &config),
    };
    let needed = initial.len().div_ceil(config.symbols_per_socket) * config.replicas;
    anyhow::ensure!(
        needed <= config.max_sockets,
        "universe needs {needed} sockets, exceeding --max-sockets {}; increase --symbols-per-socket or the connection budget",
        config.max_sockets
    );
    tracing::info!(
        markets = initial.len(),
        replicas = config.replicas,
        "discovered live regular markets"
    );
    let sink = Sink::open(
        config.output.clone(),
        !config.no_stdout,
        config.latency_log.clone(),
    )?;
    let timer = config.run_secs.map(|secs| {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
            cancel.cancel();
        })
    });
    let (output_tx, output_rx) = mpsc::channel(config.queue_capacity);
    let mut writer = tokio::task::spawn_blocking(move || sink.run(output_rx));
    let (events_tx, mut events_rx) = mpsc::channel(config.queue_capacity);
    let (universe_tx, mut universe_rx) = watch::channel(initial.clone());
    let discovery_config = config.clone();
    let discovery_exchange = exchange.clone();
    let discovery_cancel = cancel.clone();
    let discovery = tokio::spawn(async move {
        loop {
            tokio::select! { _ = discovery_cancel.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(discovery_config.discovery_secs)) => {} }
            let result = tokio::select! { _ = discovery_cancel.cancelled() => break, result = discovery_exchange.discover() => result };
            match result {
                Ok(symbols) => {
                    universe_tx.send_replace(filter_symbols(symbols, &discovery_config));
                }
                Err(error) => {
                    tracing::warn!(%error, "discovery failed; retaining last complete universe")
                }
            }
        }
    });
    let gate = Arc::new(ConnectGate::new(Duration::from_millis(
        config.connect_interval_ms,
    )));
    let mut engine = Engine::new(config.failover()).with_timing(config.latency_log.is_some());
    let mut shards = Shards::new(config.symbols_per_socket);
    let mut subscriptions = Vec::<watch::Sender<BTreeSet<String>>>::new();
    let mut repair_txs = Vec::<mpsc::UnboundedSender<String>>::new();
    let mut workers = JoinSet::new();
    let mut universe = initial;
    let mut needs_reconcile = true;
    let mut shutdown = false;
    let mut fatal = None;
    let mut writer_finished = false;
    let snapshot_period = Duration::from_secs(config.snapshot_secs);
    let mut snapshots = interval_at(
        tokio::time::Instant::now() + snapshot_period,
        snapshot_period,
    );
    let mut flush = interval(Duration::from_secs(1));
    let mut health = interval(Duration::from_millis(250));
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut metrics = interval(Duration::from_secs(5));
    for interval in [&mut snapshots, &mut flush, &mut metrics] {
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    }
    let mut max_queue_delay_us = 0i64;
    let mut metrics_at = Instant::now();
    let mut last_deltas = 0u64;
    let mut coverage_announced = false;

    loop {
        if needs_reconcile && !shutdown {
            engine.set_symbols(&universe);
            shards.reconcile(&universe);
            if shards.assignments.len() * config.replicas > config.max_sockets {
                fatal = Some(anyhow::anyhow!(
                    "market universe outgrew connection budget; increase --symbols-per-socket or --max-sockets"
                ));
                cancel.cancel();
                shutdown = true;
                needs_reconcile = false;
                continue;
            }
            let initial_ramp = subscriptions.is_empty();
            for (shard, symbols) in shards.assignments.iter().enumerate() {
                for replica in 0..config.replicas {
                    let source = shard * config.replicas + replica;
                    if source == subscriptions.len() {
                        let (desired_tx, desired_rx) = watch::channel(symbols.clone());
                        let (repair_tx, repairs) = mpsc::unbounded_channel();
                        subscriptions.push(desired_tx);
                        repair_txs.push(repair_tx);
                        let worker = Worker {
                            source,
                            config: config.clone(),
                            exchange: exchange.clone(),
                            desired: desired_rx,
                            repairs,
                            events: events_tx.clone(),
                            cancel: cancel.clone(),
                            gate: gate.clone(),
                        };
                        // Cover every shard once before spending startup slots on
                        // replicas. The same global gate still limits all starts.
                        let delay = if initial_ramp {
                            Duration::from_millis(
                                config
                                    .connect_interval_ms
                                    .saturating_mul(shards.assignments.len() as u64)
                                    .saturating_mul(replica as u64),
                            )
                        } else {
                            Duration::ZERO
                        };
                        workers.spawn(worker.run_after(delay));
                    } else if *subscriptions[source].borrow() != *symbols {
                        subscriptions[source].send_replace(symbols.clone());
                    }
                }
            }
            tracing::info!(
                first_copy_ramp_secs = shards.assignments.len().saturating_sub(1) as u64
                    * config.connect_interval_ms
                    / 1000,
                markets = universe.len(),
                shards = shards.assignments.len(),
                sockets = subscriptions.len(),
                "subscription universe reconciled"
            );
            if initial_ramp {
                tracing::info!(
                    connection_ramp_secs = subscriptions.len().saturating_sub(1) as u64
                        * config.connect_interval_ms
                        / 1000,
                    "connections start at a paced rate; market coverage grows during startup"
                );
            }
            coverage_announced = false;
            needs_reconcile = false;
        }
        if shutdown && workers.is_empty() && events_rx.is_empty() {
            break;
        }
        let mut to_write = Vec::new();
        tokio::select! {
            _ = cancel.cancelled(), if !shutdown => {
                shutdown = true;
                tracing::info!("shutdown requested; closing sockets and draining captured data");
            }
            result = &mut writer, if !writer_finished => {
                writer_finished = true;
                fatal = Some(result.context("output writer panicked").and_then(|r| r).err().unwrap_or_else(|| anyhow::anyhow!("output writer stopped unexpectedly")));
                cancel.cancel(); shutdown = true;
            }
            Some(result) = workers.join_next(), if !workers.is_empty() => {
                if let Err(error) = result.context("socket task panicked").and_then(|r| r) {
                    fatal = Some(error); cancel.cancel(); shutdown = true;
                }
            }
            changed = universe_rx.changed(), if !shutdown => {
                if changed.is_ok() {
                    let updated = universe_rx.borrow_and_update().clone();
                    if updated != universe { universe = updated; needs_reconcile = true; }
                }
                else if !cancel.is_cancelled() { fatal = Some(anyhow::anyhow!("market discovery task stopped")); cancel.cancel(); shutdown = true; }
            }
            Some(event) = events_rx.recv() => {
                let now = Instant::now();
                let timestamp = Utc::now();
                match event {
                    Event::Data { source, data, received_at, now: received_mono } => {
                        max_queue_delay_us = max_queue_delay_us.max((timestamp - received_at).num_microseconds().unwrap_or(0));
                        let Outcome { records, repairs, observation } = engine.ingest(source, data, received_at, received_mono);
                        to_write.extend(records.into_iter().map(Output::Record));
                        if config.latency_log.is_some() && let Some(o) = observation { to_write.push(Output::Observation(o)); }
                        for (source, symbol) in repairs { if let Some(tx) = repair_txs.get(source) { let _ = tx.send(symbol); } }
                    }
                    Event::Invalid { source, symbols } => {
                        if !cancel.is_cancelled() { to_write.extend(engine.invalidate(source, symbols.as_ref(), timestamp, now).into_iter().map(Output::Record)); }
                    }
                }
            }
            _ = snapshots.tick(), if !shutdown => {
                to_write.extend(engine.snapshots(Utc::now()).into_iter().map(Output::Record));
                to_write.push(Output::Checkpoint);
            }
            _ = flush.tick(), if !shutdown => { to_write.push(Output::Flush); }
            _ = health.tick(), if !shutdown => {
                // Drain captured duplicates before diagnosing a source stall. Collector
                // queue pressure must not masquerade as exchange-side data loss.
                if events_rx.is_empty() { to_write.extend(engine.check_lagging(Utc::now(), Instant::now()).into_iter().map(Output::Record)); }
            }
            _ = metrics.tick(), if !shutdown => {
                let now = Instant::now();
                let elapsed = now.duration_since(metrics_at).as_secs_f64();
                let updates_per_sec = (engine.deltas_emitted - last_deltas) as f64 / elapsed.max(0.001);
                let ready = engine.ready_markets();
                let coverage_pct = ready as f64 * 100.0 / engine.market_count().max(1) as f64;
                tracing::info!(markets = engine.market_count(), ready, coverage_pct = format_args!("{coverage_pct:.1}"),
                    updates_per_sec = format_args!("{updates_per_sec:.0}"), deltas = engine.deltas_emitted,
                    snapshots = engine.snapshots_emitted, failovers = engine.failovers, max_queue_delay_us,
                    queue_depth = events_rx.len(), "collector health");
                max_queue_delay_us = 0;
                metrics_at = now;
                last_deltas = engine.deltas_emitted;
                if !coverage_announced && ready == engine.market_count() && ready > 0 {
                    tracing::info!(markets = ready, "all current markets have initialized books");
                    coverage_announced = true;
                }
            }
        }
        if !writer_finished {
            for item in to_write {
                if output_tx.send(item).await.is_err() {
                    cancel.cancel();
                    shutdown = true;
                    break;
                }
            }
        }
    }
    cancel.cancel();
    if let Some(timer) = timer {
        timer.abort();
    }
    discovery.await.context("discovery task panicked")?;
    drop(events_tx);
    drop(output_tx);
    if !writer_finished
        && let Err(error) = writer
            .await
            .context("output writer panicked")
            .and_then(|r| r)
    {
        fatal = Some(error);
    }
    tracing::info!(
        deltas = engine.deltas_emitted,
        snapshots = engine.snapshots_emitted,
        "collector stopped; output flushed and closed"
    );
    if let Some(error) = fatal {
        return Err(error);
    }
    anyhow::ensure!(
        engine.market_count() == 0 || engine.snapshots_emitted > 0,
        "no market snapshots collected; check credentials, endpoints, and connectivity"
    );
    Ok(())
}

fn filter_symbols(mut symbols: BTreeSet<String>, config: &Config) -> BTreeSet<String> {
    if !config.symbols.is_empty() {
        symbols.retain(|symbol| config.symbols.contains(symbol));
    }
    symbols
}
