// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use haneul_indexer_alt_metrics::{MetricsArgs, MetricsService};
use haneul_pg_db::{Db, DbArgs};
use perp_api::config::Deployment;
use perp_api::feed::{Feed, FeedConfig};
use perp_api::hub::Hub;
use perp_api::metrics::ApiMetrics;
use perp_api::rest::{self, AppState};
use perp_api::ws::{WsConfig, WsContext};
use prometheus::Registry;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[command(flatten)]
    db_args: DbArgs,
    /// The indexer's database. A read-only role is enough.
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/perp_indexer"
    )]
    database_url: Url,
    /// The deployment description: the markets to list, their tickers and the collateral. It
    /// is the file the front end reads (`perp.<network>.json`).
    #[clap(env, long)]
    deployment: PathBuf,
    #[clap(env, long, default_value = "0.0.0.0:3002")]
    listen_address: SocketAddr,
    #[clap(env, long, default_value = "0.0.0.0:9185")]
    metrics_address: SocketAddr,
    /// How often to look for newly indexed checkpoints, in milliseconds.
    #[clap(long, default_value_t = 100)]
    poll_interval_ms: u64,
    /// The least time between two updates of the markets channel, in milliseconds.
    #[clap(long, default_value_t = 1_000)]
    markets_interval_ms: u64,
    /// How long 24 hour market statistics are reused, in milliseconds.
    #[clap(long, default_value_t = 10_000)]
    stats_interval_ms: u64,
    /// Checkpoints the feed bridges with updates in one go. Past a wider gap, which only an
    /// indexer backfill or an outage opens, clients are made to start over.
    #[clap(long, default_value_t = 5_000)]
    max_round_checkpoints: i64,
    /// Rounds a WebSocket client may fall behind before it is disconnected.
    #[clap(long, default_value_t = 1_024)]
    ws_backlog: usize,
    /// WebSocket connections served at once.
    #[clap(long, default_value_t = 20_000)]
    ws_max_connections: usize,
    #[clap(long, default_value_t = 64)]
    ws_max_subscriptions: usize,
    /// Seconds a WebSocket write may take before the client is considered gone.
    #[clap(long, default_value_t = 10)]
    ws_send_timeout_secs: u64,
    #[clap(long, default_value_t = 30)]
    ws_ping_interval_secs: u64,
    /// Messages a WebSocket client may send per second.
    #[clap(long, default_value_t = 20.0)]
    ws_message_rate: f64,
    /// Messages a WebSocket client may send at once after a pause.
    #[clap(long, default_value_t = 100)]
    ws_message_burst: u32,
    /// Order book levels per side in snapshots.
    #[clap(long, default_value_t = 500)]
    book_depth: usize,
    /// `/health` fails when the indexed chain time is older than this many seconds.
    #[clap(long, default_value_t = 60)]
    max_lag_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();
    let args = Args::parse();

    let deployment = Arc::new(Deployment::load(&args.deployment)?);
    for (ticker, market) in deployment.markets() {
        info!(ticker, market, "Listing market");
    }

    let registry = Registry::new_custom(Some("perp_api".into()), None)
        .context("Failed to create Prometheus registry.")?;
    let metrics_args = MetricsArgs {
        metrics_address: args.metrics_address,
    };
    let metrics_service = MetricsService::new(metrics_args, registry.clone());
    let metrics = ApiMetrics::new(metrics_service.registry());

    let db = Db::for_read(args.database_url, args.db_args)
        .await
        .context("Failed to connect to the database.")?
        .register_metrics(Some("perp_api_db"), &registry)?;

    let cancel = CancellationToken::new();
    let hub = Hub::new(metrics, args.ws_backlog);
    let ws = Arc::new(WsContext {
        hub: hub.clone(),
        db: db.clone(),
        deployment: deployment.clone(),
        config: WsConfig {
            max_connections: args.ws_max_connections,
            max_subscriptions: args.ws_max_subscriptions,
            book_depth: args.book_depth,
            send_timeout: Duration::from_secs(args.ws_send_timeout_secs),
            ping_interval: Duration::from_secs(args.ws_ping_interval_secs),
            max_buffered_rounds: args.ws_backlog,
            message_rate: args.ws_message_rate,
            message_burst: args.ws_message_burst,
        },
        cancel: cancel.clone(),
    });
    let state = Arc::new(AppState {
        db: db.clone(),
        hub: hub.clone(),
        deployment: deployment.clone(),
        ws,
        book_depth: args.book_depth,
        max_lag: Duration::from_secs(args.max_lag_secs),
    });

    let feed = Feed::new(
        db,
        hub,
        deployment,
        FeedConfig {
            poll_interval: Duration::from_millis(args.poll_interval_ms),
            markets_interval: Duration::from_millis(args.markets_interval_ms),
            stats_interval: Duration::from_millis(args.stats_interval_ms),
            max_round_checkpoints: args.max_round_checkpoints,
        },
    );
    let feed = tokio::spawn(feed.run(cancel.clone()));
    let metrics_service = metrics_service.run().await?;

    let listener = TcpListener::bind(args.listen_address)
        .await
        .with_context(|| format!("Failed to bind {}", args.listen_address))?;
    info!(address = %args.listen_address, "Serving the API");

    let shutdown = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("Shutting down");
        shutdown.cancel();
    });

    axum::serve(listener, rest::router(state))
        .with_graceful_shutdown(cancel.clone().cancelled_owned())
        .await?;
    cancel.cancel();
    feed.await?;
    drop(metrics_service);
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("installing a signal handler");
        tokio::select! {
            _ = interrupt => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}
