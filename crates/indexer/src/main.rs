// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use haneul_indexer_alt_framework::ingestion::{ClientArgs, IngestionConfig};
use haneul_indexer_alt_framework::service::Error;
use haneul_indexer_alt_framework::{Indexer, IndexerArgs};
use haneul_indexer_alt_metrics::{MetricsArgs, MetricsService};
use haneul_pg_db::DbArgs;
use perp_indexer::handlers::raw_events::RawEvents;
use perp_indexer::handlers::state::State;
use perp_indexer::metrics::IndexerMetrics;
use perp_indexer::packages::{PackageArg, Packages};
use perp_schema::MIGRATIONS;
use prometheus::Registry;
use tracing::info;
use url::Url;

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[command(flatten)]
    db_args: DbArgs,
    #[command(flatten)]
    indexer_args: IndexerArgs,
    #[command(flatten)]
    client_args: ClientArgs,
    #[clap(env, long, default_value = "0.0.0.0:9184")]
    metrics_address: SocketAddr,
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/perp_indexer"
    )]
    database_url: Url,
    /// A package to index, as `<name>=<address>`. Repeat it for every package, and for every
    /// version of a package that introduced new event types. Events of `perpetuals`,
    /// `perpetuals_orders` and `oracle_aggregator` are decoded; any other name is recorded as raw
    /// bytes only.
    #[clap(
        long = "package",
        env = "PERP_PACKAGES",
        value_delimiter = ',',
        required = true
    )]
    packages: Vec<PackageArg>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    let Args {
        db_args,
        indexer_args,
        client_args,
        metrics_address,
        database_url,
        packages,
    } = Args::parse();

    // A run bounded by --last-checkpoint is a backfill: being interrupted means it did not finish.
    let is_bounded = indexer_args.last_checkpoint.is_some();

    let packages = Packages::new(packages)?;
    for (address, package) in packages.iter() {
        let decoded = package.decoder.is_some();
        info!(package = &*package.name, %address, decoded, "Indexing package");
    }

    let registry = Registry::new_custom(Some("perp_indexer".into()), None)
        .context("Failed to create Prometheus registry.")?;
    let metrics = MetricsService::new(MetricsArgs { metrics_address }, registry.clone());
    let indexer_metrics = IndexerMetrics::new(metrics.registry());

    let mut indexer = Indexer::new_from_pg(
        database_url,
        db_args,
        indexer_args,
        client_args,
        IngestionConfig::default(),
        Some(&MIGRATIONS),
        None,
        metrics.registry(),
    )
    .await?;

    indexer
        .concurrent_pipeline(
            RawEvents::new(packages.clone(), indexer_metrics),
            Default::default(),
        )
        .await?;
    indexer
        .sequential_pipeline(State::new(packages), Default::default())
        .await?;

    let s_indexer = indexer.run().await?;
    let s_metrics = metrics.run().await?;
    match s_indexer.attach(s_metrics).main().await {
        Ok(()) => Ok(()),
        Err(Error::Terminated) if !is_bounded => Ok(()),
        Err(e) => Err(e.into()),
    }
}
