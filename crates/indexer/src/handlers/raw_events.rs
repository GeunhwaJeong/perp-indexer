// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The ledger pipeline: records every event of the indexed packages as it was emitted.

use std::sync::Arc;

use diesel_async::RunQueryDsl;
use haneul_indexer_alt_framework::pipeline::Processor;
use haneul_indexer_alt_framework::postgres::Connection;
use haneul_indexer_alt_framework::postgres::handler::Handler;
use haneul_indexer_alt_framework::types::effects::TransactionEffectsAPI;
use haneul_indexer_alt_framework::types::event::Event;
use haneul_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;
use perp_events::{EVENTS_MODULE, PerpEvent};
use perp_schema::models::RawEvent;
use perp_schema::schema::raw_events;
use tracing::error;

use crate::metrics::IndexerMetrics;
use crate::packages::{IndexedPackage, Packages};

pub struct RawEvents {
    packages: Packages,
    metrics: IndexerMetrics,
}

impl RawEvents {
    pub fn new(packages: Packages, metrics: IndexerMetrics) -> Self {
        Self { packages, metrics }
    }

    /// Decodes an event's payload into `(data, decode_error)`.
    ///
    /// A payload that cannot be decoded is recorded rather than treated as fatal: the ledger keeps
    /// the raw bytes, so it stays complete and the row can be decoded once the decoders catch up.
    fn decode(
        &self,
        package: &IndexedPackage,
        event: &Event,
    ) -> (Option<serde_json::Value>, Option<String>) {
        let module = event.type_.module.as_str();
        let name = event.type_.name.as_str();
        let Some(decoder) = package.decoder.filter(|_| module == EVENTS_MODULE) else {
            return (None, None);
        };
        match PerpEvent::decode(decoder, name, &event.contents) {
            Ok(decoded) => (Some(decoded.to_json()), None),
            Err(e) => {
                self.metrics
                    .undecoded_events
                    .with_label_values(&[&*package.name, name])
                    .inc();
                error!(
                    package = &*package.name,
                    name, "Failed to decode event: {e}"
                );
                (None, Some(e.to_string()))
            }
        }
    }
}

#[async_trait::async_trait]
impl Processor for RawEvents {
    const NAME: &'static str = "raw_events";

    type Value = RawEvent;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<RawEvent>> {
        let mut rows = vec![];
        for (tx_index, tx) in checkpoint.transactions.iter().enumerate() {
            let Some(events) = &tx.events else { continue };
            for (event_index, event) in events.data.iter().enumerate() {
                let Some(package) = self.packages.get(&event.type_.address) else {
                    continue;
                };
                let name = event.type_.name.as_str();
                let (data, decode_error) = self.decode(package, event);
                let market = data
                    .as_ref()
                    .and_then(|data| data.get("ch_id")?.as_str())
                    .map(str::to_owned);

                self.metrics
                    .events
                    .with_label_values(&[&*package.name, name])
                    .inc();
                rows.push(RawEvent {
                    checkpoint: checkpoint.summary.sequence_number as i64,
                    tx_index: tx_index as i64,
                    event_index: event_index as i64,
                    tx_digest: tx.effects.transaction_digest().to_string(),
                    timestamp_ms: checkpoint.summary.timestamp_ms as i64,
                    sender: event.sender.to_string(),
                    package: package.name.to_string(),
                    package_id: event.type_.address.to_canonical_string(true),
                    module: event.type_.module.to_string(),
                    name: name.to_owned(),
                    event_type: event.type_.to_canonical_string(true),
                    market,
                    bcs: event.contents.clone(),
                    data,
                    decode_error,
                });
            }
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl Handler for RawEvents {
    async fn commit<'a>(values: &[RawEvent], conn: &mut Connection<'a>) -> anyhow::Result<usize> {
        Ok(diesel::insert_into(raw_events::table)
            .values(values)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?)
    }
}
