// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The state pipeline: markets, accounts, positions, orders, fills, candles, funding, collateral
//! transfers and order tickets, maintained in chain order.
//!
//! Unlike the ledger, these tables depend on the order changes are applied in, so the pipeline is
//! sequential: every batch of checkpoints is written in one database transaction together with
//! the watermark, and is therefore applied exactly once.

use std::sync::Arc;

use haneul_indexer_alt_framework::pipeline::Processor;
use haneul_indexer_alt_framework::pipeline::sequential::Handler;
use haneul_indexer_alt_framework::postgres::{Connection, Db};
use haneul_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::packages::Packages;

mod apply;
pub mod batch;
pub mod change;
pub mod extract;

pub struct State {
    packages: Packages,
}

impl State {
    pub fn new(packages: Packages) -> Self {
        Self { packages }
    }
}

#[async_trait::async_trait]
impl Processor for State {
    const NAME: &'static str = "state";

    type Value = change::Change;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<change::Change>> {
        extract::changes(checkpoint, &self.packages)
    }
}

#[async_trait::async_trait]
impl Handler for State {
    type Store = Db;
    type Batch = batch::Batch;

    fn batch(&self, batch: &mut batch::Batch, values: std::vec::IntoIter<change::Change>) {
        for change in values {
            batch.push(change);
        }
    }

    async fn commit<'a>(
        &self,
        batch: &batch::Batch,
        conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        apply::commit(batch, conn).await
    }
}
