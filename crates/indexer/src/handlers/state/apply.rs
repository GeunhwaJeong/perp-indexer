// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Writes a batch to the database. The caller runs this inside the transaction that also advances
//! the pipeline's watermark, so a batch is applied exactly once.

use std::collections::{BTreeMap, BTreeSet};

use bigdecimal::BigDecimal;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Numeric, SmallInt, Text};
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;
use haneul_indexer_alt_framework::postgres::Connection;
use perp_schema::models::{Fill, FundingPayment, PositionEpisode};
use perp_schema::schema::{
    account_caps, accounts, candles, collateral_transfers, fills, funding_payments,
    funding_updates, markets, oracle_prices, order_tickets, orders, positions,
};
use tracing::warn;

use super::batch::{Batch, CapWrite};
use super::change::TicketChange;
use super::episode::{EpisodeState, PositionEvent};

/// Rows per insert statement. Postgres allows 65535 bind parameters per statement, and the
/// widest table written in bulk has 21 columns.
const CHUNK_ROWS: usize = 2_000;

pub async fn commit(batch: &Batch, conn: &mut Connection<'_>) -> anyhow::Result<usize> {
    let mut rows = 0;

    // Markets. A snapshot creates the row, so it goes before the updates from events.
    for snapshot in batch.market_snapshots.values() {
        rows += diesel::insert_into(markets::table)
            .values(snapshot)
            .on_conflict(markets::market)
            .do_update()
            .set(snapshot)
            .execute(conn)
            .await?;
    }
    // New markets are numbered in the order they appeared on chain.
    for market in &batch.market_order {
        rows += diesel::sql_query(
            "UPDATE markets \
             SET market_index = (SELECT COALESCE(MAX(market_index), -1) + 1 FROM markets) \
             WHERE market = $1 AND market_index IS NULL",
        )
        .bind::<Text, _>(market)
        .execute(conn)
        .await?;
    }
    for created in &batch.markets_created {
        rows += diesel::update(markets::table.find(&created.market))
            .set((
                markets::collateral_decimals.eq(created.collateral_decimals),
                markets::created_checkpoint.eq(created.checkpoint),
                markets::created_at_ms.eq(created.timestamp_ms),
            ))
            .execute(conn)
            .await?;
    }
    for market in &batch.markets_closed {
        rows += diesel::update(markets::table.find(market))
            .set(markets::closed.eq(true))
            .execute(conn)
            .await?;
    }
    for (market, settlement) in &batch.market_settlements {
        rows += diesel::update(markets::table.find(market))
            .set((
                markets::settlement_enabled.eq(settlement.enabled),
                markets::settlement_base_price.eq(&settlement.base_price),
                markets::settlement_collateral_price.eq(&settlement.collateral_price),
            ))
            .execute(conn)
            .await?;
    }
    for (market, prices) in &batch.market_prices {
        rows += diesel::update(markets::table.find(market))
            .set((
                prices
                    .mark_price
                    .as_ref()
                    .map(|p| markets::mark_price.eq(p)),
                prices
                    .index_price
                    .as_ref()
                    .map(|p| markets::index_price.eq(p)),
                prices
                    .book_price
                    .as_ref()
                    .map(|p| markets::book_price.eq(p)),
                markets::prices_updated_at_ms.eq(prices.timestamp_ms),
            ))
            .execute(conn)
            .await?;
    }

    // Accounts.
    for snapshot in batch.account_snapshots.values() {
        rows += diesel::insert_into(accounts::table)
            .values(snapshot)
            .on_conflict(accounts::account_id)
            .do_update()
            .set(snapshot)
            .execute(conn)
            .await?;
    }
    for created in &batch.accounts_created {
        rows += diesel::update(accounts::table.find(created.account_id))
            .set((
                accounts::creator.eq(&created.creator),
                accounts::created_checkpoint.eq(created.checkpoint),
                accounts::created_at_ms.eq(created.timestamp_ms),
            ))
            .execute(conn)
            .await?;
    }

    for (account_id, net) in &batch.net_transfers {
        let updated = diesel::update(accounts::table.find(account_id))
            .set(accounts::net_transfers.eq(accounts::net_transfers + net))
            .execute(conn)
            .await?;
        // A transfer rewrites the account object, so its row was written just above.
        if updated == 0 {
            warn!(account_id, %net, "Transfers of an account that has no row were not counted");
        }
        rows += updated;
    }

    for (cap_id, cap) in &batch.account_caps {
        rows += match cap {
            CapWrite::Held(cap) => {
                diesel::insert_into(account_caps::table)
                    .values(cap)
                    .on_conflict(account_caps::cap_id)
                    .do_update()
                    .set(cap)
                    .execute(conn)
                    .await?
            }
            // The row stays, without an owner, so that readers following the table by
            // checkpoint see the capability go.
            CapWrite::Removed(checkpoint) => {
                diesel::update(account_caps::table.find(cap_id))
                    .set((
                        account_caps::owner.eq(None::<String>),
                        account_caps::updated_checkpoint.eq(checkpoint),
                    ))
                    .execute(conn)
                    .await?
            }
        };
    }

    // Positions. Their running totals continue from where the previous batch left them, so
    // those are read before the snapshots overwrite the sizes they were left at.
    let (episodes, fills, funding_payments) = fold_episodes(batch, conn).await?;
    for write in batch.positions.values() {
        rows += diesel::insert_into(positions::table)
            .values((
                &write.snapshot,
                positions::created_checkpoint.eq(write.first_checkpoint),
                positions::created_at_ms.eq(write.first_timestamp_ms),
            ))
            .on_conflict((positions::market, positions::account_id))
            .do_update()
            .set(&write.snapshot)
            .execute(conn)
            .await?;
    }

    for ((market, account_id), state) in &episodes {
        rows += diesel::update(positions::table.find((market, account_id)))
            .set(&state.episode)
            .execute(conn)
            .await?;
    }

    // Orders. An order is always posted before it is filled or canceled, so inserting the new
    // ones first and then applying the deltas preserves chain order.
    let new_orders: Vec<_> = batch.new_orders.values().collect();
    for chunk in new_orders.chunks(CHUNK_ROWS) {
        rows += diesel::insert_into(orders::table)
            .values(chunk.to_vec())
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
    }
    for ((market, order_id), delta) in &batch.order_deltas {
        rows += diesel::sql_query(
            "UPDATE orders SET \
                 filled = filled + $1, \
                 canceled = canceled + $2, \
                 remaining = $3, \
                 cancel_reason = COALESCE($4, cancel_reason), \
                 status = CASE WHEN $3 > 0 THEN 'open' \
                               WHEN canceled + $2 > 0 THEN 'canceled' \
                               ELSE 'filled' END, \
                 updated_checkpoint = $5, \
                 updated_at_ms = $6 \
             WHERE market = $7 AND order_id = $8",
        )
        .bind::<Numeric, _>(&delta.filled)
        .bind::<Numeric, _>(&delta.canceled)
        .bind::<Numeric, _>(&delta.remaining)
        .bind::<Nullable<SmallInt>, _>(delta.cancel_reason)
        .bind::<BigInt, _>(delta.checkpoint)
        .bind::<BigInt, _>(delta.timestamp_ms)
        .bind::<Text, _>(market)
        .bind::<Numeric, _>(order_id)
        .execute(conn)
        .await?;
    }

    // Fills and the candles built from them.
    for chunk in fills.chunks(CHUNK_ROWS) {
        rows += diesel::insert_into(fills::table)
            .values(chunk)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
    }
    let candles: Vec<_> = batch.candles.values().collect();
    for chunk in candles.chunks(CHUNK_ROWS) {
        // A candle that already exists keeps its open and absorbs the batch's trades.
        rows += diesel::insert_into(candles::table)
            .values(chunk.to_vec())
            .on_conflict((candles::market, candles::resolution_ms, candles::start_ms))
            .do_update()
            .set((
                candles::high.eq(diesel::dsl::sql("GREATEST(candles.high, excluded.high)")),
                candles::low.eq(diesel::dsl::sql("LEAST(candles.low, excluded.low)")),
                candles::close.eq(excluded(candles::close)),
                candles::base_volume.eq(candles::base_volume + excluded(candles::base_volume)),
                candles::quote_volume.eq(candles::quote_volume + excluded(candles::quote_volume)),
                candles::trades.eq(candles::trades + excluded(candles::trades)),
            ))
            .execute(conn)
            .await?;
    }

    for chunk in batch.funding_updates.chunks(CHUNK_ROWS) {
        rows += diesel::insert_into(funding_updates::table)
            .values(chunk)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
    }
    for chunk in funding_payments.chunks(CHUNK_ROWS) {
        rows += diesel::insert_into(funding_payments::table)
            .values(chunk)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
    }
    for chunk in batch.collateral_transfers.chunks(CHUNK_ROWS) {
        rows += diesel::insert_into(collateral_transfers::table)
            .values(chunk)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
    }

    for ((storage_id, source_id), price) in &batch.oracle_prices {
        rows += match price {
            Some(price) => {
                diesel::insert_into(oracle_prices::table)
                    .values(price)
                    .on_conflict((oracle_prices::storage_id, oracle_prices::source_id))
                    .do_update()
                    .set(price)
                    .execute(conn)
                    .await?
            }
            None => {
                diesel::delete(oracle_prices::table.find((storage_id, source_id)))
                    .execute(conn)
                    .await?
            }
        };
    }

    // Funding rows whose checkpoint reported no index price take the one the market last
    // reported, or the oracle's while the market has not reported any.
    let unpriced_updates = batch
        .funding_updates
        .iter()
        .filter(|update| update.index_price.is_none())
        .map(|update| update.checkpoint)
        .min();
    let unpriced_payments = funding_payments
        .iter()
        .filter(|payment| payment.index_price.is_none())
        .map(|payment| payment.checkpoint)
        .min();
    for (table, first) in [
        ("funding_updates", unpriced_updates),
        ("funding_payments", unpriced_payments),
    ] {
        let Some(first) = first else { continue };
        rows += diesel::sql_query(format!(
            "UPDATE {table} f SET index_price = COALESCE(m.index_price, o.price) \
             FROM markets m \
             LEFT JOIN oracle_prices o \
                    ON o.storage_id = m.base_storage_id AND o.source_id = m.base_source_id \
             WHERE f.market = m.market AND f.index_price IS NULL AND f.checkpoint >= $1"
        ))
        .bind::<BigInt, _>(first)
        .execute(conn)
        .await?;
    }

    for change in &batch.tickets {
        rows += apply_ticket(change, conn).await?;
    }

    Ok(rows)
}

type Episodes = BTreeMap<(String, i64), EpisodeState>;

/// Runs the batch's fills and funding payments through the running totals of their positions.
///
/// Returns the totals to store, and the fills and payments annotated with the position each
/// found. Must be called before the batch's position snapshots are written.
async fn fold_episodes(
    batch: &Batch,
    conn: &mut Connection<'_>,
) -> anyhow::Result<(Episodes, Vec<Fill>, Vec<FundingPayment>)> {
    let mut fills = batch.fills.clone();
    let mut funding_payments = batch.funding_payments.clone();
    let mut episodes = Episodes::new();
    if batch.position_events.is_empty() {
        return Ok((episodes, fills, funding_payments));
    }

    // Every position of the batch's accounts in the batch's markets: a superset of the ones
    // needed, found with two short lists instead of a list of pairs.
    let market_ids: BTreeSet<&str> = batch
        .position_events
        .keys()
        .map(|(market, _)| market.as_str())
        .collect();
    let account_ids: Vec<i64> = batch
        .position_events
        .keys()
        .map(|(_, account_id)| *account_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for chunk in account_ids.chunks(CHUNK_ROWS) {
        let stored: Vec<(String, i64, BigDecimal, PositionEpisode)> = positions::table
            .filter(positions::market.eq_any(&market_ids))
            .filter(positions::account_id.eq_any(chunk))
            .select((
                positions::market,
                positions::account_id,
                positions::base,
                PositionEpisode::as_select(),
            ))
            .load(conn)
            .await?;
        for (market, account_id, base, episode) in stored {
            let key = (market, account_id);
            if batch.position_events.contains_key(&key) {
                episodes.insert(key, EpisodeState { base, episode });
            }
        }
    }

    for (key, events) in &batch.position_events {
        let state = episodes.entry(key.clone()).or_default();
        for event in events {
            match *event {
                PositionEvent::Fill(i) => state.apply_fill(&mut fills[i]),
                PositionEvent::Funding(i) => state.apply_funding(&mut funding_payments[i]),
            }
        }
        if let Some(write) = batch.positions.get(key) {
            let snapshot = &write.snapshot;
            let folded = state.base.clone();
            if state.reconcile(
                &snapshot.base,
                &snapshot.quote_notional,
                snapshot.updated_checkpoint,
                snapshot.updated_at_ms,
            ) {
                warn!(
                    market = key.0,
                    account_id = key.1,
                    %folded,
                    object = %snapshot.base,
                    checkpoint = snapshot.updated_checkpoint,
                    "Fills do not add up to the position: totals reset to the object"
                );
            }
        }
    }
    Ok((episodes, fills, funding_payments))
}

async fn apply_ticket(change: &TicketChange, conn: &mut Connection<'_>) -> anyhow::Result<usize> {
    use order_tickets::dsl as t;

    Ok(match change {
        TicketChange::Created(ticket) => {
            diesel::insert_into(order_tickets::table)
                .values(&**ticket)
                .on_conflict_do_nothing()
                .execute(conn)
                .await?
        }
        TicketChange::Status {
            ticket_id,
            status,
            checkpoint,
            timestamp_ms,
        } => {
            // A ticket is deleted after it was executed, finalized or canceled, and that outcome
            // is the status worth keeping: deletion only settles a ticket that is still open.
            let any_status = diesel::dsl::sql::<diesel::sql_types::Bool>(match *status {
                "deleted" => "FALSE",
                _ => "TRUE",
            });
            diesel::update(order_tickets::table.find(ticket_id))
                .filter(t::status.eq("open").or(any_status))
                .set((
                    t::status.eq(status),
                    t::updated_checkpoint.eq(checkpoint),
                    t::updated_at_ms.eq(timestamp_ms),
                ))
                .execute(conn)
                .await?
        }
        TicketChange::Details {
            ticket_id,
            encrypted_details,
            checkpoint,
            timestamp_ms,
        } => {
            diesel::update(order_tickets::table.find(ticket_id))
                .set((
                    t::encrypted_details.eq(encrypted_details),
                    t::updated_checkpoint.eq(checkpoint),
                    t::updated_at_ms.eq(timestamp_ms),
                ))
                .execute(conn)
                .await?
        }
        TicketChange::Executors {
            ticket_id,
            executors,
            checkpoint,
            timestamp_ms,
        } => {
            diesel::update(order_tickets::table.find(ticket_id))
                .set((
                    t::executors.eq(executors),
                    t::updated_checkpoint.eq(checkpoint),
                    t::updated_at_ms.eq(timestamp_ms),
                ))
                .execute(conn)
                .await?
        }
        TicketChange::Progress {
            ticket_id,
            progress,
            checkpoint,
            timestamp_ms,
        } => {
            diesel::update(order_tickets::table.find(ticket_id))
                .set((
                    t::twap_progress.eq(progress),
                    t::updated_checkpoint.eq(checkpoint),
                    t::updated_at_ms.eq(timestamp_ms),
                ))
                .execute(conn)
                .await?
        }
    })
}
