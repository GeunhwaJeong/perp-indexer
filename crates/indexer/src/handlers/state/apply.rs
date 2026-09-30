// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Writes a batch to the database. The caller runs this inside the transaction that also advances
//! the pipeline's watermark, so a batch is applied exactly once.

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Numeric, SmallInt, Text};
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;
use haneul_indexer_alt_framework::postgres::Connection;
use perp_schema::schema::{
    accounts, candles, collateral_transfers, fills, funding_payments, funding_updates, markets,
    oracle_prices, order_tickets, orders, positions,
};

use super::batch::Batch;
use super::change::TicketChange;

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

    // Positions.
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
    for chunk in batch.fills.chunks(CHUNK_ROWS) {
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
    for chunk in batch.funding_payments.chunks(CHUNK_ROWS) {
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

    for change in &batch.tickets {
        rows += apply_ticket(change, conn).await?;
    }

    Ok(rows)
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
