// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use diesel::prelude::*;
use haneul_field_count::FieldCount;

use crate::schema::raw_events;

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable, FieldCount)]
#[diesel(table_name = raw_events)]
pub struct RawEvent {
    pub checkpoint: i64,
    pub tx_index: i64,
    pub event_index: i64,
    pub tx_digest: String,
    pub timestamp_ms: i64,
    pub sender: String,
    pub package: String,
    pub package_id: String,
    pub module: String,
    pub name: String,
    pub event_type: String,
    pub market: Option<String>,
    pub bcs: Vec<u8>,
    pub data: Option<serde_json::Value>,
    pub decode_error: Option<String>,
}

use bigdecimal::BigDecimal;

use crate::schema::{
    account_caps, accounts, candles, collateral_transfers, fills, funding_payments,
    funding_updates, markets, oracle_prices, order_tickets, orders, pnl_tick_runs, pnl_ticks,
    positions,
};

/// The part of a market row that mirrors the clearing house object.
#[derive(Clone, Debug, PartialEq, Insertable, AsChangeset)]
#[diesel(table_name = markets, primary_key(market), treat_none_as_null = true)]
pub struct MarketSnapshot {
    pub market: String,
    pub collateral_type: String,
    pub version: i64,
    pub paused: i16,
    pub base_storage_id: i64,
    pub base_source_id: i32,
    pub collateral_storage_id: i64,
    pub collateral_source_id: i32,
    pub lot_size: BigDecimal,
    pub tick_size: BigDecimal,
    pub margin_ratio_initial: BigDecimal,
    pub margin_ratio_maintenance: BigDecimal,
    pub maker_fee: BigDecimal,
    pub taker_fee: BigDecimal,
    pub params: serde_json::Value,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub funding_last_upd_ms: i64,
    pub premium_twap: BigDecimal,
    pub premium_twap_last_upd_ms: i64,
    pub spread_twap: BigDecimal,
    pub spread_twap_last_upd_ms: i64,
    pub open_interest: BigDecimal,
    pub fees_accrued: BigDecimal,
    pub order_counter: BigDecimal,
    pub best_ask_price: Option<BigDecimal>,
    pub best_bid_price: Option<BigDecimal>,
    pub updated_checkpoint: i64,
    pub updated_at_ms: i64,
}

/// The part of an account row that mirrors the account object.
#[derive(Clone, Debug, PartialEq, Insertable, AsChangeset)]
#[diesel(table_name = accounts, primary_key(account_id))]
pub struct AccountSnapshot {
    pub account_id: i64,
    pub object_id: String,
    pub collateral_type: String,
    pub collateral: BigDecimal,
    pub updated_checkpoint: i64,
    pub updated_at_ms: i64,
}

/// A position as its dynamic field object holds it.
#[derive(Clone, Debug, PartialEq, Insertable, AsChangeset)]
#[diesel(table_name = positions, primary_key(market, account_id))]
pub struct PositionSnapshot {
    pub market: String,
    pub account_id: i64,
    pub object_id: String,
    pub collateral: BigDecimal,
    pub base: BigDecimal,
    pub quote_notional: BigDecimal,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub asks_quantity: BigDecimal,
    pub bids_quantity: BigDecimal,
    pub pending_orders: i64,
    pub initial_margin_ratio: BigDecimal,
    pub updated_checkpoint: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = orders)]
pub struct Order {
    pub market: String,
    pub order_id: BigDecimal,
    pub account_id: i64,
    pub is_ask: bool,
    pub price: BigDecimal,
    pub size: BigDecimal,
    pub remaining: BigDecimal,
    pub filled: BigDecimal,
    pub canceled: BigDecimal,
    pub status: String,
    pub cancel_reason: Option<i16>,
    pub reduce_only: bool,
    pub expiration_timestamp_ms: Option<BigDecimal>,
    pub client_order_id: Option<BigDecimal>,
    pub integrator_id: Option<i64>,
    pub integrator_fee_rate: i64,
    pub created_checkpoint: i64,
    pub created_at_ms: i64,
    pub created_tx: String,
    pub updated_checkpoint: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = fills)]
pub struct Fill {
    pub checkpoint: i64,
    pub tx_index: i64,
    pub event_index: i64,
    pub fill_index: i64,
    pub tx_digest: String,
    pub timestamp_ms: i64,
    pub market: String,
    pub account_id: i64,
    pub counterparty_account_id: Option<i64>,
    pub is_ask: bool,
    pub liquidity: String,
    pub kind: String,
    pub price: Option<BigDecimal>,
    pub size: BigDecimal,
    pub quote: Option<BigDecimal>,
    pub fee: BigDecimal,
    pub integrator_fee: BigDecimal,
    pub pnl: BigDecimal,
    pub order_id: Option<BigDecimal>,
    pub client_order_id: Option<BigDecimal>,
    pub mark_price: Option<BigDecimal>,
    pub position_base_before: Option<BigDecimal>,
    pub entry_price_before: Option<BigDecimal>,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = candles)]
pub struct Candle {
    pub market: String,
    pub resolution_ms: i64,
    pub start_ms: i64,
    pub open: BigDecimal,
    pub high: BigDecimal,
    pub low: BigDecimal,
    pub close: BigDecimal,
    pub base_volume: BigDecimal,
    pub quote_volume: BigDecimal,
    pub trades: i64,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = funding_updates)]
pub struct FundingUpdate {
    pub checkpoint: i64,
    pub tx_index: i64,
    pub event_index: i64,
    pub timestamp_ms: i64,
    pub market: String,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub funding_last_upd_ms: i64,
    pub index_price: Option<BigDecimal>,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = funding_payments)]
pub struct FundingPayment {
    pub checkpoint: i64,
    pub tx_index: i64,
    pub event_index: i64,
    pub tx_digest: String,
    pub timestamp_ms: i64,
    pub market: String,
    pub account_id: i64,
    pub collateral_change_usd: BigDecimal,
    pub collateral_after: BigDecimal,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub position_base: Option<BigDecimal>,
    pub index_price: Option<BigDecimal>,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = collateral_transfers)]
pub struct CollateralTransfer {
    pub checkpoint: i64,
    pub tx_index: i64,
    pub event_index: i64,
    pub tx_digest: String,
    pub timestamp_ms: i64,
    pub account_id: i64,
    pub kind: String,
    pub market: Option<String>,
    pub amount: BigDecimal,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = oracle_prices, primary_key(storage_id, source_id))]
pub struct OraclePrice {
    pub storage_id: i64,
    pub source_id: i32,
    pub price: BigDecimal,
    pub twap_price: BigDecimal,
    pub timestamp_ms: i64,
    pub updated_checkpoint: i64,
}

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = order_tickets)]
pub struct OrderTicket {
    pub ticket_id: String,
    pub kind: String,
    pub account_id: i64,
    pub collateral_type: String,
    pub market: Option<String>,
    pub status: String,
    pub executors: serde_json::Value,
    pub execution_domain: Option<String>,
    pub gas: BigDecimal,
    pub stop_order_type: Option<BigDecimal>,
    pub encrypted_details: Vec<u8>,
    pub twap_progress: Option<serde_json::Value>,
    pub created_checkpoint: i64,
    pub created_at_ms: i64,
    pub updated_checkpoint: i64,
    pub updated_at_ms: i64,
}

/// A capability over an account, as its object says and as it is owned.
#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = account_caps, primary_key(cap_id), treat_none_as_null = true)]
pub struct AccountCap {
    pub cap_id: String,
    pub account_object_id: String,
    pub role: String,
    pub owner: Option<String>,
    pub updated_checkpoint: i64,
}

/// The running totals of a position since it was last opened.
#[derive(Clone, Debug, Default, PartialEq, Queryable, Selectable, AsChangeset)]
#[diesel(table_name = positions, primary_key(market, account_id), treat_none_as_null = true)]
pub struct PositionEpisode {
    pub opened_checkpoint: Option<i64>,
    pub opened_at_ms: Option<i64>,
    pub max_size: BigDecimal,
    pub sum_open: BigDecimal,
    pub sum_close: BigDecimal,
    pub close_quote: BigDecimal,
    pub entry_quote: BigDecimal,
    pub realized_pnl: BigDecimal,
    pub net_funding: BigDecimal,
}

/// What an account was worth at one checkpoint.
#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = pnl_ticks)]
pub struct PnlTick {
    pub account_id: i64,
    pub bucket_ms: i64,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
    pub equity: BigDecimal,
    pub net_transfers: BigDecimal,
    pub total_pnl: BigDecimal,
}

/// An interval that ticks were taken for.
#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable)]
#[diesel(table_name = pnl_tick_runs)]
pub struct PnlTickRun {
    pub bucket_ms: i64,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
    pub accounts: i64,
}
