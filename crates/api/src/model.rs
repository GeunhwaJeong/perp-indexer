// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The objects of the dYdX v4 indexer API, as the front end validates them.
//!
//! Numbers are decimal strings, times are ISO 8601 strings and heights are checkpoint sequence
//! numbers as strings. A field the front end requires is never optional here.

use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpetualMarket {
    pub clob_pair_id: String,
    pub ticker: String,
    pub status: &'static str,
    pub oracle_price: String,
    #[serde(rename = "priceChange24H")]
    pub price_change_24h: String,
    #[serde(rename = "volume24H")]
    pub volume_24h: String,
    #[serde(rename = "trades24H")]
    pub trades_24h: i64,
    pub next_funding_rate: String,
    pub initial_margin_fraction: String,
    pub maintenance_margin_fraction: String,
    pub open_interest: String,
    pub atomic_resolution: i32,
    pub quantum_conversion_exponent: i32,
    pub tick_size: String,
    pub step_size: String,
    pub step_base_quantums: i64,
    pub subticks_per_tick: i64,
    pub market_type: &'static str,
    pub open_interest_lower_cap: String,
    pub open_interest_upper_cap: String,
    pub base_open_interest: String,
    #[serde(rename = "defaultFundingRate1H")]
    pub default_funding_rate_1h: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub id: String,
    pub subaccount_id: String,
    pub client_id: String,
    pub clob_pair_id: String,
    pub side: &'static str,
    pub size: String,
    pub total_filled: String,
    pub price: String,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub reduce_only: bool,
    pub order_flags: &'static str,
    pub good_til_block_time: Option<String>,
    pub created_at_height: String,
    pub client_metadata: &'static str,
    pub time_in_force: &'static str,
    pub status: &'static str,
    pub post_only: bool,
    pub ticker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removal_reason: Option<&'static str>,
    pub updated_at: String,
    pub updated_at_height: String,
    pub subaccount_number: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    pub id: String,
    pub side: &'static str,
    pub liquidity: &'static str,
    #[serde(rename = "type")]
    pub fill_type: &'static str,
    pub market: String,
    pub market_type: &'static str,
    pub price: String,
    pub size: String,
    pub fee: String,
    pub affiliate_rev_share: &'static str,
    pub created_at: String,
    pub created_at_height: String,
    pub order_id: Option<String>,
    pub client_metadata: Option<&'static str>,
    pub subaccount_number: i64,
    pub builder_fee: Option<String>,
    pub position_size_before: Option<String>,
    pub entry_price_before: Option<String>,
    pub position_side_before: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub id: String,
    pub side: &'static str,
    pub size: String,
    pub price: String,
    #[serde(rename = "type")]
    pub trade_type: &'static str,
    pub created_at: String,
    pub created_at_height: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    /// Present over REST only: the WebSocket identifies a candle by its start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub started_at: String,
    pub ticker: String,
    pub resolution: &'static str,
    pub low: String,
    pub high: String,
    pub open: String,
    pub close: String,
    pub base_token_volume: String,
    pub usd_volume: String,
    pub trades: i64,
    pub starting_open_interest: &'static str,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpetualPosition {
    pub market: String,
    pub status: &'static str,
    pub side: &'static str,
    /// Signed: negative for shorts.
    pub size: String,
    pub max_size: String,
    pub entry_price: String,
    pub realized_pnl: String,
    pub created_at: String,
    pub created_at_height: String,
    pub sum_open: String,
    pub sum_close: String,
    pub net_funding: String,
    pub unrealized_pnl: String,
    pub closed_at: Option<String>,
    pub exit_price: Option<String>,
    pub subaccount_number: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetPosition {
    pub symbol: &'static str,
    pub side: &'static str,
    pub size: String,
    pub asset_id: &'static str,
    pub subaccount_number: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Subaccount {
    pub address: String,
    pub subaccount_number: i64,
    pub equity: String,
    pub free_collateral: String,
    pub open_perpetual_positions: BTreeMap<String, PerpetualPosition>,
    pub asset_positions: BTreeMap<&'static str, AssetPosition>,
    pub margin_enabled: bool,
    pub updated_at_height: String,
    pub latest_processed_block_height: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentSubaccount {
    pub address: String,
    pub parent_subaccount_number: i64,
    pub equity: String,
    pub free_collateral: String,
    pub child_subaccounts: Vec<Subaccount>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferParty {
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subaccount_number: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transfer {
    /// Present over REST only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub sender: TransferParty,
    pub recipient: TransferParty,
    pub size: String,
    pub created_at: String,
    pub created_at_height: String,
    pub symbol: &'static str,
    #[serde(rename = "type")]
    pub transfer_type: &'static str,
    pub transaction_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingPayment {
    pub created_at: String,
    pub created_at_height: String,
    pub perpetual_id: String,
    pub ticker: String,
    pub oracle_price: String,
    pub size: String,
    pub side: &'static str,
    pub rate: String,
    pub payment: String,
    pub subaccount_number: String,
    pub funding_index: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalFunding {
    pub ticker: String,
    pub rate: String,
    pub price: String,
    pub effective_at: String,
    pub effective_at_height: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeHistory {
    pub id: String,
    pub market_id: String,
    pub order_id: Option<String>,
    pub side: &'static str,
    pub position_side: Option<&'static str>,
    pub entry_price: Option<String>,
    pub execution_price: String,
    pub value: String,
    pub prev_size: String,
    pub additional_size: String,
    pub net_fee: String,
    pub time: String,
    pub action: &'static str,
    pub margin_mode: &'static str,
    pub order_type: &'static str,
    pub net_realized_pnl: Option<String>,
    pub net_realized_pnl_percent: Option<String>,
    pub subaccount_number: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Orderbook {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// What an account was worth at one time, as `/v4/pnl` reports it.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PnlTick {
    pub equity: String,
    pub net_transfers: String,
    pub total_pnl: String,
    pub created_at: String,
    pub created_at_height: String,
}

/// The same, in the older shape of `/v4/historical-pnl`.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalPnlTick {
    pub equity: String,
    pub total_pnl: String,
    pub net_transfers: String,
    pub created_at: String,
    pub block_height: String,
    pub block_time: String,
}
