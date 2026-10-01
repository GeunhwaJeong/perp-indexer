// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The on-chain objects the indexer reads state from.
//!
//! Events say what happened; these objects say what the engine's state is afterwards. Reading
//! positions, markets and account balances from the objects a transaction wrote means the indexer
//! never has to reproduce the engine's arithmetic to know them.

use serde::{Deserialize, Serialize};

use crate::types::*;

move_structs! {
    struct Position {
        collateral: U256,
        base_asset_amount: U256,
        quote_asset_notional_amount: U256,
        cum_funding_rate_long: U256,
        cum_funding_rate_short: U256,
        asks_quantity: U256,
        bids_quantity: U256,
        pending_orders: u64,
        initial_margin_ratio: U256,
    }

    struct PositionKey {
        account_id: u64,
    }

    struct CoreParams {
        base_storage_id: u32,
        collateral_storage_id: u32,
        base_source_id: u16,
        collateral_source_id: u16,
        base_pfs_tolerance: u64,
        collateral_pfs_tolerance: u64,
        lot_size: u64,
        tick_size: u64,
        scaling_factor: U256,
        collateral_haircut: U256,
        margin_ratio_initial: U256,
        margin_ratio_maintenance: U256,
    }

    struct FeesParams {
        maker_fee: U256,
        taker_fee: U256,
        liquidation_fee: U256,
        insurance_fund_fee: U256,
        priority_taker_fee: Option<U256>,
    }

    struct TwapParams {
        funding_frequency_ms: u64,
        funding_period_ms: u64,
        premium_twap_frequency_ms: u64,
        premium_twap_period_ms: u64,
        spread_twap_frequency_ms: u64,
        spread_twap_period_ms: u64,
    }

    struct LimitsParams {
        min_order_usd_value: U256,
        max_pending_orders: u64,
        max_open_interest: U256,
        max_open_interest_threshold: U256,
        max_open_interest_position_percent: U256,
        max_book_index_spread: U256,
        max_index_twap_divergence: U256,
        max_bad_debt: U256,
        max_socialize_losses_mr_decrease: U256,
        max_funding_rate: U256,
    }

    struct MarketParams {
        core_params: CoreParams,
        fees_params: FeesParams,
        twap_params: TwapParams,
        limits_params: LimitsParams,
    }

    struct MarketState {
        cum_funding_rate_long: U256,
        cum_funding_rate_short: U256,
        funding_last_upd_ms: u64,
        premium_twap: U256,
        premium_twap_last_upd_ms: u64,
        spread_twap: U256,
        spread_twap_last_upd_ms: u64,
        open_interest: U256,
        fees_accrued: U256,
    }

    struct Orderbook {
        id: Id,
        counter: u64,
        best_ask_price: Option<u64>,
        best_bid_price: Option<u64>,
    }

    struct ClearingHouse {
        id: Id,
        version: u64,
        paused: u8,
        market_params: MarketParams,
        market_state: MarketState,
        orderbook: Orderbook,
    }

    struct Account {
        id: Id,
        account_id: u64,
        collateral: u64,
        active_assistants: Vec<Id>,
    }

    struct AuthorityCap {
        id: Id,
        r#for: Id,
    }
}

/// A dynamic field object, `0x2::dynamic_field::Field<Name, Value>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field<Name, Value> {
    pub id: Id,
    pub name: Name,
    pub value: Value,
}

/// A position as stored on chain: a dynamic field of its clearing house, keyed by account.
pub type PositionField = Field<PositionKey, Position>;
