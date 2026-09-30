// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Events of `perpetuals::events`: accounts, collateral, orders, fills, funding, liquidations and market administration.

#![allow(clippy::upper_case_acronyms)]

use crate::types::*;

move_events! {
    struct UpgradedVersion {
        id: Id,
        version: u64,
    }

    struct CreatedAccount {
        account_obj_id: Id,
        user: Address,
        account_id: u64,
    }

    struct DepositedCollateral {
        account_id: u64,
        collateral: u64,
    }

    struct AllocatedCollateral {
        ch_id: Id,
        account_id: u64,
        collateral: u64,
    }

    struct WithdrewCollateral {
        account_id: u64,
        collateral: u64,
    }

    struct RegisteredCollateralInfo {
        storage_id: u32,
        source_id: u16,
        scaling_factor: U256,
    }

    struct DeallocatedCollateral {
        ch_id: Id,
        account_id: u64,
        collateral: u64,
    }

    struct CreatedClearingHouse {
        ch_id: Id,
        collateral: String,
        coin_decimals: u64,
        margin_ratio_initial: U256,
        margin_ratio_maintenance: U256,
        base_storage_id: u32,
        base_source_id: u16,
        collateral_storage_id: u32,
        collateral_source_id: u16,
        funding_frequency_ms: u64,
        funding_period_ms: u64,
        premium_twap_frequency_ms: u64,
        premium_twap_period_ms: u64,
        spread_twap_frequency_ms: u64,
        spread_twap_period_ms: u64,
        maker_fee: U256,
        taker_fee: U256,
        liquidation_fee: U256,
        insurance_fund_fee: U256,
        lot_size: u64,
        tick_size: u64,
        max_bad_debt: U256,
        max_socialize_losses_mr_decrease: U256,
        priority_taker_fee: Option<U256>,
    }

    struct ClosedMarket {
        ch_id: Id,
    }

    struct UpdatedSettlementPrices {
        ch_id: Id,
        base_settlement_price: U256,
        collateral_settlement_price: U256,
        settlement_enabled: bool,
    }

    struct UpdatedIntegratorAddress {
        integrator_id: u32,
        previous_integrator_address: Address,
        new_integrator_address: Address,
    }

    struct UpdatedPremiumTwap {
        ch_id: Id,
        actual_book_price: U256,
        clipped_book_price: U256,
        index_price: U256,
        premium_twap: U256,
        premium_twap_last_upd_ms: u64,
    }

    struct UpdatedSpreadTwap {
        ch_id: Id,
        actual_book_price: U256,
        clipped_book_price: U256,
        index_price: U256,
        spread_twap: U256,
        spread_twap_last_upd_ms: u64,
    }

    struct UpdatedFunding {
        ch_id: Id,
        cum_funding_rate_long: U256,
        cum_funding_rate_short: U256,
        funding_last_upd_ms: u64,
    }

    struct SettledFunding {
        ch_id: Id,
        account_id: u64,
        collateral_change_usd: U256,
        collateral_after: U256,
        mkt_funding_rate_long: U256,
        mkt_funding_rate_short: U256,
    }

    struct SetPositionInitialMarginRatio {
        ch_id: Id,
        account_id: u64,
        initial_margin_ratio: U256,
    }

    struct FilledMakerOrders {
        events: Vec<FilledMakerOrder>,
        book_price: Option<u64>,
    }

    struct FilledMakerOrder {
        ch_id: Id,
        maker_account_id: u64,
        taker_account_id: u64,
        order_id: U128,
        client_order_id: Option<u64>,
        filled_size: u64,
        remaining_size: u64,
        canceled_size: u64,
        cancelation_reason: Option<u8>,
        pnl: U256,
        maker_fees: U256,
        mark_price: U256,
        integrator_id: Option<u32>,
        integrator_fee_paid_usd: U256,
    }

    struct FilledTakerOrder {
        ch_id: Id,
        taker_account_id: u64,
        taker_pnl: U256,
        taker_fees: U256,
        integrator_id: Option<u32>,
        integrator_fee_paid_usd: U256,
        base_asset_delta_ask: U256,
        quote_asset_delta_ask: U256,
        base_asset_delta_bid: U256,
        quote_asset_delta_bid: U256,
        mark_price: U256,
    }

    struct ClosedPositionAtSettlementPrices {
        ch_id: Id,
        account_id: u64,
        pnl: U256,
        base_asset_amount: U256,
        quote_asset_amount: U256,
        deallocated_collateral: u64,
        bad_debt: U256,
    }

    struct PostedOrder {
        ch_id: Id,
        account_id: u64,
        order_id: U128,
        client_order_id: Option<u64>,
        order_size: u64,
        reduce_only: bool,
        expiration_timestamp_ms: Option<u64>,
        integrator_id: Option<u32>,
        integrator_fee_rate: u32,
        mark_price: U256,
        book_price: Option<u64>,
    }

    struct CanceledOrder {
        ch_id: Id,
        account_id: u64,
        size: u64,
        order_id: U128,
        client_order_id: Option<u64>,
        cancelation_reason: u8,
        book_price: Option<u64>,
    }

    struct LiquidatedPosition {
        ch_id: Id,
        liqee_account_id: u64,
        liqor_account_id: u64,
        is_liqee_long: bool,
        base_liquidated: U256,
        quote_liquidated: U256,
        liqee_pnl: U256,
        liquidation_fees: U256,
        insurance_fund_fees: U256,
        bad_debt: U256,
        mark_price: U256,
    }

    struct PerformedLiquidation {
        ch_id: Id,
        liqee_account_id: u64,
        liqor_account_id: u64,
        is_liqee_long: bool,
        base_liquidated: U256,
        quote_liquidated: U256,
        liqor_pnl: U256,
        liqor_fees: U256,
        mark_price: U256,
    }

    struct PerformedADL {
        ch_id: Id,
        bad_debt_account_id: u64,
        size_reduced: u64,
        collateral_transferred: U256,
        adl_price: u64,
        counterparty_account_id: u64,
        bad_debt_is_long: bool,
    }

    struct SocializedBadDebt {
        ch_id: Id,
        bad_debt_usd: U256,
        socialized_fundings: U256,
        added_to_long: bool,
        cum_funding_rate_long: U256,
        cum_funding_rate_short: U256,
    }

    struct CreatedPosition {
        ch_id: Id,
        account_id: u64,
        mkt_funding_rate_long: U256,
        mkt_funding_rate_short: U256,
    }

    struct UpdatedMarginRatios {
        ch_id: Id,
        margin_ratio_initial: U256,
        margin_ratio_maintenance: U256,
    }

    struct SetFeeParams {
        ch_id: Id,
        maker_fee: U256,
        taker_fee: U256,
        liquidation_fee: U256,
        insurance_fund_fee: U256,
        priority_taker_fee: Option<U256>,
    }

    struct SetFeeMultiplier {
        ch_id: Id,
        account_id: u64,
        taker_multiplier: U256,
        maker_multiplier: U256,
        expires_ms: u64,
    }

    struct SetTwapParams {
        ch_id: Id,
        funding_frequency_ms: u64,
        funding_period_ms: u64,
        premium_twap_frequency_ms: u64,
        premium_twap_period_ms: u64,
        spread_twap_frequency_ms: u64,
        spread_twap_period_ms: u64,
    }

    struct SetCoreParams {
        ch_id: Id,
        lot_size: u64,
        tick_size: u64,
        collateral_haircut: U256,
    }

    struct SetBaseOracleParams {
        ch_id: Id,
        storage_id: u32,
        source_id: u16,
        pfs_tolerance: u64,
    }

    struct SetCollateralOracleParams {
        ch_id: Id,
        storage_id: u32,
        source_id: u16,
        pfs_tolerance: u64,
    }

    struct SetRiskLimitParams {
        ch_id: Id,
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

    struct DonatedToInsuranceFund {
        sender: Address,
        ch_id: Id,
        amount: u64,
        new_balance: u64,
    }

    struct WithdrewFees {
        sender: Address,
        ch_id: Id,
        amount: u64,
        vault_balance_after: u64,
    }

    struct WithdrewInsuranceFund {
        sender: Address,
        ch_id: Id,
        amount: u64,
        insurance_fund_balance_after: u64,
    }

    struct UpdatedOpenInterestAndFeesAccrued {
        ch_id: Id,
        open_interest: U256,
        fees_accrued: U256,
    }

    struct RegisteredVendor {
        vendor_key: TypeName,
        vendor_admin_cap_id: Id,
    }

    struct Froze {
        id: Id,
        resume_version: u64,
        guardian_cap_id: Id,
    }

    struct Unfroze {
        id: Id,
        version: u64,
    }
}
