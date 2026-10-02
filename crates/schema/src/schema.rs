// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

diesel::table! {
    raw_events (checkpoint, tx_index, event_index) {
        checkpoint -> Int8,
        tx_index -> Int8,
        event_index -> Int8,
        tx_digest -> Text,
        timestamp_ms -> Int8,
        sender -> Text,
        package -> Text,
        package_id -> Text,
        module -> Text,
        name -> Text,
        event_type -> Text,
        market -> Nullable<Text>,
        bcs -> Bytea,
        data -> Nullable<Jsonb>,
        decode_error -> Nullable<Text>,
    }
}

diesel::table! {
    markets (market) {
        market -> Text,
        collateral_type -> Text,
        version -> Int8,
        paused -> Int2,
        base_storage_id -> Int8,
        base_source_id -> Int4,
        collateral_storage_id -> Int8,
        collateral_source_id -> Int4,
        lot_size -> Numeric,
        tick_size -> Numeric,
        margin_ratio_initial -> Numeric,
        margin_ratio_maintenance -> Numeric,
        maker_fee -> Numeric,
        taker_fee -> Numeric,
        params -> Jsonb,
        cum_funding_rate_long -> Numeric,
        cum_funding_rate_short -> Numeric,
        funding_last_upd_ms -> Int8,
        premium_twap -> Numeric,
        premium_twap_last_upd_ms -> Int8,
        spread_twap -> Numeric,
        spread_twap_last_upd_ms -> Int8,
        open_interest -> Numeric,
        fees_accrued -> Numeric,
        order_counter -> Numeric,
        best_ask_price -> Nullable<Numeric>,
        best_bid_price -> Nullable<Numeric>,
        updated_checkpoint -> Int8,
        updated_at_ms -> Int8,
        collateral_decimals -> Nullable<Int8>,
        created_checkpoint -> Nullable<Int8>,
        created_at_ms -> Nullable<Int8>,
        closed -> Bool,
        settlement_enabled -> Bool,
        settlement_base_price -> Nullable<Numeric>,
        settlement_collateral_price -> Nullable<Numeric>,
        mark_price -> Nullable<Numeric>,
        index_price -> Nullable<Numeric>,
        book_price -> Nullable<Numeric>,
        prices_updated_at_ms -> Nullable<Int8>,
        market_index -> Nullable<Int8>,
    }
}

diesel::table! {
    oracle_prices (storage_id, source_id) {
        storage_id -> Int8,
        source_id -> Int4,
        price -> Numeric,
        twap_price -> Numeric,
        timestamp_ms -> Int8,
        updated_checkpoint -> Int8,
    }
}

diesel::table! {
    accounts (account_id) {
        account_id -> Int8,
        object_id -> Text,
        collateral_type -> Text,
        collateral -> Numeric,
        updated_checkpoint -> Int8,
        updated_at_ms -> Int8,
        creator -> Nullable<Text>,
        created_checkpoint -> Nullable<Int8>,
        created_at_ms -> Nullable<Int8>,
        net_transfers -> Numeric,
    }
}

diesel::table! {
    positions (market, account_id) {
        market -> Text,
        account_id -> Int8,
        object_id -> Text,
        collateral -> Numeric,
        base -> Numeric,
        quote_notional -> Numeric,
        cum_funding_rate_long -> Numeric,
        cum_funding_rate_short -> Numeric,
        asks_quantity -> Numeric,
        bids_quantity -> Numeric,
        pending_orders -> Int8,
        initial_margin_ratio -> Numeric,
        created_checkpoint -> Int8,
        created_at_ms -> Int8,
        updated_checkpoint -> Int8,
        updated_at_ms -> Int8,
        opened_checkpoint -> Nullable<Int8>,
        opened_at_ms -> Nullable<Int8>,
        max_size -> Numeric,
        sum_open -> Numeric,
        sum_close -> Numeric,
        close_quote -> Numeric,
        entry_quote -> Numeric,
        realized_pnl -> Numeric,
        net_funding -> Numeric,
    }
}

diesel::table! {
    orders (market, order_id) {
        market -> Text,
        order_id -> Numeric,
        account_id -> Int8,
        is_ask -> Bool,
        price -> Numeric,
        size -> Numeric,
        remaining -> Numeric,
        filled -> Numeric,
        canceled -> Numeric,
        status -> Text,
        cancel_reason -> Nullable<Int2>,
        reduce_only -> Bool,
        expiration_timestamp_ms -> Nullable<Numeric>,
        client_order_id -> Nullable<Numeric>,
        integrator_id -> Nullable<Int8>,
        integrator_fee_rate -> Int8,
        created_checkpoint -> Int8,
        created_at_ms -> Int8,
        created_tx -> Text,
        updated_checkpoint -> Int8,
        updated_at_ms -> Int8,
    }
}

diesel::table! {
    fills (checkpoint, tx_index, event_index, fill_index) {
        checkpoint -> Int8,
        tx_index -> Int8,
        event_index -> Int8,
        fill_index -> Int8,
        tx_digest -> Text,
        timestamp_ms -> Int8,
        market -> Text,
        account_id -> Int8,
        counterparty_account_id -> Nullable<Int8>,
        is_ask -> Bool,
        liquidity -> Text,
        kind -> Text,
        price -> Nullable<Numeric>,
        size -> Numeric,
        quote -> Nullable<Numeric>,
        fee -> Numeric,
        integrator_fee -> Numeric,
        pnl -> Numeric,
        order_id -> Nullable<Numeric>,
        client_order_id -> Nullable<Numeric>,
        mark_price -> Nullable<Numeric>,
        position_base_before -> Nullable<Numeric>,
        entry_price_before -> Nullable<Numeric>,
    }
}

diesel::table! {
    candles (market, resolution_ms, start_ms) {
        market -> Text,
        resolution_ms -> Int8,
        start_ms -> Int8,
        open -> Numeric,
        high -> Numeric,
        low -> Numeric,
        close -> Numeric,
        base_volume -> Numeric,
        quote_volume -> Numeric,
        trades -> Int8,
    }
}

diesel::table! {
    funding_updates (checkpoint, tx_index, event_index) {
        checkpoint -> Int8,
        tx_index -> Int8,
        event_index -> Int8,
        timestamp_ms -> Int8,
        market -> Text,
        cum_funding_rate_long -> Numeric,
        cum_funding_rate_short -> Numeric,
        funding_last_upd_ms -> Int8,
        index_price -> Nullable<Numeric>,
    }
}

diesel::table! {
    funding_payments (checkpoint, tx_index, event_index) {
        checkpoint -> Int8,
        tx_index -> Int8,
        event_index -> Int8,
        tx_digest -> Text,
        timestamp_ms -> Int8,
        market -> Text,
        account_id -> Int8,
        collateral_change_usd -> Numeric,
        collateral_after -> Numeric,
        cum_funding_rate_long -> Numeric,
        cum_funding_rate_short -> Numeric,
        position_base -> Nullable<Numeric>,
        index_price -> Nullable<Numeric>,
    }
}

diesel::table! {
    collateral_transfers (checkpoint, tx_index, event_index) {
        checkpoint -> Int8,
        tx_index -> Int8,
        event_index -> Int8,
        tx_digest -> Text,
        timestamp_ms -> Int8,
        account_id -> Int8,
        kind -> Text,
        market -> Nullable<Text>,
        amount -> Numeric,
    }
}

diesel::table! {
    order_tickets (ticket_id) {
        ticket_id -> Text,
        kind -> Text,
        account_id -> Int8,
        collateral_type -> Text,
        market -> Nullable<Text>,
        status -> Text,
        executors -> Jsonb,
        execution_domain -> Nullable<Text>,
        gas -> Numeric,
        stop_order_type -> Nullable<Numeric>,
        encrypted_details -> Bytea,
        twap_progress -> Nullable<Jsonb>,
        created_checkpoint -> Int8,
        created_at_ms -> Int8,
        updated_checkpoint -> Int8,
        updated_at_ms -> Int8,
    }
}

diesel::table! {
    account_caps (cap_id) {
        cap_id -> Text,
        account_object_id -> Text,
        role -> Text,
        owner -> Nullable<Text>,
        updated_checkpoint -> Int8,
    }
}

diesel::table! {
    pnl_ticks (account_id, bucket_ms) {
        account_id -> Int8,
        bucket_ms -> Int8,
        checkpoint -> Int8,
        timestamp_ms -> Int8,
        equity -> Numeric,
        net_transfers -> Numeric,
        total_pnl -> Numeric,
    }
}

diesel::table! {
    pnl_tick_runs (bucket_ms) {
        bucket_ms -> Int8,
        checkpoint -> Int8,
        timestamp_ms -> Int8,
        accounts -> Int8,
    }
}
