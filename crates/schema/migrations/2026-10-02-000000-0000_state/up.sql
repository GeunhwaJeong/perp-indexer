-- State derived from the chain, maintained checkpoint by checkpoint by the `state` pipeline.
--
-- Two kinds of data live here. Current state (markets, accounts, positions) is copied from the
-- objects each transaction wrote, so it is exactly what the engine holds. History (orders, fills,
-- funding, transfers, candles, tickets) is built from events.
--
-- Units: prices and base sizes are decimals (the engine's 9-decimal integers divided by 10^9),
-- `ifixed` values are decimals (divided by 10^18), and collateral coin amounts are raw coin units
-- (see markets.collateral_decimals). Order IDs are the engine's raw u128.

CREATE TABLE markets (
    -- The clearing house object ID.
    market                       TEXT PRIMARY KEY,
    collateral_type              TEXT    NOT NULL,
    version                      BIGINT  NOT NULL,
    paused                       SMALLINT NOT NULL,
    base_storage_id              BIGINT  NOT NULL,
    base_source_id               INTEGER NOT NULL,
    collateral_storage_id        BIGINT  NOT NULL,
    collateral_source_id         INTEGER NOT NULL,
    lot_size                     NUMERIC NOT NULL,
    tick_size                    NUMERIC NOT NULL,
    margin_ratio_initial         NUMERIC NOT NULL,
    margin_ratio_maintenance     NUMERIC NOT NULL,
    maker_fee                    NUMERIC NOT NULL,
    taker_fee                    NUMERIC NOT NULL,
    -- Every market parameter as stored on chain (raw integers).
    params                       JSONB   NOT NULL,
    cum_funding_rate_long        NUMERIC NOT NULL,
    cum_funding_rate_short       NUMERIC NOT NULL,
    funding_last_upd_ms          BIGINT  NOT NULL,
    premium_twap                 NUMERIC NOT NULL,
    premium_twap_last_upd_ms     BIGINT  NOT NULL,
    spread_twap                  NUMERIC NOT NULL,
    spread_twap_last_upd_ms      BIGINT  NOT NULL,
    open_interest                NUMERIC NOT NULL,
    fees_accrued                 NUMERIC NOT NULL,
    order_counter                NUMERIC NOT NULL,
    best_ask_price               NUMERIC,
    best_bid_price               NUMERIC,
    updated_checkpoint           BIGINT  NOT NULL,
    updated_at_ms                BIGINT  NOT NULL,

    -- Set from events, so NULL when the indexer started after the event was emitted.
    collateral_decimals          BIGINT,
    created_checkpoint           BIGINT,
    created_at_ms                BIGINT,
    closed                       BOOLEAN NOT NULL DEFAULT FALSE,
    settlement_enabled           BOOLEAN NOT NULL DEFAULT FALSE,
    settlement_base_price        NUMERIC,
    settlement_collateral_price  NUMERIC,
    -- Last prices reported by the engine's events.
    mark_price                   NUMERIC,
    index_price                  NUMERIC,
    book_price                   NUMERIC,
    prices_updated_at_ms         BIGINT
);

CREATE TABLE oracle_prices (
    storage_id          BIGINT  NOT NULL,
    source_id           INTEGER NOT NULL,
    price               NUMERIC NOT NULL,
    twap_price          NUMERIC NOT NULL,
    -- The feed's own timestamp.
    timestamp_ms        BIGINT  NOT NULL,
    updated_checkpoint  BIGINT  NOT NULL,
    PRIMARY KEY (storage_id, source_id)
);

CREATE TABLE accounts (
    account_id          BIGINT PRIMARY KEY,
    object_id           TEXT    NOT NULL,
    collateral_type     TEXT    NOT NULL,
    -- Collateral not allocated to any market, in coin units.
    collateral          NUMERIC NOT NULL,
    updated_checkpoint  BIGINT  NOT NULL,
    updated_at_ms       BIGINT  NOT NULL,
    -- The address that created the account. Authority over it is held through capability
    -- objects, which can change hands without an event.
    creator             TEXT,
    created_checkpoint  BIGINT,
    created_at_ms       BIGINT
);

CREATE INDEX accounts_by_creator ON accounts (creator);

CREATE TABLE positions (
    market                  TEXT    NOT NULL,
    account_id              BIGINT  NOT NULL,
    object_id               TEXT    NOT NULL,
    -- In USD terms of the collateral, as the engine keeps it.
    collateral              NUMERIC NOT NULL,
    -- Positive for longs, negative for shorts.
    base                    NUMERIC NOT NULL,
    -- Carries the sign of `base`; |quote_notional / base| is the average entry price.
    quote_notional          NUMERIC NOT NULL,
    -- The market's cumulative funding rates when funding was last settled for this position.
    cum_funding_rate_long   NUMERIC NOT NULL,
    cum_funding_rate_short  NUMERIC NOT NULL,
    -- Total size of the position's resting asks and bids.
    asks_quantity           NUMERIC NOT NULL,
    bids_quantity           NUMERIC NOT NULL,
    pending_orders          BIGINT  NOT NULL,
    initial_margin_ratio    NUMERIC NOT NULL,
    created_checkpoint      BIGINT  NOT NULL,
    created_at_ms           BIGINT  NOT NULL,
    updated_checkpoint      BIGINT  NOT NULL,
    updated_at_ms           BIGINT  NOT NULL,
    PRIMARY KEY (market, account_id)
);

CREATE INDEX positions_by_account ON positions (account_id);

CREATE TABLE orders (
    market                   TEXT     NOT NULL,
    order_id                 NUMERIC  NOT NULL,
    account_id               BIGINT   NOT NULL,
    is_ask                   BOOLEAN  NOT NULL,
    price                    NUMERIC  NOT NULL,
    -- Size when the order was posted to the book. A taker order's immediate fills are not part of
    -- it: only the remainder that rests is posted.
    size                     NUMERIC  NOT NULL,
    remaining                NUMERIC  NOT NULL,
    filled                   NUMERIC  NOT NULL DEFAULT 0,
    canceled                 NUMERIC  NOT NULL DEFAULT 0,
    -- 'open', 'filled' or 'canceled'. A canceled order may be partly filled.
    status                   TEXT     NOT NULL,
    -- 0 user, 1 liquidation, 3 market closed, 4 reduce-only clip, 5 expired, 6 self-trade,
    -- 7 fill would leave the maker in bad debt or above its open interest share.
    cancel_reason            SMALLINT,
    reduce_only              BOOLEAN  NOT NULL,
    expiration_timestamp_ms  NUMERIC,
    client_order_id          NUMERIC,
    integrator_id            BIGINT,
    integrator_fee_rate      BIGINT   NOT NULL,
    created_checkpoint       BIGINT   NOT NULL,
    created_at_ms            BIGINT   NOT NULL,
    created_tx               TEXT     NOT NULL,
    updated_checkpoint       BIGINT   NOT NULL,
    updated_at_ms            BIGINT   NOT NULL,
    PRIMARY KEY (market, order_id)
);

-- The book: open orders of one side of a market in price order.
CREATE INDEX orders_book ON orders (market, is_ask, price) WHERE status = 'open';
CREATE INDEX orders_by_account ON orders (account_id, market, created_checkpoint DESC);

CREATE TABLE fills (
    checkpoint               BIGINT  NOT NULL,
    tx_index                 BIGINT  NOT NULL,
    event_index              BIGINT  NOT NULL,
    -- Position within the event: a maker batch holds many fills, and a liquidation or ADL event
    -- yields one fill per side.
    fill_index               BIGINT  NOT NULL,
    tx_digest                TEXT    NOT NULL,
    timestamp_ms             BIGINT  NOT NULL,
    market                   TEXT    NOT NULL,
    account_id               BIGINT  NOT NULL,
    counterparty_account_id  BIGINT,
    -- TRUE when this account sold.
    is_ask                   BOOLEAN NOT NULL,
    -- 'maker' or 'taker'. A taker row nets all of a session's fills on one side.
    liquidity                TEXT    NOT NULL,
    -- 'trade', 'liquidated' (the liquidated account), 'liquidation' (the liquidator), 'adl' or
    -- 'settlement'.
    kind                     TEXT    NOT NULL,
    -- NULL for settlements, which close at the market's settlement price.
    price                    NUMERIC,
    size                     NUMERIC NOT NULL,
    quote                    NUMERIC,
    -- Fees paid by the account, in USD; negative when it received them.
    fee                      NUMERIC NOT NULL,
    integrator_fee           NUMERIC NOT NULL,
    pnl                      NUMERIC NOT NULL,
    order_id                 NUMERIC,
    client_order_id          NUMERIC,
    mark_price               NUMERIC,
    PRIMARY KEY (checkpoint, tx_index, event_index, fill_index)
);

CREATE INDEX fills_by_account ON fills (account_id, market, checkpoint DESC);
-- The public tape: each match appears once, as its maker fill.
CREATE INDEX fills_tape ON fills (market, checkpoint DESC, tx_index DESC, event_index DESC, fill_index DESC)
    WHERE kind = 'trade' AND liquidity = 'maker';

CREATE TABLE candles (
    market        TEXT    NOT NULL,
    -- Bucket width: 60000, 300000, 900000, 1800000, 3600000, 14400000 or 86400000.
    resolution_ms BIGINT  NOT NULL,
    start_ms      BIGINT  NOT NULL,
    open          NUMERIC NOT NULL,
    high          NUMERIC NOT NULL,
    low           NUMERIC NOT NULL,
    close         NUMERIC NOT NULL,
    base_volume   NUMERIC NOT NULL,
    quote_volume  NUMERIC NOT NULL,
    trades        BIGINT  NOT NULL,
    PRIMARY KEY (market, resolution_ms, start_ms)
);

CREATE TABLE funding_updates (
    checkpoint              BIGINT  NOT NULL,
    tx_index                BIGINT  NOT NULL,
    event_index             BIGINT  NOT NULL,
    timestamp_ms            BIGINT  NOT NULL,
    market                  TEXT    NOT NULL,
    cum_funding_rate_long   NUMERIC NOT NULL,
    cum_funding_rate_short  NUMERIC NOT NULL,
    funding_last_upd_ms     BIGINT  NOT NULL,
    PRIMARY KEY (checkpoint, tx_index, event_index)
);

CREATE INDEX funding_updates_by_market ON funding_updates (market, funding_last_upd_ms DESC);

CREATE TABLE funding_payments (
    checkpoint              BIGINT  NOT NULL,
    tx_index                BIGINT  NOT NULL,
    event_index             BIGINT  NOT NULL,
    tx_digest               TEXT    NOT NULL,
    timestamp_ms            BIGINT  NOT NULL,
    market                  TEXT    NOT NULL,
    account_id              BIGINT  NOT NULL,
    -- Positive when the account received funding.
    collateral_change_usd   NUMERIC NOT NULL,
    collateral_after        NUMERIC NOT NULL,
    cum_funding_rate_long   NUMERIC NOT NULL,
    cum_funding_rate_short  NUMERIC NOT NULL,
    PRIMARY KEY (checkpoint, tx_index, event_index)
);

CREATE INDEX funding_payments_by_account ON funding_payments (account_id, market, checkpoint DESC);

CREATE TABLE collateral_transfers (
    checkpoint    BIGINT  NOT NULL,
    tx_index      BIGINT  NOT NULL,
    event_index   BIGINT  NOT NULL,
    tx_digest     TEXT    NOT NULL,
    timestamp_ms  BIGINT  NOT NULL,
    account_id    BIGINT  NOT NULL,
    -- 'deposit' and 'withdraw' move coins in and out of the account; 'allocate' and 'deallocate'
    -- move them between the account and a market, and 'settlement' returns a closed market's
    -- collateral to the account.
    kind          TEXT    NOT NULL,
    market        TEXT,
    -- In coin units.
    amount        NUMERIC NOT NULL,
    PRIMARY KEY (checkpoint, tx_index, event_index)
);

CREATE INDEX collateral_transfers_by_account ON collateral_transfers (account_id, checkpoint DESC);

-- Stop order and TWAP order tickets. Their order details are committed on chain as a hash, so
-- only the ticket's lifecycle is known here.
CREATE TABLE order_tickets (
    ticket_id           TEXT PRIMARY KEY,
    -- 'stop' or 'twap'.
    kind                TEXT    NOT NULL,
    account_id          BIGINT  NOT NULL,
    collateral_type     TEXT    NOT NULL,
    -- NULL for stop orders, which name their market only when executed.
    market              TEXT,
    -- 'open', 'executed' (stop), 'finalized' or 'canceled' (TWAP), 'deleted'.
    status              TEXT    NOT NULL,
    executors           JSONB   NOT NULL,
    execution_domain    TEXT,
    gas                 NUMERIC NOT NULL,
    stop_order_type     NUMERIC,
    encrypted_details   BYTEA   NOT NULL,
    -- TWAP progress as of the last processing step (raw engine values).
    twap_progress       JSONB,
    created_checkpoint  BIGINT  NOT NULL,
    created_at_ms       BIGINT  NOT NULL,
    updated_checkpoint  BIGINT  NOT NULL,
    updated_at_ms       BIGINT  NOT NULL
);

CREATE INDEX order_tickets_by_account ON order_tickets (account_id, status);
