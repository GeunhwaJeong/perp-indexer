-- What the API needs on top of the state tables: a stable number per market, who may act for an
-- account, and the running totals of each position since it was last opened.

-- A small stable number for each market, assigned in the order markets first appear on chain.
ALTER TABLE markets ADD COLUMN market_index BIGINT UNIQUE;

UPDATE markets m
SET market_index = numbered.market_index
FROM (
    SELECT market,
           ROW_NUMBER() OVER (ORDER BY created_checkpoint NULLS LAST, updated_checkpoint, market) - 1
               AS market_index
    FROM markets
) numbered
WHERE m.market = numbered.market;

-- Capability objects over accounts. Authority over an account is held through these, and they
-- move between owners like any object, so the owner here is whoever holds the capability now.
CREATE TABLE account_caps (
    cap_id              TEXT PRIMARY KEY,
    -- The account object the capability is for (accounts.object_id).
    account_object_id   TEXT    NOT NULL,
    -- 'admin' or 'assistant'.
    role                TEXT    NOT NULL,
    -- The address holding the capability. NULL while it is held by an object or shared, and
    -- after it was deleted.
    owner               TEXT,
    updated_checkpoint  BIGINT  NOT NULL
);

CREATE INDEX account_caps_by_owner ON account_caps (owner, role);
CREATE INDEX account_caps_by_account ON account_caps (account_object_id);
CREATE INDEX accounts_by_object ON accounts (object_id);

-- Running totals of a position since it last went from flat to open. They are built from the
-- position's fills and funding payments; its size, collateral and entry notional stay copies of
-- the on-chain object.
ALTER TABLE positions
    ADD COLUMN opened_checkpoint BIGINT,
    ADD COLUMN opened_at_ms      BIGINT,
    -- Largest absolute size reached.
    ADD COLUMN max_size          NUMERIC NOT NULL DEFAULT 0,
    -- Total size added and removed, and the notional the removals traded at.
    ADD COLUMN sum_open          NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN sum_close         NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN close_quote       NUMERIC NOT NULL DEFAULT 0,
    -- Cost basis of the open size, at the prices it was added at.
    ADD COLUMN entry_quote       NUMERIC NOT NULL DEFAULT 0,
    -- Profit reported by the engine on the position's fills, net of the fees they paid.
    ADD COLUMN realized_pnl      NUMERIC NOT NULL DEFAULT 0,
    -- Funding settled; positive when received.
    ADD COLUMN net_funding       NUMERIC NOT NULL DEFAULT 0;

-- Positions that predate these columns start from what the object says. Replaying the state
-- pipeline from the first checkpoint rebuilds the exact totals.
UPDATE positions
SET opened_checkpoint = created_checkpoint,
    opened_at_ms = created_at_ms,
    max_size = ABS(base),
    sum_open = ABS(base),
    entry_quote = ABS(quote_notional)
WHERE base <> 0;

-- The position each fill and funding payment found, for trade history.
ALTER TABLE fills
    -- Signed size before the fill.
    ADD COLUMN position_base_before NUMERIC,
    -- Average entry price before the fill; NULL when the position was flat.
    ADD COLUMN entry_price_before   NUMERIC;

-- The index price when funding was updated or settled, to express it as a rate.
ALTER TABLE funding_payments
    -- Signed size the payment was settled on.
    ADD COLUMN position_base NUMERIC,
    ADD COLUMN index_price   NUMERIC;

ALTER TABLE funding_updates ADD COLUMN index_price NUMERIC;

-- What changed since a checkpoint, for the feed that pushes updates to subscribers.
CREATE INDEX orders_by_update ON orders (updated_checkpoint);
CREATE INDEX positions_by_update ON positions (updated_checkpoint);
CREATE INDEX accounts_by_update ON accounts (updated_checkpoint);
CREATE INDEX account_caps_by_update ON account_caps (updated_checkpoint);

-- An account's history across markets, newest first.
CREATE INDEX fills_by_account_time
    ON fills (account_id, checkpoint DESC, tx_index DESC, event_index DESC, fill_index DESC);
CREATE INDEX orders_by_account_time ON orders (account_id, updated_checkpoint DESC);
CREATE INDEX funding_payments_by_account_time ON funding_payments (account_id, checkpoint DESC);
