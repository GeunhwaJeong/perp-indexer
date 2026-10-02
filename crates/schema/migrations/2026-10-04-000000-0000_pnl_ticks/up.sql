-- The history of what each account was worth: a tick per account per interval (an hour by
-- default), taken by the state pipeline in the transaction of the first batch that reaches the
-- interval. A tick is therefore the account's value at one exact checkpoint.

-- Coins deposited into an account less coins withdrawn from it, in coin units. Kept as a running
-- total so that a tick does not have to add up the account's whole history.
ALTER TABLE accounts ADD COLUMN net_transfers NUMERIC NOT NULL DEFAULT 0;

UPDATE accounts a
SET net_transfers = t.net
FROM (
    SELECT account_id,
           SUM(CASE kind WHEN 'deposit' THEN amount WHEN 'withdraw' THEN -amount ELSE 0 END) AS net
    FROM collateral_transfers
    GROUP BY account_id
) t
WHERE a.account_id = t.account_id;

CREATE TABLE pnl_ticks (
    account_id     BIGINT  NOT NULL,
    -- Start of the interval the tick stands for.
    bucket_ms      BIGINT  NOT NULL,
    -- The checkpoint the account was valued at, and its time.
    checkpoint     BIGINT  NOT NULL,
    timestamp_ms   BIGINT  NOT NULL,
    -- In USD: the unallocated balance, plus the margin of every position at the mark price.
    equity         NUMERIC NOT NULL,
    -- In USD at the collateral's price when the tick was taken.
    net_transfers  NUMERIC NOT NULL,
    -- equity - net_transfers: what trading, funding and fees have made or lost.
    total_pnl      NUMERIC NOT NULL,
    PRIMARY KEY (account_id, bucket_ms)
);

-- The intervals ticks were taken for. A batch of checkpoints that spans several intervals
-- takes ticks for the last one only, so intervals crossed while backfilling have no row.
CREATE TABLE pnl_tick_runs (
    bucket_ms     BIGINT PRIMARY KEY,
    checkpoint    BIGINT NOT NULL,
    timestamp_ms  BIGINT NOT NULL,
    accounts      BIGINT NOT NULL
);

-- Which accounts and positions hold anything, for the tick's scan.
CREATE INDEX accounts_funded ON accounts (account_id) WHERE collateral <> 0;
CREATE INDEX positions_funded ON positions (account_id) WHERE collateral <> 0 OR base <> 0;
