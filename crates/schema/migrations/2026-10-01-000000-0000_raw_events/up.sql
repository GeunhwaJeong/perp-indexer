-- The ledger: every event emitted by the indexed packages, in chain order.
--
-- Rows are append-only and keyed by their position on chain, so re-processing a checkpoint is a
-- no-op. `bcs` is the payload exactly as emitted; everything else in the database can be rebuilt
-- from this table alone.
CREATE TABLE raw_events (
    checkpoint       BIGINT NOT NULL,
    -- Position of the transaction within its checkpoint.
    tx_index         BIGINT NOT NULL,
    -- Position of the event within its transaction.
    event_index      BIGINT NOT NULL,
    tx_digest        TEXT   NOT NULL,
    timestamp_ms     BIGINT NOT NULL,
    sender           TEXT   NOT NULL,
    -- Name the package was configured under, e.g. 'perpetuals'.
    package          TEXT   NOT NULL,
    -- Address the event type is defined at. Differs between versions of an upgraded package.
    package_id       TEXT   NOT NULL,
    module           TEXT   NOT NULL,
    name             TEXT   NOT NULL,
    -- Full event type, including type parameters.
    event_type       TEXT   NOT NULL,
    -- The clearing house (`ch_id`) the event belongs to, when it has one.
    market           TEXT,
    bcs              BYTEA  NOT NULL,
    -- Decoded payload. NULL when the package has no decoders or decoding failed.
    data             JSONB,
    -- Why decoding failed. Non-NULL rows mean the decoders lag the deployed package.
    decode_error     TEXT,
    PRIMARY KEY (checkpoint, tx_index, event_index)
);

CREATE INDEX raw_events_by_name ON raw_events (package, name, checkpoint);
CREATE INDEX raw_events_by_tx ON raw_events (tx_digest);
CREATE INDEX raw_events_by_market ON raw_events (market, checkpoint) WHERE market IS NOT NULL;
CREATE INDEX raw_events_undecoded ON raw_events (checkpoint) WHERE decode_error IS NOT NULL;
