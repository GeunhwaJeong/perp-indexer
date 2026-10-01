DROP INDEX funding_payments_by_account_time;
DROP INDEX orders_by_account_time;
DROP INDEX fills_by_account_time;
DROP INDEX accounts_by_update;
DROP INDEX positions_by_update;
DROP INDEX orders_by_update;
ALTER TABLE funding_updates DROP COLUMN index_price;
ALTER TABLE funding_payments DROP COLUMN position_base, DROP COLUMN index_price;
ALTER TABLE fills DROP COLUMN position_base_before, DROP COLUMN entry_price_before;
ALTER TABLE positions
    DROP COLUMN opened_checkpoint,
    DROP COLUMN opened_at_ms,
    DROP COLUMN max_size,
    DROP COLUMN sum_open,
    DROP COLUMN sum_close,
    DROP COLUMN close_quote,
    DROP COLUMN entry_quote,
    DROP COLUMN realized_pnl,
    DROP COLUMN net_funding;
DROP INDEX accounts_by_object;
DROP TABLE account_caps;
ALTER TABLE markets DROP COLUMN market_index;
