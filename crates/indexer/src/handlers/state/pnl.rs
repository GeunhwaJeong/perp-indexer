// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The history of what accounts were worth.
//!
//! Once per interval, in the transaction of the first batch that reaches it, every account that
//! holds anything is valued the way the API values it: its unallocated balance plus the margin
//! of each of its positions at the mark price. The tick is taken after the batch was applied,
//! so it is the account's value at exactly the batch's last checkpoint, and it is written with
//! the watermark, so it is taken once.
//!
//! A batch that spans several intervals, as while backfilling, takes ticks for the last one
//! only: the state in between is no longer there to value.

use std::collections::{BTreeMap, HashMap};

use bigdecimal::{BigDecimal, Zero};
use diesel::sql_types::{BigInt, Bool, Nullable, Numeric, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use haneul_indexer_alt_framework::postgres::Connection;
use perp_engine::{MarketState, Position, Valuation, collateral_value};
use perp_schema::models::{PnlTick, PnlTickRun};
use perp_schema::schema::{pnl_tick_runs, pnl_ticks};
use tracing::{info, warn};

/// Rows per insert statement.
const CHUNK_ROWS: usize = 5_000;

#[derive(QueryableByName)]
struct Watermark {
    #[diesel(sql_type = BigInt)]
    checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    timestamp_ms: i64,
}

#[derive(QueryableByName)]
struct LastRun {
    #[diesel(sql_type = Nullable<BigInt>)]
    bucket_ms: Option<i64>,
}

#[derive(QueryableByName)]
struct MarketRow {
    #[diesel(sql_type = Text)]
    market: String,
    #[diesel(sql_type = Text)]
    collateral_type: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    collateral_decimals: Option<i64>,
    #[diesel(sql_type = Bool)]
    settlement_enabled: bool,
    #[diesel(sql_type = Nullable<Numeric>)]
    settlement_base_price: Option<BigDecimal>,
    #[diesel(sql_type = Numeric)]
    margin_ratio_initial: BigDecimal,
    #[diesel(sql_type = Numeric)]
    cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = BigInt)]
    funding_last_upd_ms: i64,
    #[diesel(sql_type = BigInt)]
    funding_frequency_ms: i64,
    #[diesel(sql_type = BigInt)]
    funding_period_ms: i64,
    #[diesel(sql_type = Numeric)]
    premium_twap: BigDecimal,
    #[diesel(sql_type = Numeric)]
    spread_twap: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    best_bid_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    best_ask_price: Option<BigDecimal>,
    #[diesel(sql_type = Numeric)]
    collateral_haircut: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    oracle_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    oracle_twap_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    collateral_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    event_index_price: Option<BigDecimal>,
}

#[derive(QueryableByName)]
struct AccountRow {
    #[diesel(sql_type = BigInt)]
    account_id: i64,
    #[diesel(sql_type = Text)]
    collateral_type: String,
    #[diesel(sql_type = Numeric)]
    collateral: BigDecimal,
    #[diesel(sql_type = Numeric)]
    net_transfers: BigDecimal,
}

#[derive(QueryableByName)]
struct PositionRow {
    #[diesel(sql_type = Text)]
    market: String,
    #[diesel(sql_type = BigInt)]
    account_id: i64,
    #[diesel(sql_type = Numeric)]
    collateral: BigDecimal,
    #[diesel(sql_type = Numeric)]
    base: BigDecimal,
    #[diesel(sql_type = Numeric)]
    quote_notional: BigDecimal,
    #[diesel(sql_type = Numeric)]
    cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = Numeric)]
    asks_quantity: BigDecimal,
    #[diesel(sql_type = Numeric)]
    bids_quantity: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pending_orders: i64,
    #[diesel(sql_type = Numeric)]
    initial_margin_ratio: BigDecimal,
}

/// How a collateral coin is valued: its decimals and its oracle price.
#[derive(Clone, Debug, PartialEq)]
pub struct Collateral {
    pub decimals: u32,
    pub price: BigDecimal,
}

/// An account's unallocated balance and its transfers so far, both in coin units.
#[derive(Clone, Debug, PartialEq)]
pub struct Account {
    pub account_id: i64,
    pub collateral: BigDecimal,
    pub net_transfers: BigDecimal,
}

/// The start of the interval `timestamp_ms` falls in.
pub fn bucket_start(timestamp_ms: i64, interval_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(interval_ms)
}

/// `(equity, net_transfers, total_pnl)` of an account, in USD.
///
/// `margins` are the margins of its positions. A position whose margin is negative is in bad
/// debt, which is the market's loss and not the account's: it counts as zero, as it does in the
/// account's equity over the API.
pub fn value(
    account: &Account,
    collateral: &Collateral,
    margins: &[BigDecimal],
) -> (BigDecimal, BigDecimal, BigDecimal) {
    let zero = BigDecimal::zero();
    let balance = collateral_value(&account.collateral, collateral.decimals, &collateral.price);
    let equity = margins
        .iter()
        .fold(balance, |sum, margin| sum + margin.max(&zero));
    let net_transfers = collateral_value(
        &account.net_transfers,
        collateral.decimals,
        &collateral.price,
    );
    let total_pnl = &equity - &net_transfers;
    (equity, net_transfers, total_pnl)
}

/// Takes the ticks of the interval the batch just committed has reached, unless they were taken
/// already. Runs inside the batch's transaction, after its changes were applied.
pub async fn tick(interval_ms: i64, conn: &mut Connection<'_>) -> anyhow::Result<usize> {
    // The framework has already moved the pipeline's watermark to the batch's last checkpoint in
    // this transaction.
    let Some(watermark) = sql_query(
        "SELECT checkpoint_hi_inclusive AS checkpoint, timestamp_ms_hi_inclusive AS timestamp_ms \
         FROM watermarks WHERE pipeline = 'state'",
    )
    .get_results::<Watermark>(conn)
    .await?
    .pop() else {
        return Ok(0);
    };
    let bucket_ms = bucket_start(watermark.timestamp_ms, interval_ms);
    let last: LastRun = sql_query("SELECT MAX(bucket_ms) AS bucket_ms FROM pnl_tick_runs")
        .get_result(conn)
        .await?;
    if last.bucket_ms.is_some_and(|last| last >= bucket_ms) {
        return Ok(0);
    }

    let mut collaterals: HashMap<String, Collateral> = HashMap::new();
    let mut valuations: HashMap<String, Valuation> = HashMap::new();
    for row in markets(conn).await? {
        if let Some(decimals) = row.collateral_decimals {
            // A collateral whose price feed has not been seen counts at par, as over the API.
            let price = row.collateral_price.clone();
            collaterals
                .entry(row.collateral_type.clone())
                .or_insert_with(|| Collateral {
                    decimals: decimals as u32,
                    price: price.unwrap_or_else(|| BigDecimal::from(1)),
                });
        }
        let state = MarketState {
            settlement_enabled: row.settlement_enabled,
            settlement_base_price: row.settlement_base_price,
            margin_ratio_initial: row.margin_ratio_initial,
            cum_funding_rate_long: row.cum_funding_rate_long,
            cum_funding_rate_short: row.cum_funding_rate_short,
            funding_last_upd_ms: row.funding_last_upd_ms,
            funding_frequency_ms: row.funding_frequency_ms,
            funding_period_ms: row.funding_period_ms,
            premium_twap: row.premium_twap,
            spread_twap: row.spread_twap,
            best_bid_price: row.best_bid_price,
            best_ask_price: row.best_ask_price,
            collateral_haircut: row.collateral_haircut,
            oracle_price: row.oracle_price,
            oracle_twap_price: row.oracle_twap_price,
            collateral_price: row.collateral_price,
            event_index_price: row.event_index_price,
        };
        // A market no price has been seen for cannot be valued yet, here or over the API.
        if let Some((_, valuation)) = state.price(watermark.timestamp_ms) {
            valuations.insert(row.market, valuation);
        }
    }

    let mut margins: BTreeMap<i64, Vec<BigDecimal>> = BTreeMap::new();
    for row in positions(conn).await? {
        let Some(valuation) = valuations.get(&row.market) else {
            continue;
        };
        let position = Position {
            collateral: row.collateral,
            base: row.base,
            quote_notional: row.quote_notional,
            cum_funding_rate_long: row.cum_funding_rate_long,
            cum_funding_rate_short: row.cum_funding_rate_short,
            asks_quantity: row.asks_quantity,
            bids_quantity: row.bids_quantity,
            pending_orders: row.pending_orders,
            initial_margin_ratio: row.initial_margin_ratio,
        };
        margins
            .entry(row.account_id)
            .or_default()
            .push(position.margin(valuation).margin);
    }

    let mut ticks = vec![];
    let mut unvalued = 0;
    for row in accounts(conn, last.bucket_ms).await? {
        let Some(collateral) = collaterals.get(&row.collateral_type) else {
            unvalued += 1;
            continue;
        };
        let account = Account {
            account_id: row.account_id,
            collateral: row.collateral,
            net_transfers: row.net_transfers,
        };
        let margins = margins.get(&row.account_id).map_or(&[][..], Vec::as_slice);
        let (equity, net_transfers, total_pnl) = value(&account, collateral, margins);
        ticks.push(PnlTick {
            account_id: row.account_id,
            bucket_ms,
            checkpoint: watermark.checkpoint,
            timestamp_ms: watermark.timestamp_ms,
            equity,
            net_transfers,
            total_pnl,
        });
    }
    if unvalued > 0 {
        // The decimals of a collateral come from the event that created its first market.
        warn!(
            accounts = unvalued,
            "No ticks for accounts whose collateral has no known decimals"
        );
    }

    let mut rows = 0;
    for chunk in ticks.chunks(CHUNK_ROWS) {
        rows += diesel::insert_into(pnl_ticks::table)
            .values(chunk)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;
    }
    rows += diesel::insert_into(pnl_tick_runs::table)
        .values(PnlTickRun {
            bucket_ms,
            checkpoint: watermark.checkpoint,
            timestamp_ms: watermark.timestamp_ms,
            accounts: ticks.len() as i64,
        })
        .execute(conn)
        .await?;
    info!(
        bucket_ms,
        checkpoint = watermark.checkpoint,
        accounts = ticks.len(),
        "Took PnL ticks"
    );
    Ok(rows)
}

/// Every market with the oracle prices it reads. Market parameters are stored as the raw
/// integers the chain holds, so the ones needed are scaled here.
async fn markets(conn: &mut Connection<'_>) -> anyhow::Result<Vec<MarketRow>> {
    Ok(sql_query(
        "SELECT m.market, m.collateral_type, m.collateral_decimals, m.settlement_enabled, \
                m.settlement_base_price, m.margin_ratio_initial, m.cum_funding_rate_long, \
                m.cum_funding_rate_short, m.funding_last_upd_ms, \
                (m.params->'twap_params'->>'funding_frequency_ms')::BIGINT \
                    AS funding_frequency_ms, \
                (m.params->'twap_params'->>'funding_period_ms')::BIGINT AS funding_period_ms, \
                m.premium_twap, m.spread_twap, m.best_bid_price, m.best_ask_price, \
                (m.params->'core_params'->>'collateral_haircut')::NUMERIC / 1e18 \
                    AS collateral_haircut, \
                b.price AS oracle_price, b.twap_price AS oracle_twap_price, \
                c.price AS collateral_price, m.index_price AS event_index_price \
         FROM markets m \
         LEFT JOIN oracle_prices b \
                ON b.storage_id = m.base_storage_id AND b.source_id = m.base_source_id \
         LEFT JOIN oracle_prices c \
                ON c.storage_id = m.collateral_storage_id AND c.source_id = m.collateral_source_id \
         ORDER BY m.market_index NULLS LAST, m.market",
    )
    .load(conn)
    .await?)
}

/// The positions that hold collateral or size.
async fn positions(conn: &mut Connection<'_>) -> anyhow::Result<Vec<PositionRow>> {
    Ok(sql_query(
        "SELECT market, account_id, collateral, base, quote_notional, cum_funding_rate_long, \
                cum_funding_rate_short, asks_quantity, bids_quantity, pending_orders, \
                initial_margin_ratio \
         FROM positions WHERE collateral <> 0 OR base <> 0",
    )
    .load(conn)
    .await?)
}

/// The accounts that hold anything, and the ones whose last tick was not zero: an account that
/// was emptied gets one more tick, so that its history ends at zero rather than at what it last
/// held.
async fn accounts(
    conn: &mut Connection<'_>,
    last_bucket_ms: Option<i64>,
) -> anyhow::Result<Vec<AccountRow>> {
    Ok(sql_query(
        "SELECT a.account_id, a.collateral_type, a.collateral, a.net_transfers \
         FROM accounts a \
         WHERE a.collateral <> 0 \
            OR EXISTS (SELECT 1 FROM positions p \
                        WHERE p.account_id = a.account_id AND (p.collateral <> 0 OR p.base <> 0)) \
            OR EXISTS (SELECT 1 FROM pnl_ticks t \
                        WHERE t.account_id = a.account_id AND t.bucket_ms = $1 AND t.equity <> 0) \
         ORDER BY a.account_id",
    )
    .bind::<Nullable<BigInt>, _>(last_bucket_ms)
    .load(conn)
    .await?)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn account(collateral: &str, net_transfers: &str) -> Account {
        Account {
            account_id: 7,
            collateral: dec(collateral),
            net_transfers: dec(net_transfers),
        }
    }

    #[test]
    fn ticks_fall_on_the_start_of_their_interval() {
        assert_eq!(bucket_start(3_599_999, 3_600_000), 0);
        assert_eq!(bucket_start(3_600_000, 3_600_000), 3_600_000);
        assert_eq!(bucket_start(7_300_000, 3_600_000), 7_200_000);
        assert_eq!(bucket_start(12_345, 5_000), 10_000);
    }

    #[test]
    fn equity_is_the_balance_plus_the_margin_of_every_position() {
        let usd = Collateral {
            decimals: 6,
            price: dec("1"),
        };
        // 400 unallocated, 1,000 deposited in all, positions worth 650 and 80.
        let (equity, net_transfers, total_pnl) = value(
            &account("400000000", "1000000000"),
            &usd,
            &[dec("650"), dec("80")],
        );
        assert_eq!(equity, dec("1130"));
        assert_eq!(net_transfers, dec("1000"));
        assert_eq!(total_pnl, dec("130"));

        // An account that only ever deposited has made and lost nothing.
        let (equity, _, total_pnl) = value(&account("1000000000", "1000000000"), &usd, &[]);
        assert_eq!((equity, total_pnl), (dec("1000"), dec("0")));
    }

    #[test]
    fn a_position_in_bad_debt_does_not_take_from_the_rest() {
        let usd = Collateral {
            decimals: 6,
            price: dec("1"),
        };
        let (equity, _, total_pnl) = value(
            &account("100000000", "600000000"),
            &usd,
            &[dec("-25"), dec("40")],
        );
        assert_eq!(equity, dec("140"));
        assert_eq!(total_pnl, dec("-460"));
    }

    #[test]
    fn balances_and_transfers_are_valued_at_the_collateral_price() {
        let coin = Collateral {
            decimals: 9,
            price: dec("0.5"),
        };
        let (equity, net_transfers, total_pnl) =
            value(&account("3000000000", "2000000000"), &coin, &[dec("10")]);
        // 3 coins at 0.5, plus 10 of margin.
        assert_eq!(equity, dec("11.5"));
        assert_eq!(net_transfers, dec("1"));
        assert_eq!(total_pnl, dec("10.5"));

        // An emptied account that withdrew more than it put in ends at zero equity.
        let (equity, _, total_pnl) = value(&account("0", "-4000000000"), &coin, &[]);
        assert_eq!((equity, total_pnl), (dec("0"), dec("2")));
    }
}
