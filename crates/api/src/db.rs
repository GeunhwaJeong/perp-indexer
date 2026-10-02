// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The queries the API runs against the indexer's tables.
//!
//! What has to belong to a single checkpoint (a subscription's initial data, a round of the
//! feed) is read through a [`Snapshot`].

use bigdecimal::BigDecimal;
use diesel::sql_types::{Array, BigInt, Bool, Nullable, Numeric, SmallInt, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::{AsyncConnection, RunQueryDsl, TransactionManager};
use haneul_pg_db::{Connection, Db};

/// The pipeline whose watermark says how far the state tables have been built.
const STATE_PIPELINE: &str = "state";

/// One view of the database: a read-only transaction in which every query sees the data as it
/// was when the first one ran.
///
/// Call [`Snapshot::finish`] when done. A snapshot that is dropped instead, as on an early
/// return, leaves its transaction open; the pool notices and discards the connection.
pub struct Snapshot<'a>(Connection<'a>);

type Transactions<'a> = <Connection<'a> as AsyncConnection>::TransactionManager;

pub async fn snapshot(db: &Db) -> anyhow::Result<Snapshot<'_>> {
    let mut conn = db.connect().await?;
    Transactions::begin_transaction(&mut conn).await?;
    let mut snapshot = Snapshot(conn);
    sql_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(snapshot.conn())
        .await?;
    Ok(snapshot)
}

impl<'a> Snapshot<'a> {
    pub fn conn(&mut self) -> &mut Connection<'a> {
        &mut self.0
    }

    /// Ends the transaction and returns the connection to the pool.
    pub async fn finish(mut self) -> anyhow::Result<()> {
        Ok(Transactions::rollback_transaction(&mut self.0).await?)
    }
}

/// How far the state tables have been built.
#[derive(Clone, Copy, Debug, PartialEq, Eq, QueryableByName)]
pub struct Watermark {
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub timestamp_ms: i64,
}

/// `None` until the indexer has committed its first batch.
pub async fn watermark(conn: &mut Connection<'_>) -> anyhow::Result<Option<Watermark>> {
    Ok(sql_query(
        "SELECT checkpoint_hi_inclusive AS checkpoint, timestamp_ms_hi_inclusive AS timestamp_ms \
         FROM watermarks WHERE pipeline = $1",
    )
    .bind::<Text, _>(STATE_PIPELINE)
    .get_results::<Watermark>(conn)
    .await?
    .pop())
}

#[derive(Clone, Debug, QueryableByName)]
pub struct MarketRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub market_index: Option<i64>,
    #[diesel(sql_type = SmallInt)]
    pub paused: i16,
    #[diesel(sql_type = Bool)]
    pub closed: bool,
    #[diesel(sql_type = Bool)]
    pub settlement_enabled: bool,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub settlement_base_price: Option<BigDecimal>,
    #[diesel(sql_type = Numeric)]
    pub lot_size: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub tick_size: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub margin_ratio_initial: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub margin_ratio_maintenance: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub funding_last_upd_ms: i64,
    #[diesel(sql_type = Numeric)]
    pub premium_twap: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub spread_twap: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub open_interest: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub best_ask_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub best_bid_price: Option<BigDecimal>,
    #[diesel(sql_type = BigInt)]
    pub funding_frequency_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub funding_period_ms: i64,
    #[diesel(sql_type = Numeric)]
    pub collateral_haircut: BigDecimal,
    /// The base asset's oracle price and its TWAP.
    #[diesel(sql_type = Nullable<Numeric>)]
    pub oracle_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub oracle_twap_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub oracle_updated_checkpoint: Option<i64>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub collateral_price: Option<BigDecimal>,
    /// The index price the engine last reported in an event.
    #[diesel(sql_type = Nullable<Numeric>)]
    pub event_index_price: Option<BigDecimal>,
}

/// The listed markets, in no particular order. Market parameters are stored as the raw integers
/// the chain holds, so the ones needed are scaled here.
pub async fn markets(
    conn: &mut Connection<'_>,
    market_ids: &[String],
) -> anyhow::Result<Vec<MarketRow>> {
    Ok(sql_query(
        "SELECT m.market, m.market_index, m.paused, m.closed, m.settlement_enabled, \
                m.settlement_base_price, m.lot_size, m.tick_size, m.margin_ratio_initial, \
                m.margin_ratio_maintenance, m.cum_funding_rate_long, m.cum_funding_rate_short, \
                m.funding_last_upd_ms, m.premium_twap, m.spread_twap, m.open_interest, \
                m.best_ask_price, m.best_bid_price, \
                (m.params->'twap_params'->>'funding_frequency_ms')::BIGINT \
                    AS funding_frequency_ms, \
                (m.params->'twap_params'->>'funding_period_ms')::BIGINT AS funding_period_ms, \
                (m.params->'core_params'->>'collateral_haircut')::NUMERIC / 1e18 \
                    AS collateral_haircut, \
                b.price AS oracle_price, b.twap_price AS oracle_twap_price, \
                b.updated_checkpoint AS oracle_updated_checkpoint, \
                c.price AS collateral_price, m.index_price AS event_index_price \
         FROM markets m \
         LEFT JOIN oracle_prices b \
                ON b.storage_id = m.base_storage_id AND b.source_id = m.base_source_id \
         LEFT JOIN oracle_prices c \
                ON c.storage_id = m.collateral_storage_id AND c.source_id = m.collateral_source_id \
         WHERE m.market = ANY($1)",
    )
    .bind::<Array<Text>, _>(market_ids)
    .load(conn)
    .await?)
}

/// A market's trading over the last 24 hours.
#[derive(Clone, Debug, QueryableByName)]
pub struct MarketStatsRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = Numeric)]
    pub volume: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub trades: i64,
    /// The last trade price.
    #[diesel(sql_type = Nullable<Numeric>)]
    pub last_price: Option<BigDecimal>,
    /// The last trade price a day ago, or the day's first trade price if there was none.
    #[diesel(sql_type = Nullable<Numeric>)]
    pub reference_price: Option<BigDecimal>,
}

/// Stats of the 24 hours before `now_ms`, from the hourly candles.
pub async fn market_stats(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    now_ms: i64,
) -> anyhow::Result<Vec<MarketStatsRow>> {
    Ok(sql_query(
        "SELECT m.market, \
                COALESCE(day.volume, 0) AS volume, \
                COALESCE(day.trades, 0)::BIGINT AS trades, \
                (SELECT close FROM candles c \
                  WHERE c.market = m.market AND c.resolution_ms = 3600000 \
                  ORDER BY c.start_ms DESC LIMIT 1) AS last_price, \
                COALESCE( \
                    (SELECT close FROM candles c \
                      WHERE c.market = m.market AND c.resolution_ms = 3600000 \
                        AND c.start_ms <= $2 - 86400000 \
                      ORDER BY c.start_ms DESC LIMIT 1), \
                    (SELECT open FROM candles c \
                      WHERE c.market = m.market AND c.resolution_ms = 3600000 \
                        AND c.start_ms > $2 - 86400000 \
                      ORDER BY c.start_ms ASC LIMIT 1)) AS reference_price \
         FROM UNNEST($1) AS m(market) \
         LEFT JOIN LATERAL ( \
             SELECT SUM(quote_volume) AS volume, SUM(trades) AS trades FROM candles c \
              WHERE c.market = m.market AND c.resolution_ms = 3600000 \
                AND c.start_ms > $2 - 86400000) day ON TRUE",
    )
    .bind::<Array<Text>, _>(market_ids)
    .bind::<BigInt, _>(now_ms)
    .load(conn)
    .await?)
}

/// The total size resting at one price on one side of a market's book.
#[derive(Clone, Debug, QueryableByName)]
pub struct LevelRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = Bool)]
    pub is_ask: bool,
    #[diesel(sql_type = Numeric)]
    pub price: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub size: BigDecimal,
}

/// Every level of the listed markets' books.
pub async fn book_levels(
    conn: &mut Connection<'_>,
    market_ids: &[String],
) -> anyhow::Result<Vec<LevelRow>> {
    Ok(sql_query(
        "SELECT market, is_ask, price, SUM(remaining) AS size FROM orders \
         WHERE status = 'open' AND market = ANY($1) \
         GROUP BY market, is_ask, price",
    )
    .bind::<Array<Text>, _>(market_ids)
    .load(conn)
    .await?)
}

/// The levels that orders updated in `(lo, hi]` rest or rested at, with their size now. A level
/// that emptied comes back with size zero.
pub async fn changed_levels(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<LevelRow>> {
    Ok(sql_query(
        "WITH touched AS ( \
             SELECT DISTINCT market, is_ask, price FROM orders \
              WHERE updated_checkpoint > $2 AND updated_checkpoint <= $3 AND market = ANY($1)) \
         SELECT t.market, t.is_ask, t.price, COALESCE(SUM(o.remaining), 0) AS size \
         FROM touched t \
         LEFT JOIN orders o \
                ON o.market = t.market AND o.is_ask = t.is_ask AND o.price = t.price \
               AND o.status = 'open' \
         GROUP BY t.market, t.is_ask, t.price",
    )
    .bind::<Array<Text>, _>(market_ids)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

#[derive(Clone, Debug, QueryableByName)]
pub struct OrderRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = BigInt)]
    pub market_index: i64,
    #[diesel(sql_type = Numeric)]
    pub order_id: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    #[diesel(sql_type = Bool)]
    pub is_ask: bool,
    #[diesel(sql_type = Numeric)]
    pub price: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub size: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub filled: BigDecimal,
    #[diesel(sql_type = Text)]
    pub status: String,
    /// 'limit' for an order the engine posted, 'market' for one made out of a taker fill.
    #[diesel(sql_type = Text)]
    pub kind: String,
    #[diesel(sql_type = Nullable<SmallInt>)]
    pub cancel_reason: Option<i16>,
    #[diesel(sql_type = Bool)]
    pub reduce_only: bool,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub expiration_timestamp_ms: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub client_order_id: Option<BigDecimal>,
    #[diesel(sql_type = BigInt)]
    pub created_checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub updated_checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub updated_at_ms: i64,
}

const ORDER_COLUMNS: &str = "o.market, m.market_index, o.order_id, o.account_id, o.is_ask, \
     o.price, o.size, o.filled, o.status, o.kind, o.cancel_reason, o.reduce_only, \
     o.expiration_timestamp_ms, o.client_order_id, o.created_checkpoint, o.updated_checkpoint, \
     o.updated_at_ms \
     FROM orders o JOIN markets m ON m.market = o.market AND m.market_index IS NOT NULL";

/// Which of an account's orders to return.
#[derive(Clone, Debug, Default)]
pub struct OrderFilter {
    pub market: Option<String>,
    pub is_ask: Option<bool>,
    /// 'open', 'filled' or 'canceled'.
    pub status: Option<String>,
    pub limit: i64,
}

/// An account's orders: open ones first, then the most recently updated.
pub async fn account_orders(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_id: i64,
    filter: &OrderFilter,
) -> anyhow::Result<Vec<OrderRow>> {
    Ok(sql_query(format!(
        "SELECT {ORDER_COLUMNS} \
         WHERE o.account_id = $1 AND o.market = ANY($2) \
           AND ($3::TEXT IS NULL OR o.market = $3) \
           AND ($4::BOOL IS NULL OR o.is_ask = $4) \
           AND ($5::TEXT IS NULL OR o.status = $5) \
         ORDER BY (o.status = 'open') DESC, o.updated_checkpoint DESC, o.order_id DESC \
         LIMIT $6"
    ))
    .bind::<BigInt, _>(account_id)
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Nullable<Text>, _>(&filter.market)
    .bind::<Nullable<Bool>, _>(filter.is_ask)
    .bind::<Nullable<Text>, _>(&filter.status)
    .bind::<BigInt, _>(filter.limit)
    .load(conn)
    .await?)
}

/// The orders of `account_ids` updated in `(lo, hi]`.
pub async fn changed_orders(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_ids: &[i64],
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<OrderRow>> {
    Ok(sql_query(format!(
        "SELECT {ORDER_COLUMNS} \
         WHERE o.updated_checkpoint > $3 AND o.updated_checkpoint <= $4 \
           AND o.account_id = ANY($2) AND o.market = ANY($1) \
         ORDER BY o.updated_checkpoint, o.order_id"
    ))
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Array<BigInt>, _>(account_ids)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

#[derive(Clone, Debug, QueryableByName)]
pub struct FillRow {
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub tx_index: i64,
    #[diesel(sql_type = BigInt)]
    pub event_index: i64,
    #[diesel(sql_type = BigInt)]
    pub fill_index: i64,
    #[diesel(sql_type = BigInt)]
    pub timestamp_ms: i64,
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = BigInt)]
    pub market_index: i64,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub settlement_base_price: Option<BigDecimal>,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    #[diesel(sql_type = Bool)]
    pub is_ask: bool,
    #[diesel(sql_type = Text)]
    pub liquidity: String,
    #[diesel(sql_type = Text)]
    pub kind: String,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub price: Option<BigDecimal>,
    #[diesel(sql_type = Numeric)]
    pub size: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub fee: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub integrator_fee: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub pnl: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub order_id: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub client_order_id: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub position_base_before: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub entry_price_before: Option<BigDecimal>,
    /// The kind of the fill's order, when it has one.
    #[diesel(sql_type = Nullable<Text>)]
    pub order_kind: Option<String>,
}

const FILL_COLUMNS: &str = "f.checkpoint, f.tx_index, f.event_index, f.fill_index, \
     f.timestamp_ms, f.market, m.market_index, m.settlement_base_price, f.account_id, f.is_ask, \
     f.liquidity, f.kind, f.price, f.size, f.fee, f.integrator_fee, f.pnl, f.order_id, \
     f.client_order_id, f.position_base_before, f.entry_price_before, fo.kind AS order_kind \
     FROM fills f JOIN markets m ON m.market = f.market AND m.market_index IS NOT NULL \
     LEFT JOIN orders fo ON fo.market = f.market AND fo.order_id = f.order_id";

/// Newest first.
const FILL_ORDER: &str =
    "f.checkpoint DESC, f.tx_index DESC, f.event_index DESC, f.fill_index DESC";

/// A page of a history that is read newest first.
#[derive(Clone, Copy, Debug)]
pub struct Page {
    pub limit: i64,
    pub offset: i64,
    /// Only rows at or before this checkpoint.
    pub before_checkpoint: Option<i64>,
    /// Only rows at or before this time.
    pub before_ms: Option<i64>,
}

/// An account's fills, newest first.
pub async fn account_fills(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_id: i64,
    market: Option<&str>,
    page: Page,
) -> anyhow::Result<Vec<FillRow>> {
    Ok(sql_query(format!(
        "SELECT {FILL_COLUMNS} \
         WHERE f.account_id = $1 AND f.market = ANY($2) \
           AND ($3::TEXT IS NULL OR f.market = $3) \
           AND ($4::BIGINT IS NULL OR f.checkpoint <= $4) \
           AND ($5::BIGINT IS NULL OR f.timestamp_ms <= $5) \
         ORDER BY {FILL_ORDER} LIMIT $6 OFFSET $7"
    ))
    .bind::<BigInt, _>(account_id)
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Nullable<Text>, _>(market)
    .bind::<Nullable<BigInt>, _>(page.before_checkpoint)
    .bind::<Nullable<BigInt>, _>(page.before_ms)
    .bind::<BigInt, _>(page.limit)
    .bind::<BigInt, _>(page.offset)
    .load(conn)
    .await?)
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

/// How many fills [`account_fills`] pages over.
pub async fn count_account_fills(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_id: i64,
    market: Option<&str>,
    page: Page,
) -> anyhow::Result<i64> {
    let count: Count = sql_query(
        "SELECT COUNT(*) AS count FROM fills f \
         WHERE f.account_id = $1 AND f.market = ANY($2) AND ($3::TEXT IS NULL OR f.market = $3) \
           AND ($4::BIGINT IS NULL OR f.checkpoint <= $4) \
           AND ($5::BIGINT IS NULL OR f.timestamp_ms <= $5)",
    )
    .bind::<BigInt, _>(account_id)
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Nullable<Text>, _>(market)
    .bind::<Nullable<BigInt>, _>(page.before_checkpoint)
    .bind::<Nullable<BigInt>, _>(page.before_ms)
    .get_result(conn)
    .await?;
    Ok(count.count)
}

/// A market's trades, newest first. Each match appears once, as its maker fill.
pub async fn market_trades(
    conn: &mut Connection<'_>,
    market: &str,
    page: Page,
) -> anyhow::Result<Vec<FillRow>> {
    Ok(sql_query(format!(
        "SELECT {FILL_COLUMNS} \
         WHERE f.market = $1 AND f.kind = 'trade' AND f.liquidity = 'maker' \
           AND ($2::BIGINT IS NULL OR f.checkpoint <= $2) \
           AND ($3::BIGINT IS NULL OR f.timestamp_ms <= $3) \
         ORDER BY {FILL_ORDER} LIMIT $4 OFFSET $5"
    ))
    .bind::<Text, _>(market)
    .bind::<Nullable<BigInt>, _>(page.before_checkpoint)
    .bind::<Nullable<BigInt>, _>(page.before_ms)
    .bind::<BigInt, _>(page.limit)
    .bind::<BigInt, _>(page.offset)
    .load(conn)
    .await?)
}

/// The latest `limit` trades of each listed market, newest first within a market.
pub async fn recent_trades(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    limit: i64,
) -> anyhow::Result<Vec<FillRow>> {
    Ok(sql_query(format!(
        "SELECT t.* FROM UNNEST($1) AS listed(market) \
         CROSS JOIN LATERAL ( \
             SELECT {FILL_COLUMNS} \
              WHERE f.market = listed.market AND f.kind = 'trade' AND f.liquidity = 'maker' \
              ORDER BY {FILL_ORDER} LIMIT $2) t"
    ))
    .bind::<Array<Text>, _>(market_ids)
    .bind::<BigInt, _>(limit)
    .load(conn)
    .await?)
}

/// The fills in `(lo, hi]` that are trades, or that belong to one of `account_ids`, in chain
/// order.
pub async fn changed_fills(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_ids: &[i64],
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<FillRow>> {
    Ok(sql_query(format!(
        "SELECT {FILL_COLUMNS} \
         WHERE f.checkpoint > $3 AND f.checkpoint <= $4 AND f.market = ANY($1) \
           AND ((f.kind = 'trade' AND f.liquidity = 'maker') OR f.account_id = ANY($2)) \
         ORDER BY f.checkpoint, f.tx_index, f.event_index, f.fill_index"
    ))
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Array<BigInt>, _>(account_ids)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

#[derive(Clone, Debug, QueryableByName)]
pub struct CandleRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = BigInt)]
    pub resolution_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub start_ms: i64,
    #[diesel(sql_type = Numeric)]
    pub open: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub high: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub low: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub close: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub base_volume: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub quote_volume: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub trades: i64,
}

const CANDLE_COLUMNS: &str = "market, resolution_ms, start_ms, open, high, low, close, \
     base_volume, quote_volume, trades FROM candles";

/// A market's candles at one resolution, newest first. `from_ms` is inclusive and `to_ms`
/// exclusive, as in the dYdX indexer: the chart pages backwards by asking for the candles
/// before the oldest one it holds, and would be handed that candle again for ever otherwise.
pub async fn candles(
    conn: &mut Connection<'_>,
    market: &str,
    resolution_ms: i64,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    limit: i64,
) -> anyhow::Result<Vec<CandleRow>> {
    Ok(sql_query(format!(
        "SELECT {CANDLE_COLUMNS} \
         WHERE market = $1 AND resolution_ms = $2 \
           AND ($3::BIGINT IS NULL OR start_ms >= $3) \
           AND ($4::BIGINT IS NULL OR start_ms < $4) \
         ORDER BY start_ms DESC LIMIT $5"
    ))
    .bind::<Text, _>(market)
    .bind::<BigInt, _>(resolution_ms)
    .bind::<Nullable<BigInt>, _>(from_ms)
    .bind::<Nullable<BigInt>, _>(to_ms)
    .bind::<BigInt, _>(limit)
    .load(conn)
    .await?)
}

/// The candles of the listed markets that start at one of `starts_ms`: a superset of the
/// candles a set of trades fell into, for the caller to narrow down.
pub async fn candles_starting_at(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    starts_ms: &[i64],
) -> anyhow::Result<Vec<CandleRow>> {
    Ok(sql_query(format!(
        "SELECT {CANDLE_COLUMNS} WHERE market = ANY($1) AND start_ms = ANY($2)"
    ))
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Array<BigInt>, _>(starts_ms)
    .load(conn)
    .await?)
}

/// The closes of the latest candles of every listed market, newest first within a market.
pub async fn sparklines(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    resolution_ms: i64,
    since_ms: i64,
) -> anyhow::Result<Vec<CandleRow>> {
    Ok(sql_query(format!(
        "SELECT {CANDLE_COLUMNS} \
         WHERE market = ANY($1) AND resolution_ms = $2 AND start_ms >= $3 \
         ORDER BY market, start_ms DESC"
    ))
    .bind::<Array<Text>, _>(market_ids)
    .bind::<BigInt, _>(resolution_ms)
    .bind::<BigInt, _>(since_ms)
    .load(conn)
    .await?)
}

#[derive(Clone, Debug, QueryableByName)]
pub struct AccountRow {
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    /// Collateral not allocated to any market, in coin units.
    #[diesel(sql_type = Numeric)]
    pub collateral: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub updated_checkpoint: i64,
}

/// The account `owner` holds its `rank`-th admin capability for, counting accounts of the
/// collateral in the order they were created.
pub async fn account_of(
    conn: &mut Connection<'_>,
    owner: &str,
    collateral_type: &str,
    rank: i64,
) -> anyhow::Result<Option<AccountRow>> {
    Ok(sql_query(
        "SELECT a.account_id, a.collateral, a.updated_checkpoint \
         FROM account_caps c JOIN accounts a ON a.object_id = c.account_object_id \
         WHERE c.owner = $1 AND c.role = 'admin' AND a.collateral_type = $2 \
         ORDER BY a.account_id LIMIT 1 OFFSET $3",
    )
    .bind::<Text, _>(owner)
    .bind::<Text, _>(collateral_type)
    .bind::<BigInt, _>(rank)
    .get_results::<AccountRow>(conn)
    .await?
    .pop())
}

/// The accounts among `account_ids` whose balance changed in `(lo, hi]`.
pub async fn changed_accounts(
    conn: &mut Connection<'_>,
    account_ids: &[i64],
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<AccountRow>> {
    Ok(sql_query(
        "SELECT account_id, collateral, updated_checkpoint FROM accounts \
         WHERE updated_checkpoint > $2 AND updated_checkpoint <= $3 AND account_id = ANY($1)",
    )
    .bind::<Array<BigInt>, _>(account_ids)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

/// A change of who holds the admin or assistant capability of an account.
#[derive(Clone, Debug, QueryableByName)]
pub struct CapRow {
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    #[diesel(sql_type = Text)]
    pub role: String,
    /// `None` when the capability was deleted or is no longer held by an address.
    #[diesel(sql_type = Nullable<Text>)]
    pub owner: Option<String>,
}

/// The capabilities over accounts of the collateral that changed hands in `(lo, hi]`.
pub async fn changed_caps(
    conn: &mut Connection<'_>,
    collateral_type: &str,
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<CapRow>> {
    Ok(sql_query(
        "SELECT a.account_id, c.role, c.owner \
         FROM account_caps c JOIN accounts a ON a.object_id = c.account_object_id \
         WHERE c.updated_checkpoint > $2 AND c.updated_checkpoint <= $3 \
           AND a.collateral_type = $1",
    )
    .bind::<Text, _>(collateral_type)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

#[derive(Clone, Debug, QueryableByName)]
pub struct PositionRow {
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    #[diesel(sql_type = Numeric)]
    pub collateral: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub base: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub quote_notional: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub asks_quantity: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub bids_quantity: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub pending_orders: i64,
    #[diesel(sql_type = Numeric)]
    pub initial_margin_ratio: BigDecimal,
    #[diesel(sql_type = BigInt)]
    pub created_checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub created_at_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub updated_checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub updated_at_ms: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub opened_checkpoint: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub opened_at_ms: Option<i64>,
    #[diesel(sql_type = Numeric)]
    pub max_size: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub sum_open: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub sum_close: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub close_quote: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub realized_pnl: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub net_funding: BigDecimal,
}

const POSITION_COLUMNS: &str = "market, account_id, collateral, base, quote_notional, \
     cum_funding_rate_long, cum_funding_rate_short, asks_quantity, bids_quantity, \
     pending_orders, initial_margin_ratio, created_checkpoint, created_at_ms, \
     updated_checkpoint, updated_at_ms, opened_checkpoint, opened_at_ms, max_size, sum_open, \
     sum_close, close_quote, realized_pnl, net_funding FROM positions";

/// An account's positions in the listed markets.
pub async fn account_positions(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_id: i64,
) -> anyhow::Result<Vec<PositionRow>> {
    Ok(sql_query(format!(
        "SELECT {POSITION_COLUMNS} WHERE account_id = $1 AND market = ANY($2)"
    ))
    .bind::<BigInt, _>(account_id)
    .bind::<Array<Text>, _>(market_ids)
    .load(conn)
    .await?)
}

/// The positions of `account_ids` that changed in `(lo, hi]`, or that are open in one of
/// `repriced_markets`: markets whose cumulative funding rates moved, which changes what every
/// open position in them has accrued.
pub async fn changed_positions(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_ids: &[i64],
    repriced_markets: &[String],
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<PositionRow>> {
    Ok(sql_query(format!(
        "SELECT {POSITION_COLUMNS} \
         WHERE account_id = ANY($2) AND market = ANY($1) \
           AND ((updated_checkpoint > $4 AND updated_checkpoint <= $5) \
                OR (base <> 0 AND market = ANY($3)))"
    ))
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Array<BigInt>, _>(account_ids)
    .bind::<Array<Text>, _>(repriced_markets)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

/// A deposit into or withdrawal from an account.
#[derive(Clone, Debug, QueryableByName)]
pub struct TransferRow {
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub tx_index: i64,
    #[diesel(sql_type = BigInt)]
    pub event_index: i64,
    #[diesel(sql_type = Text)]
    pub tx_digest: String,
    #[diesel(sql_type = BigInt)]
    pub timestamp_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub account_id: i64,
    #[diesel(sql_type = Text)]
    pub kind: String,
    /// In coin units.
    #[diesel(sql_type = Numeric)]
    pub amount: BigDecimal,
}

const TRANSFER_COLUMNS: &str = "checkpoint, tx_index, event_index, tx_digest, timestamp_ms, \
     account_id, kind, amount FROM collateral_transfers";

/// An account's deposits and withdrawals, newest first. Moves between the account and its
/// markets are not transfers in or out of it.
pub async fn account_transfers(
    conn: &mut Connection<'_>,
    account_id: i64,
    page: Page,
) -> anyhow::Result<Vec<TransferRow>> {
    Ok(sql_query(format!(
        "SELECT {TRANSFER_COLUMNS} \
         WHERE account_id = $1 AND kind IN ('deposit', 'withdraw') \
           AND ($2::BIGINT IS NULL OR checkpoint <= $2) \
           AND ($3::BIGINT IS NULL OR timestamp_ms <= $3) \
         ORDER BY checkpoint DESC, tx_index DESC, event_index DESC LIMIT $4 OFFSET $5"
    ))
    .bind::<BigInt, _>(account_id)
    .bind::<Nullable<BigInt>, _>(page.before_checkpoint)
    .bind::<Nullable<BigInt>, _>(page.before_ms)
    .bind::<BigInt, _>(page.limit)
    .bind::<BigInt, _>(page.offset)
    .load(conn)
    .await?)
}

/// How many transfers [`account_transfers`] pages over.
pub async fn count_account_transfers(
    conn: &mut Connection<'_>,
    account_id: i64,
    page: Page,
) -> anyhow::Result<i64> {
    let count: Count = sql_query(
        "SELECT COUNT(*) AS count FROM collateral_transfers \
         WHERE account_id = $1 AND kind IN ('deposit', 'withdraw') \
           AND ($2::BIGINT IS NULL OR checkpoint <= $2) \
           AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)",
    )
    .bind::<BigInt, _>(account_id)
    .bind::<Nullable<BigInt>, _>(page.before_checkpoint)
    .bind::<Nullable<BigInt>, _>(page.before_ms)
    .get_result(conn)
    .await?;
    Ok(count.count)
}

/// The deposits and withdrawals of `account_ids` in `(lo, hi]`, in chain order.
pub async fn changed_transfers(
    conn: &mut Connection<'_>,
    account_ids: &[i64],
    lo: i64,
    hi: i64,
) -> anyhow::Result<Vec<TransferRow>> {
    Ok(sql_query(format!(
        "SELECT {TRANSFER_COLUMNS} \
         WHERE checkpoint > $2 AND checkpoint <= $3 AND account_id = ANY($1) \
           AND kind IN ('deposit', 'withdraw') \
         ORDER BY checkpoint, tx_index, event_index"
    ))
    .bind::<Array<BigInt>, _>(account_ids)
    .bind::<BigInt, _>(lo)
    .bind::<BigInt, _>(hi)
    .load(conn)
    .await?)
}

#[derive(Clone, Debug, QueryableByName)]
pub struct FundingPaymentRow {
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub timestamp_ms: i64,
    #[diesel(sql_type = Text)]
    pub market: String,
    #[diesel(sql_type = BigInt)]
    pub market_index: i64,
    /// Positive when the account received funding.
    #[diesel(sql_type = Numeric)]
    pub collateral_change_usd: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_short: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub position_base: Option<BigDecimal>,
    /// The market's index price when the payment was settled.
    #[diesel(sql_type = Nullable<Numeric>)]
    pub index_price: Option<BigDecimal>,
}

/// An account's funding payments, newest first.
///
/// The engine also reports a settlement when it only brings a flat position's funding snapshot
/// up to date. Nothing was paid then, and those are left out.
pub async fn account_funding_payments(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_id: i64,
    market: Option<&str>,
    after_ms: Option<i64>,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<FundingPaymentRow>> {
    Ok(sql_query(
        "SELECT p.checkpoint, p.timestamp_ms, p.market, m.market_index, \
                p.collateral_change_usd, p.cum_funding_rate_long, p.cum_funding_rate_short, \
                p.position_base, p.index_price \
         FROM funding_payments p \
         JOIN markets m ON m.market = p.market AND m.market_index IS NOT NULL \
         WHERE p.account_id = $1 AND p.market = ANY($2) AND p.collateral_change_usd <> 0 \
           AND ($3::TEXT IS NULL OR p.market = $3) \
           AND ($4::BIGINT IS NULL OR p.timestamp_ms >= $4) \
         ORDER BY p.checkpoint DESC, p.tx_index DESC, p.event_index DESC LIMIT $5 OFFSET $6",
    )
    .bind::<BigInt, _>(account_id)
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Nullable<Text>, _>(market)
    .bind::<Nullable<BigInt>, _>(after_ms)
    .bind::<BigInt, _>(limit)
    .bind::<BigInt, _>(offset)
    .load(conn)
    .await?)
}

/// How many payments [`account_funding_payments`] pages over.
pub async fn count_account_funding_payments(
    conn: &mut Connection<'_>,
    market_ids: &[String],
    account_id: i64,
    market: Option<&str>,
    after_ms: Option<i64>,
) -> anyhow::Result<i64> {
    let count: Count = sql_query(
        "SELECT COUNT(*) AS count FROM funding_payments p \
         WHERE p.account_id = $1 AND p.market = ANY($2) AND p.collateral_change_usd <> 0 \
           AND ($3::TEXT IS NULL OR p.market = $3) \
           AND ($4::BIGINT IS NULL OR p.timestamp_ms >= $4)",
    )
    .bind::<BigInt, _>(account_id)
    .bind::<Array<Text>, _>(market_ids)
    .bind::<Nullable<Text>, _>(market)
    .bind::<Nullable<BigInt>, _>(after_ms)
    .get_result(conn)
    .await?;
    Ok(count.count)
}

/// One update of a market's cumulative funding rate, with the update before it.
#[derive(Clone, Debug, QueryableByName)]
pub struct FundingUpdateRow {
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    #[diesel(sql_type = BigInt)]
    pub funding_last_upd_ms: i64,
    #[diesel(sql_type = Numeric)]
    pub cum_funding_rate_long: BigDecimal,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub index_price: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<Numeric>)]
    pub previous_cum_funding_rate_long: Option<BigDecimal>,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub previous_funding_last_upd_ms: Option<i64>,
}

/// A market's funding updates, newest first.
pub async fn funding_updates(
    conn: &mut Connection<'_>,
    market: &str,
    before_checkpoint: Option<i64>,
    before_ms: Option<i64>,
    limit: i64,
) -> anyhow::Result<Vec<FundingUpdateRow>> {
    Ok(sql_query(
        "SELECT * FROM ( \
             SELECT checkpoint, funding_last_upd_ms, cum_funding_rate_long, index_price, \
                    LAG(cum_funding_rate_long) OVER chain AS previous_cum_funding_rate_long, \
                    LAG(funding_last_upd_ms) OVER chain AS previous_funding_last_upd_ms \
             FROM funding_updates WHERE market = $1 \
             WINDOW chain AS (ORDER BY funding_last_upd_ms, checkpoint, tx_index, event_index) \
         ) updates \
         WHERE ($2::BIGINT IS NULL OR checkpoint <= $2) \
           AND ($3::BIGINT IS NULL OR funding_last_upd_ms <= $3) \
         ORDER BY funding_last_upd_ms DESC, checkpoint DESC LIMIT $4",
    )
    .bind::<Text, _>(market)
    .bind::<Nullable<BigInt>, _>(before_checkpoint)
    .bind::<Nullable<BigInt>, _>(before_ms)
    .bind::<BigInt, _>(limit)
    .load(conn)
    .await?)
}

/// What an account was worth at one checkpoint.
#[derive(Clone, Debug, QueryableByName)]
pub struct PnlTickRow {
    /// Start of the interval the tick stands for, or of its day in a daily series.
    #[diesel(sql_type = BigInt)]
    pub start_ms: i64,
    #[diesel(sql_type = BigInt)]
    pub checkpoint: i64,
    /// When the account was valued.
    #[diesel(sql_type = BigInt)]
    pub timestamp_ms: i64,
    #[diesel(sql_type = Numeric)]
    pub equity: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub net_transfers: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub total_pnl: BigDecimal,
}

/// Which ticks of an account's history to return.
#[derive(Clone, Copy, Debug, Default)]
pub struct PnlFilter {
    /// One tick per UTC day, the day's first, rather than every tick.
    pub daily: bool,
    pub before_checkpoint: Option<i64>,
    pub before_ms: Option<i64>,
    pub after_checkpoint: Option<i64>,
    pub after_ms: Option<i64>,
    pub limit: i64,
}

/// An account's PnL history, newest first. Times are compared with the start of a tick's
/// interval, which is the time a tick is reported at.
pub async fn account_pnl_ticks(
    conn: &mut Connection<'_>,
    account_id: i64,
    filter: PnlFilter,
) -> anyhow::Result<Vec<PnlTickRow>> {
    const FILTER: &str = "account_id = $1 \
         AND ($2::BIGINT IS NULL OR checkpoint <= $2) \
         AND ($3::BIGINT IS NULL OR bucket_ms <= $3) \
         AND ($4::BIGINT IS NULL OR checkpoint >= $4) \
         AND ($5::BIGINT IS NULL OR bucket_ms >= $5)";
    let query = if filter.daily {
        format!(
            "SELECT * FROM ( \
                 SELECT DISTINCT ON (bucket_ms / 86400000) \
                        bucket_ms / 86400000 * 86400000 AS start_ms, checkpoint, timestamp_ms, \
                        equity, net_transfers, total_pnl \
                 FROM pnl_ticks WHERE {FILTER} \
                 ORDER BY bucket_ms / 86400000, bucket_ms \
             ) days ORDER BY start_ms DESC LIMIT $6"
        )
    } else {
        format!(
            "SELECT bucket_ms AS start_ms, checkpoint, timestamp_ms, equity, net_transfers, \
                    total_pnl \
             FROM pnl_ticks WHERE {FILTER} ORDER BY bucket_ms DESC LIMIT $6"
        )
    };
    Ok(sql_query(query)
        .bind::<BigInt, _>(account_id)
        .bind::<Nullable<BigInt>, _>(filter.before_checkpoint)
        .bind::<Nullable<BigInt>, _>(filter.before_ms)
        .bind::<Nullable<BigInt>, _>(filter.after_checkpoint)
        .bind::<Nullable<BigInt>, _>(filter.after_ms)
        .bind::<BigInt, _>(filter.limit)
        .load(conn)
        .await?)
}
