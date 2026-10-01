// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Turns what the indexer commits into the updates subscribers receive.
//!
//! The feed polls the state pipeline's watermark. When it has moved, the feed reads, in one
//! database snapshot, everything that changed over the new checkpoints and publishes it as a
//! round. It reads by checkpoint range rather than being told what changed, so it needs nothing
//! from the indexer but its tables, survives restarts of either side, and can run in several
//! API processes over one database.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bigdecimal::BigDecimal;
use haneul_pg_db::Db;
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::Deployment;
use crate::db::{
    self, AccountRow, CandleRow, CapRow, FillRow, LevelRow, MarketStatsRow, OrderRow, PositionRow,
    TransferRow, Watermark,
};
use crate::decimal::plain;
use crate::hub::{AccountUpdate, Hub, PublicState, PublicUpdate, RECENT_TRADES, Round, TradeItem};
use crate::model::PerpetualMarket;
use crate::snapshot::market_views;
use crate::time::iso;
use crate::views::{
    Child, MarketViews, RESOLUTIONS, account_asset, account_balance, candle_object, child_number,
    collateral_price, fill_object, order_object, resolution_name, trade_object,
};

#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// How often the watermark is checked.
    pub poll_interval: Duration,
    /// The least time between two updates of the markets channel. Mark prices drift with every
    /// checkpoint, and nobody needs them four times a second.
    pub markets_interval: Duration,
    /// How long the 24 hour statistics are reused.
    pub stats_interval: Duration,
    /// A gap of more checkpoints than this is not bridged with updates: clients start over.
    /// It is only reached when the indexer is backfilling or the API was cut off from it.
    pub max_round_checkpoints: i64,
}

type Markets = BTreeMap<String, PerpetualMarket>;

/// Everything read from the database for one round.
struct Changes {
    watermark: Watermark,
    views: MarketViews,
    /// `Some` when the markets channel is due an update.
    stats: Option<Vec<MarketStatsRow>>,
    levels: Vec<LevelRow>,
    fills: Vec<FillRow>,
    candles: Vec<CandleRow>,
    watched: Vec<i64>,
    positions: Vec<PositionRow>,
    accounts: Vec<AccountRow>,
    orders: Vec<OrderRow>,
    transfers: Vec<TransferRow>,
    caps: Vec<CapRow>,
}

enum Step {
    /// Nothing new, or nothing to serve yet.
    Idle,
    Published,
    /// The gap is too wide, or the database went backwards.
    Reload,
}

pub struct Feed {
    db: Db,
    hub: Arc<Hub>,
    deployment: Arc<Deployment>,
    config: FeedConfig,
    /// The checkpoint published up to.
    checkpoint: i64,
    /// The markets as subscribers last saw them.
    markets: Markets,
    markets_sent: Instant,
    stats: HashMap<String, MarketStatsRow>,
    stats_read: Option<Instant>,
    /// Each market's cumulative funding rates as of the last round.
    funding: HashMap<String, (BigDecimal, BigDecimal)>,
}

fn is_trade(fill: &FillRow) -> bool {
    fill.kind == "trade" && fill.liquidity == "maker"
}

fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

impl Feed {
    pub fn new(db: Db, hub: Arc<Hub>, deployment: Arc<Deployment>, config: FeedConfig) -> Self {
        Self {
            db,
            hub,
            deployment,
            config,
            checkpoint: 0,
            markets: Markets::new(),
            markets_sent: Instant::now(),
            stats: HashMap::new(),
            stats_read: None,
            funding: HashMap::new(),
        }
    }

    /// Runs until `cancel` fires. A round that fails is retried on the next tick: the feed's
    /// position only moves when a round was published.
    pub async fn run(mut self, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(self.config.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            if let Err(e) = self.step().await {
                self.hub.metrics.feed_errors.inc();
                warn!("Feed round failed, retrying: {e:#}");
            }
        }
    }

    async fn step(&mut self) -> anyhow::Result<()> {
        if self.hub.public().is_none() {
            return self.load().await;
        }
        let started = Instant::now();
        match self.advance().await? {
            Step::Idle => {}
            Step::Published => {
                let metrics = &self.hub.metrics;
                metrics.feed_rounds.inc();
                metrics
                    .feed_round_seconds
                    .observe(started.elapsed().as_secs_f64());
            }
            Step::Reload => {
                self.hub.metrics.feed_resets.inc();
                self.load().await?;
            }
        }
        Ok(())
    }

    fn observe(&self, watermark: Watermark) {
        let metrics = &self.hub.metrics;
        metrics.feed_checkpoint.set(watermark.checkpoint);
        metrics
            .feed_lag_ms
            .set((wall_clock_ms() - watermark.timestamp_ms).max(0));
    }

    /// Reads the public state from scratch and makes every connection start over from it.
    async fn load(&mut self) -> anyhow::Result<()> {
        let market_ids = self.deployment.market_ids();
        let mut snapshot = db::snapshot(&self.db).await?;
        let conn = snapshot.conn();
        let Some(watermark) = db::watermark(conn).await? else {
            // The indexer has not committed anything yet.
            return snapshot.finish().await;
        };
        let views = market_views(conn, &self.deployment, watermark.timestamp_ms).await?;
        let stats = db::market_stats(conn, &market_ids, watermark.timestamp_ms).await?;
        let levels = db::book_levels(conn, &market_ids).await?;
        let trades = db::recent_trades(conn, &market_ids, RECENT_TRADES as i64).await?;
        snapshot.finish().await?;

        self.stats = stats.into_iter().map(|s| (s.market.clone(), s)).collect();
        self.stats_read = Some(Instant::now());
        self.markets = self.market_objects(&views);
        self.markets_sent = Instant::now();
        self.funding = funding_rates(&views);
        self.checkpoint = watermark.checkpoint;

        let mut state = PublicState {
            checkpoint: watermark.checkpoint,
            timestamp_ms: watermark.timestamp_ms,
            markets: self.markets.clone(),
            ..PublicState::default()
        };
        for (ticker, _) in self.deployment.markets() {
            state.books.entry(ticker.to_owned()).or_default();
            state.trades.entry(ticker.to_owned()).or_default();
        }
        for level in levels {
            if let Some(ticker) = self.deployment.ticker(&level.market) {
                let book = state.books.entry(ticker.to_owned()).or_default();
                book.set(level.is_ask, level.price, level.size);
            }
        }
        // Newest first within a market, which is the order they are kept in.
        for trade in &trades {
            if let Some(ticker) = self.deployment.ticker(&trade.market) {
                let trades: &mut VecDeque<_> = state.trades.entry(ticker.to_owned()).or_default();
                trades.push_back(trade_item(trade)?);
            }
        }

        info!(
            checkpoint = watermark.checkpoint,
            markets = state.markets.len(),
            "Feed loaded the public state"
        );
        self.observe(watermark);
        self.hub.reset(state);
        Ok(())
    }

    fn market_objects(&self, views: &MarketViews) -> Markets {
        views
            .iter()
            .map(|(market, view)| (view.ticker.clone(), view.object(self.stats.get(market))))
            .collect()
    }

    /// Publishes the checkpoints committed since the last round, if any.
    async fn advance(&mut self) -> anyhow::Result<Step> {
        let lo = self.checkpoint;
        let max_checkpoints = self.config.max_round_checkpoints;
        let markets_due = self.markets_sent.elapsed() >= self.config.markets_interval;
        let stats_due = self
            .stats_read
            .is_none_or(|read| read.elapsed() >= self.config.stats_interval);
        let market_ids = self.deployment.market_ids();

        let mut snapshot = db::snapshot(&self.db).await?;
        let conn = snapshot.conn();
        // The first query fixes the view everything below is read from.
        let watermark = match db::watermark(conn).await? {
            Some(watermark) if watermark.checkpoint == lo => Err(Step::Idle),
            Some(watermark)
                if watermark.checkpoint > lo && watermark.checkpoint - lo <= max_checkpoints =>
            {
                Ok(watermark)
            }
            // The gap is too wide to bridge, or the database went backwards or was emptied.
            _ => Err(Step::Reload),
        };
        let watermark = match watermark {
            Ok(watermark) => watermark,
            Err(step) => {
                snapshot.finish().await?;
                return Ok(step);
            }
        };
        let hi = watermark.checkpoint;
        // Read after the view was fixed: an account watched from here on takes its initial data
        // from a view at least this new, so it misses nothing by not being in this round.
        let watched = self.hub.watched();

        let views = market_views(conn, &self.deployment, watermark.timestamp_ms).await?;
        let stats = if markets_due && stats_due {
            Some(db::market_stats(conn, &market_ids, watermark.timestamp_ms).await?)
        } else {
            None
        };
        let levels = db::changed_levels(conn, &market_ids, lo, hi).await?;
        let fills = db::changed_fills(conn, &market_ids, &watched, lo, hi).await?;

        let starts: Vec<i64> = fills
            .iter()
            .filter(|fill| is_trade(fill))
            .flat_map(|fill| RESOLUTIONS.map(|(_, ms)| candle_start(fill.timestamp_ms, ms)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let candles = if starts.is_empty() {
            vec![]
        } else {
            db::candles_starting_at(conn, &market_ids, &starts).await?
        };

        let (positions, accounts, orders, transfers) = if watched.is_empty() {
            (vec![], vec![], vec![], vec![])
        } else {
            // A market's cumulative funding rates move when funding is updated and when bad
            // debt is socialized. Either changes what every open position in it has accrued,
            // without the positions themselves being written.
            let repriced: Vec<String> = funding_rates(&views)
                .into_iter()
                .filter(|(market, rates)| self.funding.get(market).is_some_and(|was| was != rates))
                .map(|(market, _)| market)
                .collect();
            (
                db::changed_positions(conn, &market_ids, &watched, &repriced, lo, hi).await?,
                db::changed_accounts(conn, &watched, lo, hi).await?,
                db::changed_orders(conn, &market_ids, &watched, lo, hi).await?,
                db::changed_transfers(conn, &watched, lo, hi).await?,
            )
        };
        let caps = db::changed_caps(conn, &self.deployment.collateral_type, lo, hi).await?;
        snapshot.finish().await?;

        let changes = Changes {
            watermark,
            views,
            stats,
            levels,
            fills,
            candles,
            watched,
            positions,
            accounts,
            orders,
            transfers,
            caps,
        };

        if let Some(stats) = &changes.stats {
            self.stats = stats
                .iter()
                .map(|s| (s.market.clone(), s.clone()))
                .collect();
            self.stats_read = Some(Instant::now());
        }
        let markets = markets_due.then(|| self.market_objects(&changes.views));
        self.funding = funding_rates(&changes.views);
        let watermark = changes.watermark;
        let (round, update) = assemble(&self.deployment, lo, changes, &self.markets, markets)?;

        if let Some(markets) = &update.markets {
            self.markets = markets.clone();
            self.markets_sent = Instant::now();
        }
        self.checkpoint = watermark.checkpoint;
        self.observe(watermark);
        self.hub.publish(round, update);
        Ok(Step::Published)
    }
}

fn funding_rates(views: &MarketViews) -> HashMap<String, (BigDecimal, BigDecimal)> {
    views
        .iter()
        .map(|(market, view)| {
            let rates = &view.valuation;
            let rates = (
                rates.cum_funding_rate_long.clone(),
                rates.cum_funding_rate_short.clone(),
            );
            (market.clone(), rates)
        })
        .collect()
}

fn candle_start(timestamp_ms: i64, resolution_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(resolution_ms)
}

fn trade_item(fill: &FillRow) -> anyhow::Result<TradeItem> {
    Ok(TradeItem {
        checkpoint: fill.checkpoint,
        json: Arc::from(to_raw_value(&trade_object(fill))?),
    })
}

/// The update of the markets channel that takes `previous` to `current`: the fields that
/// changed per market, with the price apart as an oracle price update.
fn markets_update(
    previous: &Markets,
    current: &Markets,
    watermark: Watermark,
) -> anyhow::Result<Option<Box<RawValue>>> {
    let mut trading = Map::new();
    let mut oracle_prices = Map::new();
    for (ticker, market) in current {
        let Value::Object(mut now) = serde_json::to_value(market)? else {
            unreachable!("a market serializes to an object");
        };
        let Some(before) = previous.get(ticker) else {
            // A market the subscriber has not seen is sent whole.
            trading.insert(ticker.clone(), Value::Object(now));
            continue;
        };
        if market == before {
            continue;
        }
        let Value::Object(before) = serde_json::to_value(before)? else {
            unreachable!("a market serializes to an object");
        };
        now.retain(|field, value| before.get(field) != Some(value));
        if let Some(price) = now.remove("oraclePrice") {
            oracle_prices.insert(
                ticker.clone(),
                json!({
                    "oraclePrice": price,
                    "effectiveAt": iso(watermark.timestamp_ms),
                    "effectiveAtHeight": watermark.checkpoint.to_string(),
                    "marketId": market.clob_pair_id.parse::<i64>().unwrap_or_default(),
                }),
            );
        }
        if !now.is_empty() {
            trading.insert(ticker.clone(), Value::Object(now));
        }
    }

    let mut update = Map::new();
    if !trading.is_empty() {
        update.insert("trading".to_owned(), Value::Object(trading));
    }
    if !oracle_prices.is_empty() {
        update.insert("oraclePrices".to_owned(), Value::Object(oracle_prices));
    }
    if update.is_empty() {
        return Ok(None);
    }
    Ok(Some(to_raw_value(&update)?))
}

/// Builds the round and the change to the public state out of what was read.
fn assemble(
    deployment: &Deployment,
    lo: i64,
    changes: Changes,
    previous_markets: &Markets,
    markets: Option<Markets>,
) -> anyhow::Result<(Round, PublicUpdate)> {
    let watermark = changes.watermark;
    let mut round = Round {
        lo,
        checkpoint: watermark.checkpoint,
        timestamp_ms: watermark.timestamp_ms,
        caps: changes.caps,
        ..Round::default()
    };
    let mut update = PublicUpdate::default();

    if let Some(markets) = markets {
        round.markets = markets_update(previous_markets, &markets, watermark)?;
        if round.markets.is_some() {
            update.markets = Some(markets);
        }
    }

    // Books: the levels that changed, as `[price, size]` pairs per side.
    type Levels = Vec<[String; 2]>;
    let mut books: BTreeMap<&str, (Levels, Levels)> = BTreeMap::new();
    for level in &changes.levels {
        let Some(ticker) = deployment.ticker(&level.market) else {
            continue;
        };
        let (bids, asks) = books.entry(ticker).or_default();
        let side = if level.is_ask { asks } else { bids };
        side.push([plain(&level.price), plain(&level.size)]);
    }
    for (ticker, (bids, asks)) in books {
        let mut content = Map::new();
        if !bids.is_empty() {
            content.insert("bids".to_owned(), json!(bids));
        }
        if !asks.is_empty() {
            content.insert("asks".to_owned(), json!(asks));
        }
        round
            .books
            .insert(ticker.to_owned(), to_raw_value(&content)?);
    }
    for level in changes.levels {
        if let Some(ticker) = deployment.ticker(&level.market) {
            update
                .levels
                .push((ticker.to_owned(), level.is_ask, level.price, level.size));
        }
    }

    // The tape and the candles its trades fell into.
    let mut traded = HashSet::new();
    for fill in changes.fills.iter().filter(|fill| is_trade(fill)) {
        let Some(ticker) = deployment.ticker(&fill.market) else {
            continue;
        };
        let item = trade_item(fill)?;
        round
            .trades
            .entry(ticker.to_owned())
            .or_default()
            .push(item.clone());
        update.trades.push((ticker.to_owned(), item));
        for (_, resolution_ms) in RESOLUTIONS {
            let start_ms = candle_start(fill.timestamp_ms, resolution_ms);
            traded.insert((fill.market.as_str(), resolution_ms, start_ms));
        }
    }
    for candle in &changes.candles {
        if !traded.contains(&(
            candle.market.as_str(),
            candle.resolution_ms,
            candle.start_ms,
        )) {
            continue;
        }
        let (Some(ticker), Some(resolution)) = (
            deployment.ticker(&candle.market),
            resolution_name(candle.resolution_ms),
        ) else {
            continue;
        };
        round
            .candles
            .entry(format!("{ticker}/{resolution}"))
            .or_default()
            .push(to_raw_value(&candle_object(candle, ticker, false))?);
    }

    // Accounts someone is subscribed to.
    let watched: HashSet<i64> = changes.watched.into_iter().collect();
    fn account(round: &mut Round, account_id: i64) -> &mut AccountUpdate {
        round.accounts.entry(account_id).or_default()
    }
    let collateral_price = collateral_price(&changes.views);
    for row in &changes.accounts {
        let balance = account_balance(row, deployment.collateral_decimals, &collateral_price);
        let child = account(&mut round, row.account_id)
            .children
            .entry(0)
            .or_default();
        child.asset_positions.push(account_asset(&balance, 0));
    }
    for row in &changes.positions {
        let Some(view) = changes.views.get(&row.market) else {
            continue;
        };
        let child = Child::new(row, view, 0);
        let entry = account(&mut round, row.account_id)
            .children
            .entry(child.subaccount_number)
            .or_default();
        entry.asset_positions.push(child.asset);
        entry.perpetual_positions.extend(child.position);
    }
    for row in &changes.orders {
        let Some(ticker) = deployment.ticker(&row.market) else {
            continue;
        };
        let order = order_object(row, ticker, 0);
        let entry = account(&mut round, row.account_id)
            .children
            .entry(order.subaccount_number)
            .or_default();
        entry.orders.push(order);
    }
    for row in changes
        .fills
        .iter()
        .filter(|f| watched.contains(&f.account_id))
    {
        let Some(ticker) = deployment.ticker(&row.market) else {
            continue;
        };
        let entry = account(&mut round, row.account_id)
            .children
            .entry(child_number(0, row.market_index))
            .or_default();
        entry
            .fills
            .push((row.checkpoint, fill_object(row, ticker, 0)));
    }
    for row in changes.transfers {
        account(&mut round, row.account_id).transfers.push(row);
    }

    Ok((round, update))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(ticker: &str, price: &str, open_interest: &str) -> PerpetualMarket {
        PerpetualMarket {
            clob_pair_id: "3".to_owned(),
            ticker: ticker.to_owned(),
            status: "ACTIVE",
            oracle_price: price.to_owned(),
            price_change_24h: "0".to_owned(),
            volume_24h: "0".to_owned(),
            trades_24h: 0,
            next_funding_rate: "0".to_owned(),
            initial_margin_fraction: "0.1".to_owned(),
            maintenance_margin_fraction: "0.05".to_owned(),
            open_interest: open_interest.to_owned(),
            atomic_resolution: -9,
            quantum_conversion_exponent: -9,
            tick_size: "1".to_owned(),
            step_size: "0.001".to_owned(),
            step_base_quantums: 1_000_000,
            subticks_per_tick: 1_000_000_000,
            market_type: "ISOLATED",
            open_interest_lower_cap: "0".to_owned(),
            open_interest_upper_cap: "0".to_owned(),
            base_open_interest: open_interest.to_owned(),
            default_funding_rate_1h: "0".to_owned(),
        }
    }

    fn update(previous: &Markets, current: &Markets) -> Option<Value> {
        let watermark = Watermark {
            checkpoint: 42,
            timestamp_ms: 0,
        };
        markets_update(previous, current, watermark)
            .unwrap()
            .map(|raw| serde_json::from_str(raw.get()).unwrap())
    }

    #[test]
    fn market_updates_carry_only_what_changed() {
        let before: Markets = [("BTC-USD".to_owned(), market("BTC-USD", "100", "5"))].into();
        assert_eq!(update(&before, &before), None);

        let mut after = before.clone();
        after.insert("BTC-USD".to_owned(), market("BTC-USD", "101", "6"));
        assert_eq!(
            update(&before, &after),
            Some(json!({
                "trading": {"BTC-USD": {"openInterest": "6", "baseOpenInterest": "6"}},
                "oraclePrices": {"BTC-USD": {
                    "oraclePrice": "101",
                    "effectiveAt": "1970-01-01T00:00:00.000Z",
                    "effectiveAtHeight": "42",
                    "marketId": 3,
                }},
            }))
        );

        // A price move alone is an oracle price update.
        let mut repriced = before.clone();
        repriced.insert("BTC-USD".to_owned(), market("BTC-USD", "99", "5"));
        let sent = update(&before, &repriced).unwrap();
        assert!(sent.get("trading").is_none());
        assert_eq!(sent["oraclePrices"]["BTC-USD"]["oraclePrice"], "99");
    }

    #[test]
    fn a_new_market_is_sent_whole() {
        let before = Markets::new();
        let after: Markets = [("ETH-USD".to_owned(), market("ETH-USD", "10", "1"))].into();
        let sent = update(&before, &after).unwrap();
        assert_eq!(sent["trading"]["ETH-USD"]["ticker"], "ETH-USD");
        assert_eq!(sent["trading"]["ETH-USD"]["oraclePrice"], "10");
        assert_eq!(sent["trading"]["ETH-USD"]["priceChange24H"], "0");
        assert!(sent.get("oraclePrices").is_none());
    }

    #[test]
    fn candles_start_on_their_resolution() {
        assert_eq!(candle_start(119_999, 60_000), 60_000);
        assert_eq!(candle_start(120_000, 60_000), 120_000);
        assert_eq!(candle_start(86_400_001, 86_400_000), 86_400_000);
    }
}
