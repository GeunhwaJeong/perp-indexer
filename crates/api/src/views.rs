// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Maps the indexer's rows onto the objects of the dYdX API.
//!
//! The two models differ in how an account is laid out. dYdX gives every isolated position its
//! own child subaccount holding a quote balance; the engine gives an account one balance and
//! lets it allocate collateral to each market it trades. The mapping keeps the front end's
//! arithmetic true: the parent subaccount holds the account's unallocated balance, and market
//! number `i` becomes child subaccount `parent + 128 * (i + 1)` whose quote balance is the
//! collateral allocated to the market minus what the position cost, so that balance plus
//! position value equals the engine's margin.

use std::collections::{BTreeMap, HashMap};

use bigdecimal::{BigDecimal, Signed, ToPrimitive, Zero};

use crate::db::{
    AccountRow, CandleRow, FillRow, FundingPaymentRow, FundingUpdateRow, MarketRow, MarketStatsRow,
    OrderRow, PositionRow, TransferRow,
};
use crate::decimal::{plain, pow10, ratio};
use crate::engine::{self, Margin, Pricing, Valuation, settled_funding_rate_1h};
use crate::model::{
    AssetPosition, Candle, Fill, FundingPayment, HistoricalFunding, Order, ParentSubaccount,
    PerpetualMarket, PerpetualPosition, Subaccount, Trade, TradeHistory, Transfer, TransferParty,
};
use crate::time::iso;

/// dYdX numbers the children of parent subaccount `p` as `p + 128 * k`.
const SUBACCOUNTS_PER_PARENT: i64 = 128;

/// The collateral is presented as the quote asset the front end knows.
const QUOTE_SYMBOL: &str = "USDC";
const QUOTE_ASSET_ID: &str = "0";

/// Candle resolutions as the API names them, with their width in milliseconds.
pub const RESOLUTIONS: [(&str, i64); 7] = [
    ("1MIN", 60_000),
    ("5MINS", 300_000),
    ("15MINS", 900_000),
    ("30MINS", 1_800_000),
    ("1HOUR", 3_600_000),
    ("4HOURS", 14_400_000),
    ("1DAY", 86_400_000),
];

pub fn resolution_ms(name: &str) -> Option<i64> {
    RESOLUTIONS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, ms)| *ms)
}

pub fn resolution_name(ms: i64) -> Option<&'static str> {
    RESOLUTIONS.iter().find(|(_, m)| *m == ms).map(|(n, _)| *n)
}

/// The child subaccount that stands for an account's position in market number `market_index`.
pub fn child_number(parent: i64, market_index: i64) -> i64 {
    parent + SUBACCOUNTS_PER_PARENT * (market_index + 1)
}

fn side(is_ask: bool) -> &'static str {
    if is_ask { "SELL" } else { "BUY" }
}

fn position_side(base: &BigDecimal) -> &'static str {
    if base.is_negative() { "SHORT" } else { "LONG" }
}

fn height(checkpoint: i64) -> String {
    checkpoint.to_string()
}

/// A market with the prices derived from its state at some time.
#[derive(Clone, Debug)]
pub struct MarketView {
    pub ticker: String,
    pub index: i64,
    pub row: MarketRow,
    pub pricing: Pricing,
    pub valuation: Valuation,
}

impl MarketView {
    /// `None` for a market that cannot be served yet: it has no number, or no price for its
    /// base asset has been seen.
    pub fn new(row: MarketRow, ticker: &str, now_ms: i64) -> Option<Self> {
        let index = row.market_index?;
        let index_price = row
            .oracle_price
            .clone()
            .or_else(|| row.event_index_price.clone())?;
        let pricing = Pricing {
            index_twap_price: row
                .oracle_twap_price
                .clone()
                .unwrap_or_else(|| index_price.clone()),
            index_price,
            premium_twap: row.premium_twap.clone(),
            spread_twap: row.spread_twap.clone(),
            best_bid: row.best_bid_price.clone(),
            best_ask: row.best_ask_price.clone(),
            funding_last_upd_ms: row.funding_last_upd_ms,
            funding_frequency_ms: row.funding_frequency_ms,
            funding_period_ms: row.funding_period_ms,
        };
        // A settled market values every position at its settlement price.
        let mark_price = match (&row.settlement_base_price, row.settlement_enabled) {
            (Some(price), true) => price.clone(),
            _ => pricing.mark_price(now_ms),
        };
        let valuation = Valuation {
            mark_price,
            collateral_price: row
                .collateral_price
                .clone()
                .unwrap_or_else(|| BigDecimal::from(1)),
            collateral_haircut: row.collateral_haircut.clone(),
            cum_funding_rate_long: row.cum_funding_rate_long.clone(),
            cum_funding_rate_short: row.cum_funding_rate_short.clone(),
            margin_ratio_initial: row.margin_ratio_initial.clone(),
        };
        Some(Self {
            ticker: ticker.to_owned(),
            index,
            row,
            pricing,
            valuation,
        })
    }

    fn status(&self) -> &'static str {
        if self.row.closed || self.row.settlement_enabled {
            "FINAL_SETTLEMENT"
        } else {
            match self.row.paused {
                0 => "ACTIVE",
                2 => "CANCEL_ONLY",
                _ => "PAUSED",
            }
        }
    }

    /// The market as the API lists it.
    ///
    /// `oraclePrice` is the engine's mark price: it is the price the front end values positions
    /// and margin at, and the engine does that at the mark.
    pub fn object(&self, stats: Option<&MarketStatsRow>) -> PerpetualMarket {
        let price_change = stats
            .and_then(|s| Some(s.last_price.as_ref()? - s.reference_price.as_ref()?))
            .unwrap_or_default();
        // Sizes and prices are integers with 9 decimals on chain.
        let raw = |value: &BigDecimal| (value * pow10(9)).to_i64().unwrap_or(i64::MAX);
        let open_interest = plain(&self.row.open_interest);
        PerpetualMarket {
            clob_pair_id: self.index.to_string(),
            ticker: self.ticker.clone(),
            status: self.status(),
            oracle_price: plain(&self.valuation.mark_price),
            price_change_24h: plain(&price_change),
            volume_24h: stats
                .map(|s| plain(&s.volume))
                .unwrap_or_else(|| "0".into()),
            trades_24h: stats.map(|s| s.trades).unwrap_or_default(),
            next_funding_rate: plain(&self.pricing.funding_rate_1h()),
            initial_margin_fraction: plain(&self.row.margin_ratio_initial),
            maintenance_margin_fraction: plain(&self.row.margin_ratio_maintenance),
            open_interest: open_interest.clone(),
            atomic_resolution: -9,
            quantum_conversion_exponent: -9,
            tick_size: plain(&self.row.tick_size),
            step_size: plain(&self.row.lot_size),
            step_base_quantums: raw(&self.row.lot_size),
            subticks_per_tick: raw(&self.row.tick_size),
            market_type: "ISOLATED",
            // Equal caps switch off dYdX's scaling of the margin fraction with open interest,
            // which the engine does not have.
            open_interest_lower_cap: "0".to_owned(),
            open_interest_upper_cap: "0".to_owned(),
            base_open_interest: open_interest,
            default_funding_rate_1h: "0".to_owned(),
        }
    }
}

/// The listed markets by clearing house ID.
pub type MarketViews = HashMap<String, MarketView>;

pub fn order_id(id: &BigDecimal) -> String {
    plain(id)
}

pub fn order_object(row: &OrderRow, ticker: &str, parent: i64) -> Order {
    let subaccount_number = child_number(parent, row.market_index);
    let (status, removal_reason) = match row.status.as_str() {
        "open" => ("OPEN", None),
        "filled" => ("FILLED", None),
        _ => (
            "CANCELED",
            Some(match row.cancel_reason {
                Some(1) | Some(7) => "UNDERCOLLATERALIZED",
                Some(2) => "DELEVERAGED",
                Some(3) => "FINAL_SETTLEMENT",
                Some(4) => "REDUCE_ONLY_RESIZE",
                Some(5) => "EXPIRED",
                Some(6) => "SELF_TRADE",
                _ => "USER_CANCELED",
            }),
        ),
    };
    Order {
        // The engine's order ID, which is what a cancelation has to name.
        id: order_id(&row.order_id),
        subaccount_id: format!("{}/{subaccount_number}", row.account_id),
        client_id: row
            .client_order_id
            .as_ref()
            .map(plain)
            .unwrap_or_else(|| "0".to_owned()),
        clob_pair_id: row.market_index.to_string(),
        side: side(row.is_ask),
        size: plain(&row.size),
        total_filled: plain(&row.filled),
        price: plain(&row.price),
        // Only orders that rest on the book are indexed, and those are limit orders.
        order_type: "LIMIT",
        reduce_only: row.reduce_only,
        // dYdX's flag for orders that live on chain until filled, canceled or expired.
        order_flags: "64",
        good_til_block_time: row
            .expiration_timestamp_ms
            .as_ref()
            .and_then(|ms| ms.to_i64())
            .map(iso),
        created_at_height: height(row.created_checkpoint),
        client_metadata: "0",
        time_in_force: "GTT",
        status,
        post_only: false,
        ticker: ticker.to_owned(),
        removal_reason,
        updated_at: iso(row.updated_at_ms),
        updated_at_height: height(row.updated_checkpoint),
        subaccount_number,
    }
}

pub fn fill_id(row: &FillRow) -> String {
    format!(
        "{}-{}-{}-{}",
        row.checkpoint, row.tx_index, row.event_index, row.fill_index
    )
}

/// The price a fill executed at. Settlements close at the market's settlement price.
fn fill_price(row: &FillRow) -> BigDecimal {
    row.price
        .clone()
        .or_else(|| row.settlement_base_price.clone())
        .unwrap_or_default()
}

pub fn fill_object(row: &FillRow, ticker: &str, parent: i64) -> Fill {
    let before = row.position_base_before.as_ref();
    Fill {
        id: fill_id(row),
        side: side(row.is_ask),
        liquidity: if row.liquidity == "maker" {
            "MAKER"
        } else {
            "TAKER"
        },
        fill_type: match (row.kind.as_str(), row.fill_index) {
            ("liquidated", _) => "LIQUIDATED",
            ("liquidation", _) => "LIQUIDATION",
            // The account in bad debt comes first, the account it was closed against second.
            ("adl", 0) | ("settlement", _) => "DELEVERAGED",
            ("adl", _) => "OFFSETTING",
            _ => "LIMIT",
        },
        market: ticker.to_owned(),
        market_type: "PERPETUAL",
        price: plain(&fill_price(row)),
        size: plain(&row.size),
        fee: plain(&row.fee),
        affiliate_rev_share: "0",
        created_at: iso(row.timestamp_ms),
        created_at_height: height(row.checkpoint),
        order_id: row.order_id.as_ref().map(order_id),
        client_metadata: None,
        subaccount_number: child_number(parent, row.market_index),
        builder_fee: (!row.integrator_fee.is_zero()).then(|| plain(&row.integrator_fee)),
        position_size_before: before.map(|base| plain(&base.abs())),
        entry_price_before: row.entry_price_before.as_ref().map(plain),
        position_side_before: before.filter(|base| !base.is_zero()).map(position_side),
    }
}

/// A trade on the public tape, from its maker fill. The side is the taker's.
pub fn trade_object(row: &FillRow) -> Trade {
    Trade {
        id: fill_id(row),
        side: side(!row.is_ask),
        size: plain(&row.size),
        price: plain(&fill_price(row)),
        trade_type: "LIMIT",
        created_at: iso(row.timestamp_ms),
        created_at_height: height(row.checkpoint),
    }
}

/// A fill as an entry of the account's trade history: what it did to the position it found.
pub fn trade_history_object(row: &FillRow, ticker: &str, parent: i64) -> TradeHistory {
    let zero = BigDecimal::zero();
    let before = row.position_base_before.as_ref().unwrap_or(&zero);
    let held = before.abs();
    let price = fill_price(row);
    let increases = before.is_zero() || before.is_negative() == row.is_ask;
    let liquidated = row.kind == "liquidated";

    let action = if before.is_zero() {
        "OPEN"
    } else if increases {
        "EXTEND"
    } else {
        match (liquidated, row.size < held) {
            (false, true) => "PARTIAL_CLOSE",
            (false, false) => "CLOSE",
            (true, true) => "LIQUIDATION_PARTIAL_CLOSE",
            (true, false) => "LIQUIDATION_CLOSE",
        }
    };

    let net_fee = &row.fee + &row.integrator_fee;
    let (net_realized_pnl, net_realized_pnl_percent) = if increases {
        (None, None)
    } else {
        let net = &row.pnl - &net_fee;
        let cost = row
            .entry_price_before
            .as_ref()
            .map(|entry| entry * (&row.size).min(&held));
        let percent = cost
            .filter(|cost| !cost.is_zero())
            .map(|cost| plain(&(ratio(&net, &cost) * BigDecimal::from(100))));
        (Some(plain(&net)), percent)
    };

    TradeHistory {
        id: fill_id(row),
        market_id: ticker.to_owned(),
        order_id: row.order_id.as_ref().map(order_id),
        side: side(row.is_ask),
        // The side of the position the fill added to or took from.
        position_side: Some(if increases {
            if row.is_ask { "SHORT" } else { "LONG" }
        } else {
            position_side(before)
        }),
        entry_price: Some(plain(row.entry_price_before.as_ref().unwrap_or(&price))),
        execution_price: plain(&price),
        value: plain(&(&row.size * &price)),
        prev_size: plain(&held),
        additional_size: plain(&row.size),
        net_fee: plain(&net_fee),
        time: iso(row.timestamp_ms),
        action,
        margin_mode: "ISOLATED",
        order_type: if row.liquidity == "maker" {
            "LIMIT"
        } else {
            "MARKET"
        },
        net_realized_pnl,
        net_realized_pnl_percent,
        subaccount_number: child_number(parent, row.market_index),
    }
}

pub fn candle_object(row: &CandleRow, ticker: &str, with_id: bool) -> Candle {
    let resolution = resolution_name(row.resolution_ms).unwrap_or("1MIN");
    Candle {
        id: with_id.then(|| format!("{ticker}-{resolution}-{}", row.start_ms)),
        started_at: iso(row.start_ms),
        ticker: ticker.to_owned(),
        resolution,
        low: plain(&row.low),
        high: plain(&row.high),
        open: plain(&row.open),
        close: plain(&row.close),
        base_token_volume: plain(&row.base_volume),
        usd_volume: plain(&row.quote_volume),
        trades: row.trades,
        starting_open_interest: "0",
    }
}

/// A deposit or withdrawal. The other party is the wallet that holds the account.
pub fn transfer_object(
    row: &TransferRow,
    address: &str,
    parent: i64,
    collateral_decimals: u32,
    with_id: bool,
) -> Transfer {
    let account = TransferParty {
        address: address.to_owned(),
        subaccount_number: Some(parent),
    };
    let wallet = TransferParty {
        address: address.to_owned(),
        subaccount_number: None,
    };
    let deposit = row.kind == "deposit";
    let (sender, recipient) = if deposit {
        (wallet, account)
    } else {
        (account, wallet)
    };
    Transfer {
        id: with_id.then(|| format!("{}-{}-{}", row.checkpoint, row.tx_index, row.event_index)),
        sender,
        recipient,
        size: plain(&(&row.amount / pow10(collateral_decimals))),
        created_at: iso(row.timestamp_ms),
        created_at_height: height(row.checkpoint),
        symbol: QUOTE_SYMBOL,
        transfer_type: if deposit { "DEPOSIT" } else { "WITHDRAWAL" },
        transaction_hash: row.tx_digest.clone(),
    }
}

pub fn funding_payment_object(
    row: &FundingPaymentRow,
    ticker: &str,
    parent: i64,
) -> FundingPayment {
    let zero = BigDecimal::zero();
    let base = row.position_base.as_ref().unwrap_or(&zero);
    let price = row.index_price.as_ref().unwrap_or(&zero);
    // The payment as a fraction of the position's value; positive when the position paid.
    let rate = ratio(&-&row.collateral_change_usd, &(base.abs() * price));
    FundingPayment {
        created_at: iso(row.timestamp_ms),
        created_at_height: height(row.checkpoint),
        perpetual_id: row.market_index.to_string(),
        ticker: ticker.to_owned(),
        oracle_price: plain(price),
        size: plain(&base.abs()),
        side: position_side(base),
        rate: plain(&rate),
        payment: plain(&row.collateral_change_usd),
        subaccount_number: child_number(parent, row.market_index).to_string(),
        funding_index: plain(if base.is_negative() {
            &row.cum_funding_rate_short
        } else {
            &row.cum_funding_rate_long
        }),
    }
}

pub fn historical_funding_object(
    row: &FundingUpdateRow,
    ticker: &str,
    funding_frequency_ms: i64,
) -> HistoricalFunding {
    let zero = BigDecimal::zero();
    let price = row.index_price.as_ref().unwrap_or(&zero);
    // The first update moved the rate from zero, one interval after the market opened.
    let previous = row.previous_cum_funding_rate_long.as_ref().unwrap_or(&zero);
    let previous_ms = row
        .previous_funding_last_upd_ms
        .unwrap_or(row.funding_last_upd_ms - funding_frequency_ms);
    let rate = settled_funding_rate_1h(
        &(&row.cum_funding_rate_long - previous),
        price,
        previous_ms,
        row.funding_last_upd_ms,
        funding_frequency_ms,
    );
    HistoricalFunding {
        ticker: ticker.to_owned(),
        rate: plain(&rate),
        price: plain(price),
        effective_at: iso(row.funding_last_upd_ms),
        effective_at_height: height(row.checkpoint),
    }
}

fn quote_position(balance: &BigDecimal, subaccount_number: i64) -> AssetPosition {
    AssetPosition {
        symbol: QUOTE_SYMBOL,
        side: position_side(balance),
        size: plain(&balance.abs()),
        asset_id: QUOTE_ASSET_ID,
        subaccount_number,
    }
}

/// An account's position in one market, as a child subaccount.
#[derive(Clone, Debug, PartialEq)]
pub struct Child {
    pub subaccount_number: i64,
    /// The quote balance that, added to the position's value at the mark price, gives the
    /// engine's margin: collateral value minus the position's entry notional.
    pub balance: BigDecimal,
    pub asset: AssetPosition,
    /// The open position, or the position as it was closed; `None` if there never was one.
    pub position: Option<PerpetualPosition>,
    pub margin: Margin,
    pub updated_checkpoint: i64,
}

impl Child {
    pub fn new(row: &PositionRow, market: &MarketView, parent: i64) -> Self {
        let subaccount_number = child_number(parent, market.index);
        let position = engine::Position {
            collateral: row.collateral.clone(),
            base: row.base.clone(),
            quote_notional: row.quote_notional.clone(),
            cum_funding_rate_long: row.cum_funding_rate_long.clone(),
            cum_funding_rate_short: row.cum_funding_rate_short.clone(),
            asks_quantity: row.asks_quantity.clone(),
            bids_quantity: row.bids_quantity.clone(),
            pending_orders: row.pending_orders,
            initial_margin_ratio: row.initial_margin_ratio.clone(),
        };
        let margin = position.margin(&market.valuation);
        let balance = &margin.collateral_value - &row.quote_notional;

        let open = !row.base.is_zero();
        let object = (open || row.opened_checkpoint.is_some()).then(|| PerpetualPosition {
            market: market.ticker.clone(),
            status: if open { "OPEN" } else { "CLOSED" },
            side: position_side(&row.base),
            size: plain(&row.base),
            max_size: plain(&row.max_size),
            entry_price: plain(&ratio(&row.quote_notional, &row.base).abs()),
            realized_pnl: plain(&row.realized_pnl),
            created_at: iso(row.opened_at_ms.unwrap_or(row.created_at_ms)),
            created_at_height: height(row.opened_checkpoint.unwrap_or(row.created_checkpoint)),
            sum_open: plain(&row.sum_open),
            sum_close: plain(&row.sum_close),
            net_funding: plain(&(&row.net_funding + &margin.unsettled_funding)),
            unrealized_pnl: plain(&margin.unrealized_pnl),
            closed_at: (!open).then(|| iso(row.updated_at_ms)),
            exit_price: (!row.sum_close.is_zero())
                .then(|| plain(&ratio(&row.close_quote, &row.sum_close))),
            subaccount_number,
        });

        Self {
            subaccount_number,
            asset: quote_position(&balance, subaccount_number),
            balance,
            position: object,
            margin,
            updated_checkpoint: row.updated_checkpoint,
        }
    }

    pub fn is_open(&self) -> bool {
        self.position.as_ref().is_some_and(|p| p.status == "OPEN")
    }

    /// Whether the child holds anything: an open position, or collateral allocated to its
    /// market, as when only orders rest there.
    pub fn is_live(&self) -> bool {
        self.is_open() || !self.balance.is_zero()
    }

    /// The child as a subaccount of parent subaccount `parent`. A child is numbered as one of
    /// parent 0 until it is presented under the parent the request named.
    fn subaccount(&self, address: &str, height: &str, parent: i64) -> Subaccount {
        let subaccount_number = self.subaccount_number + parent;
        let mut open_perpetual_positions = BTreeMap::new();
        if let Some(position) = self.position.as_ref().filter(|p| p.status == "OPEN") {
            let position = PerpetualPosition {
                subaccount_number,
                ..position.clone()
            };
            open_perpetual_positions.insert(position.market.clone(), position);
        }
        let mut asset_positions = BTreeMap::new();
        if !self.balance.is_zero() {
            let asset = AssetPosition {
                subaccount_number,
                ..self.asset.clone()
            };
            asset_positions.insert(QUOTE_SYMBOL, asset);
        }
        Subaccount {
            address: address.to_owned(),
            subaccount_number,
            equity: plain((&self.margin.margin).max(&BigDecimal::zero())),
            free_collateral: plain(&self.margin.free),
            open_perpetual_positions,
            asset_positions,
            margin_enabled: true,
            updated_at_height: self.updated_checkpoint.to_string(),
            latest_processed_block_height: height.to_owned(),
        }
    }
}

/// What an account's unallocated balance is worth in USD.
pub fn account_balance(
    account: &AccountRow,
    collateral_decimals: u32,
    collateral_price: &BigDecimal,
) -> BigDecimal {
    (&account.collateral / pow10(collateral_decimals) * collateral_price).with_scale(18)
}

/// The quote position of the parent subaccount: the account's unallocated balance.
pub fn account_asset(balance: &BigDecimal, parent: i64) -> AssetPosition {
    quote_position(balance, parent)
}

/// The price of the collateral the listed markets trade in, from any of them.
pub fn collateral_price(markets: &MarketViews) -> BigDecimal {
    markets
        .values()
        .next()
        .map(|market| market.valuation.collateral_price.clone())
        .unwrap_or_else(|| BigDecimal::from(1))
}

/// An account as parent subaccount `parent`, with one child per market it holds anything in.
/// `children` are numbered as children of parent 0.
pub fn parent_subaccount(
    address: &str,
    parent: i64,
    balance: &BigDecimal,
    account: &AccountRow,
    children: &[Child],
    checkpoint: i64,
) -> ParentSubaccount {
    let height = height(checkpoint);
    let mut asset_positions = BTreeMap::new();
    if !balance.is_zero() {
        asset_positions.insert(QUOTE_SYMBOL, account_asset(balance, parent));
    }
    let own = Subaccount {
        address: address.to_owned(),
        subaccount_number: parent,
        equity: plain(balance),
        free_collateral: plain(balance),
        open_perpetual_positions: BTreeMap::new(),
        asset_positions,
        margin_enabled: true,
        updated_at_height: account.updated_checkpoint.to_string(),
        latest_processed_block_height: height.clone(),
    };

    let zero = BigDecimal::zero();
    let equity = children
        .iter()
        .filter(|child| child.is_live())
        .fold(balance.clone(), |sum, child| {
            sum + (&child.margin.margin).max(&zero)
        });
    let mut child_subaccounts = vec![own];
    child_subaccounts.extend(
        children
            .iter()
            .filter(|child| child.is_live())
            .map(|child| child.subaccount(address, &height, parent)),
    );
    ParentSubaccount {
        address: address.to_owned(),
        parent_subaccount_number: parent,
        equity: plain(&equity),
        free_collateral: plain(balance),
        child_subaccounts,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn market_row() -> MarketRow {
        MarketRow {
            market: "0xm".to_owned(),
            market_index: Some(2),
            paused: 0,
            closed: false,
            settlement_enabled: false,
            settlement_base_price: None,
            lot_size: dec("0.001000000"),
            tick_size: dec("1.000000000"),
            margin_ratio_initial: dec("0.1"),
            margin_ratio_maintenance: dec("0.05"),
            cum_funding_rate_long: dec("2"),
            cum_funding_rate_short: dec("2"),
            funding_last_upd_ms: 3_600_000,
            premium_twap: dec("0"),
            spread_twap: dec("0"),
            open_interest: dec("1.500000000000000000"),
            best_ask_price: None,
            best_bid_price: None,
            funding_frequency_ms: 3_600_000,
            funding_period_ms: 86_400_000,
            collateral_haircut: dec("0"),
            oracle_price: Some(dec("100")),
            oracle_twap_price: Some(dec("100")),
            oracle_updated_checkpoint: Some(5),
            collateral_price: Some(dec("1")),
            event_index_price: None,
        }
    }

    fn view() -> MarketView {
        MarketView::new(market_row(), "BTC-USD", 3_600_000).unwrap()
    }

    fn fill_row(is_ask: bool, size: &str, before: &str, kind: &str) -> FillRow {
        FillRow {
            checkpoint: 9,
            tx_index: 1,
            event_index: 2,
            fill_index: 0,
            timestamp_ms: 1_000,
            market: "0xm".to_owned(),
            market_index: 2,
            settlement_base_price: None,
            account_id: 7,
            is_ask,
            liquidity: "taker".to_owned(),
            kind: kind.to_owned(),
            price: Some(dec("110")),
            size: dec(size),
            fee: dec("0.5"),
            integrator_fee: dec("0"),
            pnl: dec("10"),
            order_id: None,
            client_order_id: None,
            position_base_before: Some(dec(before)),
            entry_price_before: (dec(before) != dec("0")).then(|| dec("100")),
        }
    }

    fn position_row(base: &str, quote: &str) -> PositionRow {
        PositionRow {
            market: "0xm".to_owned(),
            account_id: 7,
            collateral: dec("1000"),
            base: dec(base),
            quote_notional: dec(quote),
            cum_funding_rate_long: dec("1.5"),
            cum_funding_rate_short: dec("1.5"),
            asks_quantity: dec("0"),
            bids_quantity: dec("0"),
            pending_orders: 0,
            initial_margin_ratio: dec("0.1"),
            created_checkpoint: 3,
            created_at_ms: 3_000,
            updated_checkpoint: 8,
            updated_at_ms: 8_000,
            opened_checkpoint: Some(4),
            opened_at_ms: Some(4_000),
            max_size: dec("3"),
            sum_open: dec("3"),
            sum_close: dec("1"),
            close_quote: dec("120"),
            realized_pnl: dec("19"),
            net_funding: dec("-0.25"),
        }
    }

    #[test]
    fn a_market_is_listed_with_the_mark_price_and_its_grid() {
        let market = view().object(None);
        assert_eq!(market.clob_pair_id, "2");
        assert_eq!(market.status, "ACTIVE");
        assert_eq!(market.oracle_price, "100");
        assert_eq!(market.tick_size, "1");
        assert_eq!(market.step_size, "0.001");
        assert_eq!(market.step_base_quantums, 1_000_000);
        assert_eq!(market.subticks_per_tick, 1_000_000_000);
        assert_eq!(market.open_interest, "1.5");
        assert_eq!((market.volume_24h.as_str(), market.trades_24h), ("0", 0));

        let stats = MarketStatsRow {
            market: "0xm".to_owned(),
            volume: dec("1234.50"),
            trades: 7,
            last_price: Some(dec("101")),
            reference_price: Some(dec("98.5")),
        };
        let market = view().object(Some(&stats));
        assert_eq!(market.price_change_24h, "2.5");
        assert_eq!(
            (market.volume_24h.as_str(), market.trades_24h),
            ("1234.5", 7)
        );
    }

    #[test]
    fn market_status_follows_pausing_and_settlement() {
        let status = |edit: fn(&mut MarketRow)| {
            let mut row = market_row();
            edit(&mut row);
            MarketView::new(row, "BTC-USD", 0).unwrap().status()
        };
        assert_eq!(status(|row| row.paused = 1), "PAUSED");
        assert_eq!(status(|row| row.paused = 2), "CANCEL_ONLY");
        assert_eq!(status(|row| row.closed = true), "FINAL_SETTLEMENT");

        // A settled market is valued at its settlement price whatever the oracle says.
        let mut row = market_row();
        row.settlement_enabled = true;
        row.settlement_base_price = Some(dec("42"));
        let settled = MarketView::new(row, "BTC-USD", 0).unwrap();
        assert_eq!(settled.status(), "FINAL_SETTLEMENT");
        assert_eq!(settled.valuation.mark_price, dec("42"));
    }

    #[test]
    fn a_market_without_a_number_or_a_price_is_not_served() {
        let mut row = market_row();
        row.market_index = None;
        assert!(MarketView::new(row, "BTC-USD", 0).is_none());

        let mut row = market_row();
        row.oracle_price = None;
        assert!(MarketView::new(row.clone(), "BTC-USD", 0).is_none());
        // The index price from the engine's own events will do until the oracle is seen.
        row.event_index_price = Some(dec("99"));
        assert_eq!(
            MarketView::new(row, "BTC-USD", 0)
                .unwrap()
                .pricing
                .index_price,
            dec("99")
        );
    }

    #[test]
    fn children_are_numbered_by_market_under_their_parent() {
        assert_eq!(child_number(0, 0), 128);
        assert_eq!(child_number(0, 2), 384);
        assert_eq!(child_number(5, 2), 389);
        assert_eq!(resolution_ms("4HOURS"), Some(14_400_000));
        assert_eq!(resolution_name(60_000), Some("1MIN"));
        assert_eq!(resolution_ms("2MIN"), None);
    }

    #[test]
    fn an_order_keeps_the_engine_id_and_says_why_it_ended() {
        let mut row = OrderRow {
            market: "0xm".to_owned(),
            market_index: 2,
            order_id: dec("1844674407370955161600000000007"),
            account_id: 7,
            is_ask: true,
            price: dec("100.000000000"),
            size: dec("1.000000000"),
            filled: dec("0.250000000"),
            status: "open".to_owned(),
            cancel_reason: None,
            reduce_only: true,
            expiration_timestamp_ms: Some(dec("1000")),
            client_order_id: Some(dec("42")),
            created_checkpoint: 3,
            updated_checkpoint: 8,
            updated_at_ms: 8_000,
        };
        let order = order_object(&row, "BTC-USD", 1);
        assert_eq!(order.id, "1844674407370955161600000000007");
        assert_eq!((order.side, order.status), ("SELL", "OPEN"));
        assert_eq!(
            (order.size.as_str(), order.total_filled.as_str()),
            ("1", "0.25")
        );
        assert_eq!(order.client_id, "42");
        assert_eq!(order.subaccount_number, 385);
        assert_eq!(
            order.good_til_block_time.as_deref(),
            Some("1970-01-01T00:00:01.000Z")
        );
        assert_eq!(order.removal_reason, None);

        row.status = "canceled".to_owned();
        row.cancel_reason = Some(5);
        let order = order_object(&row, "BTC-USD", 0);
        assert_eq!(
            (order.status, order.removal_reason),
            ("CANCELED", Some("EXPIRED"))
        );
        row.status = "filled".to_owned();
        assert_eq!(order_object(&row, "BTC-USD", 0).status, "FILLED");
    }

    #[test]
    fn fills_map_to_the_kinds_the_front_end_knows() {
        let fill = fill_object(&fill_row(false, "1", "-2", "trade"), "BTC-USD", 0);
        assert_eq!(fill.id, "9-1-2-0");
        assert_eq!(
            (fill.side, fill.liquidity, fill.fill_type),
            ("BUY", "TAKER", "LIMIT")
        );
        assert_eq!(fill.subaccount_number, 384);
        assert_eq!(fill.position_size_before.as_deref(), Some("2"));
        assert_eq!(fill.position_side_before, Some("SHORT"));
        assert_eq!(fill.entry_price_before.as_deref(), Some("100"));

        let kind = |kind: &str, fill_index: i64| {
            let mut row = fill_row(true, "1", "0", kind);
            row.fill_index = fill_index;
            fill_object(&row, "BTC-USD", 0).fill_type
        };
        assert_eq!(kind("liquidated", 0), "LIQUIDATED");
        assert_eq!(kind("liquidation", 0), "LIQUIDATION");
        assert_eq!(kind("adl", 0), "DELEVERAGED");
        assert_eq!(kind("adl", 1), "OFFSETTING");

        // A settlement has no price of its own.
        let mut row = fill_row(true, "1", "1", "settlement");
        row.price = None;
        row.settlement_base_price = Some(dec("95"));
        let fill = fill_object(&row, "BTC-USD", 0);
        assert_eq!((fill.fill_type, fill.price.as_str()), ("DELEVERAGED", "95"));

        // The tape shows the taker's side of a maker fill.
        let mut maker = fill_row(true, "1", "0", "trade");
        maker.liquidity = "maker".to_owned();
        assert_eq!(trade_object(&maker).side, "BUY");
    }

    #[test]
    fn trade_history_says_what_each_fill_did_to_the_position() {
        let action = |is_ask, size, before, kind| {
            let entry = trade_history_object(&fill_row(is_ask, size, before, kind), "BTC-USD", 0);
            (entry.action, entry.position_side, entry.net_realized_pnl)
        };
        assert_eq!(
            action(false, "1", "0", "trade"),
            ("OPEN", Some("LONG"), None)
        );
        assert_eq!(
            action(true, "1", "0", "trade"),
            ("OPEN", Some("SHORT"), None)
        );
        assert_eq!(
            action(false, "1", "2", "trade"),
            ("EXTEND", Some("LONG"), None)
        );
        assert_eq!(
            action(true, "1", "2", "trade"),
            ("PARTIAL_CLOSE", Some("LONG"), Some("9.5".to_owned()))
        );
        assert_eq!(action(true, "2", "2", "trade").0, "CLOSE");
        // A fill through zero closes what was held.
        assert_eq!(action(false, "5", "-2", "trade").0, "CLOSE");
        assert_eq!(
            action(true, "1", "2", "liquidated").0,
            "LIQUIDATION_PARTIAL_CLOSE"
        );
        assert_eq!(action(true, "2", "2", "liquidated").0, "LIQUIDATION_CLOSE");

        let entry = trade_history_object(&fill_row(true, "1", "2", "trade"), "BTC-USD", 0);
        assert_eq!(entry.entry_price.as_deref(), Some("100"));
        assert_eq!(entry.execution_price, "110");
        assert_eq!(
            (entry.prev_size.as_str(), entry.additional_size.as_str()),
            ("2", "1")
        );
        // 9.5 made on the 100 the closed unit cost.
        assert_eq!(entry.net_realized_pnl_percent.as_deref(), Some("9.5"));
        assert_eq!(entry.order_type, "MARKET");
    }

    #[test]
    fn a_child_balance_plus_the_position_value_is_the_margin() {
        let market = view();
        let child = Child::new(&position_row("2", "210"), &market, 0);
        // Funding accrued: the long rate rose from 1.5 to 2 on 2 units.
        assert_eq!(child.margin.unsettled_funding, dec("-1"));
        // Collateral 1000 less funding 1, less the 210 the position cost.
        assert_eq!(child.balance, dec("789"));
        assert_eq!(
            (child.asset.side, child.asset.size.as_str()),
            ("LONG", "789")
        );
        // 789 + 2 * 100 at the mark price.
        assert_eq!(child.margin.margin, dec("989"));

        let position = child.position.clone().unwrap();
        assert_eq!(
            (position.status, position.side, position.size.as_str()),
            ("OPEN", "LONG", "2")
        );
        assert_eq!(position.entry_price, "105");
        assert_eq!(position.unrealized_pnl, "-10");
        assert_eq!(position.net_funding, "-1.25");
        assert_eq!(position.exit_price.as_deref(), Some("120"));
        assert_eq!(position.created_at_height, "4");
        assert_eq!(position.subaccount_number, 384);
        assert!(child.is_open() && child.is_live());

        let short = Child::new(&position_row("-2", "-210"), &market, 0);
        assert_eq!(short.position.as_ref().unwrap().side, "SHORT");
        assert_eq!(short.position.as_ref().unwrap().size, "-2");
        // 1000 + 1 of funding received + 210 sold for.
        assert_eq!(
            (short.asset.side, short.asset.size.as_str()),
            ("LONG", "1211")
        );
    }

    #[test]
    fn a_closed_position_is_reported_closed_and_then_forgotten() {
        let market = view();
        let closed = Child::new(&position_row("0", "0"), &market, 0);
        let position = closed.position.clone().unwrap();
        assert_eq!((position.status, position.size.as_str()), ("CLOSED", "0"));
        assert_eq!(
            position.closed_at.as_deref(),
            Some("1970-01-01T00:00:08.000Z")
        );
        // Its collateral is still allocated to the market.
        assert!(!closed.is_open() && closed.is_live());

        let mut never = position_row("0", "0");
        never.opened_checkpoint = None;
        never.collateral = dec("0");
        let never = Child::new(&never, &market, 0);
        assert!(never.position.is_none() && !never.is_live());
    }

    #[test]
    fn an_account_is_a_parent_with_a_child_per_market() {
        let market = view();
        let account = AccountRow {
            account_id: 7,
            collateral: dec("2500000"),
            updated_checkpoint: 6,
        };
        let balance = account_balance(&account, 6, &dec("1"));
        assert_eq!(balance, dec("2.5"));
        let children = [
            Child::new(&position_row("2", "210"), &market, 0),
            Child::new(
                &{
                    let mut row = position_row("0", "0");
                    row.opened_checkpoint = None;
                    row.collateral = dec("0");
                    row
                },
                &market,
                0,
            ),
        ];

        let parent = parent_subaccount("0xa", 3, &balance, &account, &children, 9);
        assert_eq!(parent.parent_subaccount_number, 3);
        // The account's own balance, and the one market it holds something in.
        let numbers: Vec<_> = parent
            .child_subaccounts
            .iter()
            .map(|c| c.subaccount_number)
            .collect();
        assert_eq!(numbers, [3, 387]);
        assert_eq!(
            parent.child_subaccounts[0].asset_positions["USDC"].size,
            "2.5"
        );
        let child = &parent.child_subaccounts[1];
        assert_eq!(
            child.open_perpetual_positions["BTC-USD"].subaccount_number,
            387
        );
        assert_eq!(child.asset_positions["USDC"].subaccount_number, 387);
        assert_eq!(child.equity, "989");
        // 989 of margin with the loss counted, less 2 * 100 * 0.1 required.
        assert_eq!(child.free_collateral, "969");
        assert_eq!(
            (parent.equity.as_str(), parent.free_collateral.as_str()),
            ("991.5", "2.5")
        );
    }

    #[test]
    fn transfers_run_between_the_wallet_and_the_parent_subaccount() {
        let mut row = TransferRow {
            checkpoint: 9,
            tx_index: 1,
            event_index: 0,
            tx_digest: "digest".to_owned(),
            timestamp_ms: 1_000,
            account_id: 7,
            kind: "deposit".to_owned(),
            amount: dec("1500000"),
        };
        let deposit = transfer_object(&row, "0xa", 2, 6, true);
        assert_eq!(
            (deposit.transfer_type, deposit.size.as_str()),
            ("DEPOSIT", "1.5")
        );
        assert_eq!(deposit.sender.subaccount_number, None);
        assert_eq!(deposit.recipient.subaccount_number, Some(2));
        assert_eq!(deposit.id.as_deref(), Some("9-1-0"));

        row.kind = "withdraw".to_owned();
        let withdrawal = transfer_object(&row, "0xa", 2, 6, false);
        assert_eq!(withdrawal.transfer_type, "WITHDRAWAL");
        assert_eq!(withdrawal.sender.subaccount_number, Some(2));
        assert_eq!(withdrawal.id, None);
    }

    #[test]
    fn funding_is_reported_as_a_rate_of_the_position_value() {
        let payment = FundingPaymentRow {
            checkpoint: 9,
            timestamp_ms: 1_000,
            market: "0xm".to_owned(),
            market_index: 2,
            collateral_change_usd: dec("-0.4"),
            cum_funding_rate_long: dec("2"),
            cum_funding_rate_short: dec("1"),
            position_base: Some(dec("-2")),
            index_price: Some(dec("100")),
        };
        let object = funding_payment_object(&payment, "BTC-USD", 0);
        // 0.4 paid on 2 units worth 100 each.
        assert_eq!(
            (object.rate.as_str(), object.payment.as_str()),
            ("0.002", "-0.4")
        );
        assert_eq!((object.side, object.size.as_str()), ("SHORT", "2"));
        assert_eq!(object.funding_index, "1");
        assert_eq!(object.subaccount_number, "384");

        let update = FundingUpdateRow {
            checkpoint: 9,
            funding_last_upd_ms: 7_200_000,
            cum_funding_rate_long: dec("2.1"),
            index_price: Some(dec("100")),
            previous_cum_funding_rate_long: Some(dec("2")),
            previous_funding_last_upd_ms: Some(3_600_000),
        };
        let object = historical_funding_object(&update, "BTC-USD", 3_600_000);
        assert_eq!(
            (object.rate.as_str(), object.price.as_str()),
            ("0.001", "100")
        );
        assert_eq!(object.effective_at, "1970-01-01T02:00:00.000Z");

        // The first update of a market moved the rate from zero.
        let first = FundingUpdateRow {
            previous_cum_funding_rate_long: None,
            previous_funding_last_upd_ms: None,
            cum_funding_rate_long: dec("0.2"),
            ..update
        };
        assert_eq!(
            historical_funding_object(&first, "BTC-USD", 3_600_000).rate,
            "0.002"
        );
    }
}
