// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The engine's pricing and margin formulas, restated over decimals.
//!
//! The indexer copies state from the chain rather than computing it, but three numbers exist
//! only as view functions of the engine: the mark price, the funding a position has accrued
//! since it was last settled, and what its collateral is worth. They are reproduced here from
//! `perpetuals::market` and `position::position`. Results are truncated to `ifixed` precision
//! at the end rather than after every operation, so they can differ from the engine's in the
//! last of 18 decimals.
//!
//! Both the API (what an account is worth now) and the indexer (what it was worth at each
//! tick of its history) value accounts with these, so the two agree.

use bigdecimal::{BigDecimal, One, Signed, Zero};

use crate::decimal::{fixed, ratio};

pub mod decimal;

const HOUR_MS: i64 = 3_600_000;

/// What the mark price of a market depends on.
#[derive(Clone, Debug)]
pub struct Pricing {
    pub index_price: BigDecimal,
    pub index_twap_price: BigDecimal,
    pub premium_twap: BigDecimal,
    pub spread_twap: BigDecimal,
    pub best_bid: Option<BigDecimal>,
    pub best_ask: Option<BigDecimal>,
    pub funding_last_upd_ms: i64,
    pub funding_frequency_ms: i64,
    pub funding_period_ms: i64,
}

impl Pricing {
    /// The middle of the book, on the engine's 9-decimal price grid, or the index price when a
    /// side is empty.
    pub fn book_price(&self) -> BigDecimal {
        match (&self.best_bid, &self.best_ask) {
            // Prices are integers with 9 decimals on chain and their midpoint rounds down.
            (Some(bid), Some(ask)) => ((bid + ask) / BigDecimal::from(2)).with_scale(9),
            _ => self.index_price.clone(),
        }
    }

    /// When funding is next updated: the start of the interval after the last update's.
    pub fn next_funding_ms(&self) -> i64 {
        if self.funding_frequency_ms <= 0 {
            return self.funding_last_upd_ms;
        }
        self.funding_last_upd_ms
            - self
                .funding_last_upd_ms
                .rem_euclid(self.funding_frequency_ms)
            + self.funding_frequency_ms
    }

    /// The index TWAP plus the premium still to be paid as funding before the next update.
    fn funding_price(&self, now_ms: i64) -> BigDecimal {
        let remaining_ms = (self.next_funding_ms() - now_ms).max(0);
        let remaining = ratio(
            &BigDecimal::from(remaining_ms),
            &BigDecimal::from(self.funding_period_ms),
        );
        &self.index_twap_price + &self.premium_twap * remaining
    }

    /// The price positions are valued at: the median of the funding price, the index TWAP plus
    /// the spread TWAP, and the book price.
    pub fn mark_price(&self, now_ms: i64) -> BigDecimal {
        let funding = self.funding_price(now_ms);
        let spread = &self.index_twap_price + &self.spread_twap;
        let book = self.book_price();
        let low = (&spread).min(&funding).clone();
        let high = spread.max(funding);
        fixed(low.max(high.min(book)))
    }

    /// The funding rate the current premium implies, per hour, as a fraction of the position's
    /// value. Positive when longs pay.
    pub fn funding_rate_1h(&self) -> BigDecimal {
        if self.funding_period_ms <= 0 {
            return BigDecimal::zero();
        }
        let premium = ratio(&self.premium_twap, &self.index_price);
        fixed(premium * BigDecimal::from(HOUR_MS) / BigDecimal::from(self.funding_period_ms))
    }
}

/// A market as the indexer's tables hold it: everything its prices and the value of a position
/// in it follow from.
#[derive(Clone, Debug)]
pub struct MarketState {
    pub settlement_enabled: bool,
    pub settlement_base_price: Option<BigDecimal>,
    pub margin_ratio_initial: BigDecimal,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub funding_last_upd_ms: i64,
    pub funding_frequency_ms: i64,
    pub funding_period_ms: i64,
    pub premium_twap: BigDecimal,
    pub spread_twap: BigDecimal,
    pub best_bid_price: Option<BigDecimal>,
    pub best_ask_price: Option<BigDecimal>,
    pub collateral_haircut: BigDecimal,
    /// The base asset's oracle price and its TWAP.
    pub oracle_price: Option<BigDecimal>,
    pub oracle_twap_price: Option<BigDecimal>,
    pub collateral_price: Option<BigDecimal>,
    /// The index price the engine last reported in an event.
    pub event_index_price: Option<BigDecimal>,
}

impl MarketState {
    /// The market's prices as of `now_ms`, and what positions in it are valued against. `None`
    /// while no price for the base asset has been seen: the index price from the engine's own
    /// events will do until the oracle is.
    pub fn price(&self, now_ms: i64) -> Option<(Pricing, Valuation)> {
        let index_price = self
            .oracle_price
            .clone()
            .or_else(|| self.event_index_price.clone())?;
        let pricing = Pricing {
            index_twap_price: self
                .oracle_twap_price
                .clone()
                .unwrap_or_else(|| index_price.clone()),
            index_price,
            premium_twap: self.premium_twap.clone(),
            spread_twap: self.spread_twap.clone(),
            best_bid: self.best_bid_price.clone(),
            best_ask: self.best_ask_price.clone(),
            funding_last_upd_ms: self.funding_last_upd_ms,
            funding_frequency_ms: self.funding_frequency_ms,
            funding_period_ms: self.funding_period_ms,
        };
        // A settled market values every position at its settlement price.
        let mark_price = match (&self.settlement_base_price, self.settlement_enabled) {
            (Some(price), true) => price.clone(),
            _ => pricing.mark_price(now_ms),
        };
        let valuation = Valuation {
            mark_price,
            collateral_price: self
                .collateral_price
                .clone()
                .unwrap_or_else(|| BigDecimal::from(1)),
            collateral_haircut: self.collateral_haircut.clone(),
            cum_funding_rate_long: self.cum_funding_rate_long.clone(),
            cum_funding_rate_short: self.cum_funding_rate_short.clone(),
            margin_ratio_initial: self.margin_ratio_initial.clone(),
        };
        Some((pricing, valuation))
    }
}

/// What `coins` raw units of a collateral with `decimals` decimals are worth at `price`.
pub fn collateral_value(coins: &BigDecimal, decimals: u32, price: &BigDecimal) -> BigDecimal {
    fixed(coins / decimal::pow10(decimals) * price)
}

/// The funding rate one funding update charged, per hour, as a fraction of the position's value.
///
/// The cumulative rate moves by the premium times the share of the funding period that the
/// elapsed intervals cover; dividing by the price and by those intervals gives the hourly rate.
pub fn settled_funding_rate_1h(
    cumulative_change: &BigDecimal,
    index_price: &BigDecimal,
    previous_update_ms: i64,
    update_ms: i64,
    funding_frequency_ms: i64,
) -> BigDecimal {
    if funding_frequency_ms <= 0 {
        return BigDecimal::zero();
    }
    let intervals = update_ms / funding_frequency_ms - previous_update_ms / funding_frequency_ms;
    let elapsed_ms = intervals.clamp(1, 3) * funding_frequency_ms;
    let rate = ratio(cumulative_change, index_price);
    fixed(rate * BigDecimal::from(HOUR_MS) / BigDecimal::from(elapsed_ms))
}

/// A position as its on-chain object holds it.
#[derive(Clone, Debug)]
pub struct Position {
    /// In units of the collateral coin.
    pub collateral: BigDecimal,
    pub base: BigDecimal,
    pub quote_notional: BigDecimal,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub asks_quantity: BigDecimal,
    pub bids_quantity: BigDecimal,
    pub pending_orders: i64,
    pub initial_margin_ratio: BigDecimal,
}

/// The market state a position is valued against.
#[derive(Clone, Debug)]
pub struct Valuation {
    pub mark_price: BigDecimal,
    pub collateral_price: BigDecimal,
    pub collateral_haircut: BigDecimal,
    pub cum_funding_rate_long: BigDecimal,
    pub cum_funding_rate_short: BigDecimal,
    pub margin_ratio_initial: BigDecimal,
}

/// What a position is worth, in USD.
#[derive(Clone, Debug, PartialEq)]
pub struct Margin {
    /// Funding accrued since the position was last settled; negative when owed.
    pub unsettled_funding: BigDecimal,
    /// The collateral with unsettled funding, after the haircut.
    pub collateral_value: BigDecimal,
    pub unrealized_pnl: BigDecimal,
    /// Collateral value plus unrealized profit: what liquidation compares with the requirement.
    pub margin: BigDecimal,
    /// The initial margin the position and its resting orders need.
    pub required: BigDecimal,
    /// What could be taken out. Unrealized profit cannot be withdrawn; losses count.
    pub free: BigDecimal,
}

impl Position {
    /// A rising cumulative rate is a cost for longs and income for shorts.
    pub fn unsettled_funding(&self, market: &Valuation) -> BigDecimal {
        let (now, before) = if self.base.is_negative() {
            (&market.cum_funding_rate_short, &self.cum_funding_rate_short)
        } else {
            (&market.cum_funding_rate_long, &self.cum_funding_rate_long)
        };
        fixed((now - before) * -&self.base)
    }

    /// The larger size the position could reach if all of its bids or all of its asks filled.
    pub fn abs_net_base(&self) -> BigDecimal {
        let after_bids = (&self.base + &self.bids_quantity).abs();
        let after_asks = (&self.base - &self.asks_quantity).abs();
        after_bids.max(after_asks)
    }

    /// The margin a liquidation compares with (`position::margin_requirement` at the market's
    /// maintenance ratio): the position's largest size with its resting orders, valued at the
    /// mark price.
    pub fn maintenance_requirement(
        &self,
        mark_price: &BigDecimal,
        margin_ratio_maintenance: &BigDecimal,
    ) -> BigDecimal {
        fixed(self.abs_net_base() * mark_price * margin_ratio_maintenance)
    }

    pub fn margin(&self, market: &Valuation) -> Margin {
        let unsettled_funding = self.unsettled_funding(market);
        let mut collateral_value = &self.collateral * &market.collateral_price + &unsettled_funding;
        // The haircut only applies to positive collateral.
        if !market.collateral_haircut.is_zero() && !collateral_value.is_negative() {
            collateral_value *= BigDecimal::one() - &market.collateral_haircut;
        }
        let collateral_value = fixed(collateral_value);
        let unrealized_pnl = fixed(&self.base * &market.mark_price - &self.quote_notional);
        let margin = &collateral_value + &unrealized_pnl;

        let ratio = (&self.initial_margin_ratio).max(&market.margin_ratio_initial);
        let required = fixed(self.abs_net_base() * &market.mark_price * ratio);

        let free = if self.pending_orders == 0 && self.base.is_zero() {
            collateral_value.clone().max(BigDecimal::zero())
        } else {
            let withdrawable = if unrealized_pnl.is_negative() {
                &margin
            } else {
                &collateral_value
            };
            (withdrawable - &required).max(BigDecimal::zero())
        };

        Margin {
            unsettled_funding,
            collateral_value,
            unrealized_pnl,
            margin,
            required,
            free,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::decimal::plain;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn pricing() -> Pricing {
        Pricing {
            index_price: dec("100"),
            index_twap_price: dec("100"),
            premium_twap: dec("0"),
            spread_twap: dec("0"),
            best_bid: None,
            best_ask: None,
            funding_last_upd_ms: 7_200_000,
            funding_frequency_ms: 3_600_000,
            funding_period_ms: 86_400_000,
        }
    }

    #[test]
    fn book_price_rounds_down_on_the_price_grid() {
        let mut p = pricing();
        assert_eq!(p.book_price(), dec("100"));
        p.best_bid = Some(dec("99.000000001"));
        assert_eq!(p.book_price(), dec("100"), "one side is not a book");
        p.best_ask = Some(dec("99.000000002"));
        assert_eq!(plain(&p.book_price()), "99.000000001");
    }

    #[test]
    fn mark_price_is_the_median_of_three() {
        let mut p = pricing();
        p.best_bid = Some(dec("103"));
        p.best_ask = Some(dec("105"));
        // funding 100, spread 100, book 104.
        assert_eq!(plain(&p.mark_price(7_200_000)), "100");

        p.spread_twap = dec("3");
        // funding 100, spread 103, book 104.
        assert_eq!(plain(&p.mark_price(7_200_000)), "103");

        p.premium_twap = dec("240");
        // A full interval remains of a 24-interval period: funding 100 + 240 / 24 = 110.
        assert_eq!(plain(&p.mark_price(7_200_000)), "104");
        // Half an interval remains: funding 105.
        assert_eq!(plain(&p.mark_price(9_000_000)), "104");
        // Past the next update nothing remains: funding 100.
        assert_eq!(plain(&p.mark_price(11_000_000)), "103");
    }

    #[test]
    fn funding_rates_are_hourly_fractions() {
        let mut p = pricing();
        p.premium_twap = dec("2.4");
        // 2.4 over a 24 hour period on a price of 100.
        assert_eq!(plain(&p.funding_rate_1h()), "0.001");
        assert_eq!(p.next_funding_ms(), 10_800_000);

        // One interval of a 24-interval period moved the cumulative rate by 0.1.
        let rate =
            settled_funding_rate_1h(&dec("0.1"), &dec("100"), 3_600_000, 7_200_000, 3_600_000);
        assert_eq!(plain(&rate), "0.001");
        // Two intervals caught up at once charge twice as much for twice as long.
        let rate =
            settled_funding_rate_1h(&dec("0.2"), &dec("100"), 3_600_000, 10_800_000, 3_600_000);
        assert_eq!(plain(&rate), "0.001");
    }

    fn position(base: &str, quote: &str) -> Position {
        Position {
            collateral: dec("1000"),
            base: dec(base),
            quote_notional: dec(quote),
            cum_funding_rate_long: dec("1"),
            cum_funding_rate_short: dec("1"),
            asks_quantity: dec("0"),
            bids_quantity: dec("0"),
            pending_orders: 0,
            initial_margin_ratio: dec("0.1"),
        }
    }

    fn valuation(mark: &str) -> Valuation {
        Valuation {
            mark_price: dec(mark),
            collateral_price: dec("1"),
            collateral_haircut: dec("0"),
            cum_funding_rate_long: dec("1"),
            cum_funding_rate_short: dec("1"),
            margin_ratio_initial: dec("0.05"),
        }
    }

    #[test]
    fn profit_counts_toward_margin_but_cannot_be_withdrawn() {
        let long = position("2", "200");
        let m = long.margin(&valuation("110"));
        assert_eq!(plain(&m.unrealized_pnl), "20");
        assert_eq!(plain(&m.margin), "1020");
        // The position's own ratio is stricter than the market's: 2 * 110 * 0.1.
        assert_eq!(plain(&m.required), "22");
        assert_eq!(plain(&m.free), "978");

        let m = long.margin(&valuation("90"));
        assert_eq!(plain(&m.unrealized_pnl), "-20");
        assert_eq!(plain(&m.free), "962");

        let short = position("-2", "-200");
        assert_eq!(plain(&short.margin(&valuation("90")).unrealized_pnl), "20");
    }

    #[test]
    fn funding_follows_the_side_and_the_haircut_follows_the_sign() {
        let mut market = valuation("100");
        market.cum_funding_rate_long = dec("1.5");
        market.cum_funding_rate_short = dec("1.25");
        market.collateral_haircut = dec("0.1");
        market.collateral_price = dec("0.5");

        let long = position("2", "200").margin(&market);
        assert_eq!(plain(&long.unsettled_funding), "-1");
        // (1000 * 0.5 - 1) * 0.9
        assert_eq!(plain(&long.collateral_value), "449.1");

        let short = position("-2", "-200").margin(&market);
        assert_eq!(plain(&short.unsettled_funding), "0.5");

        let mut underwater = position("2", "200");
        underwater.collateral = dec("-10");
        assert_eq!(plain(&underwater.margin(&market).collateral_value), "-6");
    }

    #[test]
    fn maintenance_counts_the_size_resting_orders_could_add() {
        let mut long = position("2", "200");
        // 2 at 90 at 5%.
        assert_eq!(
            plain(&long.maintenance_requirement(&dec("90"), &dec("0.05"))),
            "9"
        );
        // Resting bids of 1 could make it 3; asks of 4 could make it -2.
        long.bids_quantity = dec("1");
        long.asks_quantity = dec("4");
        assert_eq!(plain(&long.abs_net_base()), "3");
        assert_eq!(
            plain(&long.maintenance_requirement(&dec("90"), &dec("0.05"))),
            "13.5"
        );
    }

    #[test]
    fn resting_orders_are_margined_and_idle_collateral_is_free() {
        let mut flat = position("0", "0");
        assert_eq!(plain(&flat.margin(&valuation("100")).free), "1000");

        flat.pending_orders = 1;
        flat.bids_quantity = dec("3");
        flat.asks_quantity = dec("1");
        let m = flat.margin(&valuation("100"));
        assert_eq!(plain(&m.required), "30");
        assert_eq!(plain(&m.free), "970");
    }
}
