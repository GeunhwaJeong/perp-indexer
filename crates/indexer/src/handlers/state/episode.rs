// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The running totals of a position since it was last opened.
//!
//! The engine keeps a position's size, collateral and entry notional; it does not keep how the
//! position got there. These totals are built from the position's fills and funding payments, in
//! chain order: an episode starts when a flat position is opened and ends when it is flat again,
//! and a fill that takes a position through zero closes one episode and opens the next.

use bigdecimal::{BigDecimal, Signed, Zero};
use perp_schema::models::{Fill, FundingPayment, PositionEpisode};

/// A fill or funding payment of one position, by its index in the batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionEvent {
    Fill(usize),
    Funding(usize),
}

/// A position's signed size and totals at some point in the chain.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EpisodeState {
    pub base: BigDecimal,
    pub episode: PositionEpisode,
}

/// Decimal places kept when a division does not terminate: the precision of `ifixed`.
const SCALE: i64 = 18;

impl EpisodeState {
    /// The average price the open size was added at.
    fn entry_price(&self) -> Option<BigDecimal> {
        (!self.base.is_zero()).then(|| (&self.episode.entry_quote / self.base.abs()).round(SCALE))
    }

    /// Applies `fill`, and records on it the position it found.
    pub fn apply_fill(&mut self, fill: &mut Fill) {
        fill.position_base_before = Some(self.base.clone());
        fill.entry_price_before = self.entry_price();

        let opened = |sum_open: BigDecimal, entry_quote: BigDecimal| PositionEpisode {
            opened_checkpoint: Some(fill.checkpoint),
            opened_at_ms: Some(fill.timestamp_ms),
            max_size: sum_open.clone(),
            sum_open,
            entry_quote,
            ..PositionEpisode::default()
        };

        let size = &fill.size;
        // A settlement has no price of its own: it closes at the market's settlement price, and
        // its notional is left out.
        let quote = fill.price.as_ref().map(|price| size * price);
        let quote = quote.unwrap_or_default();
        let net_pnl = &fill.pnl - &fill.fee - &fill.integrator_fee;
        let delta = if fill.is_ask { -size } else { size.clone() };

        let held = self.base.abs();
        if held.is_zero() {
            self.episode = opened(size.clone(), quote);
            self.episode.realized_pnl = net_pnl;
        } else if self.base.is_positive() == delta.is_positive() {
            self.episode.sum_open += size;
            self.episode.entry_quote += quote;
            self.episode.realized_pnl += net_pnl;
        } else {
            let closed = size.min(&held);
            let price = fill.price.clone().unwrap_or_default();
            self.episode.sum_close += closed;
            self.episode.close_quote += closed * &price;
            self.episode.realized_pnl += net_pnl;
            // The closed share of the size takes its share of the cost basis with it.
            self.episode.entry_quote = if *closed == held {
                BigDecimal::zero()
            } else {
                let released = (&self.episode.entry_quote * closed / &held).round(SCALE);
                &self.episode.entry_quote - released
            };
            if *size > held {
                let rest = size - &held;
                self.episode = opened(rest.clone(), rest * price);
            }
        }

        self.base += delta;
        let now = self.base.abs();
        if now > self.episode.max_size {
            self.episode.max_size = now;
        }
    }

    /// Applies a funding payment, and records on it the size it was settled on.
    pub fn apply_funding(&mut self, payment: &mut FundingPayment) {
        payment.position_base = Some(self.base.clone());
        self.episode.net_funding += &payment.collateral_change_usd;
    }

    /// Brings the totals in line with the position object when the fills did not add up to it,
    /// which means the engine changed a position through a path this indexer does not know.
    /// Returns whether anything had to be corrected.
    pub fn reconcile(
        &mut self,
        base: &BigDecimal,
        quote_notional: &BigDecimal,
        checkpoint: i64,
        timestamp_ms: i64,
    ) -> bool {
        if self.base == *base {
            return false;
        }
        self.base = base.clone();
        if base.is_zero() {
            return true;
        }
        if self.episode.opened_checkpoint.is_none() {
            self.episode.opened_checkpoint = Some(checkpoint);
            self.episode.opened_at_ms = Some(timestamp_ms);
        }
        self.episode.entry_quote = quote_notional.abs();
        if base.abs() > self.episode.max_size {
            self.episode.max_size = base.abs();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn fill(checkpoint: i64, is_ask: bool, size: &str, price: &str, pnl: &str, fee: &str) -> Fill {
        Fill {
            checkpoint,
            tx_index: 0,
            event_index: 0,
            fill_index: 0,
            tx_digest: "tx".to_owned(),
            timestamp_ms: checkpoint * 1_000,
            market: "0xm".to_owned(),
            account_id: 1,
            counterparty_account_id: None,
            is_ask,
            liquidity: "taker".to_owned(),
            kind: "trade".to_owned(),
            price: Some(dec(price)),
            size: dec(size),
            quote: Some(dec(size) * dec(price)),
            fee: dec(fee),
            integrator_fee: dec("0"),
            pnl: dec(pnl),
            order_id: None,
            client_order_id: None,
            mark_price: None,
            position_base_before: None,
            entry_price_before: None,
        }
    }

    #[test]
    fn opening_extending_and_closing_a_long() {
        let mut state = EpisodeState::default();

        let mut open = fill(10, false, "1", "100", "0", "0.1");
        state.apply_fill(&mut open);
        assert_eq!(open.position_base_before, Some(dec("0")));
        assert_eq!(open.entry_price_before, None);
        assert_eq!(state.base, dec("1"));
        assert_eq!(state.episode.opened_checkpoint, Some(10));

        let mut extend = fill(11, false, "1", "110", "0", "0.1");
        state.apply_fill(&mut extend);
        assert_eq!(extend.entry_price_before, Some(dec("100")));
        assert_eq!(state.episode.sum_open, dec("2"));
        assert_eq!(state.episode.entry_quote, dec("210"));

        let mut reduce = fill(12, true, "0.5", "120", "7.5", "0.1");
        state.apply_fill(&mut reduce);
        assert_eq!(reduce.position_base_before, Some(dec("2")));
        assert_eq!(reduce.entry_price_before, Some(dec("105")));
        assert_eq!(state.base, dec("1.5"));
        assert_eq!(state.episode.sum_close, dec("0.5"));
        assert_eq!(state.episode.close_quote, dec("60"));
        // Three quarters of the size keep three quarters of the cost basis.
        assert_eq!(state.episode.entry_quote, dec("157.5"));

        let mut close = fill(13, true, "1.5", "90", "-22.5", "0.1");
        state.apply_fill(&mut close);
        assert_eq!(state.base, dec("0"));
        assert_eq!(state.episode.entry_quote, dec("0"));
        assert_eq!(state.episode.sum_close, dec("2"));
        assert_eq!(state.episode.max_size, dec("2"));
        assert_eq!(state.episode.realized_pnl, dec("-15.4"));
        // The totals of a closed episode stay until the position is opened again.
        assert_eq!(state.episode.opened_checkpoint, Some(10));

        state.apply_fill(&mut fill(20, true, "3", "95", "0", "0"));
        assert_eq!(state.base, dec("-3"));
        assert_eq!(state.episode.opened_checkpoint, Some(20));
        assert_eq!(state.episode.sum_close, dec("0"));
        assert_eq!(state.episode.realized_pnl, dec("0"));
    }

    #[test]
    fn a_fill_through_zero_closes_one_episode_and_opens_the_next() {
        let mut state = EpisodeState::default();
        state.apply_fill(&mut fill(1, false, "1", "100", "0", "0"));

        let mut flip = fill(2, true, "3", "110", "10", "1");
        state.apply_fill(&mut flip);
        assert_eq!(flip.position_base_before, Some(dec("1")));
        assert_eq!(state.base, dec("-2"));
        assert_eq!(state.episode.opened_checkpoint, Some(2));
        assert_eq!(state.episode.sum_open, dec("2"));
        assert_eq!(state.episode.max_size, dec("2"));
        assert_eq!(state.episode.entry_quote, dec("220"));
        assert_eq!(state.episode.sum_close, dec("0"));
        // The fill's profit and fee belong to the episode it closed.
        assert_eq!(state.episode.realized_pnl, dec("0"));
    }

    #[test]
    fn funding_counts_toward_the_open_episode() {
        let mut state = EpisodeState::default();
        state.apply_fill(&mut fill(1, true, "2", "100", "0", "0"));
        let mut payment = FundingPayment {
            checkpoint: 2,
            tx_index: 0,
            event_index: 0,
            tx_digest: "tx".to_owned(),
            timestamp_ms: 2_000,
            market: "0xm".to_owned(),
            account_id: 1,
            collateral_change_usd: dec("-0.25"),
            collateral_after: dec("10"),
            cum_funding_rate_long: dec("0"),
            cum_funding_rate_short: dec("0"),
            position_base: None,
            index_price: None,
        };
        state.apply_funding(&mut payment);
        assert_eq!(payment.position_base, Some(dec("-2")));
        assert_eq!(state.episode.net_funding, dec("-0.25"));
    }

    #[test]
    fn totals_follow_the_object_when_fills_do_not_add_up() {
        let mut state = EpisodeState::default();
        state.apply_fill(&mut fill(1, false, "1", "100", "0", "0"));
        assert!(!state.reconcile(&dec("1.000"), &dec("100"), 5, 5_000));

        assert!(state.reconcile(&dec("4"), &dec("440"), 5, 5_000));
        assert_eq!(state.base, dec("4"));
        assert_eq!(state.episode.max_size, dec("4"));
        assert_eq!(state.episode.entry_quote, dec("440"));
        assert_eq!(state.episode.opened_checkpoint, Some(1));

        let mut unseen = EpisodeState::default();
        assert!(unseen.reconcile(&dec("-2"), &dec("-200"), 7, 7_000));
        assert_eq!(unseen.episode.opened_checkpoint, Some(7));
        assert_eq!(unseen.episode.entry_quote, dec("200"));
    }
}
