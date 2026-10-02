// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Accumulates the changes of consecutive checkpoints into the writes of one database
//! transaction, merging what can be merged: a snapshot supersedes earlier snapshots of the same
//! object, updates to an order fold into one, and trades fold into candles.

use std::collections::{BTreeMap, BTreeSet};

use bigdecimal::{BigDecimal, Zero};
use perp_schema::models::{
    AccountCap, AccountSnapshot, Candle, CollateralTransfer, Fill, FundingPayment, FundingUpdate,
    MarketSnapshot, OraclePrice, Order, PositionSnapshot,
};

use super::change::{Change, MarketPrices, OrderUpdate, TicketChange};
use super::episode::PositionEvent;

/// Candle widths, in milliseconds: 1, 5, 15 and 30 minutes, 1 and 4 hours, 1 day.
pub const RESOLUTIONS_MS: [i64; 7] = [
    60_000, 300_000, 900_000, 1_800_000, 3_600_000, 14_400_000, 86_400_000,
];

type OrderKey = (String, BigDecimal);

pub struct MarketCreated {
    pub market: String,
    pub collateral_decimals: i64,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
}

pub struct MarketSettlement {
    pub enabled: bool,
    pub base_price: BigDecimal,
    pub collateral_price: BigDecimal,
}

pub struct AccountCreated {
    pub account_id: i64,
    pub creator: String,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
}

/// The last thing a batch saw happen to a capability.
pub enum CapWrite {
    Held(AccountCap),
    /// Deleted or wrapped, at this checkpoint.
    Removed(i64),
}

/// A position's latest snapshot, with when the batch first saw it (its creation, if it is new).
pub struct PositionWrite {
    pub snapshot: PositionSnapshot,
    pub first_checkpoint: i64,
    pub first_timestamp_ms: i64,
}

/// The net effect of a batch's fills and cancelations on an order already in the database.
pub struct OrderDelta {
    pub filled: BigDecimal,
    pub canceled: BigDecimal,
    pub remaining: BigDecimal,
    pub cancel_reason: Option<i16>,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
}

#[derive(Default)]
pub struct Batch {
    pub market_snapshots: BTreeMap<String, MarketSnapshot>,
    /// Markets in the order the batch first saw them, which is the order new ones are numbered.
    pub market_order: Vec<String>,
    pub markets_created: Vec<MarketCreated>,
    pub markets_closed: BTreeSet<String>,
    pub market_settlements: BTreeMap<String, MarketSettlement>,
    pub market_prices: BTreeMap<String, MarketPrices>,
    pub account_snapshots: BTreeMap<i64, AccountSnapshot>,
    pub accounts_created: Vec<AccountCreated>,
    pub account_caps: BTreeMap<String, CapWrite>,
    pub positions: BTreeMap<(String, i64), PositionWrite>,
    /// Orders posted in this batch, already carrying any fills and cancelations that followed.
    pub new_orders: BTreeMap<OrderKey, Order>,
    pub order_deltas: BTreeMap<OrderKey, OrderDelta>,
    pub fills: Vec<Fill>,
    /// Each position's fills and funding payments, in chain order.
    pub position_events: BTreeMap<(String, i64), Vec<PositionEvent>>,
    pub candles: BTreeMap<(String, i64, i64), Candle>,
    pub funding_updates: Vec<FundingUpdate>,
    pub funding_payments: Vec<FundingPayment>,
    pub collateral_transfers: Vec<CollateralTransfer>,
    /// Coins each account was sent less coins it paid out, over the batch.
    pub net_transfers: BTreeMap<i64, BigDecimal>,
    /// `None` for a feed that was removed.
    pub oracle_prices: BTreeMap<(i64, i32), Option<OraclePrice>>,
    pub tickets: Vec<TicketChange>,
}

/// The status of an order given what is left of it and how much of it was ever canceled.
pub fn order_status(remaining: &BigDecimal, canceled: &BigDecimal) -> &'static str {
    if !remaining.is_zero() {
        "open"
    } else if !canceled.is_zero() {
        "canceled"
    } else {
        "filled"
    }
}

impl Batch {
    /// Adds the next change in chain order.
    pub fn push(&mut self, change: Change) {
        match change {
            Change::MarketSnapshot(snapshot) => {
                let previous = self
                    .market_snapshots
                    .insert(snapshot.market.clone(), *snapshot.clone());
                if previous.is_none() {
                    self.market_order.push(snapshot.market);
                }
            }
            Change::MarketCreated {
                market,
                collateral_decimals,
                checkpoint,
                timestamp_ms,
            } => self.markets_created.push(MarketCreated {
                market,
                collateral_decimals,
                checkpoint,
                timestamp_ms,
            }),
            Change::MarketClosed { market } => {
                self.markets_closed.insert(market);
            }
            Change::MarketSettlement {
                market,
                enabled,
                base_price,
                collateral_price,
            } => {
                self.market_settlements.insert(
                    market,
                    MarketSettlement {
                        enabled,
                        base_price,
                        collateral_price,
                    },
                );
            }
            Change::MarketPrices(prices) => match self.market_prices.get_mut(&prices.market) {
                Some(merged) => {
                    merged.mark_price = prices.mark_price.or(merged.mark_price.take());
                    merged.index_price = prices.index_price.or(merged.index_price.take());
                    merged.book_price = prices.book_price.or(merged.book_price.take());
                    merged.timestamp_ms = prices.timestamp_ms;
                }
                None => {
                    self.market_prices.insert(prices.market.clone(), prices);
                }
            },
            Change::AccountSnapshot(snapshot) => {
                self.account_snapshots.insert(snapshot.account_id, snapshot);
            }
            Change::AccountCreated {
                account_id,
                creator,
                checkpoint,
                timestamp_ms,
            } => self.accounts_created.push(AccountCreated {
                account_id,
                creator,
                checkpoint,
                timestamp_ms,
            }),
            Change::AccountCap(cap) => {
                self.account_caps
                    .insert(cap.cap_id.clone(), CapWrite::Held(cap));
            }
            Change::AccountCapRemoved { cap_id, checkpoint } => {
                self.account_caps
                    .insert(cap_id, CapWrite::Removed(checkpoint));
            }
            Change::PositionSnapshot(snapshot) => {
                let key = (snapshot.market.clone(), snapshot.account_id);
                match self.positions.get_mut(&key) {
                    Some(write) => write.snapshot = snapshot,
                    None => {
                        let write = PositionWrite {
                            first_checkpoint: snapshot.updated_checkpoint,
                            first_timestamp_ms: snapshot.updated_at_ms,
                            snapshot,
                        };
                        self.positions.insert(key, write);
                    }
                }
            }
            Change::OrderPosted(order) => {
                self.new_orders
                    .insert((order.market.clone(), order.order_id.clone()), order);
            }
            Change::OrderUpdated(update) => self.update_order(update),
            Change::Fill(fill) => {
                if fill.kind == "trade" && fill.liquidity == "maker" {
                    self.add_trade(&fill);
                }
                self.position_events
                    .entry((fill.market.clone(), fill.account_id))
                    .or_default()
                    .push(PositionEvent::Fill(self.fills.len()));
                self.fills.push(fill);
            }
            Change::FundingUpdate(update) => self.funding_updates.push(update),
            Change::FundingPayment(payment) => {
                self.position_events
                    .entry((payment.market.clone(), payment.account_id))
                    .or_default()
                    .push(PositionEvent::Funding(self.funding_payments.len()));
                self.funding_payments.push(payment);
            }
            Change::CollateralTransfer(transfer) => {
                // Moves between an account and its markets stay inside the account.
                let net = self.net_transfers.entry(transfer.account_id).or_default();
                match transfer.kind.as_str() {
                    "deposit" => *net += &transfer.amount,
                    "withdraw" => *net -= &transfer.amount,
                    _ => {}
                }
                self.collateral_transfers.push(transfer);
            }
            Change::OraclePrice(price) => {
                self.oracle_prices
                    .insert((price.storage_id, price.source_id), Some(price));
            }
            Change::OraclePriceRemoved {
                storage_id,
                source_id,
            } => {
                self.oracle_prices.insert((storage_id, source_id), None);
            }
            Change::Ticket(change) => self.tickets.push(change),
        }
    }

    fn update_order(&mut self, update: OrderUpdate) {
        let key = (update.market, update.order_id);
        if let Some(order) = self.new_orders.get_mut(&key) {
            order.filled += &update.filled;
            order.canceled += &update.canceled;
            order.remaining = update.remaining;
            order.cancel_reason = update.cancel_reason.or(order.cancel_reason);
            order.status = order_status(&order.remaining, &order.canceled).to_owned();
            order.updated_checkpoint = update.checkpoint;
            order.updated_at_ms = update.timestamp_ms;
            return;
        }
        match self.order_deltas.get_mut(&key) {
            Some(delta) => {
                delta.filled += &update.filled;
                delta.canceled += &update.canceled;
                delta.remaining = update.remaining;
                delta.cancel_reason = update.cancel_reason.or(delta.cancel_reason);
                delta.checkpoint = update.checkpoint;
                delta.timestamp_ms = update.timestamp_ms;
            }
            None => {
                let delta = OrderDelta {
                    filled: update.filled,
                    canceled: update.canceled,
                    remaining: update.remaining,
                    cancel_reason: update.cancel_reason,
                    checkpoint: update.checkpoint,
                    timestamp_ms: update.timestamp_ms,
                };
                self.order_deltas.insert(key, delta);
            }
        }
    }

    /// Folds a trade into its candle at every resolution.
    fn add_trade(&mut self, fill: &Fill) {
        let (Some(price), Some(quote)) = (&fill.price, &fill.quote) else {
            return;
        };
        for resolution_ms in RESOLUTIONS_MS {
            let start_ms = fill.timestamp_ms - fill.timestamp_ms.rem_euclid(resolution_ms);
            let candle = self
                .candles
                .entry((fill.market.clone(), resolution_ms, start_ms))
                .or_insert_with(|| Candle {
                    market: fill.market.clone(),
                    resolution_ms,
                    start_ms,
                    open: price.clone(),
                    high: price.clone(),
                    low: price.clone(),
                    close: price.clone(),
                    base_volume: BigDecimal::zero(),
                    quote_volume: BigDecimal::zero(),
                    trades: 0,
                });
            if *price > candle.high {
                candle.high = price.clone();
            }
            if *price < candle.low {
                candle.low = price.clone();
            }
            candle.close = price.clone();
            candle.base_volume += &fill.size;
            candle.quote_volume += quote;
            candle.trades += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn order(id: &str, size: &str) -> Order {
        Order {
            market: "0xm".to_owned(),
            order_id: dec(id),
            account_id: 1,
            is_ask: true,
            price: dec("100"),
            size: dec(size),
            remaining: dec(size),
            filled: dec("0"),
            canceled: dec("0"),
            status: "open".to_owned(),
            kind: "limit".to_owned(),
            cancel_reason: None,
            reduce_only: false,
            expiration_timestamp_ms: None,
            client_order_id: None,
            integrator_id: None,
            integrator_fee_rate: 0,
            created_checkpoint: 1,
            created_at_ms: 1_000,
            created_tx: "tx".to_owned(),
            updated_checkpoint: 1,
            updated_at_ms: 1_000,
        }
    }

    fn update(
        id: &str,
        filled: &str,
        canceled: &str,
        remaining: &str,
        reason: Option<i16>,
    ) -> Change {
        Change::OrderUpdated(OrderUpdate {
            market: "0xm".to_owned(),
            order_id: dec(id),
            filled: dec(filled),
            canceled: dec(canceled),
            remaining: dec(remaining),
            cancel_reason: reason,
            checkpoint: 2,
            timestamp_ms: 2_000,
        })
    }

    fn trade(timestamp_ms: i64, price: &str, size: &str) -> Change {
        Change::Fill(Fill {
            checkpoint: 1,
            tx_index: 0,
            event_index: 0,
            fill_index: 0,
            tx_digest: "tx".to_owned(),
            timestamp_ms,
            market: "0xm".to_owned(),
            account_id: 1,
            counterparty_account_id: Some(2),
            is_ask: true,
            liquidity: "maker".to_owned(),
            kind: "trade".to_owned(),
            price: Some(dec(price)),
            size: dec(size),
            quote: Some(dec(price) * dec(size)),
            fee: dec("0"),
            integrator_fee: dec("0"),
            pnl: dec("0"),
            order_id: None,
            client_order_id: None,
            mark_price: None,
            position_base_before: None,
            entry_price_before: None,
        })
    }

    #[test]
    fn fills_and_cancels_fold_into_an_order_posted_in_the_same_batch() {
        let mut batch = Batch::default();
        batch.push(Change::OrderPosted(order("7", "1.0")));
        batch.push(update("7", "0.4", "0", "0.6", None));
        batch.push(update("7", "0.1", "0.5", "0", Some(4)));

        let order = &batch.new_orders[&("0xm".to_owned(), dec("7"))];
        assert_eq!(order.filled, dec("0.5"));
        assert_eq!(order.canceled, dec("0.5"));
        assert_eq!(order.remaining, dec("0"));
        assert_eq!(order.status, "canceled");
        assert_eq!(order.cancel_reason, Some(4));
        assert_eq!(order.updated_checkpoint, 2);
        assert!(batch.order_deltas.is_empty());
    }

    #[test]
    fn updates_to_an_existing_order_fold_into_one_delta() {
        let mut batch = Batch::default();
        batch.push(update("7", "0.4", "0", "0.6", None));
        batch.push(update("7", "0.6", "0", "0", None));

        let delta = &batch.order_deltas[&("0xm".to_owned(), dec("7"))];
        assert_eq!(delta.filled, dec("1.0"));
        assert_eq!(delta.remaining, dec("0"));
        assert_eq!(order_status(&delta.remaining, &delta.canceled), "filled");
    }

    #[test]
    fn trades_fold_into_candles_at_every_resolution() {
        let mut batch = Batch::default();
        batch.push(trade(60_000, "100", "1"));
        batch.push(trade(61_000, "105", "2"));
        batch.push(trade(119_999, "95", "1"));
        batch.push(trade(120_000, "101", "1"));

        assert_eq!(batch.fills.len(), 4);
        assert_eq!(
            batch.position_events[&("0xm".to_owned(), 1)],
            (0..4).map(PositionEvent::Fill).collect::<Vec<_>>()
        );
        let minute = &batch.candles[&("0xm".to_owned(), 60_000, 60_000)];
        assert_eq!(
            (&minute.open, &minute.high, &minute.low, &minute.close),
            (&dec("100"), &dec("105"), &dec("95"), &dec("95"))
        );
        assert_eq!(minute.base_volume, dec("4"));
        assert_eq!(minute.quote_volume, dec("405"));
        assert_eq!(minute.trades, 3);
        assert_eq!(
            batch.candles[&("0xm".to_owned(), 60_000, 120_000)].trades,
            1
        );
        let day = &batch.candles[&("0xm".to_owned(), 86_400_000, 0)];
        assert_eq!((day.trades, &day.close), (4, &dec("101")));
        assert_eq!(batch.candles.len(), 2 + 6);
    }

    #[test]
    fn taker_and_liquidation_fills_do_not_count_as_trades() {
        let mut batch = Batch::default();
        let Change::Fill(mut fill) = trade(60_000, "100", "1") else {
            unreachable!()
        };
        fill.liquidity = "taker".to_owned();
        batch.push(Change::Fill(fill.clone()));
        fill.kind = "liquidated".to_owned();
        batch.push(Change::Fill(fill));
        assert_eq!(batch.fills.len(), 2);
        assert!(batch.candles.is_empty());
    }

    #[test]
    fn deposits_and_withdrawals_net_per_account() {
        let transfer = |account_id, kind: &str, amount: &str| {
            Change::CollateralTransfer(CollateralTransfer {
                checkpoint: 1,
                tx_index: 0,
                event_index: 0,
                tx_digest: "tx".to_owned(),
                timestamp_ms: 1_000,
                account_id,
                kind: kind.to_owned(),
                market: None,
                amount: dec(amount),
            })
        };
        let mut batch = Batch::default();
        batch.push(transfer(1, "deposit", "100"));
        batch.push(transfer(1, "allocate", "60"));
        batch.push(transfer(1, "withdraw", "30"));
        batch.push(transfer(1, "settlement", "5"));
        batch.push(transfer(2, "withdraw", "7"));

        assert_eq!(batch.collateral_transfers.len(), 5);
        assert_eq!(batch.net_transfers[&1], dec("70"));
        assert_eq!(batch.net_transfers[&2], dec("-7"));
    }

    #[test]
    fn later_prices_keep_the_fields_they_do_not_report() {
        let mut batch = Batch::default();
        let prices = |mark: Option<&str>, index: Option<&str>, timestamp_ms| {
            Change::MarketPrices(MarketPrices {
                market: "0xm".to_owned(),
                mark_price: mark.map(dec),
                index_price: index.map(dec),
                book_price: None,
                timestamp_ms,
            })
        };
        batch.push(prices(Some("100"), Some("99"), 1));
        batch.push(prices(Some("101"), None, 2));
        let merged = &batch.market_prices["0xm"];
        assert_eq!(merged.mark_price, Some(dec("101")));
        assert_eq!(merged.index_price, Some(dec("99")));
        assert_eq!(merged.timestamp_ms, 2);
    }
}
