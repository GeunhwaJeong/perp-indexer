// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Gives the fills of orders that crossed the book an order to belong to.
//!
//! The engine reports an order when it posts it to the book (`PostedOrder`, with the size that
//! was left to rest) and reports what a session took from the book as one net fill per side
//! (`FilledTakerOrder`), which names no order. An order that fills at once therefore appears
//! in an account's fills and nowhere in its orders, and a limit order that fills in part
//! before resting appears with only the size that rested.
//!
//! A taker fill is attributed within its own transaction. If the transaction posted exactly
//! one order of the same account, market and side, the fill is the part of that order that
//! crossed: the order's size and filled size grow by it. Otherwise the fill gets an order of
//! its own, already filled, of kind `market`.

use std::collections::HashMap;

use bigdecimal::{BigDecimal, Zero};
use num_bigint::BigInt;
use perp_schema::models::{Fill, Order};

use super::change::Change;

/// The ID of an order made for a taker fill. Above 2^128, so that it cannot be the ID of an
/// order the engine posted, and derived from where the fill sits on chain, so that it is the
/// same on every replay.
pub fn market_order_id(fill: &Fill) -> BigDecimal {
    let position = ((BigInt::from(fill.checkpoint) << 32 | BigInt::from(fill.tx_index)) << 32
        | BigInt::from(fill.event_index))
        << 8
        | BigInt::from(fill.fill_index);
    BigDecimal::from((BigInt::from(1) << 128) + position)
}

fn is_taker_trade(fill: &Fill) -> bool {
    fill.liquidity == "taker" && fill.kind == "trade" && fill.order_id.is_none()
}

/// Attributes the taker fills among `changes[from..]`, the changes of one transaction's events.
pub fn attribute_taker_fills(changes: &mut Vec<Change>, from: usize, tx_digest: &str) {
    // The orders the transaction posted, by whose they are and which side they are on.
    let mut posted: HashMap<(String, i64, bool), Vec<usize>> = HashMap::new();
    let mut fills = vec![];
    for (i, change) in changes.iter().enumerate().skip(from) {
        match change {
            Change::OrderPosted(order) => posted
                .entry((order.market.clone(), order.account_id, order.is_ask))
                .or_default()
                .push(i),
            Change::Fill(fill) if is_taker_trade(fill) => fills.push(i),
            _ => {}
        }
    }

    for i in fills {
        let Change::Fill(fill) = &changes[i] else {
            unreachable!("indexed as a fill");
        };
        let key = (fill.market.clone(), fill.account_id, fill.is_ask);
        let size = fill.size.clone();
        let own = match posted.get(&key).map(Vec::as_slice) {
            Some(&[order]) => Some(order),
            _ => None,
        };
        let (order_id, client_order_id) = match own {
            Some(order) => {
                let Change::OrderPosted(order) = &mut changes[order] else {
                    unreachable!("indexed as a posted order");
                };
                order.size += &size;
                order.filled += &size;
                (order.order_id.clone(), order.client_order_id.clone())
            }
            None => {
                let order = market_order(fill, tx_digest);
                let order_id = order.order_id.clone();
                changes.push(Change::OrderPosted(order));
                (order_id, None)
            }
        };
        let Change::Fill(fill) = &mut changes[i] else {
            unreachable!("indexed as a fill");
        };
        fill.order_id = Some(order_id);
        fill.client_order_id = client_order_id;
    }
}

/// The order a taker fill stands for when the transaction posted none it could be part of.
fn market_order(fill: &Fill, tx_digest: &str) -> Order {
    Order {
        market: fill.market.clone(),
        order_id: market_order_id(fill),
        account_id: fill.account_id,
        is_ask: fill.is_ask,
        // What it traded at on average; it had no price of its own that the chain kept.
        price: fill.price.clone().unwrap_or_default(),
        size: fill.size.clone(),
        remaining: BigDecimal::zero(),
        filled: fill.size.clone(),
        canceled: BigDecimal::zero(),
        status: "filled".to_owned(),
        kind: "market".to_owned(),
        cancel_reason: None,
        reduce_only: false,
        expiration_timestamp_ms: None,
        client_order_id: None,
        integrator_id: None,
        integrator_fee_rate: 0,
        created_checkpoint: fill.checkpoint,
        created_at_ms: fill.timestamp_ms,
        created_tx: tx_digest.to_owned(),
        updated_checkpoint: fill.checkpoint,
        updated_at_ms: fill.timestamp_ms,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn fill(account_id: i64, is_ask: bool, size: &str, liquidity: &str, kind: &str) -> Change {
        Change::Fill(Fill {
            checkpoint: 7,
            tx_index: 2,
            event_index: 4,
            fill_index: 1,
            tx_digest: "tx".to_owned(),
            timestamp_ms: 7_000,
            market: "0xm".to_owned(),
            account_id,
            counterparty_account_id: None,
            is_ask,
            liquidity: liquidity.to_owned(),
            kind: kind.to_owned(),
            price: Some(dec("100")),
            size: dec(size),
            quote: Some(dec(size) * dec("100")),
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

    fn posted(account_id: i64, is_ask: bool, id: &str, size: &str) -> Change {
        Change::OrderPosted(Order {
            market: "0xm".to_owned(),
            order_id: dec(id),
            account_id,
            is_ask,
            price: dec("101"),
            size: dec(size),
            remaining: dec(size),
            filled: dec("0"),
            canceled: dec("0"),
            status: "open".to_owned(),
            kind: "limit".to_owned(),
            cancel_reason: None,
            reduce_only: false,
            expiration_timestamp_ms: None,
            client_order_id: Some(dec("42")),
            integrator_id: None,
            integrator_fee_rate: 0,
            created_checkpoint: 7,
            created_at_ms: 7_000,
            created_tx: "tx".to_owned(),
            updated_checkpoint: 7,
            updated_at_ms: 7_000,
        })
    }

    fn orders(changes: &[Change]) -> Vec<&Order> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::OrderPosted(order) => Some(order),
                _ => None,
            })
            .collect()
    }

    fn fills(changes: &[Change]) -> Vec<&Fill> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::Fill(fill) => Some(fill),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_fill_with_no_order_gets_a_market_order_already_filled() {
        let mut changes = vec![fill(1, false, "0.2", "taker", "trade")];
        attribute_taker_fills(&mut changes, 0, "digest");

        let [order] = orders(&changes)[..] else {
            panic!("one order expected");
        };
        assert_eq!(
            (order.kind.as_str(), order.status.as_str()),
            ("market", "filled")
        );
        assert_eq!(
            (&order.size, &order.filled, &order.remaining),
            (&dec("0.2"), &dec("0.2"), &dec("0"))
        );
        assert_eq!((order.is_ask, &order.price), (false, &dec("100")));
        assert_eq!(order.created_tx, "digest");
        // Above every ID the engine can give an order.
        assert!(order.order_id > dec("340282366920938463463374607431768211455"));
        assert_eq!(fills(&changes)[0].order_id.as_ref(), Some(&order.order_id));
    }

    #[test]
    fn the_id_of_a_market_order_is_where_its_fill_sits_on_chain() {
        let Change::Fill(one) = fill(1, false, "1", "taker", "trade") else {
            unreachable!()
        };
        let mut other = one.clone();
        assert_eq!(market_order_id(&one), market_order_id(&other));
        other.fill_index = 0;
        assert_ne!(market_order_id(&one), market_order_id(&other));
        other = one.clone();
        other.event_index = 5;
        assert_ne!(market_order_id(&one), market_order_id(&other));
        other = one.clone();
        other.checkpoint = 8;
        assert!(market_order_id(&other) > market_order_id(&one));
    }

    #[test]
    fn a_fill_is_the_crossing_part_of_the_one_order_its_transaction_posted() {
        // A limit buy of 1 that took 0.4 and rested 0.6.
        let mut changes = vec![
            posted(1, false, "9", "0.6"),
            fill(1, false, "0.4", "taker", "trade"),
        ];
        attribute_taker_fills(&mut changes, 0, "digest");

        let [order] = orders(&changes)[..] else {
            panic!("no order is added");
        };
        assert_eq!(
            (&order.size, &order.filled, &order.remaining),
            (&dec("1.0"), &dec("0.4"), &dec("0.6"))
        );
        assert_eq!(
            (order.kind.as_str(), order.status.as_str()),
            ("limit", "open")
        );
        let fill = fills(&changes)[0];
        assert_eq!(fill.order_id, Some(dec("9")));
        assert_eq!(fill.client_order_id, Some(dec("42")));
    }

    #[test]
    fn a_fill_is_not_guessed_into_one_of_several_orders_or_another_side_or_account() {
        for others in [
            vec![posted(1, false, "8", "1"), posted(1, false, "9", "1")],
            vec![posted(1, true, "9", "1")],
            vec![posted(2, false, "9", "1")],
        ] {
            let count = others.len();
            let mut changes = others;
            changes.push(fill(1, false, "0.4", "taker", "trade"));
            attribute_taker_fills(&mut changes, 0, "digest");
            let orders = orders(&changes);
            assert_eq!(orders.len(), count + 1);
            assert!(orders[..count].iter().all(|o| o.filled == dec("0")));
            assert_eq!(orders[count].kind, "market");
        }
    }

    #[test]
    fn only_taker_trades_of_this_transaction_are_attributed() {
        let mut changes = vec![
            // An earlier transaction's fill, already handled.
            fill(1, false, "0.1", "taker", "trade"),
            fill(1, true, "0.2", "maker", "trade"),
            fill(1, true, "0.3", "taker", "liquidated"),
            fill(1, true, "0.4", "taker", "trade"),
        ];
        attribute_taker_fills(&mut changes, 1, "digest");
        let ids: Vec<bool> = fills(&changes)
            .iter()
            .map(|fill| fill.order_id.is_some())
            .collect();
        assert_eq!(ids, [false, false, false, true]);
        assert_eq!(orders(&changes).len(), 1);
    }
}
