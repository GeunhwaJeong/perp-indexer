// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What connects the feed to the subscribers.
//!
//! The feed publishes rounds: everything that changed over a range of checkpoints. The hub
//! applies each round to a copy of the public state it keeps in memory (markets, books, recent
//! trades) and then broadcasts it. A new subscription takes its initial data from that copy, or
//! from the database, and then applies the rounds after it.
//!
//! The order matters. The copy is updated before the round is broadcast, and the database is
//! always at least as new as the last round, so initial data read after subscribing is never
//! older than a round the subscriber has not been sent. A round at or below the checkpoint of
//! the initial data is skipped; everything else in a round is either state that can be applied
//! twice or an event tagged with its checkpoint.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

use bigdecimal::{BigDecimal, Zero};
use serde_json::value::RawValue;
use tokio::sync::broadcast;

use crate::db::{CapRow, TransferRow};
use crate::decimal::plain;
use crate::metrics::ApiMetrics;
use crate::model::{
    AssetPosition, Fill, Order, Orderbook, PerpetualMarket, PerpetualPosition, PriceLevel,
};

/// Trades kept per market for new subscribers.
pub const RECENT_TRADES: usize = 100;

/// One side of a book: total size by price.
type Side = BTreeMap<BigDecimal, BigDecimal>;

#[derive(Clone, Debug, Default)]
pub struct Book {
    bids: Side,
    asks: Side,
}

impl Book {
    /// Sets the total size resting at `price`. Zero removes the level.
    pub fn set(&mut self, is_ask: bool, price: BigDecimal, size: BigDecimal) {
        let side = if is_ask {
            &mut self.asks
        } else {
            &mut self.bids
        };
        if size.is_zero() {
            side.remove(&price);
        } else {
            side.insert(price, size);
        }
    }

    /// The best `depth` levels of each side, best first.
    pub fn snapshot(&self, depth: usize) -> Orderbook {
        let level = |(price, size): (&BigDecimal, &BigDecimal)| PriceLevel {
            price: plain(price),
            size: plain(size),
        };
        Orderbook {
            bids: self.bids.iter().rev().take(depth).map(level).collect(),
            asks: self.asks.iter().take(depth).map(level).collect(),
        }
    }
}

/// A trade on the tape, serialized once for everyone it is sent to.
#[derive(Clone, Debug)]
pub struct TradeItem {
    pub checkpoint: i64,
    pub json: Arc<RawValue>,
}

/// The state every client sees, as of `checkpoint`.
#[derive(Clone, Debug, Default)]
pub struct PublicState {
    pub checkpoint: i64,
    pub timestamp_ms: i64,
    /// By ticker.
    pub markets: BTreeMap<String, PerpetualMarket>,
    /// By ticker.
    pub books: HashMap<String, Book>,
    /// By ticker, newest first.
    pub trades: HashMap<String, VecDeque<TradeItem>>,
}

/// What a round changes in an account, grouped by the child subaccount it shows up in. Child
/// numbers are those of parent subaccount 0.
#[derive(Clone, Debug, Default)]
pub struct AccountUpdate {
    pub children: BTreeMap<i64, ChildUpdate>,
    /// Deposits and withdrawals, with the checkpoint each happened at.
    pub transfers: Vec<TransferRow>,
}

#[derive(Clone, Debug, Default)]
pub struct ChildUpdate {
    pub asset_positions: Vec<AssetPosition>,
    pub perpetual_positions: Vec<PerpetualPosition>,
    pub orders: Vec<Order>,
    /// With the checkpoint each happened at.
    pub fills: Vec<(i64, Fill)>,
}

/// Everything that changed over the checkpoints `(lo, checkpoint]`.
#[derive(Debug, Default)]
pub struct Round {
    pub lo: i64,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
    /// The update for the markets channel, if any market changed.
    pub markets: Option<Box<RawValue>>,
    /// By ticker: the levels that changed, with their sizes now.
    pub books: HashMap<String, Box<RawValue>>,
    /// By ticker, oldest first.
    pub trades: HashMap<String, Vec<TradeItem>>,
    /// By `<ticker>/<resolution>`: the candles that changed, as they are now.
    pub candles: HashMap<String, Vec<Box<RawValue>>>,
    /// By account ID, for the accounts someone is subscribed to.
    pub accounts: HashMap<i64, AccountUpdate>,
    /// Capabilities over accounts that changed hands.
    pub caps: Vec<CapRow>,
}

/// What the feed tells every connection.
#[derive(Clone, Debug)]
pub enum Event {
    Round(Arc<Round>),
    /// The feed could not bridge a gap with updates: every subscription has to start over.
    Reset,
}

/// How a round changes the public state.
#[derive(Debug, Default)]
pub struct PublicUpdate {
    /// The full set of markets, when any changed.
    pub markets: Option<BTreeMap<String, PerpetualMarket>>,
    /// `(ticker, is_ask, price, size)`.
    pub levels: Vec<(String, bool, BigDecimal, BigDecimal)>,
    /// `(ticker, trade)`, oldest first.
    pub trades: Vec<(String, TradeItem)>,
}

pub struct Hub {
    public: RwLock<Option<PublicState>>,
    events: broadcast::Sender<Event>,
    /// How many subscriptions watch each account.
    watched: Mutex<HashMap<i64, usize>>,
    pub metrics: ApiMetrics,
}

impl Hub {
    /// `backlog` is how many rounds a connection may fall behind before it is dropped.
    pub fn new(metrics: ApiMetrics, backlog: usize) -> Arc<Self> {
        let (events, _) = broadcast::channel(backlog);
        Arc::new(Self {
            public: RwLock::new(None),
            events,
            watched: Mutex::new(HashMap::new()),
            metrics,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// The public state, or `None` until the feed has loaded it.
    pub fn public(&self) -> Option<PublicView<'_>> {
        let guard = self.public.read().unwrap();
        guard.is_some().then_some(PublicView(guard))
    }

    /// The checkpoint the public state is at, if it has been loaded.
    pub fn checkpoint(&self) -> Option<(i64, i64)> {
        self.public().map(|p| (p.checkpoint, p.timestamp_ms))
    }

    /// Replaces the public state and makes every connection start over.
    pub fn reset(&self, state: PublicState) {
        *self.public.write().unwrap() = Some(state);
        // No receivers is not an error: nobody is connected.
        let _ = self.events.send(Event::Reset);
    }

    /// Applies a round to the public state, then sends it to every connection.
    pub fn publish(&self, round: Round, update: PublicUpdate) {
        {
            let mut guard = self.public.write().unwrap();
            let state = guard
                .as_mut()
                .expect("published before the state was loaded");
            state.checkpoint = round.checkpoint;
            state.timestamp_ms = round.timestamp_ms;
            if let Some(markets) = update.markets {
                state.markets = markets;
            }
            for (ticker, is_ask, price, size) in update.levels {
                state
                    .books
                    .entry(ticker)
                    .or_default()
                    .set(is_ask, price, size);
            }
            for (ticker, trade) in update.trades {
                let trades = state.trades.entry(ticker).or_default();
                trades.push_front(trade);
                trades.truncate(RECENT_TRADES);
            }
        }
        let _ = self.events.send(Event::Round(Arc::new(round)));
    }

    /// Registers interest in an account's updates until the guard is dropped.
    pub fn watch(self: &Arc<Self>, account_id: i64) -> Watch {
        *self.watched.lock().unwrap().entry(account_id).or_default() += 1;
        Watch {
            hub: self.clone(),
            account_id,
        }
    }

    /// The accounts someone is subscribed to.
    pub fn watched(&self) -> Vec<i64> {
        self.watched.lock().unwrap().keys().copied().collect()
    }
}

pub struct PublicView<'a>(RwLockReadGuard<'a, Option<PublicState>>);

impl std::ops::Deref for PublicView<'_> {
    type Target = PublicState;

    fn deref(&self) -> &PublicState {
        self.0.as_ref().expect("checked when the view was taken")
    }
}

/// Keeps an account watched while a subscription to it is alive.
pub struct Watch {
    hub: Arc<Hub>,
    pub account_id: i64,
}

impl Drop for Watch {
    fn drop(&mut self) {
        let mut watched = self.hub.watched.lock().unwrap();
        if let Some(count) = watched.get_mut(&self.account_id) {
            *count -= 1;
            if *count == 0 {
                watched.remove(&self.account_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use prometheus::Registry;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn book_levels_are_replaced_removed_and_read_best_first() {
        let mut book = Book::default();
        book.set(false, dec("99"), dec("1"));
        book.set(false, dec("98.5"), dec("2"));
        book.set(true, dec("101"), dec("3"));
        book.set(true, dec("100.5"), dec("4"));
        // The same price at another scale is the same level.
        book.set(true, dec("101.000"), dec("5"));
        book.set(false, dec("98.5"), dec("0"));

        let snapshot = book.snapshot(10);
        let levels = |levels: &[PriceLevel]| {
            levels
                .iter()
                .map(|l| format!("{}@{}", l.size, l.price))
                .collect::<Vec<_>>()
        };
        assert_eq!(levels(&snapshot.bids), ["1@99"]);
        assert_eq!(levels(&snapshot.asks), ["4@100.5", "5@101"]);
        assert_eq!(book.snapshot(1).asks.len(), 1);
    }

    #[test]
    fn rounds_reach_the_state_before_they_reach_subscribers() {
        let hub = Hub::new(ApiMetrics::new(&Registry::new()), 8);
        assert!(hub.public().is_none());
        hub.reset(PublicState {
            checkpoint: 10,
            ..PublicState::default()
        });
        let mut events = hub.subscribe();

        let trade = |checkpoint| TradeItem {
            checkpoint,
            json: RawValue::from_string("{}".to_owned()).unwrap().into(),
        };
        let round = Round {
            lo: 10,
            checkpoint: 12,
            ..Round::default()
        };
        let update = PublicUpdate {
            markets: None,
            levels: vec![("BTC-USD".to_owned(), true, dec("101"), dec("2"))],
            trades: vec![
                ("BTC-USD".to_owned(), trade(11)),
                ("BTC-USD".to_owned(), trade(12)),
            ],
        };
        hub.publish(round, update);

        let public = hub.public().unwrap();
        assert_eq!(public.checkpoint, 12);
        assert_eq!(public.books["BTC-USD"].snapshot(5).asks.len(), 1);
        // Newest first.
        assert_eq!(public.trades["BTC-USD"][0].checkpoint, 12);
        assert!(matches!(events.try_recv(), Ok(Event::Round(r)) if r.checkpoint == 12));
    }

    #[test]
    fn accounts_stay_watched_while_any_subscription_holds_them() {
        let hub = Hub::new(ApiMetrics::new(&Registry::new()), 8);
        let first = hub.watch(7);
        let second = hub.watch(7);
        assert_eq!(hub.watched(), [7]);
        drop(first);
        assert_eq!(hub.watched(), [7]);
        drop(second);
        assert!(hub.watched().is_empty());
    }
}
