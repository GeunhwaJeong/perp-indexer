// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The WebSocket: the dYdX v4 indexer protocol over the hub's rounds.
//!
//! A client subscribes to channels and gets, for each, a `subscribed` message with the
//! channel's state followed by `channel_data` (or, when it asked for batches,
//! `channel_batch_data`) messages with the changes. Every message of a connection carries a
//! `message_id` one above the last, which is how clients detect a gap.
//!
//! One task owns each connection. It never queues without bound: rounds arrive over a bounded
//! broadcast channel, and a client that cannot keep up is disconnected and starts over, as is
//! one that stops reading. A peer that has gone without closing is found by the pings it leaves
//! unanswered.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use haneul_pg_db::Db;
use serde::{Deserialize, Serialize};
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Map, Value, json};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::config::{Deployment, canonical_address};
use crate::db;
use crate::hub::{AccountUpdate, Event, Hub, Round, Watch};
use crate::snapshot::{self, AccountSnapshot};
use crate::views::{candle_object, resolution_ms, transfer_object};

const MARKETS: &str = "v4_markets";
const ORDERBOOK: &str = "v4_orderbook";
const TRADES: &str = "v4_trades";
const CANDLES: &str = "v4_candles";
const SUBACCOUNTS: &str = "v4_parent_subaccounts";

/// The version clients are told the channel data has.
const DATA_VERSION: &str = "1.0.0";

/// dYdX allows 128 parent subaccounts per address.
const MAX_PARENT_SUBACCOUNT: i64 = 127;

/// Candles sent when subscribing to a candle channel.
const INITIAL_CANDLES: i64 = 100;

#[derive(Clone, Debug)]
pub struct WsConfig {
    /// Connections served at once. Past it, upgrades are refused.
    pub max_connections: usize,
    /// Subscriptions one connection may hold.
    pub max_subscriptions: usize,
    /// Levels per side sent when subscribing to a book.
    pub book_depth: usize,
    /// How long a write may take before the client is considered gone.
    pub send_timeout: Duration,
    pub ping_interval: Duration,
    /// How long a ping may go unanswered before the peer is considered gone.
    pub pong_timeout: Duration,
    /// Rounds held for a subscription whose initial data is still loading.
    pub max_buffered_rounds: usize,
    /// Messages a client may send per second, and how many it may send at once after a pause.
    /// A client only ever subscribes and unsubscribes, so anything past this is abuse.
    pub message_rate: f64,
    pub message_burst: u32,
}

pub struct WsContext {
    pub hub: Arc<Hub>,
    pub db: Db,
    pub deployment: Arc<Deployment>,
    pub config: WsConfig,
    pub cancel: CancellationToken,
}

#[derive(Debug, Deserialize)]
struct ClientMessage {
    #[serde(rename = "type")]
    kind: String,
    channel: Option<String>,
    id: Option<String>,
    #[serde(default)]
    batched: bool,
}

/// A message to the client. Fields that do not apply are left out.
#[derive(Serialize)]
struct ServerMessage<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    connection_id: &'a str,
    message_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
    #[serde(rename = "subaccountNumber", skip_serializing_if = "Option::is_none")]
    subaccount_number: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contents: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

/// What a subscription is to.
#[derive(Clone, Debug)]
enum Target {
    Markets,
    Orderbook {
        ticker: String,
    },
    Trades {
        ticker: String,
    },
    Candles {
        ticker: String,
        market: String,
        resolution_ms: i64,
    },
    Subaccount {
        address: String,
        parent: i64,
    },
}

impl Target {
    fn channel(&self) -> &'static str {
        match self {
            Target::Markets => MARKETS,
            Target::Orderbook { .. } => ORDERBOOK,
            Target::Trades { .. } => TRADES,
            Target::Candles { .. } => CANDLES,
            Target::Subaccount { .. } => SUBACCOUNTS,
        }
    }
}

enum Progress {
    /// Initial data is being read; rounds that arrive meanwhile wait here.
    Loading { buffered: Vec<Arc<Round>> },
    /// Rounds above `since` are sent.
    Live { since: i64 },
}

struct Subscription {
    target: Target,
    batched: bool,
    progress: Progress,
    /// Told to the client already. A subscription to an address without an account is live
    /// from the start, and loads again once the account appears.
    announced: bool,
    /// The account behind a subaccount subscription, watched for as long as it lives.
    account: Option<Watch>,
    /// Tells apart the results of loads started for different subscriptions to the same key.
    generation: u64,
}

/// The initial data of a subscription that reads the database.
enum Loaded {
    Candles {
        checkpoint: i64,
        contents: Box<RawValue>,
    },
    Account {
        snapshot: AccountSnapshot,
        /// Taken before the snapshot was read. See [`load_account`].
        watch: Option<Watch>,
    },
}

struct LoadResult {
    key: Key,
    generation: u64,
    result: anyhow::Result<Loaded>,
    started: Instant,
}

/// `(channel, id)`; the markets channel has no ID.
type Key = (&'static str, Option<String>);

/// Why the server ends a connection.
enum Close {
    /// The client left, or the socket failed.
    Gone,
    /// The client did not read fast enough to keep up with the rounds.
    Lagged,
    /// Every subscription has to start over.
    Reset,
    /// An account the client is subscribed to changed hands.
    AccountMoved,
    /// The client sent more messages than it is allowed.
    RateLimited,
    /// The peer left a ping unanswered.
    Unresponsive,
    Shutdown,
}

impl Close {
    fn reason(&self) -> &'static str {
        match self {
            Close::Gone => "gone",
            Close::Lagged => "lagged",
            Close::Reset => "reset",
            Close::AccountMoved => "account_moved",
            Close::RateLimited => "rate_limited",
            Close::Unresponsive => "unresponsive",
            Close::Shutdown => "shutdown",
        }
    }
}

/// A budget of messages that refills at a steady rate up to a burst.
struct Budget {
    tokens: f64,
    rate: f64,
    burst: f64,
    refilled: Instant,
}

impl Budget {
    fn new(rate: f64, burst: u32) -> Self {
        Self {
            tokens: f64::from(burst),
            rate,
            burst: f64::from(burst),
            refilled: Instant::now(),
        }
    }

    /// Takes one message out of the budget as of `now`. `false` when there is none left.
    fn take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.refilled).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.refilled = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

/// Tells a peer that has gone from one that is merely quiet.
///
/// A connection whose peer vanished without closing (a dropped network, a suspended laptop)
/// looks idle, and a write to it succeeds for as long as the kernel has room to buffer it. Only
/// a ping left unanswered shows that nobody is there.
struct Heartbeat {
    timeout: Duration,
    /// When the oldest ping still unanswered was sent.
    awaiting_since: Option<Instant>,
}

impl Heartbeat {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            awaiting_since: None,
        }
    }

    fn ping_sent(&mut self, now: Instant) {
        self.awaiting_since.get_or_insert(now);
    }

    /// Any pong will do: it shows the peer is reading and writing.
    fn pong_received(&mut self) {
        self.awaiting_since = None;
    }

    /// When the peer is given up on, if a ping is outstanding.
    fn deadline(&self) -> Option<Instant> {
        self.awaiting_since.map(|sent| sent + self.timeout)
    }
}

static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);

struct Connection {
    ctx: Arc<WsContext>,
    socket: WebSocket,
    id: String,
    next_message_id: u64,
    subscriptions: HashMap<Key, Subscription>,
    generation: u64,
    loads: mpsc::Sender<LoadResult>,
    budget: Budget,
    heartbeat: Heartbeat,
}

pub async fn serve(socket: WebSocket, ctx: Arc<WsContext>) {
    // Subscribing before anything else means no round is missed between a subscription's
    // initial data and its updates.
    let mut events = ctx.hub.subscribe();
    let (loads, mut loaded) = mpsc::channel(ctx.config.max_subscriptions.max(1));
    let started = std::process::id();
    let mut connection = Connection {
        id: format!(
            "{started:x}-{:x}",
            NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed)
        ),
        ctx: ctx.clone(),
        socket,
        next_message_id: 0,
        subscriptions: HashMap::new(),
        generation: 0,
        loads,
        budget: Budget::new(ctx.config.message_rate, ctx.config.message_burst),
        heartbeat: Heartbeat::new(ctx.config.pong_timeout),
    };

    ctx.hub.metrics.ws_connections.inc();
    let close = connection.run(&mut events, &mut loaded).await;
    ctx.hub.metrics.ws_connections.dec();
    for subscription in connection.subscriptions.values() {
        let channel = subscription.target.channel();
        ctx.hub
            .metrics
            .ws_subscriptions
            .with_label_values(&[channel])
            .dec();
    }
    ctx.hub
        .metrics
        .ws_closed
        .with_label_values(&[close.reason()])
        .inc();
    debug!(
        connection = connection.id,
        reason = close.reason(),
        "Connection closed"
    );

    let frame = match close {
        Close::Gone | Close::Unresponsive => return,
        Close::Shutdown => (close_code::AWAY, "server shutting down"),
        Close::Lagged => (close_code::POLICY, "client too slow"),
        Close::RateLimited => (close_code::POLICY, "too many messages"),
        Close::Reset | Close::AccountMoved => (close_code::RESTART, "resubscribe"),
    };
    let frame = CloseFrame {
        code: frame.0,
        reason: frame.1.into(),
    };
    let goodbye = async {
        if connection
            .socket
            .send(Message::Close(Some(frame)))
            .await
            .is_ok()
        {
            // Read on until the client closes its side. A socket closed with unread data is
            // reset, and the reset would throw away the close frame before the client saw it.
            while let Some(Ok(_)) = connection.socket.recv().await {}
        }
    };
    let _ = tokio::time::timeout(ctx.config.send_timeout, goodbye).await;
}

impl Connection {
    async fn run(
        &mut self,
        events: &mut broadcast::Receiver<Event>,
        loaded: &mut mpsc::Receiver<LoadResult>,
    ) -> Close {
        if let Err(close) = self.send("connected", None, None, None, None, None).await {
            return close;
        }
        let ctx = self.ctx.clone();
        let mut ping = tokio::time::interval(ctx.config.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await;

        loop {
            let unanswered = self.heartbeat.deadline();
            let step = tokio::select! {
                // In order, so that a pong already received is seen before the deadline it
                // answers: after the process itself was stalled, both are ready at once.
                biased;
                _ = ctx.cancel.cancelled() => Err(Close::Shutdown),
                message = self.socket.recv() => match message {
                    Some(Ok(Message::Text(text))) => self.on_client_message(&text).await,
                    Some(Ok(Message::Pong(_))) => {
                        self.heartbeat.pong_received();
                        Ok(())
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => Err(Close::Gone),
                    // Pings are answered by the socket itself; binary frames mean nothing here.
                    Some(Ok(_)) => Ok(()),
                },
                event = events.recv() => match event {
                    Ok(Event::Round(round)) => self.on_round(round).await,
                    Ok(Event::Reset) => Err(Close::Reset),
                    Err(broadcast::error::RecvError::Lagged(_)) => Err(Close::Lagged),
                    Err(broadcast::error::RecvError::Closed) => Err(Close::Shutdown),
                },
                Some(result) = loaded.recv() => self.on_loaded(result).await,
                _ = ping.tick() => {
                    self.heartbeat.ping_sent(Instant::now());
                    self.write(Message::Ping(Default::default())).await
                }
                _ = sleep_until(unanswered), if unanswered.is_some() => Err(Close::Unresponsive),
            };
            if let Err(close) = step {
                return close;
            }
        }
    }

    async fn write(&mut self, message: Message) -> Result<(), Close> {
        let timeout = self.ctx.config.send_timeout;
        match tokio::time::timeout(timeout, self.socket.send(message)).await {
            Ok(Ok(())) => Ok(()),
            // A failed or stalled write: the peer is gone or not reading.
            Ok(Err(_)) | Err(_) => Err(Close::Gone),
        }
    }

    async fn send(
        &mut self,
        kind: &str,
        channel: Option<&str>,
        id: Option<&str>,
        subaccount_number: Option<i64>,
        contents: Option<&RawValue>,
        message: Option<&str>,
    ) -> Result<(), Close> {
        let is_data = matches!(kind, "channel_data" | "channel_batch_data");
        let text = serde_json::to_string(&ServerMessage {
            kind,
            connection_id: &self.id,
            message_id: self.next_message_id,
            channel,
            id,
            version: is_data.then_some(DATA_VERSION),
            subaccount_number,
            contents,
            message,
        })
        .expect("messages serialize");
        self.next_message_id += 1;
        if let Some(channel) = channel {
            let metrics = &self.ctx.hub.metrics;
            metrics.ws_messages_sent.with_label_values(&[channel]).inc();
        }
        self.write(Message::Text(text.into())).await
    }

    async fn error(&mut self, message: &str, key: Option<&Key>) -> Result<(), Close> {
        let (channel, id) = match key {
            Some((channel, id)) => (Some(*channel), id.as_deref()),
            None => (None, None),
        };
        self.send("error", channel, id, None, None, Some(message))
            .await
    }

    /// Sends channel data: one batch message, or one message per item.
    async fn send_data(
        &mut self,
        key: &Key,
        batched: bool,
        subaccount_number: Option<i64>,
        items: &[Box<RawValue>],
    ) -> Result<(), Close> {
        if items.is_empty() {
            return Ok(());
        }
        let (channel, id) = (Some(key.0), key.1.as_deref());
        if batched {
            let contents = to_raw_value(items).expect("raw values serialize");
            let contents = Some(&*contents);
            self.send(
                "channel_batch_data",
                channel,
                id,
                subaccount_number,
                contents,
                None,
            )
            .await
        } else {
            for item in items {
                self.send(
                    "channel_data",
                    channel,
                    id,
                    subaccount_number,
                    Some(item),
                    None,
                )
                .await?;
            }
            Ok(())
        }
    }

    async fn on_client_message(&mut self, text: &str) -> Result<(), Close> {
        if !self.budget.take(Instant::now()) {
            return Err(Close::RateLimited);
        }
        let Ok(message) = serde_json::from_str::<ClientMessage>(text) else {
            return self.error("Invalid message: could not parse", None).await;
        };
        match message.kind.as_str() {
            "ping" => self.send("pong", None, None, None, None, None).await,
            "subscribe" => self.subscribe(message).await,
            "unsubscribe" => self.unsubscribe(message).await,
            other => {
                let text = format!("Invalid message type: {other}");
                self.error(&text, None).await
            }
        }
    }

    /// The subscription a client message names, or the error to answer it with.
    fn target(&self, message: &ClientMessage) -> Result<(Key, Target), String> {
        let deployment = &self.ctx.deployment;
        let channel = message.channel.as_deref().unwrap_or_default();
        let id = message.id.as_deref();
        let market = |ticker: &str| {
            deployment.market(ticker).map(str::to_owned).ok_or_else(|| {
                format!("Invalid subscribe message: unknown market ({channel}-{ticker})")
            })
        };
        let need_id =
            || id.ok_or_else(|| format!("Invalid subscribe message: missing id ({channel})"));

        let (channel, target) = match channel {
            MARKETS => return Ok(((MARKETS, None), Target::Markets)),
            ORDERBOOK => {
                let ticker = need_id()?;
                market(ticker)?;
                (
                    ORDERBOOK,
                    Target::Orderbook {
                        ticker: ticker.to_owned(),
                    },
                )
            }
            TRADES => {
                let ticker = need_id()?;
                market(ticker)?;
                (
                    TRADES,
                    Target::Trades {
                        ticker: ticker.to_owned(),
                    },
                )
            }
            CANDLES => {
                let id = need_id()?;
                let parsed = id
                    .rsplit_once('/')
                    .and_then(|(ticker, resolution)| Some((ticker, resolution_ms(resolution)?)));
                let Some((ticker, resolution_ms)) = parsed else {
                    return Err(format!(
                        "Invalid subscribe message: invalid id ({channel}-{id})"
                    ));
                };
                let target = Target::Candles {
                    market: market(ticker)?,
                    ticker: ticker.to_owned(),
                    resolution_ms,
                };
                (CANDLES, target)
            }
            SUBACCOUNTS => {
                let id = need_id()?;
                let parsed = id.split_once('/').and_then(|(address, parent)| {
                    let parent = parent.parse::<i64>().ok()?;
                    let address = canonical_address(address).ok()?;
                    (0..=MAX_PARENT_SUBACCOUNT)
                        .contains(&parent)
                        .then_some((address, parent))
                });
                let Some((address, parent)) = parsed else {
                    return Err(format!(
                        "Invalid subscribe message: invalid id ({channel}-{id})"
                    ));
                };
                (SUBACCOUNTS, Target::Subaccount { address, parent })
            }
            other => return Err(format!("Invalid channel: {other}")),
        };
        Ok(((channel, id.map(str::to_owned)), target))
    }

    async fn subscribe(&mut self, message: ClientMessage) -> Result<(), Close> {
        let (key, target) = match self.target(&message) {
            Ok(found) => found,
            Err(text) => return self.error(&text, None).await,
        };
        if self.subscriptions.contains_key(&key) {
            // The front end matches on this text, with the channel standing in for a missing ID.
            let id = key.1.as_deref().unwrap_or(key.0);
            let text = format!(
                "Invalid subscribe message: already subscribed ({}-{id})",
                key.0
            );
            return self.error(&text, Some(&key)).await;
        }
        if self.subscriptions.len() >= self.ctx.config.max_subscriptions {
            return self
                .error(
                    "Invalid subscribe message: too many subscriptions",
                    Some(&key),
                )
                .await;
        }

        self.generation += 1;
        let mut subscription = Subscription {
            target: target.clone(),
            batched: message.batched,
            progress: Progress::Loading { buffered: vec![] },
            announced: false,
            account: None,
            generation: self.generation,
        };
        let metrics = &self.ctx.hub.metrics;
        metrics.ws_subscriptions.with_label_values(&[key.0]).inc();

        // Markets, books and the tape are answered from the hub's copy of the public state.
        let ready = match &target {
            Target::Markets | Target::Orderbook { .. } | Target::Trades { .. } => {
                Some(self.public_snapshot(&target))
            }
            Target::Candles { .. } | Target::Subaccount { .. } => None,
        };
        match ready {
            Some(Some((checkpoint, contents))) => {
                subscription.progress = Progress::Live { since: checkpoint };
                subscription.announced = true;
                self.subscriptions.insert(key.clone(), subscription);
                self.send(
                    "subscribed",
                    Some(key.0),
                    key.1.as_deref(),
                    None,
                    Some(&contents),
                    None,
                )
                .await
            }
            Some(None) => {
                metrics.ws_subscriptions.with_label_values(&[key.0]).dec();
                self.fetch_failed(&key).await
            }
            None => {
                let generation = subscription.generation;
                self.subscriptions.insert(key.clone(), subscription);
                self.load(key, target, generation);
                Ok(())
            }
        }
    }

    /// The text the front end retries a subscription on.
    async fn fetch_failed(&mut self, key: &Key) -> Result<(), Close> {
        let id = key.1.as_deref().unwrap_or(key.0);
        let text = format!(
            "Internal error, could not fetch data for subscription: {}-{id}",
            key.0
        );
        self.error(&text, Some(key)).await
    }

    /// Initial data from the hub's copy of the public state, with the checkpoint it is at.
    /// `None` until the feed has loaded that state.
    fn public_snapshot(&self, target: &Target) -> Option<(i64, Box<RawValue>)> {
        let public = self.ctx.hub.public()?;
        let contents = match target {
            Target::Markets => to_raw_value(&json!({"markets": public.markets})),
            Target::Orderbook { ticker } => {
                let depth = self.ctx.config.book_depth;
                let book = public.books.get(ticker).map(|book| book.snapshot(depth));
                to_raw_value(&book.unwrap_or_default())
            }
            Target::Trades { ticker } => {
                let trades: Vec<&RawValue> = public
                    .trades
                    .get(ticker)
                    .map(|trades| trades.iter().map(|trade| &*trade.json).collect())
                    .unwrap_or_default();
                to_raw_value(&json!({"trades": trades}))
            }
            Target::Candles { .. } | Target::Subaccount { .. } => return None,
        };
        Some((
            public.checkpoint,
            contents.expect("public state serializes"),
        ))
    }

    /// Reads a subscription's initial data from the database, off the connection's task.
    fn load(&self, key: Key, target: Target, generation: u64) {
        let ctx = self.ctx.clone();
        let loads = self.loads.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let result = match &target {
                Target::Candles {
                    ticker,
                    market,
                    resolution_ms,
                } => load_candles(&ctx, ticker, market, *resolution_ms).await,
                Target::Subaccount { address, parent } => {
                    load_account(&ctx, address, *parent).await
                }
                _ => unreachable!("only candles and subaccounts load from the database"),
            };
            // The connection may be gone by now.
            let _ = loads
                .send(LoadResult {
                    key,
                    generation,
                    result,
                    started,
                })
                .await;
        });
    }

    async fn on_loaded(&mut self, load: LoadResult) -> Result<(), Close> {
        let LoadResult {
            key,
            generation,
            result,
            started,
        } = load;
        // The client may have unsubscribed, or subscribed again, while the data was read.
        let Some(subscription) = self.subscriptions.get_mut(&key) else {
            return Ok(());
        };
        if subscription.generation != generation {
            return Ok(());
        }
        let metrics = &self.ctx.hub.metrics;
        metrics
            .ws_snapshot_seconds
            .with_label_values(&[key.0])
            .observe(started.elapsed().as_secs_f64());

        let loaded = match result {
            Ok(loaded) => loaded,
            Err(e) => {
                warn!(channel = key.0, "Loading a subscription failed: {e:#}");
                let announced = subscription.announced;
                self.subscriptions.remove(&key);
                metrics.ws_subscriptions.with_label_values(&[key.0]).dec();
                // A subscription the client already has cannot be failed quietly.
                return if announced {
                    Err(Close::Reset)
                } else {
                    self.fetch_failed(&key).await
                };
            }
        };

        let Progress::Loading { buffered } =
            std::mem::replace(&mut subscription.progress, Progress::Live { since: 0 })
        else {
            return Ok(());
        };
        let (batched, announced) = (subscription.batched, subscription.announced);
        let since = match loaded {
            Loaded::Candles {
                checkpoint,
                contents,
            } => {
                subscription.announced = true;
                subscription.progress = Progress::Live { since: checkpoint };
                self.send(
                    "subscribed",
                    Some(key.0),
                    key.1.as_deref(),
                    None,
                    Some(&contents),
                    None,
                )
                .await?;
                checkpoint
            }
            Loaded::Account { snapshot, watch } => {
                let checkpoint = snapshot.watermark.checkpoint;
                let Target::Subaccount { address, parent } = subscription.target.clone() else {
                    return Ok(());
                };
                subscription.account = watch;
                subscription.announced = true;
                subscription.progress = Progress::Live { since: checkpoint };

                if !announced {
                    let contents = match &snapshot.account {
                        // An address without an account gets an empty answer and stays
                        // subscribed: the account may be created later.
                        None => json!({}),
                        Some(account) => json!({
                            "subaccount": account.subaccount(&address, parent, checkpoint),
                            "blockHeight": checkpoint.to_string(),
                            "orders": shifted_orders(&account.open_orders, parent),
                        }),
                    };
                    let contents = to_raw_value(&contents).expect("subaccount serializes");
                    self.send(
                        "subscribed",
                        Some(key.0),
                        key.1.as_deref(),
                        None,
                        Some(&contents),
                        None,
                    )
                    .await?;
                } else if let Some(account) = &snapshot.account {
                    // The account appeared after the client was told there was none: its
                    // state goes out as updates.
                    let update = account.as_update();
                    self.send_account(&key, batched, &address, parent, checkpoint, 0, &update)
                        .await?;
                }
                checkpoint
            }
        };

        // Rounds that arrived while loading, minus what the initial data already covers.
        for round in buffered {
            if round.checkpoint > since {
                self.send_round(&key, &round, since).await?;
            }
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, message: ClientMessage) -> Result<(), Close> {
        let (key, _) = match self.target(&message) {
            Ok(found) => found,
            Err(text) => return self.error(&text, None).await,
        };
        if self.subscriptions.remove(&key).is_some() {
            let metrics = &self.ctx.hub.metrics;
            metrics.ws_subscriptions.with_label_values(&[key.0]).dec();
        }
        self.send(
            "unsubscribed",
            Some(key.0),
            key.1.as_deref(),
            None,
            None,
            None,
        )
        .await
    }

    async fn on_round(&mut self, round: Arc<Round>) -> Result<(), Close> {
        let keys: Vec<Key> = self.subscriptions.keys().cloned().collect();
        for key in keys {
            let max_buffered = self.ctx.config.max_buffered_rounds;
            let Some(subscription) = self.subscriptions.get_mut(&key) else {
                continue;
            };
            match &mut subscription.progress {
                Progress::Loading { buffered } => {
                    if buffered.len() >= max_buffered {
                        return Err(Close::Lagged);
                    }
                    buffered.push(round.clone());
                }
                Progress::Live { since } => {
                    let since = *since;
                    if round.checkpoint > since {
                        self.send_round(&key, &round, since).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Sends what `round` holds for one subscription. `since` is the checkpoint of the
    /// subscription's initial data: events at or below it are already part of that data.
    async fn send_round(&mut self, key: &Key, round: &Round, since: i64) -> Result<(), Close> {
        let Some(subscription) = self.subscriptions.get(key) else {
            return Ok(());
        };
        let batched = subscription.batched;
        match subscription.target.clone() {
            Target::Markets => match &round.markets {
                Some(update) => {
                    self.send_data(key, batched, None, std::slice::from_ref(update))
                        .await
                }
                None => Ok(()),
            },
            Target::Orderbook { ticker } => match round.books.get(&ticker) {
                Some(update) => {
                    self.send_data(key, batched, None, std::slice::from_ref(update))
                        .await
                }
                None => Ok(()),
            },
            Target::Trades { ticker } => {
                let trades: Vec<&RawValue> = round
                    .trades
                    .get(&ticker)
                    .into_iter()
                    .flatten()
                    .filter(|trade| trade.checkpoint > since)
                    .map(|trade| &*trade.json)
                    .collect();
                if trades.is_empty() {
                    return Ok(());
                }
                let update = to_raw_value(&json!({"trades": trades})).expect("trades serialize");
                self.send_data(key, batched, None, &[update]).await
            }
            Target::Candles {
                ticker,
                resolution_ms,
                ..
            } => {
                let name = crate::views::resolution_name(resolution_ms).unwrap_or_default();
                match round.candles.get(&format!("{ticker}/{name}")) {
                    Some(candles) => self.send_data(key, batched, None, candles).await,
                    None => Ok(()),
                }
            }
            Target::Subaccount { address, parent } => {
                self.on_account_round(key, batched, &address, parent, round, since)
                    .await
            }
        }
    }

    async fn on_account_round(
        &mut self,
        key: &Key,
        batched: bool,
        address: &str,
        parent: i64,
        round: &Round,
        since: i64,
    ) -> Result<(), Close> {
        let account_id = self
            .subscriptions
            .get(key)
            .and_then(|s| s.account.as_ref())
            .map(|w| w.account_id);
        let Some(account_id) = account_id else {
            // No account yet. If the address was just given the capability of one, load it;
            // the rounds from here on wait for the load.
            let gained = round
                .caps
                .iter()
                .any(|cap| cap.role == "admin" && cap.owner.as_deref() == Some(address));
            if gained && let Some(subscription) = self.subscriptions.get_mut(key) {
                subscription.progress = Progress::Loading { buffered: vec![] };
                let (target, generation) = (subscription.target.clone(), subscription.generation);
                self.load(key.clone(), target, generation);
            }
            return Ok(());
        };

        // The account now answers to someone else: what this client holds is no longer its own.
        let moved = round.caps.iter().any(|cap| {
            cap.account_id == account_id
                && cap.role == "admin"
                && cap.owner.as_deref() != Some(address)
        });
        if moved {
            return Err(Close::AccountMoved);
        }
        match round.accounts.get(&account_id) {
            Some(update) => {
                self.send_account(
                    key,
                    batched,
                    address,
                    parent,
                    round.checkpoint,
                    since,
                    update,
                )
                .await
            }
            None => Ok(()),
        }
    }

    /// Sends an account update: one message per child subaccount it touches, since a message
    /// names the subaccount its contents belong to.
    #[allow(clippy::too_many_arguments)]
    async fn send_account(
        &mut self,
        key: &Key,
        batched: bool,
        address: &str,
        parent: i64,
        checkpoint: i64,
        since: i64,
        update: &AccountUpdate,
    ) -> Result<(), Close> {
        let height = checkpoint.to_string();
        let decimals = self.ctx.deployment.collateral_decimals;
        for (child, changes) in &update.children {
            let mut contents = Map::new();
            contents.insert("blockHeight".to_owned(), json!(height));
            let mut put = |field: &str, value: Value| {
                if value.as_array().is_some_and(|items| !items.is_empty()) {
                    contents.insert(field.to_owned(), shifted(value, parent));
                }
            };
            put("assetPositions", json!(changes.asset_positions));
            put("perpetualPositions", json!(changes.perpetual_positions));
            put("orders", json!(changes.orders));
            let fills: Vec<_> = changes
                .fills
                .iter()
                .filter(|(checkpoint, _)| *checkpoint > since)
                .map(|(_, fill)| fill)
                .collect();
            put("fills", json!(fills));
            if contents.len() == 1 {
                continue;
            }
            let item = to_raw_value(&contents).expect("account update serializes");
            self.send_data(key, batched, Some(child + parent), &[item])
                .await?;
        }

        // A message carries one transfer.
        let transfers: Vec<Box<RawValue>> = update
            .transfers
            .iter()
            .filter(|transfer| transfer.checkpoint > since)
            .map(|transfer| {
                let transfer = transfer_object(transfer, address, parent, decimals, false);
                to_raw_value(&json!({"blockHeight": height, "transfers": transfer}))
                    .expect("transfer serializes")
            })
            .collect();
        self.send_data(key, batched, Some(parent), &transfers).await
    }
}

/// Sleeps until `deadline`, or forever when there is none.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

/// Moves the subaccount numbers in a list of objects from parent subaccount 0 to `parent`.
fn shifted(mut items: Value, parent: i64) -> Value {
    if parent != 0 {
        for item in items.as_array_mut().into_iter().flatten() {
            if let Some(number) = item.get("subaccountNumber").and_then(Value::as_i64) {
                item["subaccountNumber"] = json!(number + parent);
            }
        }
    }
    items
}

fn shifted_orders(orders: &[crate::model::Order], parent: i64) -> Value {
    shifted(json!(orders), parent)
}

/// Loads the account behind a subaccount subscription.
///
/// The account is watched before its state is read. The feed decides which accounts a round
/// covers after fixing the checkpoint it reads up to, so a round either covers this account or
/// ends at a checkpoint the state read here already includes. Watching only after the read
/// would leave a window whose changes neither had.
async fn load_account(ctx: &WsContext, address: &str, parent: i64) -> anyhow::Result<Loaded> {
    let collateral = &ctx.deployment.collateral_type;
    let found = db::account_of(&mut ctx.db.connect().await?, address, collateral, parent).await?;
    let watch = found.map(|account| ctx.hub.watch(account.account_id));

    let snapshot = snapshot::account(&ctx.db, &ctx.deployment, address, parent).await?;
    let loaded = snapshot
        .account
        .as_ref()
        .map(|account| account.row.account_id);
    // The capability changed hands between the two reads. The client asks again.
    anyhow::ensure!(
        loaded == watch.as_ref().map(|watch| watch.account_id),
        "the account of {address}/{parent} changed while it was loaded"
    );
    Ok(Loaded::Account { snapshot, watch })
}

async fn load_candles(
    ctx: &WsContext,
    ticker: &str,
    market: &str,
    resolution_ms: i64,
) -> anyhow::Result<Loaded> {
    let mut snapshot = db::snapshot(&ctx.db).await?;
    let conn = snapshot.conn();
    let watermark = db::watermark(conn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the indexer has not committed anything yet"))?;
    let rows = db::candles(conn, market, resolution_ms, None, None, INITIAL_CANDLES).await?;
    snapshot.finish().await?;

    let candles: Vec<_> = rows
        .iter()
        .map(|candle| candle_object(candle, ticker, true))
        .collect();
    Ok(Loaded::Candles {
        checkpoint: watermark.checkpoint,
        contents: to_raw_value(&json!({"candles": candles}))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_budget_refills_at_its_rate_up_to_its_burst() {
        let start = Instant::now();
        let mut budget = Budget::new(2.0, 3);
        assert!((0..3).all(|_| budget.take(start)));
        assert!(!budget.take(start));
        // Half a second buys one message at two a second.
        assert!(budget.take(start + Duration::from_millis(500)));
        assert!(!budget.take(start + Duration::from_millis(500)));
        // A long pause refills the burst and no more.
        let later = start + Duration::from_secs(60);
        assert!((0..3).all(|_| budget.take(later)));
        assert!(!budget.take(later));
    }

    #[test]
    fn a_peer_is_given_up_on_when_a_ping_goes_unanswered() {
        let start = Instant::now();
        let mut heartbeat = Heartbeat::new(Duration::from_secs(10));
        assert_eq!(heartbeat.deadline(), None);

        heartbeat.ping_sent(start);
        assert_eq!(heartbeat.deadline(), Some(start + Duration::from_secs(10)));
        // A second ping does not push back the deadline of the first.
        heartbeat.ping_sent(start + Duration::from_secs(5));
        assert_eq!(heartbeat.deadline(), Some(start + Duration::from_secs(10)));

        heartbeat.pong_received();
        assert_eq!(heartbeat.deadline(), None);
        let later = start + Duration::from_secs(30);
        heartbeat.ping_sent(later);
        assert_eq!(heartbeat.deadline(), Some(later + Duration::from_secs(10)));
    }

    #[test]
    fn subaccount_numbers_follow_the_parent() {
        let items = json!([{"subaccountNumber": 128, "size": "1"}, {"id": "x"}]);
        assert_eq!(shifted(items.clone(), 0), items);
        assert_eq!(
            shifted(items, 3),
            json!([{"subaccountNumber": 131, "size": "1"}, {"id": "x"}])
        );
    }

    #[test]
    fn messages_leave_out_what_does_not_apply() {
        let contents = RawValue::from_string(r#"{"bids":[["1","2"]]}"#.to_owned()).unwrap();
        let data = serde_json::to_value(ServerMessage {
            kind: "channel_batch_data",
            connection_id: "c",
            message_id: 7,
            channel: Some(ORDERBOOK),
            id: Some("BTC-USD"),
            version: Some(DATA_VERSION),
            subaccount_number: None,
            contents: Some(&contents),
            message: None,
        })
        .unwrap();
        assert_eq!(
            data,
            json!({
                "type": "channel_batch_data",
                "connection_id": "c",
                "message_id": 7,
                "channel": "v4_orderbook",
                "id": "BTC-USD",
                "version": "1.0.0",
                "contents": {"bids": [["1", "2"]]},
            })
        );

        let connected = serde_json::to_value(ServerMessage {
            kind: "connected",
            connection_id: "c",
            message_id: 0,
            channel: None,
            id: None,
            version: None,
            subaccount_number: None,
            contents: None,
            message: None,
        })
        .unwrap();
        assert_eq!(
            connected,
            json!({"type": "connected", "connection_id": "c", "message_id": 0})
        );
    }
}
