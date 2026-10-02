// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The REST endpoints of the dYdX v4 indexer API that the front end calls.
//!
//! Account endpoints take a wallet address and a parent subaccount number, as dYdX does. The
//! number selects among the accounts the address holds an admin capability for, in the order
//! they were created; an address almost always has one, number 0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{MatchedPath, Path, Query, Request, State, WebSocketUpgrade};
use axum::http::header::CACHE_CONTROL;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use haneul_pg_db::{Connection, Db};
use serde::Deserialize;
use serde_json::{Value, json};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;

use crate::config::{Deployment, canonical_address};
use crate::db::{self, AccountRow, OrderFilter, Page, PnlFilter};
use crate::error::ApiError;
use crate::hub::Hub;
use crate::snapshot;
use crate::time::{iso, parse_iso};
use crate::views::{
    candle_object, fill_object, funding_payment_object, historical_funding_object,
    historical_pnl_tick_object, order_object, pnl_tick_object, resolution_ms, trade_history_object,
    trade_object, transfer_object,
};
use crate::ws::{self, WsContext};

/// Rows returned when the request does not say how many, and the most it may ask for.
const DEFAULT_LIMIT: i64 = 1_000;
const MAX_LIMIT: i64 = 1_000;
const DEFAULT_FUNDING_LIMIT: i64 = 100;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;

pub struct AppState {
    pub db: Db,
    pub hub: Arc<Hub>,
    pub deployment: Arc<Deployment>,
    pub ws: Arc<WsContext>,
    /// Levels per side returned for an order book.
    pub book_depth: usize,
    /// The service reports itself unhealthy when the indexed chain time is older than this.
    pub max_lag: Duration,
    /// The most rows a paged request may skip.
    pub max_pagination_offset: i64,
}

type App = State<Arc<AppState>>;
type ApiResult = Result<Json<Value>, ApiError>;

pub fn router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::OPTIONS])
        .allow_headers(Any);

    Router::new()
        .route("/v4/height", get(height))
        .route("/v4/time", get(time))
        .route("/v4/perpetualMarkets", get(perpetual_markets))
        .route("/v4/orderbooks/perpetualMarket/{ticker}", get(orderbook))
        .route("/v4/trades/perpetualMarket/{ticker}", get(trades))
        .route("/v4/candles/perpetualMarkets/{ticker}", get(candles))
        .route("/v4/sparklines", get(sparklines))
        .route("/v4/historicalFunding/{ticker}", get(historical_funding))
        .route(
            "/v4/addresses/{address}/parentSubaccountNumber/{number}",
            get(parent_subaccount),
        )
        .route("/v4/orders/parentSubaccountNumber", get(orders))
        .route("/v4/fills/parentSubaccountNumber", get(fills))
        .route("/v4/transfers/parentSubaccountNumber", get(transfers))
        .route(
            "/v4/tradeHistory/parentSubaccountNumber",
            get(trade_history),
        )
        .route(
            "/v4/fundingPayments/parentSubaccount",
            get(funding_payments),
        )
        .route("/v4/pnl/parentSubaccountNumber", get(pnl))
        .route(
            "/v4/historical-pnl/parentSubaccountNumber",
            get(historical_pnl),
        )
        // Not kept by this indexer. They answer with nothing rather than fail, so the pages
        // that ask for them still load.
        .route(
            "/v4/historicalBlockTradingRewards/{address}",
            get(async || Json(json!({"rewards": []}))),
        )
        .route(
            "/v4/historicalTradingRewardAggregations/{address}",
            get(async || Json(json!({"rewards": []}))),
        )
        .route("/v4/compliance/screen/{address}", get(compliance))
        .route("/v4/ws", get(websocket))
        .route("/health", get(health))
        .layer(middleware::from_fn(cache_control))
        .layer(middleware::from_fn_with_state(state.clone(), track))
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(cors)
        .with_state(state)
}

/// Counts and times requests by route.
async fn track(State(app): App, request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let started = Instant::now();
    let response = next.run(request).await;
    let metrics = &app.hub.metrics;
    metrics
        .rest_requests
        .with_label_values(&[route.as_str(), response.status().as_str()])
        .inc();
    metrics
        .rest_seconds
        .with_label_values(&[route.as_str()])
        .observe(started.elapsed().as_secs_f64());
    response
}

/// How long a response to `route` may be reused, by the client or by a cache in front of the
/// API. `None` for what must not be cached at all, or is not worth it.
///
/// Live data is good for a second, which is enough for a cache to absorb a burst of identical
/// requests; history that only grows at funding or tick intervals is good for ten.
fn cache_directive(route: &str) -> Option<&'static str> {
    match route {
        "/v4/time" => Some("no-cache, no-store, no-transform"),
        "/v4/height" | "/v4/compliance/screen/{address}" | "/v4/ws" | "/health" => None,
        "/v4/sparklines"
        | "/v4/historicalFunding/{ticker}"
        | "/v4/fundingPayments/parentSubaccount"
        | "/v4/pnl/parentSubaccountNumber"
        | "/v4/historical-pnl/parentSubaccountNumber"
        | "/v4/historicalBlockTradingRewards/{address}"
        | "/v4/historicalTradingRewardAggregations/{address}" => Some("public, max-age=10"),
        _ => Some("public, max-age=1"),
    }
}

/// Says how long a successful response may be cached.
async fn cache_control(request: Request, next: Next) -> Response {
    let directive = request
        .extensions()
        .get::<MatchedPath>()
        .and_then(|path| cache_directive(path.as_str()));
    let mut response = next.run(request).await;
    if let Some(directive) = directive.filter(|_| response.status().is_success()) {
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static(directive));
    }
    response
}

async fn websocket(State(app): App, upgrade: WebSocketUpgrade) -> Response {
    let ctx = app.ws.clone();
    if ctx.hub.metrics.ws_connections.get() >= ctx.config.max_connections as i64 {
        let body = json!({"errors": [{"msg": "Too many connections"}]});
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    upgrade
        // Clients only ever send small control messages: a small read buffer keeps the memory
        // an idle connection holds low.
        .read_buffer_size(4 * 1024)
        .max_message_size(16 * 1024)
        .max_frame_size(16 * 1024)
        .on_upgrade(move |socket| ws::serve(socket, ctx))
}

fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// The checkpoint and chain time the API is serving, once the feed has loaded.
fn indexed(app: &AppState) -> Result<(i64, i64), ApiError> {
    app.hub
        .checkpoint()
        .ok_or_else(|| anyhow::anyhow!("the indexer has not committed anything yet").into())
}

async fn height(State(app): App) -> ApiResult {
    let (checkpoint, timestamp_ms) = indexed(&app)?;
    Ok(Json(
        json!({"height": checkpoint.to_string(), "time": iso(timestamp_ms)}),
    ))
}

async fn time() -> Json<Value> {
    let now = wall_clock_ms();
    Json(json!({"iso": iso(now), "epoch": now as f64 / 1000.0}))
}

async fn health(State(app): App) -> Response {
    let Some((checkpoint, timestamp_ms)) = app.hub.checkpoint() else {
        let body = json!({"status": "starting"});
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    };
    let lag_ms = (wall_clock_ms() - timestamp_ms).max(0);
    let healthy = lag_ms <= app.max_lag.as_millis() as i64;
    let body = json!({
        "status": if healthy { "ok" } else { "lagging" },
        "checkpoint": checkpoint,
        "time": iso(timestamp_ms),
        "lagMs": lag_ms,
    });
    let status = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

async fn compliance() -> Json<Value> {
    Json(json!({"status": "COMPLIANT", "updatedAt": iso(wall_clock_ms())}))
}

/// Every query parameter the endpoints take. They arrive as text and are parsed where used,
/// so that a bad one is answered with the API's own error shape.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Params {
    address: Option<String>,
    parent_subaccount_number: Option<String>,
    ticker: Option<String>,
    market: Option<String>,
    side: Option<String>,
    status: Option<String>,
    limit: Option<String>,
    page: Option<String>,
    created_before_or_at_height: Option<String>,
    created_before_or_at: Option<String>,
    created_on_or_after_height: Option<String>,
    created_on_or_after: Option<String>,
    daily: Option<String>,
    effective_before_or_at_height: Option<String>,
    effective_before_or_at: Option<String>,
    after_or_at: Option<String>,
    resolution: Option<String>,
    #[serde(rename = "fromISO")]
    from_iso: Option<String>,
    #[serde(rename = "toISO")]
    to_iso: Option<String>,
    time_period: Option<String>,
}

fn integer(name: &str, value: &Option<String>) -> Result<Option<i64>, ApiError> {
    value
        .as_deref()
        .map(|text| {
            text.parse::<i64>().ok().filter(|n| *n >= 0).ok_or_else(|| {
                ApiError::bad_request(format!("{name} must be a non-negative integer"))
            })
        })
        .transpose()
}

fn timestamp(name: &str, value: &Option<String>) -> Result<Option<i64>, ApiError> {
    value
        .as_deref()
        .map(|text| {
            parse_iso(text)
                .ok_or_else(|| ApiError::bad_request(format!("{name} must be an ISO 8601 date")))
        })
        .transpose()
}

fn limit(params: &Params, default: i64) -> Result<i64, ApiError> {
    match integer("limit", &params.limit)? {
        Some(0) => Err(ApiError::bad_request("limit must be a positive integer")),
        Some(limit) => Ok(limit.min(MAX_LIMIT)),
        None => Ok(default),
    }
}

/// A page of a history, and whether the request asked for page numbering.
///
/// Skipping rows costs the database as much as returning them, so a request may skip at most
/// `max_offset`; past that, a client narrows the time range instead of paging deeper.
fn page(params: &Params, max_offset: i64) -> Result<(Page, bool), ApiError> {
    let limit = limit(params, DEFAULT_LIMIT)?;
    let number = integer("page", &params.page)?;
    if number == Some(0) {
        return Err(ApiError::bad_request("page must be a positive integer"));
    }
    let offset = number.map_or(0, |n| (n - 1).saturating_mul(limit));
    if offset > max_offset {
        return Err(ApiError::bad_request(
            "page/limit combination requests an offset that exceeds the maximum. Narrow your \
             query using the createdBeforeOrAt time-range filter instead of paging deeper.",
        ));
    }
    let page = Page {
        limit,
        offset,
        before_checkpoint: integer(
            "createdBeforeOrAtHeight",
            &params.created_before_or_at_height,
        )?,
        before_ms: timestamp("createdBeforeOrAt", &params.created_before_or_at)?,
    };
    Ok((page, number.is_some()))
}

/// Adds dYdX's paging fields to a response when the request asked for a page.
fn paged(mut body: Value, page: Page, total: Option<i64>) -> Json<Value> {
    if let Some(total) = total {
        body["pageSize"] = json!(page.limit);
        body["totalResults"] = json!(total);
        body["offset"] = json!(page.offset);
    }
    Json(body)
}

fn market<'a>(app: &'a AppState, ticker: &str) -> Result<&'a str, ApiError> {
    app.deployment.market(ticker).ok_or_else(|| {
        ApiError::not_found(format!(
            "{ticker} not found in markets of type TickerType.PERPETUAL"
        ))
    })
}

/// The market a `ticker` filter names, if the request has one.
fn market_filter<'a>(
    app: &'a AppState,
    ticker: &Option<String>,
) -> Result<Option<&'a str>, ApiError> {
    ticker
        .as_deref()
        .map(|ticker| market(app, ticker))
        .transpose()
}

/// The address and parent subaccount number an account request names.
fn account_key(params: &Params) -> Result<(String, i64), ApiError> {
    let address = params
        .address
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("address is required"))?;
    let address = canonical_address(address)
        .map_err(|_| ApiError::bad_request("address must be a valid address"))?;
    let parent = integer("parentSubaccountNumber", &params.parent_subaccount_number)?
        .filter(|n| *n < 128)
        .ok_or_else(|| ApiError::bad_request("parentSubaccountNumber must be between 0 and 127"))?;
    Ok((address, parent))
}

async fn account(
    conn: &mut Connection<'_>,
    deployment: &Deployment,
    address: &str,
    parent: i64,
) -> anyhow::Result<Option<AccountRow>> {
    db::account_of(conn, address, &deployment.collateral_type, parent).await
}

fn no_subaccount(address: &str, parent: i64) -> ApiError {
    ApiError::not_found(format!(
        "No subaccount found with address {address} and parentSubaccountNumber {parent}"
    ))
}

async fn perpetual_markets(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let public = app
        .hub
        .public()
        .ok_or_else(|| anyhow::anyhow!("the indexer has not committed anything yet"))?;
    let markets = match &params.ticker {
        Some(ticker) => {
            market(&app, ticker)?;
            json!({ticker: public.markets.get(ticker)})
        }
        None => json!(public.markets),
    };
    Ok(Json(json!({"markets": markets})))
}

async fn orderbook(State(app): App, Path(ticker): Path<String>) -> ApiResult {
    market(&app, &ticker)?;
    let public = app
        .hub
        .public()
        .ok_or_else(|| anyhow::anyhow!("the indexer has not committed anything yet"))?;
    let book = public
        .books
        .get(&ticker)
        .map(|book| book.snapshot(app.book_depth));
    Ok(Json(json!(book.unwrap_or_default())))
}

async fn trades(
    State(app): App,
    Path(ticker): Path<String>,
    Query(params): Query<Params>,
) -> ApiResult {
    let market = market(&app, &ticker)?;
    let (page, _) = page(&params, app.max_pagination_offset)?;
    let rows = db::market_trades(&mut app.db.connect().await?, market, page).await?;
    let trades: Vec<_> = rows.iter().map(trade_object).collect();
    Ok(Json(json!({"trades": trades})))
}

async fn candles(
    State(app): App,
    Path(ticker): Path<String>,
    Query(params): Query<Params>,
) -> ApiResult {
    let market = market(&app, &ticker)?;
    let resolution = params
        .resolution
        .as_deref()
        .and_then(resolution_ms)
        .ok_or_else(|| ApiError::bad_request("resolution must be a valid Candle Resolution, one of 1MIN,5MINS,15MINS,30MINS,1HOUR,4HOURS,1DAY"))?;
    let from = timestamp("fromISO", &params.from_iso)?;
    let to = timestamp("toISO", &params.to_iso)?;
    let limit = limit(&params, DEFAULT_LIMIT)?;
    let conn = &mut app.db.connect().await?;
    let rows = db::candles(conn, market, resolution, from, to, limit).await?;
    let candles: Vec<_> = rows
        .iter()
        .map(|row| candle_object(row, &ticker, true))
        .collect();
    Ok(Json(json!({"candles": candles})))
}

async fn sparklines(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let (resolution, span) = match params.time_period.as_deref().unwrap_or("ONE_DAY") {
        "ONE_DAY" => (HOUR_MS, DAY_MS),
        "SEVEN_DAYS" => (4 * HOUR_MS, 7 * DAY_MS),
        _ => {
            return Err(ApiError::bad_request(
                "timePeriod must be ONE_DAY or SEVEN_DAYS",
            ));
        }
    };
    let (_, now_ms) = indexed(&app)?;
    let market_ids = app.deployment.market_ids();
    let conn = &mut app.db.connect().await?;
    let rows = db::sparklines(conn, &market_ids, resolution, now_ms - span).await?;

    // Every listed market has an entry; closes are newest first.
    let mut lines: BTreeMap<&str, Vec<String>> = app
        .deployment
        .markets()
        .map(|(ticker, _)| (ticker, vec![]))
        .collect();
    for row in &rows {
        if let Some(ticker) = app.deployment.ticker(&row.market) {
            lines
                .entry(ticker)
                .or_default()
                .push(crate::decimal::plain(&row.close));
        }
    }
    Ok(Json(json!(lines)))
}

async fn historical_funding(
    State(app): App,
    Path(ticker): Path<String>,
    Query(params): Query<Params>,
) -> ApiResult {
    let market = market(&app, &ticker)?;
    let limit = limit(&params, DEFAULT_FUNDING_LIMIT)?;
    let before_checkpoint = integer(
        "effectiveBeforeOrAtHeight",
        &params.effective_before_or_at_height,
    )?;
    let before_ms = timestamp("effectiveBeforeOrAt", &params.effective_before_or_at)?;
    let conn = &mut app.db.connect().await?;
    let frequency_ms = db::markets(conn, &[market.to_owned()])
        .await?
        .first()
        .map(|row| row.funding_frequency_ms)
        .unwrap_or_default();
    let rows = db::funding_updates(conn, market, before_checkpoint, before_ms, limit).await?;
    let funding: Vec<_> = rows
        .iter()
        .map(|row| historical_funding_object(row, &ticker, frequency_ms))
        .collect();
    Ok(Json(json!({"historicalFunding": funding})))
}

async fn parent_subaccount(
    State(app): App,
    Path((address, number)): Path<(String, String)>,
) -> ApiResult {
    let params = Params {
        address: Some(address),
        parent_subaccount_number: Some(number),
        ..Params::default()
    };
    let (address, parent) = account_key(&params)?;
    let snapshot = snapshot::account(&app.db, &app.deployment, &address, parent).await?;
    let account = snapshot
        .account
        .ok_or_else(|| no_subaccount(&address, parent))?;
    let subaccount = account.subaccount(&address, parent, snapshot.watermark.checkpoint);
    Ok(Json(json!({"subaccount": subaccount})))
}

async fn orders(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let (address, parent) = account_key(&params)?;
    let status = match params.status.as_deref() {
        None => None,
        Some("OPEN") => Some("open"),
        Some("FILLED") => Some("filled"),
        Some("CANCELED") => Some("canceled"),
        // Statuses the engine's orders never have.
        Some("BEST_EFFORT_OPENED" | "BEST_EFFORT_CANCELED" | "UNTRIGGERED" | "ERROR") => {
            return Ok(Json(json!([])));
        }
        Some(_) => return Err(ApiError::bad_request("status must be a valid order status")),
    };
    let filter = OrderFilter {
        market: market_filter(&app, &params.ticker)?.map(str::to_owned),
        is_ask: match params.side.as_deref() {
            None => None,
            Some("BUY") => Some(false),
            Some("SELL") => Some(true),
            Some(_) => return Err(ApiError::bad_request("side must be BUY or SELL")),
        },
        status: status.map(str::to_owned),
        limit: limit(&params, DEFAULT_LIMIT)?,
    };
    let conn = &mut app.db.connect().await?;
    let Some(account) = account(conn, &app.deployment, &address, parent).await? else {
        return Ok(Json(json!([])));
    };
    let ids = app.deployment.market_ids();
    let rows = db::account_orders(conn, &ids, account.account_id, &filter).await?;
    let orders: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            Some(order_object(
                row,
                app.deployment.ticker(&row.market)?,
                parent,
            ))
        })
        .collect();
    Ok(Json(json!(orders)))
}

/// A page of an account's fills, with the total when the request asked for page numbering. An
/// address without an account has none.
async fn account_fills(
    app: &AppState,
    address: &str,
    parent: i64,
    market: Option<&str>,
    page: Page,
    numbered: bool,
) -> anyhow::Result<(Vec<db::FillRow>, Option<i64>)> {
    let conn = &mut app.db.connect().await?;
    let Some(account) = account(conn, &app.deployment, address, parent).await? else {
        return Ok((vec![], numbered.then_some(0)));
    };
    let ids = app.deployment.market_ids();
    let rows = db::account_fills(conn, &ids, account.account_id, market, page).await?;
    let total = if numbered {
        Some(db::count_account_fills(conn, &ids, account.account_id, market, page).await?)
    } else {
        None
    };
    Ok((rows, total))
}

async fn fills(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let (address, parent) = account_key(&params)?;
    let (page, numbered) = page(&params, app.max_pagination_offset)?;
    let market = market_filter(&app, &params.ticker.or(params.market))?;
    let (rows, total) = account_fills(&app, &address, parent, market, page, numbered).await?;
    let fills: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            Some(fill_object(
                row,
                app.deployment.ticker(&row.market)?,
                parent,
            ))
        })
        .collect();
    Ok(paged(json!({"fills": fills}), page, total))
}

async fn trade_history(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let (address, parent) = account_key(&params)?;
    let (page, numbered) = page(&params, app.max_pagination_offset)?;
    let market = market_filter(&app, &params.market.or(params.ticker))?;
    let (rows, total) = account_fills(&app, &address, parent, market, page, numbered).await?;
    let history: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            Some(trade_history_object(
                row,
                app.deployment.ticker(&row.market)?,
                parent,
            ))
        })
        .collect();
    Ok(paged(json!({"tradeHistory": history}), page, total))
}

async fn transfers(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let (address, parent) = account_key(&params)?;
    let (page, numbered) = page(&params, app.max_pagination_offset)?;
    let conn = &mut app.db.connect().await?;
    let account = account(conn, &app.deployment, &address, parent)
        .await?
        .ok_or_else(|| no_subaccount(&address, parent))?;
    let rows = db::account_transfers(conn, account.account_id, page).await?;
    let total = if numbered {
        Some(db::count_account_transfers(conn, account.account_id, page).await?)
    } else {
        None
    };
    let decimals = app.deployment.collateral_decimals;
    let transfers: Vec<_> = rows
        .iter()
        .map(|row| transfer_object(row, &address, parent, decimals, true))
        .collect();
    Ok(paged(json!({"transfers": transfers}), page, total))
}

async fn funding_payments(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let (address, parent) = account_key(&params)?;
    let (page, numbered) = page(&params, app.max_pagination_offset)?;
    let after_ms = timestamp("afterOrAt", &params.after_or_at)?;
    let market = market_filter(&app, &params.ticker)?;
    let conn = &mut app.db.connect().await?;
    let (rows, total) = match account(conn, &app.deployment, &address, parent).await? {
        None => (vec![], numbered.then_some(0)),
        Some(account) => {
            let (ids, id) = (app.deployment.market_ids(), account.account_id);
            let rows = db::account_funding_payments(
                conn,
                &ids,
                id,
                market,
                after_ms,
                page.limit,
                page.offset,
            )
            .await?;
            let total = if numbered {
                Some(db::count_account_funding_payments(conn, &ids, id, market, after_ms).await?)
            } else {
                None
            };
            (rows, total)
        }
    };
    let payments: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            Some(funding_payment_object(
                row,
                app.deployment.ticker(&row.market)?,
                parent,
            ))
        })
        .collect();
    Ok(paged(json!({"fundingPayments": payments}), page, total))
}

/// The ticks of an account's PnL history that a request asks for, newest first.
async fn pnl_ticks(app: &AppState, params: &Params) -> Result<Vec<db::PnlTickRow>, ApiError> {
    let (address, parent) = account_key(params)?;
    let daily = match params.daily.as_deref() {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => return Err(ApiError::bad_request("daily must be true or false")),
    };
    let filter = PnlFilter {
        daily,
        before_checkpoint: integer(
            "createdBeforeOrAtHeight",
            &params.created_before_or_at_height,
        )?,
        before_ms: timestamp("createdBeforeOrAt", &params.created_before_or_at)?,
        after_checkpoint: integer("createdOnOrAfterHeight", &params.created_on_or_after_height)?,
        after_ms: timestamp("createdOnOrAfter", &params.created_on_or_after)?,
        limit: limit(params, DEFAULT_LIMIT)?,
    };
    let conn = &mut app.db.connect().await?;
    let account = account(conn, &app.deployment, &address, parent)
        .await?
        .ok_or_else(|| no_subaccount(&address, parent))?;
    Ok(db::account_pnl_ticks(conn, account.account_id, filter).await?)
}

async fn pnl(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let rows = pnl_ticks(&app, &params).await?;
    let ticks: Vec<_> = rows.iter().map(pnl_tick_object).collect();
    Ok(Json(json!({"pnl": ticks})))
}

async fn historical_pnl(State(app): App, Query(params): Query<Params>) -> ApiResult {
    let rows = pnl_ticks(&app, &params).await?;
    let ticks: Vec<_> = rows.iter().map(historical_pnl_tick_object).collect();
    Ok(Json(json!({"historicalPnl": ticks})))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_OFFSET: i64 = 25_000;

    fn paged(params: &Params) -> Result<(Page, bool), ApiError> {
        page(params, MAX_OFFSET)
    }

    fn params(pairs: &[(&str, &str)]) -> Params {
        let object: serde_json::Map<String, Value> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), json!(value)))
            .collect();
        serde_json::from_value(Value::Object(object)).unwrap()
    }

    #[test]
    fn pages_are_one_based_and_capped() {
        let (page, numbered) = paged(&params(&[])).unwrap();
        assert_eq!((page.limit, page.offset, numbered), (1_000, 0, false));

        let (page, numbered) = paged(&params(&[("limit", "50"), ("page", "3")])).unwrap();
        assert_eq!((page.limit, page.offset, numbered), (50, 100, true));

        let (page, _) = paged(&params(&[
            ("limit", "999999"),
            ("createdBeforeOrAtHeight", "77"),
            ("createdBeforeOrAt", "1970-01-01T00:00:01.000Z"),
        ]))
        .unwrap();
        assert_eq!(page.limit, MAX_LIMIT);
        assert_eq!(
            (page.before_checkpoint, page.before_ms),
            (Some(77), Some(1_000))
        );

        for bad in [
            ("limit", "0"),
            ("limit", "-1"),
            ("page", "0"),
            ("page", "x"),
            ("createdBeforeOrAt", "now"),
        ] {
            assert!(paged(&params(&[bad])).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_request_may_not_skip_more_rows_than_the_cap() {
        // 25 pages of 1,000 skip 24,000 rows; the 26th skips exactly the cap.
        for (number, allowed) in [("25", true), ("26", true), ("27", false)] {
            let result = paged(&params(&[("page", number)]));
            assert_eq!(result.is_ok(), allowed, "page {number}");
        }
        assert!(paged(&params(&[("limit", "100"), ("page", "251")])).is_ok());
        assert!(paged(&params(&[("limit", "100"), ("page", "252")])).is_err());
        // A page number that would overflow is refused, not wrapped.
        let huge = i64::MAX.to_string();
        assert!(paged(&params(&[("page", huge.as_str())])).is_err());
    }

    #[test]
    fn responses_say_how_long_they_may_be_cached() {
        assert_eq!(
            cache_directive("/v4/orderbooks/perpetualMarket/{ticker}"),
            Some("public, max-age=1")
        );
        assert_eq!(
            cache_directive("/v4/pnl/parentSubaccountNumber"),
            Some("public, max-age=10")
        );
        assert_eq!(
            cache_directive("/v4/time"),
            Some("no-cache, no-store, no-transform")
        );
        assert_eq!(cache_directive("/v4/height"), None);
        assert_eq!(cache_directive("/health"), None);
    }

    #[test]
    fn account_requests_name_an_address_and_a_parent() {
        let (address, parent) = account_key(&params(&[
            ("address", "0xAB"),
            ("parentSubaccountNumber", "2"),
        ]))
        .unwrap();
        assert_eq!(address, format!("0x{:0>64}", "ab"));
        assert_eq!(parent, 2);

        assert!(account_key(&params(&[("parentSubaccountNumber", "0")])).is_err());
        assert!(account_key(&params(&[("address", "0xab")])).is_err());
        assert!(
            account_key(&params(&[
                ("address", "0xab"),
                ("parentSubaccountNumber", "128")
            ]))
            .is_err()
        );
        assert!(
            account_key(&params(&[
                ("address", "dydx1abc"),
                ("parentSubaccountNumber", "0")
            ]))
            .is_err()
        );
    }
}
