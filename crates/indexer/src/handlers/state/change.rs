// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use bigdecimal::BigDecimal;
use perp_schema::models::{
    AccountSnapshot, CollateralTransfer, Fill, FundingPayment, FundingUpdate, MarketSnapshot,
    OraclePrice, Order, OrderTicket, PositionSnapshot,
};

/// One effect of a checkpoint on the derived state.
///
/// Changes are produced without looking at the database, so checkpoints can be processed in
/// parallel; they are then applied strictly in chain order.
#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    MarketSnapshot(Box<MarketSnapshot>),
    MarketCreated {
        market: String,
        collateral_decimals: i64,
        checkpoint: i64,
        timestamp_ms: i64,
    },
    MarketClosed {
        market: String,
    },
    MarketSettlement {
        market: String,
        enabled: bool,
        base_price: BigDecimal,
        collateral_price: BigDecimal,
    },
    MarketPrices(MarketPrices),
    AccountSnapshot(AccountSnapshot),
    AccountCreated {
        account_id: i64,
        creator: String,
        checkpoint: i64,
        timestamp_ms: i64,
    },
    PositionSnapshot(PositionSnapshot),
    OrderPosted(Order),
    OrderUpdated(OrderUpdate),
    Fill(Fill),
    FundingUpdate(FundingUpdate),
    FundingPayment(FundingPayment),
    CollateralTransfer(CollateralTransfer),
    OraclePrice(OraclePrice),
    OraclePriceRemoved {
        storage_id: i64,
        source_id: i32,
    },
    Ticket(TicketChange),
}

/// Prices an event reported for a market. Events carry different subsets of them.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketPrices {
    pub market: String,
    pub mark_price: Option<BigDecimal>,
    pub index_price: Option<BigDecimal>,
    pub book_price: Option<BigDecimal>,
    pub timestamp_ms: i64,
}

/// What a fill or cancelation did to a resting order.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderUpdate {
    pub market: String,
    pub order_id: BigDecimal,
    /// Size filled by this event.
    pub filled: BigDecimal,
    /// Size canceled by this event.
    pub canceled: BigDecimal,
    /// Size left on the book after this event.
    pub remaining: BigDecimal,
    pub cancel_reason: Option<i16>,
    pub checkpoint: i64,
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TicketChange {
    Created(Box<OrderTicket>),
    Status {
        ticket_id: String,
        status: &'static str,
        checkpoint: i64,
        timestamp_ms: i64,
    },
    Details {
        ticket_id: String,
        encrypted_details: Vec<u8>,
        checkpoint: i64,
        timestamp_ms: i64,
    },
    Executors {
        ticket_id: String,
        executors: serde_json::Value,
        checkpoint: i64,
        timestamp_ms: i64,
    },
    Progress {
        ticket_id: String,
        progress: serde_json::Value,
        checkpoint: i64,
        timestamp_ms: i64,
    },
}
