// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The API over the indexer's tables: REST for history and a WebSocket for live data.
//!
//! It speaks the dYdX v4 indexer protocol, which is what the trading front end was written
//! against, and maps the engine's model onto it. The server only reads: the indexer writes the
//! tables, and a feed in this process turns each batch of checkpoints it commits into the
//! updates subscribers receive.

pub mod config;
pub mod db;
pub mod decimal;
pub mod engine;
pub mod error;
pub mod feed;
pub mod hub;
pub mod metrics;
pub mod model;
pub mod rest;
pub mod snapshot;
pub mod time;
pub mod views;
pub mod ws;
