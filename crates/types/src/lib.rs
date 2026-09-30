// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Typed decoders for the events and objects of the perpetuals engine.
//!
//! The engine emits all of its events from a module called `events` in each package, so an event
//! is identified by the package it belongs to and its struct name. This crate has no chain
//! dependencies: it only needs the payload bytes.

use std::fmt;
use std::str::FromStr;

#[macro_use]
mod macros;

pub mod objects;
pub mod oracle_aggregator;
pub mod perpetuals;
pub mod perpetuals_orders;
pub mod types;

/// The Move module every package emits its events from.
pub const EVENTS_MODULE: &str = "events";

/// A package whose events this crate can decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Package {
    Perpetuals,
    PerpetualsOrders,
    OracleAggregator,
}

impl Package {
    pub const ALL: [Package; 3] = [
        Package::Perpetuals,
        Package::PerpetualsOrders,
        Package::OracleAggregator,
    ];

    /// The Move package name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Package::Perpetuals => "perpetuals",
            Package::PerpetualsOrders => "perpetuals_orders",
            Package::OracleAggregator => "oracle_aggregator",
        }
    }

    /// `(struct, [(field, type)])` for every event of the package, in declaration order.
    pub fn layout(&self) -> &'static [(&'static str, &'static [(&'static str, &'static str)])] {
        match self {
            Package::Perpetuals => perpetuals::LAYOUT,
            Package::PerpetualsOrders => perpetuals_orders::LAYOUT,
            Package::OracleAggregator => oracle_aggregator::LAYOUT,
        }
    }
}

impl fmt::Display for Package {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Package {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Package::ALL
            .into_iter()
            .find(|package| package.as_str() == s)
            .ok_or_else(|| format!("no decoders for package '{s}'"))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unknown event struct '{0}'")]
    UnknownEvent(String),

    /// The payload does not match the declared layout. BCS rejects both missing and trailing
    /// bytes, so this is what a layout that drifted from the deployed package looks like.
    #[error("payload does not match the declared layout: {0}")]
    Layout(#[from] bcs::Error),
}

/// A decoded engine event.
// Events are decoded, handled and dropped one at a time, so boxing the large variant would only
// add an allocation per event.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PerpEvent {
    Perpetuals(perpetuals::Event),
    PerpetualsOrders(perpetuals_orders::Event),
    OracleAggregator(oracle_aggregator::Event),
}

impl PerpEvent {
    /// Decodes the payload of the event struct `name` emitted by `package`.
    pub fn decode(package: Package, name: &str, bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(match package {
            Package::Perpetuals => Self::Perpetuals(perpetuals::Event::decode(name, bytes)?),
            Package::PerpetualsOrders => {
                Self::PerpetualsOrders(perpetuals_orders::Event::decode(name, bytes)?)
            }
            Package::OracleAggregator => {
                Self::OracleAggregator(oracle_aggregator::Event::decode(name, bytes)?)
            }
        })
    }

    pub fn package(&self) -> Package {
        match self {
            Self::Perpetuals(_) => Package::Perpetuals,
            Self::PerpetualsOrders(_) => Package::PerpetualsOrders,
            Self::OracleAggregator(_) => Package::OracleAggregator,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Perpetuals(event) => event.name(),
            Self::PerpetualsOrders(event) => event.name(),
            Self::OracleAggregator(event) => event.name(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Perpetuals(event) => event.to_json(),
            Self::PerpetualsOrders(event) => event.to_json(),
            Self::OracleAggregator(event) => event.to_json(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::perpetuals::{Event, FilledMakerOrder, FilledMakerOrders, PostedOrder};
    use crate::types::{Address, U256};

    fn posted_order() -> PostedOrder {
        PostedOrder {
            ch_id: Address([0xab; 32]),
            account_id: 7,
            order_id: crate::types::U128((100_000u128 << 64) | 3),
            client_order_id: Some(42),
            order_size: 5_000_000,
            reduce_only: false,
            expiration_timestamp_ms: None,
            integrator_id: None,
            integrator_fee_rate: 0,
            mark_price: U256::from(100_000 * 10u128.pow(18)),
            book_price: Some(100_000_000_000_000),
        }
    }

    #[test]
    fn decodes_by_package_and_name() {
        let event = posted_order();
        let bytes = bcs::to_bytes(&event).unwrap();
        let decoded = PerpEvent::decode(Package::Perpetuals, "PostedOrder", &bytes).unwrap();
        assert_eq!(decoded, PerpEvent::Perpetuals(Event::PostedOrder(event)));
        assert_eq!(decoded.name(), "PostedOrder");
        assert_eq!(decoded.package(), Package::Perpetuals);
    }

    #[test]
    fn json_keeps_wide_integers_and_addresses_exact() {
        let json = Event::PostedOrder(posted_order()).to_json();
        assert_eq!(
            json,
            json!({
                "ch_id": format!("0x{}", "ab".repeat(32)),
                "account_id": 7,
                "order_id": "1844674407370955161600003",
                "client_order_id": 42,
                "order_size": 5_000_000,
                "reduce_only": false,
                "expiration_timestamp_ms": null,
                "integrator_id": null,
                "integrator_fee_rate": 0,
                "mark_price": "100000000000000000000000",
                "book_price": 100_000_000_000_000u64,
            })
        );
        let back: PostedOrder = serde_json::from_value(json).unwrap();
        assert_eq!(back, posted_order());
    }

    #[test]
    fn decodes_nested_events() {
        let fill = FilledMakerOrder {
            ch_id: Address([1; 32]),
            maker_account_id: 1,
            taker_account_id: 2,
            order_id: crate::types::U128(9),
            client_order_id: None,
            filled_size: 10,
            remaining_size: 0,
            canceled_size: 0,
            cancelation_reason: Some(1),
            pnl: U256::default(),
            maker_fees: U256::default(),
            mark_price: U256::default(),
            integrator_id: Some(5),
            integrator_fee_paid_usd: U256::default(),
        };
        let batch = FilledMakerOrders {
            events: vec![fill.clone(), fill],
            book_price: None,
        };
        let bytes = bcs::to_bytes(&batch).unwrap();
        let decoded = Event::decode("FilledMakerOrders", &bytes).unwrap();
        assert_eq!(decoded, Event::FilledMakerOrders(batch));
    }

    #[test]
    fn rejects_payloads_that_do_not_match_the_layout() {
        let mut bytes = bcs::to_bytes(&posted_order()).unwrap();
        bytes.push(0);
        assert!(matches!(
            Event::decode("PostedOrder", &bytes),
            Err(DecodeError::Layout(_))
        ));
        bytes.truncate(bytes.len() - 9);
        assert!(matches!(
            Event::decode("PostedOrder", &bytes),
            Err(DecodeError::Layout(_))
        ));
    }

    #[test]
    fn reports_unknown_events() {
        assert!(matches!(
            PerpEvent::decode(Package::PerpetualsOrders, "PostedOrder", &[]),
            Err(DecodeError::UnknownEvent(name)) if name == "PostedOrder"
        ));
    }

    #[test]
    fn addresses_and_u256_parse_their_display_form() {
        let address: Address = "0x2".parse().unwrap();
        assert_eq!(address.to_string(), format!("0x{:0>64}", "2"));
        let max = U256([0xff; 32]);
        assert_eq!(max.to_string().parse::<U256>().unwrap(), max);
        assert!(format!("{max}0").parse::<U256>().is_err());
    }
}
