// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use prometheus::{IntCounterVec, Registry, register_int_counter_vec_with_registry};

#[derive(Clone)]
pub struct IndexerMetrics {
    /// Events recorded in the ledger.
    pub events: IntCounterVec,
    /// Events from a package with decoders that could not be decoded. Anything above zero means
    /// the decoders lag the deployed package and the rows need to be decoded again after a fix.
    pub undecoded_events: IntCounterVec,
}

impl IndexerMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            events: register_int_counter_vec_with_registry!(
                "events_total",
                "Events recorded in the ledger, by package and event name",
                &["package", "name"],
                registry,
            )
            .unwrap(),
            undecoded_events: register_int_counter_vec_with_registry!(
                "undecoded_events_total",
                "Events that failed to decode, by package and event name",
                &["package", "name"],
                registry,
            )
            .unwrap(),
        }
    }
}
