// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry,
};

/// Latencies from a millisecond to ten seconds.
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone)]
pub struct ApiMetrics {
    /// The checkpoint the feed has published up to.
    pub feed_checkpoint: IntGauge,
    /// How old the last published checkpoint was when it was published. It grows when the
    /// indexer or the node behind it stalls.
    pub feed_lag_ms: IntGauge,
    pub feed_rounds: IntCounter,
    /// Times the feed fell too far behind to send updates and made every client start over.
    pub feed_resets: IntCounter,
    pub feed_errors: IntCounter,
    pub feed_round_seconds: Histogram,
    pub ws_connections: IntGauge,
    pub ws_subscriptions: IntGaugeVec,
    pub ws_messages_sent: IntCounterVec,
    /// Connections the server closed, by reason.
    pub ws_closed: IntCounterVec,
    pub ws_snapshot_seconds: HistogramVec,
    pub rest_requests: IntCounterVec,
    pub rest_seconds: HistogramVec,
}

impl ApiMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            feed_checkpoint: register_int_gauge_with_registry!(
                "feed_checkpoint",
                "Checkpoint the feed has published up to",
                registry,
            )
            .unwrap(),
            feed_lag_ms: register_int_gauge_with_registry!(
                "feed_lag_ms",
                "Age of the last published checkpoint when it was published",
                registry,
            )
            .unwrap(),
            feed_rounds: register_int_counter_with_registry!(
                "feed_rounds_total",
                "Batches of checkpoints the feed has published",
                registry,
            )
            .unwrap(),
            feed_resets: register_int_counter_with_registry!(
                "feed_resets_total",
                "Times the feed dropped every client to start over from a snapshot",
                registry,
            )
            .unwrap(),
            feed_errors: register_int_counter_with_registry!(
                "feed_errors_total",
                "Feed rounds that failed and were retried",
                registry,
            )
            .unwrap(),
            feed_round_seconds: register_histogram_with_registry!(
                "feed_round_seconds",
                "Time to build one feed round",
                LATENCY_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            ws_connections: register_int_gauge_with_registry!(
                "ws_connections",
                "Open WebSocket connections",
                registry,
            )
            .unwrap(),
            ws_subscriptions: register_int_gauge_vec_with_registry!(
                "ws_subscriptions",
                "Active subscriptions, by channel",
                &["channel"],
                registry,
            )
            .unwrap(),
            ws_messages_sent: register_int_counter_vec_with_registry!(
                "ws_messages_sent_total",
                "Messages sent to subscribers, by channel",
                &["channel"],
                registry,
            )
            .unwrap(),
            ws_closed: register_int_counter_vec_with_registry!(
                "ws_closed_total",
                "Connections closed by the server, by reason",
                &["reason"],
                registry,
            )
            .unwrap(),
            ws_snapshot_seconds: register_histogram_vec_with_registry!(
                "ws_snapshot_seconds",
                "Time to load the initial data of a subscription, by channel",
                &["channel"],
                LATENCY_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rest_requests: register_int_counter_vec_with_registry!(
                "rest_requests_total",
                "REST requests, by route and status",
                &["route", "status"],
                registry,
            )
            .unwrap(),
            rest_seconds: register_histogram_vec_with_registry!(
                "rest_seconds",
                "REST request latency, by route",
                &["route"],
                LATENCY_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
        }
    }
}
