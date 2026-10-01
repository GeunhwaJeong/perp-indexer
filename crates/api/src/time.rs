// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, SecondsFormat, Utc};

/// A millisecond timestamp as an ISO 8601 string, e.g. `2026-10-01T00:00:00.000Z`.
pub fn iso(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, /* use_z */ true)
}

/// The millisecond timestamp of an ISO 8601 string.
pub fn parse_iso(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|time| time.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_round_trip() {
        assert_eq!(iso(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso(1_790_000_000_123), "2026-09-21T14:13:20.123Z");
        assert_eq!(
            parse_iso("2026-09-21T14:13:20.123Z"),
            Some(1_790_000_000_123)
        );
        assert_eq!(
            parse_iso("2026-09-21T23:13:20.123+09:00"),
            Some(1_790_000_000_123)
        );
        assert_eq!(parse_iso("yesterday"), None);
    }
}
