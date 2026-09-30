// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

diesel::table! {
    raw_events (checkpoint, tx_index, event_index) {
        checkpoint -> Int8,
        tx_index -> Int8,
        event_index -> Int8,
        tx_digest -> Text,
        timestamp_ms -> Int8,
        sender -> Text,
        package -> Text,
        package_id -> Text,
        module -> Text,
        name -> Text,
        event_type -> Text,
        market -> Nullable<Text>,
        bcs -> Bytea,
        data -> Nullable<Jsonb>,
        decode_error -> Nullable<Text>,
    }
}
