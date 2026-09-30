// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

use diesel::prelude::*;
use haneul_field_count::FieldCount;

use crate::schema::raw_events;

#[derive(Clone, Debug, PartialEq, Queryable, Selectable, Insertable, FieldCount)]
#[diesel(table_name = raw_events)]
pub struct RawEvent {
    pub checkpoint: i64,
    pub tx_index: i64,
    pub event_index: i64,
    pub tx_digest: String,
    pub timestamp_ms: i64,
    pub sender: String,
    pub package: String,
    pub package_id: String,
    pub module: String,
    pub name: String,
    pub event_type: String,
    pub market: Option<String>,
    pub bcs: Vec<u8>,
    pub data: Option<serde_json::Value>,
    pub decode_error: Option<String>,
}
