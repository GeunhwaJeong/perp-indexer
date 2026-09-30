// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

/// Declares the events of one Move `events` module.
///
/// For each `struct` this generates a Rust struct that decodes from the event's BCS payload, and
/// for the module as a whole an `Event` enum, a by-name decoder and a `LAYOUT` table. BCS carries
/// no field names, so a struct must list the same fields, in the same order, as its Move
/// definition; `LAYOUT` is what the `layouts` test checks against the engine's sources.
macro_rules! move_events {
    ($( struct $name:ident { $( $field:ident : $ty:ty ),* $(,)? } )*) => {
        $(
            #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
            pub struct $name {
                $( pub $field: $ty, )*
            }
        )*

        #[derive(Clone, Debug, PartialEq, Eq)]
        pub enum Event {
            $( $name($name), )*
        }

        impl Event {
            /// Decodes the payload of the event struct called `name`.
            pub fn decode(name: &str, bytes: &[u8]) -> Result<Self, $crate::DecodeError> {
                match name {
                    $( stringify!($name) => Ok(Self::$name(bcs::from_bytes(bytes)?)), )*
                    _ => Err($crate::DecodeError::UnknownEvent(name.to_owned())),
                }
            }

            pub fn name(&self) -> &'static str {
                match self {
                    $( Self::$name(_) => stringify!($name), )*
                }
            }

            pub fn to_json(&self) -> serde_json::Value {
                match self {
                    $( Self::$name(event) => serde_json::to_value(event), )*
                }
                .expect("events serialize to JSON")
            }
        }

        /// Every event struct with its `(field, type)` pairs in declaration order.
        pub const LAYOUT: &[(&str, &[(&str, &str)])] = &[
            $( (stringify!($name), &[ $( (stringify!($field), stringify!($ty)), )* ]), )*
        ];
    };
}
