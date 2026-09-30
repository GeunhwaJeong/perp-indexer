// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

/// Declares Rust mirrors of Move structs.
///
/// Each struct decodes from the BCS of its Move counterpart, which carries no field names: it
/// must list the same fields, in the same order, as the Move definition. `LAYOUT` records what
/// was declared so the `layouts` test can check it against the engine's sources.
macro_rules! move_structs {
    ($( struct $name:ident { $( $field:ident : $ty:ty ),* $(,)? } )*) => {
        $(
            #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
            pub struct $name {
                $( pub $field: $ty, )*
            }
        )*

        /// Every struct with its `(field, type)` pairs in declaration order.
        pub const LAYOUT: &[(&str, &[(&str, &str)])] = &[
            $( (stringify!($name), &[ $( (stringify!($field), stringify!($ty)), )* ]), )*
        ];
    };
}

/// Declares the events of one Move `events` module: the structs (see [`move_structs`]), an
/// `Event` enum over them and a decoder that picks the struct by name.
macro_rules! move_events {
    ($( struct $name:ident { $( $field:ident : $ty:ty ),* $(,)? } )*) => {
        move_structs! {
            $( struct $name { $( $field : $ty, )* } )*
        }

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
    };
}
