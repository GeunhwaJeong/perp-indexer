// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Move primitives as they appear in event payloads.
//!
//! Every type here has two serde shapes. With a binary format (BCS) it matches the Move layout
//! byte for byte. With a human-readable format (JSON) wide integers become decimal strings and
//! addresses and byte vectors become `0x` hex, so the values survive JSON consumers whose numbers
//! are 64-bit floats.

use std::fmt;
use std::str::FromStr;

use num_bigint::BigUint;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An account address or object ID.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Address(pub [u8; 32]);

/// Move's `object::ID`, which wraps a single address.
pub type Id = Address;

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Address {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let digits = s.strip_prefix("0x").unwrap_or(s);
        if digits.is_empty() || digits.len() > 64 {
            return Err(format!("invalid address: {s}"));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(format!("{digits:0>64}"), &mut bytes)
            .map_err(|e| format!("invalid address {s}: {e}"))?;
        Ok(Self(bytes))
    }
}

impl Serialize for Address {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            String::deserialize(deserializer)?
                .parse()
                .map_err(D::Error::custom)
        } else {
            <[u8; 32]>::deserialize(deserializer).map(Self)
        }
    }
}

/// A `u128`, written as a decimal string in JSON.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct U128(pub u128);

impl fmt::Display for U128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for U128 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(&self.0)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for U128 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            String::deserialize(deserializer)?
                .parse()
                .map(Self)
                .map_err(D::Error::custom)
        } else {
            u128::deserialize(deserializer).map(Self)
        }
    }
}

/// A `u256` in its little-endian wire form, written as a decimal string in JSON.
///
/// The engine also stores its signed 18-decimal fixed-point numbers (`ifixed`) in this type; the
/// payload does not say which fields are signed, so interpretation is left to the consumer.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct U256(pub [u8; 32]);

impl U256 {
    pub fn to_biguint(&self) -> BigUint {
        BigUint::from_bytes_le(&self.0)
    }
}

impl From<u128> for U256 {
    fn from(value: u128) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&value.to_le_bytes());
        Self(bytes)
    }
}

impl fmt::Display for U256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_biguint().fmt(f)
    }
}

impl fmt::Debug for U256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for U256 {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value: BigUint = s.parse().map_err(|e| format!("invalid u256 {s}: {e}"))?;
        let le = value.to_bytes_le();
        if le.len() > 32 {
            return Err(format!("u256 out of range: {s}"));
        }
        let mut bytes = [0u8; 32];
        bytes[..le.len()].copy_from_slice(&le);
        Ok(Self(bytes))
    }
}

impl Serialize for U256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for U256 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            String::deserialize(deserializer)?
                .parse()
                .map_err(D::Error::custom)
        } else {
            <[u8; 32]>::deserialize(deserializer).map(Self)
        }
    }
}

/// A `vector<u8>`, written as `0x` hex in JSON.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Bytes(pub Vec<u8>);

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(&format_args!("0x{}", hex::encode(&self.0)))
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            hex::decode(s.strip_prefix("0x").unwrap_or(&s))
                .map(Self)
                .map_err(D::Error::custom)
        } else {
            Vec::<u8>::deserialize(deserializer).map(Self)
        }
    }
}

/// Move's `std::type_name::TypeName`: a fully qualified type, without the `0x` prefix.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TypeName(pub String);
