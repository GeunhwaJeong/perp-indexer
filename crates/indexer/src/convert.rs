// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Conversions from the engine's integer encodings to the decimals stored in the database.

use anyhow::Context;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use perp_types::types::{U128, U256};

/// A price or base size: the engine keeps both as integers with 9 decimals.
pub fn b9(value: u64) -> BigDecimal {
    BigDecimal::new(BigInt::from(value), 9)
}

/// An `ifixed` number: signed, 18 decimals.
pub fn ifixed(value: &U256) -> BigDecimal {
    BigDecimal::new(value.to_ifixed(), 18)
}

/// An oracle price: unsigned, 18 decimals.
pub fn oracle_price(value: U128) -> BigDecimal {
    BigDecimal::new(BigInt::from(value.0), 18)
}

/// An integer kept as is, for values that may not fit a signed 64-bit column.
pub fn integer(value: impl Into<BigInt>) -> BigDecimal {
    BigDecimal::new(value.into(), 0)
}

/// An integer for a signed 64-bit column.
pub fn int8(value: u64) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{value} does not fit a 64-bit signed column"))
}

/// The side and price encoded in an order ID.
///
/// An ID is `price << 64 | counter` for asks and `!price << 64 | counter` for bids, so that both
/// sides sort best price first and, within a price, oldest first.
pub fn order_side_and_price(order_id: u128) -> (bool, u64) {
    let is_ask = order_id < 1 << 127;
    let key = (order_id >> 64) as u64;
    (is_ask, if is_ask { key } else { !key })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_ids_carry_side_and_price() {
        let price = 100_010_000_000_000u64;
        assert_eq!(
            order_side_and_price((price as u128) << 64 | 7),
            (true, price)
        );
        assert_eq!(
            order_side_and_price((!price as u128) << 64 | 7),
            (false, price)
        );
    }

    #[test]
    fn decimals_are_exact() {
        let dec = |s: &str| s.parse::<BigDecimal>().unwrap();
        assert_eq!(b9(1_500_000_000), dec("1.5"));
        assert_eq!(ifixed(&U256([0xff; 32])), dec("-0.000000000000000001"));
        assert_eq!(ifixed(&U256::from(25 * 10u128.pow(17))), dec("2.5"));
        assert_eq!(oracle_price(U128(100_000 * 10u128.pow(18))), dec("100000"));
        assert_eq!(integer(u64::MAX), dec("18446744073709551615"));
        assert!(int8(u64::MAX).is_err());
    }
}
