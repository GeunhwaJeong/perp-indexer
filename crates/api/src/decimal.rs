// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Decimals as the API writes them.

use bigdecimal::{BigDecimal, Zero};
use num_bigint::{BigInt, Sign};

/// `value` in plain notation without trailing zeros: never an exponent, never "-0".
///
/// Clients key order book levels by the price string, so a number must always be written the
/// same way whatever scale it was stored or computed with.
pub fn plain(value: &BigDecimal) -> String {
    let (digits, scale) = value.as_bigint_and_exponent();
    let negative = digits.sign() == Sign::Minus;
    let mut digits = digits.magnitude().to_string();
    if digits == "0" {
        return "0".to_owned();
    }

    let mut out = String::with_capacity(digits.len() + 3);
    if negative {
        out.push('-');
    }
    if scale <= 0 {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
        return out;
    }

    let scale = scale as usize;
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale - digits.len() + 1));
    }
    let (whole, fraction) = digits.split_at(digits.len() - scale);
    let fraction = fraction.trim_end_matches('0');
    out.push_str(whole);
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(fraction);
    }
    out
}

/// `10^exponent`.
pub fn pow10(exponent: u32) -> BigDecimal {
    BigDecimal::new(BigInt::from(1), -i64::from(exponent))
}

/// `value` cut to `ifixed` precision, the way the engine's fixed-point arithmetic truncates.
pub fn fixed(value: BigDecimal) -> BigDecimal {
    value.with_scale(18)
}

/// `numerator / denominator` at `ifixed` precision, or zero when the denominator is zero.
pub fn ratio(numerator: &BigDecimal, denominator: &BigDecimal) -> BigDecimal {
    if denominator.is_zero() {
        BigDecimal::zero()
    } else {
        fixed(numerator / denominator)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn dec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn plain_notation_is_canonical() {
        for (input, expected) in [
            ("0", "0"),
            ("0.000", "0"),
            ("-0.0", "0"),
            ("100000.000000000", "100000"),
            ("100000.500000000", "100000.5"),
            ("0.000000001", "0.000000001"),
            ("-12.340", "-12.34"),
            ("1e5", "100000"),
            ("1.5e3", "1500"),
            ("25e-4", "0.0025"),
        ] {
            assert_eq!(plain(&dec(input)), expected, "{input}");
        }
        // The same number at different scales is the same string.
        assert_eq!(plain(&dec("99999.5")), plain(&dec("99999.500000000")));
    }

    #[test]
    fn ratios_truncate_like_the_engine() {
        assert_eq!(plain(&ratio(&dec("1"), &dec("3"))), "0.333333333333333333");
        assert_eq!(
            plain(&ratio(&dec("-2"), &dec("3"))),
            "-0.666666666666666666"
        );
        assert_eq!(plain(&ratio(&dec("1"), &dec("0"))), "0");
        assert_eq!(plain(&pow10(6)), "1000000");
    }
}
