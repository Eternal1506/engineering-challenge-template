//! Exact decimal parsing into the fixed-point integers the SBE schema uses.

/// The number of decimal places fixed point represents exactly.
const FRACTION_DIGITS: usize = 6;

/// The fixed-point value of one whole unit, `1_000_000`: 1 USDC for a price
/// and 1 contract for a quantity.
pub const FIXED_POINT_SCALE: i64 = 10_i64.pow(FRACTION_DIGITS as u32);

/// Parses a decimal string such as `"0.1234"` or `"-1.50"` into fixed point,
/// scaled by [`FIXED_POINT_SCALE`], without going through floating point.
///
/// Accepts an optional `+` or `-`, at least one integer digit, and an optional
/// `.` followed by 1 to 6 fractional digits. Anything else is an error, as is
/// a value outside the `i64` range. Errors include the offending text.
pub fn parse_fixed(text: &str) -> anyhow::Result<i64> {
    let (sign, unsigned) = split_sign(text);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    anyhow::ensure!(
        is_digits(integer),
        "invalid decimal {text:?}: expected integer digits"
    );
    anyhow::ensure!(
        !unsigned.contains('.') || is_digits(fraction),
        "invalid decimal {text:?}: expected digits after the decimal point"
    );
    anyhow::ensure!(
        fraction.len() <= FRACTION_DIGITS,
        "invalid decimal {text:?}: more than {FRACTION_DIGITS} decimal places"
    );
    scale_digits(sign, integer, fraction)
        .ok_or_else(|| anyhow::anyhow!("decimal {text:?} is out of the fixed-point range"))
}

/// Splits a leading `+` or `-` off `text`, returning `1` or `-1` and the rest.
fn split_sign(text: &str) -> (i64, &str) {
    match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text.strip_prefix('+').unwrap_or(text)),
    }
}

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// Combines validated integer and fraction digits into a signed fixed-point
/// value, or `None` on overflow. The sign is applied digit by digit so that
/// `i64::MIN` itself is reachable.
fn scale_digits(sign: i64, integer: &str, fraction: &str) -> Option<i64> {
    let padding = std::iter::repeat_n(b'0', FRACTION_DIGITS - fraction.len());
    integer
        .bytes()
        .chain(fraction.bytes())
        .chain(padding)
        .try_fold(0_i64, |value, digit| {
            value
                .checked_mul(10)?
                .checked_add(sign * i64::from(digit - b'0'))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(text: &str) -> i64 {
        parse_fixed(text).unwrap()
    }

    #[test]
    fn parses_four_decimal_places() {
        assert_eq!(parsed("0.1234"), 123_400);
    }

    #[test]
    fn parses_a_whole_number() {
        assert_eq!(parsed("1"), FIXED_POINT_SCALE);
    }

    #[test]
    fn parses_a_negative_number() {
        assert_eq!(parsed("-1.50"), -1_500_000);
    }

    #[test]
    fn parses_an_explicit_plus_sign() {
        assert_eq!(parsed("+2.5"), 2_500_000);
    }

    #[test]
    fn parses_zero() {
        assert_eq!(parsed("0"), 0);
    }

    #[test]
    fn parses_trailing_zeros() {
        assert_eq!(parsed("13.00"), 13_000_000);
    }

    #[test]
    fn parses_the_smallest_unit() {
        assert_eq!(parsed("0.000001"), 1);
    }

    #[test]
    fn parses_the_ends_of_the_i64_range() {
        assert_eq!(
            (
                parsed("9223372036854.775807"),
                parsed("-9223372036854.775808")
            ),
            (i64::MAX, i64::MIN)
        );
    }

    #[test]
    fn rejects_seven_decimal_places() {
        assert!(parse_fixed("1.0000001").is_err());
    }

    #[test]
    fn rejects_malformed_input() {
        let malformed = [
            "", "-", "+", "abc", "1.2.3", "1e3", " 1", "1 ", "1.", ".5", "-.5",
        ];
        let accepted: Vec<_> = malformed
            .into_iter()
            .filter(|text| parse_fixed(text).is_ok())
            .collect();
        assert!(accepted.is_empty(), "accepted {accepted:?}");
    }

    #[test]
    fn rejects_values_beyond_i64_without_panicking() {
        let forty_digits = "1".repeat(40);
        let out_of_range = [
            "9223372036854.775808",
            "-9223372036854.775809",
            &forty_digits,
        ];
        let rejected = out_of_range.map(|text| parse_fixed(text).is_err());
        assert_eq!(rejected, [true; 3]);
    }

    #[test]
    fn error_names_the_offending_text() {
        let error = parse_fixed("12x").unwrap_err();
        assert!(error.to_string().contains("\"12x\""));
    }
}
