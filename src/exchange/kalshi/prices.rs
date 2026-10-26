//! Kalshi prices: parsing them and mapping YES/NO price levels onto one
//! bid/ask book.

use crate::core::fixed::{FIXED_POINT_SCALE, parse_fixed};

/// Parses a Kalshi price such as `"0.5600"` into fixed point, rejecting any
/// value outside 0 to 1 inclusive. Errors include the offending text.
pub fn parse_kalshi_price(text: &str) -> anyhow::Result<i64> {
    let price = parse_fixed(text)?;
    anyhow::ensure!(
        (0..=FIXED_POINT_SCALE).contains(&price),
        "Kalshi price {text:?} is outside the range 0 to 1"
    );
    Ok(price)
}

/// The YES ask price equivalent to a NO bid at `no_price`, both in fixed point.
///
/// A contract pays 1 to whichever of YES and NO settles true, so buying NO at
/// q is the same position as selling YES at 1 − q. Kalshi only publishes bids
/// for each side, so a NO bid at q becomes an ask at 1 − q in the recorded
/// book.
///
/// `no_price` must come from [`parse_kalshi_price`], which keeps it between
/// 0 and 1, so the ask is also between 0 and 1.
pub fn no_bid_to_ask_price(no_price: i64) -> i64 {
    debug_assert!((0..=FIXED_POINT_SCALE).contains(&no_price));
    FIXED_POINT_SCALE - no_price
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_price_of_one() {
        assert_eq!(parse_kalshi_price("1.0000").unwrap(), FIXED_POINT_SCALE);
    }

    #[test]
    fn rejects_a_price_above_one() {
        assert!(parse_kalshi_price("56").is_err());
    }

    #[test]
    fn rejects_a_negative_price() {
        assert!(parse_kalshi_price("-0.01").is_err());
    }

    #[test]
    fn range_error_names_the_offending_text() {
        let error = parse_kalshi_price("56").unwrap_err();
        assert!(error.to_string().contains("\"56\""));
    }

    #[test]
    fn no_bid_becomes_the_complementary_ask() {
        let no_price = parse_kalshi_price("0.3000").unwrap();
        assert_eq!(no_bid_to_ask_price(no_price), 700_000);
    }
}
