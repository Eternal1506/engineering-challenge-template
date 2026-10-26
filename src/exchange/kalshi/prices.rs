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

/// Parses the price of a NO bid, such as `"0.3000"`, and returns the
/// equivalent YES ask price in fixed point.
///
/// A contract pays 1 to whichever of YES and NO settles true, so buying NO at
/// q is the same position as selling YES at 1 − q. Kalshi only publishes bids
/// for each side, so a NO bid at q becomes an ask at 1 − q in the recorded
/// book. Fails like [`parse_kalshi_price`], so the ask is between 0 and 1.
pub fn parse_no_bid_as_ask_price(text: &str) -> anyhow::Result<i64> {
    Ok(FIXED_POINT_SCALE - parse_kalshi_price(text)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_price_of_one() {
        assert_eq!(parse_kalshi_price("1.0000").unwrap(), FIXED_POINT_SCALE);
    }

    #[test]
    fn accepts_a_price_of_zero() {
        assert_eq!(parse_kalshi_price("0.0000").unwrap(), 0);
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
        assert_eq!(parse_no_bid_as_ask_price("0.3000").unwrap(), 700_000);
    }

    #[test]
    fn no_bids_at_the_ends_of_the_range_map_to_the_opposite_end() {
        let asks = ["0", "1"].map(|text| parse_no_bid_as_ask_price(text).unwrap());
        assert_eq!(asks, [FIXED_POINT_SCALE, 0]);
    }

    #[test]
    fn no_bid_out_of_range_is_rejected() {
        let error = parse_no_bid_as_ask_price("1.0001").unwrap_err();
        assert!(error.to_string().contains("\"1.0001\""));
    }
}
