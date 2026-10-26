//! [`Book`] holds one market's price levels and turns feed changes into the
//! absolute level quantities the recorder writes.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::core::events::QuantityChange;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// One market's order book: price → quantity on each side, in fixed point.
///
/// Only levels with a positive quantity are stored. A change that would leave
/// a level negative, or overflow it, is a data-integrity problem: the level is
/// removed and the book is flagged as needing a fresh snapshot. The book does
/// not log; callers check [`AppliedChange::consistent`] or
/// [`needs_resnapshot`](Self::needs_resnapshot).
#[derive(Debug, Default)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    last_exchange_ts: Option<DateTime<Utc>>,
    needs_resnapshot: bool,
}

/// The outcome of applying one level change to a [`Book`].
#[must_use = "an inconsistent change means the book needs a fresh snapshot"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedChange {
    /// The level's absolute quantity afterwards, `0` when the level was
    /// removed. This is what a `PriceLevelUpdate` records.
    pub quantity: i64,
    /// `false` when the change would have left the level negative or
    /// overflowed it, which also flags the book for a resnapshot.
    pub consistent: bool,
}

impl Book {
    /// Applies one level change and reports the level's quantity afterwards.
    ///
    /// The last exchange time moves to `exchange_ts` unless it is already
    /// later.
    pub fn apply(
        &mut self,
        side: Side,
        price: i64,
        change: QuantityChange,
        exchange_ts: DateTime<Utc>,
    ) -> AppliedChange {
        self.last_exchange_ts = self.last_exchange_ts.max(Some(exchange_ts));
        let levels = self.levels_mut(side);
        let current = levels.get(&price).copied().unwrap_or(0);
        let Some(quantity) = resulting_quantity(current, change) else {
            levels.remove(&price);
            self.needs_resnapshot = true;
            return AppliedChange {
                quantity: 0,
                consistent: false,
            };
        };
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        AppliedChange {
            quantity,
            consistent: true,
        }
    }

    /// Replaces both sides with the given `(price, quantity)` levels and
    /// clears [`needs_resnapshot`](Self::needs_resnapshot).
    ///
    /// When a price repeats, its last entry wins; levels left with a quantity
    /// of zero or less are dropped. The last exchange time moves to
    /// `exchange_ts` unless that is `None` or earlier.
    pub fn replace(
        &mut self,
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
        exchange_ts: Option<DateTime<Utc>>,
    ) {
        self.bids = positive_levels(bids);
        self.asks = positive_levels(asks);
        self.last_exchange_ts = self.last_exchange_ts.max(exchange_ts);
        self.needs_resnapshot = false;
    }

    /// Whether a change left the book inconsistent with the exchange, so it
    /// must be replaced from a fresh snapshot.
    pub fn needs_resnapshot(&self) -> bool {
        self.needs_resnapshot
    }

    /// The latest exchange time of any change or snapshot, if any.
    pub fn last_exchange_ts(&self) -> Option<DateTime<Utc>> {
        self.last_exchange_ts
    }

    /// Builds an SBE snapshot of the book, with each side ordered best price
    /// first. The exchange timestamp falls back to `received_at` when the book
    /// has never seen one.
    pub fn to_snapshot(&self, symbol: &str, received_at: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp: self.last_exchange_ts.unwrap_or(received_at),
            bids: self.bid_entries(),
            asks: self.ask_entries(),
            symbol: symbol.to_owned(),
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Bids from the highest price down.
    fn bid_entries(&self) -> Vec<SnapshotBidsEntryDomain> {
        self.bids
            .iter()
            .rev()
            .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect()
    }

    /// Asks from the lowest price up.
    fn ask_entries(&self) -> Vec<SnapshotAsksEntryDomain> {
        self.asks
            .iter()
            .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect()
    }
}

/// The level quantity after `change`, or `None` if it would be negative or
/// overflow.
fn resulting_quantity(current: i64, change: QuantityChange) -> Option<i64> {
    let quantity = match change {
        QuantityChange::Absolute(quantity) => Some(quantity),
        QuantityChange::Delta(delta) => current.checked_add(delta),
    };
    quantity.filter(|&quantity| quantity >= 0)
}

/// Collects levels so the last entry for a price wins, then drops levels
/// whose final quantity is not positive.
fn positive_levels(levels: impl IntoIterator<Item = (i64, i64)>) -> BTreeMap<i64, i64> {
    let mut levels: BTreeMap<i64, i64> = levels.into_iter().collect();
    levels.retain(|_, quantity| *quantity > 0);
    levels
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRICE: i64 = 400_000;

    const INCONSISTENT: AppliedChange = AppliedChange {
        quantity: 0,
        consistent: false,
    };

    type Levels = Vec<(i64, i64)>;

    fn consistent(quantity: i64) -> AppliedChange {
        AppliedChange {
            quantity,
            consistent: true,
        }
    }

    fn ts(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap()
    }

    fn book_with_bid(quantity: i64) -> Book {
        let mut book = Book::default();
        book.replace([(PRICE, quantity)], [], Some(ts(0)));
        book
    }

    /// Both sides as `(price, quantity)` pairs, best price first.
    fn side_levels(book: &Book) -> (Levels, Levels) {
        let snapshot = book.to_snapshot("T", ts(0));
        let bids = snapshot.bids.iter().map(|e| (e.price_level, e.quantity));
        let asks = snapshot.asks.iter().map(|e| (e.price_level, e.quantity));
        (bids.collect(), asks.collect())
    }

    /// The bid levels and whether the book is flagged for a resnapshot.
    fn bid_state(book: &Book) -> (Levels, bool) {
        (side_levels(book).0, book.needs_resnapshot())
    }

    fn apply_bid(book: &mut Book, change: QuantityChange) -> AppliedChange {
        book.apply(Side::Buy, PRICE, change, ts(1))
    }

    #[test]
    fn deltas_accumulate() {
        let mut book = Book::default();
        let first = apply_bid(&mut book, QuantityChange::Delta(2_000_000));
        let second = apply_bid(&mut book, QuantityChange::Delta(500_000));
        assert_eq!(
            (first, second),
            (consistent(2_000_000), consistent(2_500_000))
        );
    }

    #[test]
    fn delta_to_zero_removes_the_level_and_returns_zero() {
        let mut book = book_with_bid(1_000_000);
        let applied = apply_bid(&mut book, QuantityChange::Delta(-1_000_000));
        assert_eq!(
            (applied, bid_state(&book)),
            (consistent(0), (vec![], false))
        );
    }

    #[test]
    fn negative_result_returns_zero_removes_the_level_and_flags_the_book() {
        let mut book = book_with_bid(1_000_000);
        let applied = apply_bid(&mut book, QuantityChange::Delta(-3_000_000));
        assert_eq!((applied, bid_state(&book)), (INCONSISTENT, (vec![], true)));
    }

    #[test]
    fn absolute_sets_the_level() {
        let mut book = book_with_bid(1_000_000);
        let applied = apply_bid(&mut book, QuantityChange::Absolute(7_000_000));
        let expected_levels = vec![(PRICE, 7_000_000)];
        assert_eq!(
            (applied, bid_state(&book)),
            (consistent(7_000_000), (expected_levels, false))
        );
    }

    #[test]
    fn absolute_zero_removes_the_level() {
        let mut book = book_with_bid(1_000_000);
        let applied = apply_bid(&mut book, QuantityChange::Absolute(0));
        assert_eq!(
            (applied, bid_state(&book)),
            (consistent(0), (vec![], false))
        );
    }

    #[test]
    fn negative_absolute_flags_the_book() {
        let mut book = book_with_bid(1_000_000);
        let applied = apply_bid(&mut book, QuantityChange::Absolute(-1));
        assert_eq!((applied, bid_state(&book)), (INCONSISTENT, (vec![], true)));
    }

    #[test]
    fn add_overflow_flags_the_book_without_panicking() {
        let mut book = book_with_bid(i64::MAX);
        let applied = apply_bid(&mut book, QuantityChange::Delta(1));
        assert_eq!((applied, bid_state(&book)), (INCONSISTENT, (vec![], true)));
    }

    #[test]
    fn replace_drops_non_positive_levels_and_clears_the_flag() {
        let mut book = book_with_bid(1_000_000);
        let applied = apply_bid(&mut book, QuantityChange::Delta(-2_000_000));
        assert_eq!((applied, book.needs_resnapshot()), (INCONSISTENT, true));
        book.replace([(300_000, 0), (200_000, -5), (100_000, 4)], [], Some(ts(2)));
        assert_eq!(bid_state(&book), (vec![(100_000, 4)], false));
    }

    #[test]
    fn replace_discards_every_previous_level_on_both_sides() {
        let mut book = Book::default();
        book.replace([(100_000, 1), (200_000, 2)], [(800_000, 3)], Some(ts(0)));
        book.replace([(300_000, 4)], [(600_000, 5), (700_000, 6)], Some(ts(1)));
        assert_eq!(
            side_levels(&book),
            (vec![(300_000, 4)], vec![(600_000, 5), (700_000, 6)])
        );
    }

    #[test]
    fn replace_keeps_the_last_entry_for_a_repeated_price() {
        let mut book = Book::default();
        book.replace([(PRICE, 5), (PRICE, 0)], [], Some(ts(0)));
        assert_eq!(side_levels(&book), (vec![], vec![]));
    }

    #[test]
    fn replace_without_a_timestamp_keeps_the_previous_one() {
        let mut book = book_with_bid(1_000_000);
        book.replace([], [], None);
        assert_eq!(book.last_exchange_ts(), Some(ts(0)));
    }

    #[test]
    fn replace_never_moves_the_exchange_time_backwards() {
        let mut book = book_with_bid(1_000_000);
        book.replace([], [], Some(ts(-1)));
        assert_eq!(book.last_exchange_ts(), Some(ts(0)));
    }

    #[test]
    fn last_exchange_ts_tracks_the_latest_apply() {
        let mut book = Book::default();
        let bid = book.apply(Side::Buy, PRICE, QuantityChange::Delta(1), ts(5));
        let ask = book.apply(Side::Sell, 600_000, QuantityChange::Delta(1), ts(9));
        assert_eq!((bid, ask), (consistent(1), consistent(1)));
        assert_eq!(book.last_exchange_ts(), Some(ts(9)));
    }

    #[test]
    fn apply_never_moves_the_exchange_time_backwards() {
        let mut book = Book::default();
        let later = book.apply(Side::Buy, PRICE, QuantityChange::Delta(1), ts(9));
        let earlier = book.apply(Side::Buy, PRICE, QuantityChange::Delta(1), ts(5));
        assert_eq!((later, earlier), (consistent(1), consistent(2)));
        assert_eq!(book.last_exchange_ts(), Some(ts(9)));
    }

    #[test]
    fn to_snapshot_orders_each_side_best_first() {
        let mut book = Book::default();
        book.replace(
            [(300_000, 1), (450_000, 2), (100_000, 3)],
            [(900_000, 4), (550_000, 5), (700_000, 6)],
            Some(ts(0)),
        );
        let (bids, asks) = side_levels(&book);
        let bid_prices: Vec<_> = bids.iter().map(|&(price, _)| price).collect();
        let ask_prices: Vec<_> = asks.iter().map(|&(price, _)| price).collect();
        assert_eq!(
            (bid_prices, ask_prices),
            (
                vec![450_000, 300_000, 100_000],
                vec![550_000, 700_000, 900_000]
            )
        );
    }

    #[test]
    fn to_snapshot_carries_symbol_and_both_timestamps() {
        let book = book_with_bid(1_000_000);
        let snapshot = book.to_snapshot("KXTEST-1", ts(3));
        let timestamps = (snapshot.received_at, snapshot.exchange_timestamp);
        assert_eq!(
            (snapshot.symbol.as_str(), timestamps),
            ("KXTEST-1", (ts(3), ts(0)))
        );
    }

    #[test]
    fn to_snapshot_falls_back_to_received_at_for_exchange_time() {
        let snapshot = Book::default().to_snapshot("T", ts(3));
        assert_eq!(snapshot.exchange_timestamp, ts(3));
    }
}
