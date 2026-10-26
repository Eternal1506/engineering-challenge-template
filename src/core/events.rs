//! Types that cross the boundary between exchange integrations and the core.

/// How a feed reports the new size of one price level, in fixed point.
///
/// Some exchanges send the absolute size of a level after the change; others,
/// such as Kalshi, send the signed change to add to the current size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityChange {
    /// The level's total quantity after the change.
    Absolute(i64),
    /// The signed amount to add to the level's current quantity.
    Delta(i64),
}
