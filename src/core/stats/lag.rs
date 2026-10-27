//! [`LagHistogram`]: feed lags in log-spaced buckets, cheap enough to note
//! for every level change from many tasks at once, and read back as the
//! median and 99th percentile.
//!
//! Lags under [`EXACT`] milliseconds each have a bucket. Above that, every
//! power of two is split into [`STEPS`] buckets, so a quantile is reported
//! at most 1/16 below the true lag. Lags beyond about 4.6 hours share the
//! last bucket. Negative lags, from a local clock behind the exchange's,
//! are kept apart by size, so they still show.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lags below this many milliseconds each have a bucket of their own.
const EXACT: u64 = 16;

/// The buckets each power of two at or above [`EXACT`] is split into.
const STEPS: u64 = 16;

/// The powers of two above [`EXACT`] that get buckets: up to 2^24 ms.
const OCTAVES: u64 = 20;

/// The buckets on each side of zero.
const BUCKETS: usize = (EXACT + STEPS * OCTAVES) as usize;

/// How far below a power of two the exact buckets end: 2^4 = [`EXACT`].
const EXACT_BITS: u32 = 4;

/// Counts of feed lags, in milliseconds, since it was last read.
#[derive(Debug)]
pub(crate) struct LagHistogram {
    /// Lags of zero or more, by bucket.
    late: [AtomicU64; BUCKETS],
    /// The size of negative lags, by bucket.
    early: [AtomicU64; BUCKETS],
}

/// The median and 99th percentile of the lags noted over a period, in
/// milliseconds; `None` when none were.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LagQuantiles {
    /// The median lag.
    pub p50: Option<i64>,
    /// The 99th percentile lag.
    pub p99: Option<i64>,
}

impl Default for LagHistogram {
    fn default() -> Self {
        Self {
            late: std::array::from_fn(|_| AtomicU64::new(0)),
            early: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl LagHistogram {
    /// Counts one lag of `lag_ms` milliseconds.
    pub(crate) fn note(&self, lag_ms: i64) {
        let side = if lag_ms < 0 { &self.early } else { &self.late };
        side[bucket_of(lag_ms.unsigned_abs())].fetch_add(1, Ordering::Relaxed);
    }

    /// The quantiles of the lags noted since the last call, which starts
    /// the count over.
    pub(crate) fn take_quantiles(&self) -> LagQuantiles {
        let counts = self.take_ordered();
        let total = counts.iter().map(|&(_, count)| count).sum();
        LagQuantiles {
            p50: quantile(&counts, total, 50),
            p99: quantile(&counts, total, 99),
        }
    }

    /// Each bucket's lowest lag with its count, emptying it, from the most
    /// negative lag to the largest.
    fn take_ordered(&self) -> Vec<(i64, u64)> {
        let take = |count: &AtomicU64| count.swap(0, Ordering::Relaxed);
        let lowest = |bucket| i64::try_from(lowest_of(bucket)).unwrap_or(i64::MAX);
        let early = self.early.iter().enumerate().rev();
        let early = early.map(|(bucket, count)| (-lowest(bucket), take(count)));
        let late = self.late.iter().enumerate();
        let late = late.map(|(bucket, count)| (lowest(bucket), take(count)));
        early.chain(late).filter(|&(_, count)| count > 0).collect()
    }
}

/// The bucket of a lag of `size` milliseconds, either side of zero.
fn bucket_of(size: u64) -> usize {
    if size < EXACT {
        return size as usize;
    }
    let octave = u64::from(size.ilog2() - EXACT_BITS);
    if octave >= OCTAVES {
        return BUCKETS - 1;
    }
    let step = (size >> octave) & (STEPS - 1);
    (EXACT + octave * STEPS + step) as usize
}

/// The smallest lag size bucket `bucket` holds.
fn lowest_of(bucket: usize) -> u64 {
    let bucket = bucket as u64;
    if bucket < EXACT {
        return bucket;
    }
    let (octave, step) = ((bucket - EXACT) / STEPS, (bucket - EXACT) % STEPS);
    (STEPS + step) << octave
}

/// The lag at or below which `percent` of the `total` counted lie, from
/// `counts` ordered by lag.
fn quantile(counts: &[(i64, u64)], total: u64, percent: u64) -> Option<i64> {
    let rank = (total * percent).div_ceil(100).max(1);
    let mut seen = 0;
    let reached = counts.iter().find(|&&(_, count)| {
        seen += count;
        seen >= rank
    });
    reached.map(|&(lag, _)| lag)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram_of(lags: impl IntoIterator<Item = i64>) -> LagHistogram {
        let histogram = LagHistogram::default();
        lags.into_iter().for_each(|lag| histogram.note(lag));
        histogram
    }

    #[test]
    fn small_lags_are_exact() {
        let quantiles = histogram_of([3, 5, 5, 7, 15]).take_quantiles();
        assert_eq!((quantiles.p50, quantiles.p99), (Some(5), Some(15)));
    }

    #[test]
    fn a_large_lag_is_reported_at_most_a_sixteenth_low() {
        for lag in [16, 17, 100, 999, 1_052, 65_535, 1 << 20] {
            let reported = histogram_of([lag]).take_quantiles().p50.unwrap();
            let close = reported <= lag && reported * 17 / 16 >= lag;
            assert!(close, "{lag} {reported}");
        }
    }

    #[test]
    fn the_99th_percentile_reaches_into_the_tail() {
        let lags = std::iter::repeat_n(30, 98).chain([700, 2_000]);
        let quantiles = histogram_of(lags).take_quantiles();
        assert_eq!((quantiles.p50, quantiles.p99), (Some(30), Some(672)));
    }

    #[test]
    fn negative_lags_sort_below_zero() {
        let quantiles = histogram_of([-40, -40, -3, 2]).take_quantiles();
        assert_eq!(quantiles.p50, Some(-40));
    }

    #[test]
    fn lags_beyond_the_last_bucket_share_it() {
        let quantiles = histogram_of([i64::MAX, i64::MIN]).take_quantiles();
        let last = 31 << 19;
        assert_eq!((quantiles.p50, quantiles.p99), (Some(-last), Some(last)));
    }

    #[test]
    fn reading_starts_the_count_over() {
        let histogram = histogram_of([10]);
        histogram.take_quantiles();
        assert_eq!(histogram.take_quantiles(), LagQuantiles::default());
    }
}
