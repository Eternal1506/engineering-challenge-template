//! [`MismatchWarnings`]: at most one warning per interval about audit
//! snapshots that differ from their books, so a mismatch that repeats for
//! every market cannot flood the log. The others are logged at debug, and
//! the next warning counts them.

use std::time::Duration;

use tokio::time::Instant;

use crate::core::book::LevelDifference;

/// The least time between two warnings about audit mismatches.
pub(super) const MISMATCH_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// The most differing levels a mismatch logs.
const LOGGED_DIFFERENCES: usize = 10;

/// When the writer last warned about an audit mismatch, and how many it
/// has logged only at debug since.
#[derive(Debug, Default)]
pub(super) struct MismatchWarnings {
    last_warned: Option<Instant>,
    unwarned: u64,
}

impl MismatchWarnings {
    /// Logs that an audit snapshot of `symbol` differs from its book at
    /// `now`, with the first of the `differences`: at warn, with the count
    /// of mismatches logged at debug since the last warning, unless a
    /// warning went out within the interval; at debug otherwise.
    pub(super) fn log(&mut self, symbol: &str, differences: &[LevelDifference], now: Instant) {
        let shown = &differences[..differences.len().min(LOGGED_DIFFERENCES)];
        let differing = differences.len();
        let recent = |at: Instant| now.saturating_duration_since(at) < MISMATCH_WARNING_INTERVAL;
        if self.last_warned.is_some_and(recent) {
            self.unwarned += 1;
            tracing::debug!(symbol, differing, levels = ?shown, "an audit snapshot differs from the book, writing it");
            return;
        }
        let unwarned = std::mem::take(&mut self.unwarned);
        self.last_warned = Some(now);
        tracing::warn!(symbol, differing, levels = ?shown, unwarned, "an audit snapshot differs from the book, writing it");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::core::test_logs::logs_of;

    fn differences() -> Vec<LevelDifference> {
        vec![LevelDifference {
            side: Side::Buy,
            price: 400_000,
            book: 100,
            snapshot: 90,
        }]
    }

    /// How many lines of `logs` are warnings.
    fn warnings_in(logs: &str) -> usize {
        let warning = |line: &&str| line.split_whitespace().nth(1) == Some("WARN");
        logs.lines().filter(warning).count()
    }

    #[test]
    fn only_one_mismatch_an_interval_is_a_warning() {
        let mut warnings = MismatchWarnings::default();
        let start = Instant::now();
        let logs = logs_of(|| {
            warnings.log("A", &differences(), start);
            warnings.log("B", &differences(), start + Duration::from_secs(59));
        });
        assert_eq!(warnings_in(&logs), 1, "{logs}");
    }

    #[test]
    fn the_next_warning_counts_the_mismatches_logged_at_debug() {
        let mut warnings = MismatchWarnings::default();
        let start = Instant::now();
        warnings.log("A", &differences(), start);
        warnings.log("B", &differences(), start);
        let later = start + MISMATCH_WARNING_INTERVAL;
        let logs = logs_of(|| warnings.log("C", &differences(), later));
        let counted = logs.contains("WARN") && logs.contains("unwarned=1");
        assert!(counted, "{logs}");
    }
}
