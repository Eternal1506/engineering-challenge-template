//! [`BooklessRequests`]: when the writer last asked for a snapshot of each
//! symbol whose level changes found no book, so it asks at most once per
//! [`BOOKLESS_RESNAPSHOT_INTERVAL`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

/// How long after asking for a snapshot of a symbol whose level change
/// found no book the writer waits before asking again.
pub(super) const BOOKLESS_RESNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);

/// Symbols with no book, each with when the writer last tried to ask for a
/// snapshot of it.
#[derive(Debug, Default)]
pub(super) struct BooklessRequests {
    asked_at: HashMap<Arc<str>, Instant>,
}

impl BooklessRequests {
    /// Whether to ask for a snapshot of `symbol` at `now`: unless the last
    /// attempt was less than [`BOOKLESS_RESNAPSHOT_INTERVAL`] ago.
    pub(super) fn should_ask(&self, symbol: &str, now: Instant) -> bool {
        let asked_at = self.asked_at.get(symbol);
        asked_at.is_none_or(|at| !is_recent(*at, now))
    }

    /// Notes an attempt to ask for a snapshot of `symbol` at `now`.
    pub(super) fn note_asked(&mut self, symbol: Arc<str>, now: Instant) {
        self.asked_at.insert(symbol, now);
    }

    /// Forgets `symbol`, which has a book now or is no longer recorded.
    pub(super) fn remove(&mut self, symbol: &str) {
        self.asked_at.remove(symbol);
    }

    /// Drops the attempts at least [`BOOKLESS_RESNAPSHOT_INTERVAL`] old at
    /// `now`, which no longer hold back another.
    pub(super) fn prune(&mut self, now: Instant) {
        self.asked_at.retain(|_, at| is_recent(*at, now));
    }
}

/// Whether an attempt at `at` holds back another at `now`.
fn is_recent(at: Instant, now: Instant) -> bool {
    now.duration_since(at) < BOOKLESS_RESNAPSHOT_INTERVAL
}

#[cfg(test)]
mod tests {
    use super::*;

    const JUST_UNDER: Duration =
        BOOKLESS_RESNAPSHOT_INTERVAL.saturating_sub(Duration::from_millis(1));

    fn asked_at(start: Instant) -> BooklessRequests {
        let mut requests = BooklessRequests::default();
        requests.note_asked("A".into(), start);
        requests
    }

    #[test]
    fn a_symbol_never_asked_for_is_asked_for() {
        let requests = BooklessRequests::default();
        assert!(requests.should_ask("A", Instant::now()));
    }

    #[test]
    fn a_symbol_is_not_asked_for_again_within_the_interval() {
        let start = Instant::now();
        assert!(!asked_at(start).should_ask("A", start + JUST_UNDER));
    }

    #[test]
    fn a_symbol_is_asked_for_again_once_the_interval_has_passed() {
        let start = Instant::now();
        let requests = asked_at(start);
        assert!(requests.should_ask("A", start + BOOKLESS_RESNAPSHOT_INTERVAL));
    }

    #[test]
    fn other_symbols_are_asked_for_within_the_interval() {
        let start = Instant::now();
        assert!(asked_at(start).should_ask("B", start));
    }

    #[test]
    fn a_removed_symbol_is_asked_for_again_at_once() {
        let start = Instant::now();
        let mut requests = asked_at(start);
        requests.remove("A");
        assert!(requests.should_ask("A", start));
    }

    #[test]
    fn pruning_keeps_an_attempt_within_the_interval() {
        let start = Instant::now();
        let mut requests = asked_at(start);
        requests.prune(start + JUST_UNDER);
        assert!(requests.asked_at.contains_key("A"));
    }

    #[test]
    fn pruning_drops_an_attempt_once_the_interval_has_passed() {
        let start = Instant::now();
        let mut requests = asked_at(start);
        requests.prune(start + BOOKLESS_RESNAPSHOT_INTERVAL);
        assert!(requests.asked_at.is_empty());
    }
}
