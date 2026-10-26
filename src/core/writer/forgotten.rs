//! [`Forgotten`]: the symbols whose events the writer drops, each for
//! [`FORGOTTEN_RETENTION`] after the writer processed its `Forget`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

/// How long the writer keeps dropping a forgotten symbol's events, from
/// when it processes the symbol's [`Forget`](super::WriterMessage::Forget).
/// A removed market's old subscription has long stopped sending by then, so
/// afterwards the symbol is treated as new: its book starts from its next
/// snapshot. The writer prunes expired symbols on every snapshot tick, so a
/// symbol stays forgotten for at most this long plus one tick.
///
/// The coordinator's registry remembers a departed market for this long
/// plus a grace period longer than the writer's lag in processing the
/// `Forget` and a tick, so it keeps sending
/// [`Track`](super::WriterMessage::Track) for a returning market for as
/// long as the writer could still be dropping its events.
pub const FORGOTTEN_RETENTION: Duration = Duration::from_secs(600);

/// Forgotten symbols, each with when it was forgotten.
#[derive(Debug, Default)]
pub(crate) struct Forgotten {
    since: HashMap<Arc<str>, Instant>,
}

impl Forgotten {
    /// Forgets `symbol` from `now`.
    pub(crate) fn insert(&mut self, symbol: Arc<str>, now: Instant) {
        self.since.insert(symbol, now);
    }

    /// Accepts `symbol` again.
    pub(crate) fn remove(&mut self, symbol: &str) {
        self.since.remove(symbol);
    }

    /// Whether `symbol` is forgotten.
    pub(crate) fn contains(&self, symbol: &str) -> bool {
        self.since.contains_key(symbol)
    }

    /// Drops the symbols forgotten for longer than [`FORGOTTEN_RETENTION`]
    /// at `now`.
    pub(crate) fn prune(&mut self, now: Instant) {
        let expired = |since: &Instant| now.duration_since(*since) > FORGOTTEN_RETENTION;
        self.since.retain(|_, since| !expired(since));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forgotten_at(start: Instant) -> Forgotten {
        let mut forgotten = Forgotten::default();
        forgotten.insert("A".into(), start);
        forgotten
    }

    #[test]
    fn a_symbol_stays_forgotten_for_the_retention() {
        let start = Instant::now();
        let mut forgotten = forgotten_at(start);
        forgotten.prune(start + FORGOTTEN_RETENTION);
        assert!(forgotten.contains("A"));
    }

    #[test]
    fn a_symbol_is_accepted_again_after_the_retention() {
        let start = Instant::now();
        let mut forgotten = forgotten_at(start);
        forgotten.prune(start + FORGOTTEN_RETENTION + Duration::from_millis(1));
        assert!(!forgotten.contains("A"));
    }
}
