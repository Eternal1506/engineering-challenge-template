//! [`Resync`]: which of a shard's symbols wait for a fresh snapshot after a
//! sequence gap, and until when.
//!
//! A connection delivers its messages in order, so any snapshot of a symbol
//! that arrives after the gap already includes whatever the gap lost. Each
//! symbol leaves the resync on its own snapshot, whichever request it
//! answers; the resync is complete once every symbol has.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

/// The symbols of one shard still waiting for a snapshot, and the deadline
/// for them.
#[derive(Debug)]
pub(super) struct Resync {
    timeout: Duration,
    /// Symbols whose level changes are held back until their next snapshot.
    waiting: HashSet<Arc<str>>,
    /// When to give up on the missing snapshots; `None` with no resync
    /// running, or once it has given up.
    deadline: Option<Instant>,
}

impl Resync {
    /// No resync running; each one started may take `timeout`.
    pub(super) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            waiting: HashSet::new(),
            deadline: None,
        }
    }

    /// Starts a resync of `symbols`, replacing any running one and
    /// restarting the deadline.
    pub(super) fn start(&mut self, symbols: &BTreeSet<Arc<str>>) {
        self.waiting = symbols.iter().cloned().collect();
        let armed = !self.waiting.is_empty();
        self.deadline = armed.then(|| Instant::now() + self.timeout);
    }

    /// Whether `symbol`'s level changes are held back.
    pub(super) fn holds_back(&self, symbol: &str) -> bool {
        !self.waiting.is_empty() && self.waiting.contains(symbol)
    }

    /// `symbol` needs no snapshot any more: one arrived, or the shard
    /// dropped it. True if that completed the resync before its deadline.
    pub(super) fn settle(&mut self, symbol: &str) -> bool {
        let settled = !self.waiting.is_empty() && self.waiting.remove(symbol);
        settled && self.waiting.is_empty() && self.deadline.take().is_some()
    }

    /// When the running resync gives up, if one is running.
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// The deadline passed: stops the clock and returns how many symbols
    /// are still waiting. They keep waiting until [`Resync::clear`].
    pub(super) fn expire(&mut self) -> usize {
        self.deadline = None;
        self.waiting.len()
    }

    /// A new socket brings a fresh snapshot of every symbol before any of
    /// its level changes: ends the resync, returning how many symbols were
    /// still waiting.
    pub(super) fn clear(&mut self) -> usize {
        self.deadline = None;
        std::mem::take(&mut self.waiting).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resync_of(symbols: &[&str]) -> Resync {
        let mut resync = Resync::new(Duration::from_secs(10));
        resync.start(&symbols.iter().map(|&symbol| symbol.into()).collect());
        resync
    }

    #[test]
    fn only_the_symbols_without_a_snapshot_are_held_back() {
        let mut resync = resync_of(&["A", "B"]);
        resync.settle("A");
        let held = (resync.holds_back("A"), resync.holds_back("B"));
        assert_eq!(held, (false, true));
    }

    #[test]
    fn the_last_snapshot_completes_the_resync_and_stops_the_clock() {
        let mut resync = resync_of(&["A", "B"]);
        let first = resync.settle("A");
        let last = resync.settle("B");
        assert_eq!((first, last, resync.deadline()), (false, true, None));
    }

    #[test]
    fn a_second_snapshot_of_a_symbol_completes_nothing() {
        let mut resync = resync_of(&["A", "B"]);
        resync.settle("A");
        assert!(!resync.settle("A"));
    }

    #[test]
    fn snapshots_after_the_deadline_complete_nothing() {
        let mut resync = resync_of(&["A"]);
        let unanswered = resync.expire();
        assert_eq!((unanswered, resync.settle("A")), (1, false));
    }

    #[test]
    fn symbols_keep_waiting_after_the_deadline_until_cleared() {
        let mut resync = resync_of(&["A", "B"]);
        resync.expire();
        let held = resync.holds_back("A");
        let cleared = resync.clear();
        assert_eq!((held, cleared, resync.holds_back("A")), (true, 2, false));
    }

    #[test]
    fn a_restart_holds_back_every_symbol_again() {
        let mut resync = resync_of(&["A", "B"]);
        resync.settle("A");
        resync.start(&["A".into(), "B".into()].into());
        assert!(resync.holds_back("A") && resync.deadline().is_some());
    }

    #[test]
    fn a_resync_of_no_symbols_has_no_deadline() {
        assert_eq!(resync_of(&[]).deadline(), None);
    }
}
