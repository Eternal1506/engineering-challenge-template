//! [`Resync`]: which of a shard's symbols wait for a fresh snapshot after a
//! sequence gap, and until when.
//!
//! A connection delivers its messages in order, so any snapshot of a symbol
//! that arrives after the gap already includes whatever the gap lost. Each
//! symbol leaves the resync on its own snapshot, whichever request it
//! answers; the resync is complete once every symbol has.
//!
//! A symbol still waiting at the deadline may be a market the exchange has
//! dropped, which never answers and never changes, so it is simply kept
//! waiting. Only a change to it after the deadline shows it is live and
//! still stale, and calls for a new socket.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

/// The symbols of one shard still waiting for a snapshot, and where the
/// resync stands.
#[derive(Debug)]
pub(super) struct Resync {
    timeout: Duration,
    /// Symbols whose level changes are held back until their next snapshot.
    waiting: HashSet<Arc<str>>,
    clock: Clock,
}

/// Where a resync stands against its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Clock {
    /// No deadline: no resync is running, or its socket went down.
    Stopped,
    /// Waiting for snapshots until the deadline.
    Running(Instant),
    /// The deadline passed with symbols still waiting.
    Overdue,
    /// A waiting symbol changed after the deadline; a new socket is due.
    Replacing,
}

impl Resync {
    /// No resync running; each one started may take `timeout`.
    pub(super) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            waiting: HashSet::new(),
            clock: Clock::Stopped,
        }
    }

    /// Starts a resync of `symbols`, replacing any running one and
    /// restarting the deadline.
    pub(super) fn start(&mut self, symbols: &BTreeSet<Arc<str>>) {
        self.waiting = symbols.iter().cloned().collect();
        self.clock = if self.waiting.is_empty() {
            Clock::Stopped
        } else {
            Clock::Running(Instant::now() + self.timeout)
        };
    }

    /// Whether `symbol`'s level changes are held back.
    pub(super) fn holds_back(&self, symbol: &str) -> bool {
        self.waiting.contains(symbol)
    }

    /// `symbol` needs no snapshot any more: one arrived, or the shard
    /// dropped it. True if that completed the resync before its deadline.
    pub(super) fn settle(&mut self, symbol: &str) -> bool {
        if !self.waiting.remove(symbol) || !self.waiting.is_empty() {
            return false;
        }
        let in_time = matches!(self.clock, Clock::Running(_));
        self.clock = Clock::Stopped;
        in_time
    }

    /// When the running resync's deadline passes, while its clock runs.
    pub(super) fn deadline(&self) -> Option<Instant> {
        match self.clock {
            Clock::Running(deadline) => Some(deadline),
            _ => None,
        }
    }

    /// The deadline passed: returns how many symbols are still waiting.
    /// They keep waiting until their snapshot or a new socket.
    pub(super) fn expire(&mut self) -> usize {
        self.clock = Clock::Overdue;
        self.waiting.len()
    }

    /// A waiting symbol changed. True the first time that happens after the
    /// deadline, when only a new socket will bring its snapshot.
    pub(super) fn needs_new_socket(&mut self) -> bool {
        let overdue = self.clock == Clock::Overdue;
        if overdue {
            self.clock = Clock::Replacing;
        }
        overdue
    }

    /// The socket went down: the deadline no longer applies, and the next
    /// socket ends the resync.
    pub(super) fn stop_clock(&mut self) {
        self.clock = Clock::Stopped;
    }

    /// A new socket brings a fresh snapshot of every symbol before any of
    /// its level changes: ends the resync, returning how many symbols were
    /// still waiting.
    pub(super) fn clear(&mut self) -> usize {
        self.clock = Clock::Stopped;
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
    fn a_change_before_the_deadline_needs_no_new_socket() {
        assert!(!resync_of(&["A"]).needs_new_socket());
    }

    #[test]
    fn only_the_first_change_after_the_deadline_needs_a_new_socket() {
        let mut resync = resync_of(&["A"]);
        resync.expire();
        let first = resync.needs_new_socket();
        assert_eq!((first, resync.needs_new_socket()), (true, false));
    }

    #[test]
    fn a_stopped_clock_keeps_symbols_waiting_without_a_deadline() {
        let mut resync = resync_of(&["A"]);
        resync.stop_clock();
        let state = (resync.holds_back("A"), resync.deadline());
        assert_eq!(state, (true, None));
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
