//! [`Resync`]: which of a shard's symbols wait for a fresh snapshot after a
//! sequence gap or a failover, and what to do about one that changes while
//! it waits.
//!
//! A connection delivers its messages in order, so any snapshot of a symbol
//! that arrives after the gap already includes whatever the gap lost. Each
//! symbol leaves the resync on its own snapshot, whichever request it
//! answers; the resync is complete once every symbol has. A few symbols can
//! also be held on their own, as when they are resubscribed, which joins
//! them to any resync running or starts one of just them.
//!
//! A symbol still waiting at the deadline may be a market the exchange has
//! dropped, which never answers and never changes, so it is simply kept
//! waiting. A change to it after the deadline shows it is live and still
//! stale: its snapshot is asked for again, at most once per
//! [`ResyncTimings::resnapshot_interval`]. Only once the resync has lasted
//! [`ResyncTimings::reconnect_after`], and the socket has been the primary
//! for [`ResyncTimings::primary_cooldown`], does such a change call for a
//! new socket.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::ResyncTimings;

/// The symbols of one shard still waiting for a snapshot, and where the
/// resync stands.
#[derive(Debug)]
pub(super) struct Resync {
    timings: ResyncTimings,
    /// Symbols whose level changes are held back until their next
    /// snapshot, each with when its snapshot was last asked for again.
    waiting: HashMap<Arc<str>, Option<Instant>>,
    started: Instant,
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
    /// A waiting symbol changed past the ceiling; a new socket is due.
    Replacing,
}

/// What a change to a held symbol calls for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Remedy {
    /// Nothing: the snapshot asked for may still come.
    Wait,
    /// Ask for the symbol's snapshot again.
    Resnapshot,
    /// Replace the socket: its resubscribe brings every snapshot.
    NewSocket,
}

impl Resync {
    /// No resync running; each one started runs on `timings`.
    pub(super) fn new(timings: ResyncTimings) -> Self {
        Self {
            timings,
            waiting: HashMap::new(),
            started: Instant::now(),
            clock: Clock::Stopped,
        }
    }

    /// Starts a resync of `symbols`, replacing any running one and
    /// restarting the deadline.
    pub(super) fn start(&mut self, symbols: &BTreeSet<Arc<str>>) {
        let unasked = |symbol: &Arc<str>| (symbol.clone(), None);
        self.waiting = symbols.iter().map(unasked).collect();
        self.started = Instant::now();
        self.clock = if self.waiting.is_empty() {
            Clock::Stopped
        } else {
            Clock::Running(self.started + self.timings.timeout)
        };
    }

    /// Holds back `symbols` too, until their next snapshot, as in a
    /// resync. Starts the deadline if no resync is running; one that is
    /// keeps its own.
    pub(super) fn hold(&mut self, symbols: &[Arc<str>]) {
        for symbol in symbols {
            self.waiting.entry(symbol.clone()).or_insert(None);
        }
        if self.clock == Clock::Stopped && !self.waiting.is_empty() {
            self.started = Instant::now();
            self.clock = Clock::Running(self.started + self.timings.timeout);
        }
    }

    /// Whether `symbol`'s level changes are held back.
    pub(super) fn holds_back(&self, symbol: &str) -> bool {
        self.waiting.contains_key(symbol)
    }

    /// `symbol` needs no snapshot any more: one arrived, or the shard
    /// dropped it. True if that completed the resync before its deadline.
    pub(super) fn settle(&mut self, symbol: &str) -> bool {
        if self.waiting.remove(symbol).is_none() || !self.waiting.is_empty() {
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

    /// Held `symbol` changed on a socket that has been the primary for
    /// `primary_for`. Before the deadline the snapshot asked for may still
    /// come; after it, a long resync on a settled socket calls for a new
    /// socket, once, and otherwise the symbol's snapshot is asked for again
    /// if it was not asked for within the interval.
    pub(super) fn remedy(&mut self, symbol: &str, primary_for: Duration) -> Remedy {
        if self.clock != Clock::Overdue {
            return Remedy::Wait;
        }
        let now = Instant::now();
        let ceiling_passed = now - self.started >= self.timings.reconnect_after;
        if ceiling_passed && primary_for >= self.timings.primary_cooldown {
            self.clock = Clock::Replacing;
            return Remedy::NewSocket;
        }
        self.resnapshot_if_due(symbol, now)
    }

    fn resnapshot_if_due(&mut self, symbol: &str, now: Instant) -> Remedy {
        let Some(asked_again) = self.waiting.get_mut(symbol) else {
            return Remedy::Wait;
        };
        let interval = self.timings.resnapshot_interval;
        if asked_again.is_some_and(|at| now - at < interval) {
            return Remedy::Wait;
        }
        *asked_again = Some(now);
        Remedy::Resnapshot
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

    const HOUR: Duration = Duration::from_secs(3600);

    /// Timings whose ceiling and cooldown are `reconnect_after` and
    /// `primary_cooldown`, with a ten-second deadline and interval.
    fn timings(reconnect_after: Duration, primary_cooldown: Duration) -> ResyncTimings {
        ResyncTimings {
            timeout: Duration::from_secs(10),
            resnapshot_interval: Duration::from_secs(10),
            reconnect_after,
            primary_cooldown,
        }
    }

    fn resync_on(timings: ResyncTimings, symbols: &[&str]) -> Resync {
        let mut resync = Resync::new(timings);
        resync.start(&symbols.iter().map(|&symbol| symbol.into()).collect());
        resync
    }

    fn resync_of(symbols: &[&str]) -> Resync {
        resync_on(timings(HOUR, HOUR), symbols)
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
    fn a_change_before_the_deadline_waits_for_the_snapshot_asked_for() {
        let mut resync = resync_on(timings(Duration::ZERO, Duration::ZERO), &["A"]);
        assert_eq!(resync.remedy("A", HOUR), Remedy::Wait);
    }

    #[test]
    fn a_change_after_the_deadline_asks_again_once_per_interval() {
        let mut resync = resync_of(&["A", "B"]);
        resync.expire();
        let a = [resync.remedy("A", HOUR), resync.remedy("A", HOUR)];
        let b = resync.remedy("B", HOUR);
        let expected = ([Remedy::Resnapshot, Remedy::Wait], Remedy::Resnapshot);
        assert_eq!((a, b), expected);
    }

    #[test]
    fn only_the_first_change_past_the_ceiling_needs_a_new_socket() {
        let mut resync = resync_on(timings(Duration::ZERO, Duration::ZERO), &["A"]);
        resync.expire();
        let first = resync.remedy("A", Duration::ZERO);
        let second = resync.remedy("A", HOUR);
        assert_eq!((first, second), (Remedy::NewSocket, Remedy::Wait));
    }

    #[test]
    fn a_socket_new_to_the_primary_role_asks_again_instead_of_reconnecting() {
        let mut resync = resync_on(timings(Duration::ZERO, HOUR), &["A"]);
        resync.expire();
        let remedy = resync.remedy("A", Duration::from_secs(59));
        assert_eq!(remedy, Remedy::Resnapshot);
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

    #[test]
    fn a_hold_holds_back_only_its_symbols_and_starts_the_deadline() {
        let mut resync = resync_of(&[]);
        resync.hold(&["A".into()]);
        let held = (resync.holds_back("A"), resync.holds_back("B"));
        assert_eq!((held, resync.deadline().is_some()), ((true, false), true));
    }

    #[test]
    fn a_hold_during_a_resync_keeps_its_deadline() {
        let mut resync = resync_of(&["A"]);
        let deadline = resync.deadline();
        std::thread::sleep(Duration::from_millis(5));
        resync.hold(&["B".into()]);
        let state = (resync.holds_back("B"), resync.deadline());
        assert_eq!(state, (true, deadline));
    }

    #[test]
    fn the_snapshot_of_a_held_symbol_completes_the_hold() {
        let mut resync = resync_of(&[]);
        resync.hold(&["A".into()]);
        assert_eq!((resync.settle("A"), resync.deadline()), (true, None));
    }
}
