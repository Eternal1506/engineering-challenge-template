//! [`ActivityTracker`]: how busy each of a shard's markets is, as the
//! primary's level changes show it, and which busy ones have gone silent.
//!
//! Each market keeps the time its silence began, two exponentially
//! weighted rates of its changes, one averaged over the short window and
//! one over the long window, the exchange time of the primary's last
//! change, when a standby last saw it change, and whether it has been
//! probed. A market's rate decays while it is silent, so one that stops for
//! good falls out of the busy ones over the long window. A market with no
//! change yet is not tracked at all.
//!
//! A standby's sighting shows the market changed after the primary last
//! did only if its exchange time is later: the standby's own copy of the
//! primary's last change carries the same exchange time, however late it
//! arrives. Without exchange times on both, the sighting must arrive
//! [`RECEIVE_MARGIN`] after the primary's silence began.
//!
//! A probed market, one resubscribed or found silent when the socket was
//! taken to be dead, has had its one probe for this quiet spell: it is
//! neither watched nor silent until it next changes on the primary, or a
//! standby sees a change the primary has not delivered, or its probes are
//! cleared.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::Instant;

use super::stale::StaleChecks;
use crate::core::stats::MarketActivity;

/// How long after the primary's silence began a standby sighting without
/// an exchange time must arrive to show a later change, so the standby's
/// own copy of the primary's last change does not.
pub(super) const RECEIVE_MARGIN: Duration = Duration::from_secs(3);

/// When a replica delivered a level change: received here, and made at the
/// exchange, if the feed says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Seen {
    /// When the change arrived.
    pub(super) at: Instant,
    /// When the exchange made it.
    pub(super) exchange_ts: Option<DateTime<Utc>>,
}

impl Seen {
    /// A change arriving now, made at the exchange at `exchange_ts`.
    pub(super) fn now(exchange_ts: Option<DateTime<Utc>>) -> Self {
        Self {
            at: Instant::now(),
            exchange_ts,
        }
    }
}

/// One market's activity: when its silence began, at its last change or
/// when its silence was last counted afresh, and its change rates, in
/// changes per second, as of then; the exchange time of the primary's last
/// change; when a standby last saw it change; and whether it has been
/// probed since its last change.
#[derive(Debug, Clone, Copy)]
struct Activity {
    last: Instant,
    short: f32,
    long: f32,
    changed_ts: Option<DateTime<Utc>>,
    witness: Option<Witness>,
    probed: bool,
}

/// A standby's sighting of a market's change.
#[derive(Debug, Clone, Copy)]
struct Witness {
    seen: Seen,
    standby: usize,
}

/// What it takes, beyond a long enough silence on the primary, for a busy
/// market to count as silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Evidence {
    /// Nothing more: no standby can tell whether the market is quiet.
    Silence,
    /// A standby saw the market change after the primary last did, so the
    /// market is not just quiet.
    Standby,
}

/// How a scan judges silence: on what evidence, and with the allowed
/// silence doubled how many times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Judging {
    pub(super) evidence: Evidence,
    pub(super) backoff: u32,
}

/// A busy market found silent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Silent {
    /// The market.
    pub(super) symbol: Arc<str>,
    /// When its silence began.
    pub(super) since: Instant,
    /// The standby that saw it change since, when that was the evidence.
    pub(super) witness: Option<usize>,
}

/// What a scan of the markets found.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Scan {
    /// How many markets are busy over each window, the probed ones not
    /// counted as watched.
    pub(super) activity: MarketActivity,
    /// The watched markets that have been silent for longer than they
    /// normally are.
    pub(super) silent: Vec<Silent>,
}

/// The activity of every market of one shard that has changed.
#[derive(Debug)]
pub(super) struct ActivityTracker {
    markets: HashMap<Arc<str>, Activity>,
    short_window: f32,
    long_window: f32,
}

impl ActivityTracker {
    /// No market tracked yet; rates average over `checks`' windows.
    pub(super) fn new(checks: &StaleChecks) -> Self {
        Self {
            markets: HashMap::new(),
            short_window: checks.short_window.as_secs_f32(),
            long_window: checks.long_window.as_secs_f32(),
        }
    }

    /// `symbol` changed on the primary, as `seen`.
    pub(super) fn note_change(&mut self, symbol: &Arc<str>, seen: Seen) {
        let windows = self.windows();
        let fresh = || Activity::new(seen.at);
        let activity = match self.markets.get_mut(symbol) {
            Some(activity) => activity,
            None => self.markets.entry(symbol.clone()).or_insert_with(fresh),
        };
        activity.decay_to(seen.at, windows);
        activity.short += 1.0 / windows.0;
        activity.long += 1.0 / windows.1;
        activity.changed_ts = seen.exchange_ts;
        activity.probed = false;
    }

    /// Standby `standby` saw tracked `symbol` change, as `seen`. A sighting
    /// of a change the primary has not delivered ends the market's probe.
    pub(super) fn note_standby_change(&mut self, symbol: &str, standby: usize, seen: Seen) {
        if let Some(activity) = self.markets.get_mut(symbol) {
            activity.witness = Some(Witness { seen, standby });
            if activity.witnessed().is_some() {
                activity.probed = false;
            }
        }
    }

    /// Stops tracking `symbol`.
    pub(super) fn forget(&mut self, symbol: &str) {
        self.markets.remove(symbol);
    }

    /// Counts every market's silence from `now`, as on a new primary
    /// socket, which has had no chance to deliver anything yet. What the
    /// standbys saw no longer applies, as the roles may have changed.
    pub(super) fn restart(&mut self, now: Instant) {
        let windows = self.windows();
        for activity in self.markets.values_mut() {
            activity.decay_to(now, windows);
            activity.witness = None;
        }
    }

    /// Marks `symbols` probed at `now`, counting their silence from then.
    pub(super) fn probe(&mut self, symbols: &[Arc<str>], now: Instant) {
        let windows = self.windows();
        for symbol in symbols {
            if let Some(activity) = self.markets.get_mut(symbol) {
                activity.decay_to(now, windows);
                activity.probed = true;
            }
        }
    }

    /// Whether any of `symbols` is still tracked and busy enough over the
    /// long window, by `checks`' minimum, to be watched at `now`, probed
    /// or not.
    pub(super) fn any_busy(
        &self,
        symbols: &HashSet<Arc<str>>,
        checks: &StaleChecks,
        now: Instant,
    ) -> bool {
        let minimum = checks.min_rate_per_second();
        let windows = self.windows();
        let busy = |activity: &Activity| activity.decayed_to(now, windows).long >= minimum;
        let tracked = symbols.iter().filter_map(|symbol| self.markets.get(symbol));
        tracked.into_iter().any(busy)
    }

    /// Makes every probed market eligible again.
    pub(super) fn clear_probes(&mut self) {
        let markets = self.markets.values_mut();
        markets.for_each(|activity| activity.probed = false);
    }

    /// Counts the busy markets at `now`, by `checks`' minimum rate, and
    /// finds the watched ones whose silence has lasted longer than
    /// `checks` allows them, as `judging` says.
    pub(super) fn scan(&self, checks: &StaleChecks, now: Instant, judging: Judging) -> Scan {
        let minimum = checks.min_rate_per_second();
        let mut scan = Scan::default();
        for (symbol, activity) in &self.markets {
            let decayed = activity.decayed_to(now, self.windows());
            scan.activity.active += usize::from(decayed.short >= minimum);
            if decayed.long < minimum || decayed.probed {
                continue;
            }
            scan.activity.watched += 1;
            let allowed = checks.allowed_silence(decayed.long, judging.backoff);
            let silent = decayed.silent_beyond(allowed, now);
            let witness = silent.then(|| decayed.witnessed()).flatten();
            if silent && (judging.evidence == Evidence::Silence || witness.is_some()) {
                let symbol = symbol.clone();
                let since = decayed.last;
                scan.silent.push(Silent {
                    symbol,
                    since,
                    witness,
                });
            }
        }
        scan
    }

    /// The short and long windows, in seconds.
    fn windows(&self) -> (f32, f32) {
        (self.short_window, self.long_window)
    }
}

impl Activity {
    /// A market first seen at `now`, with no rate yet.
    fn new(now: Instant) -> Self {
        Self {
            last: now,
            short: 0.0,
            long: 0.0,
            changed_ts: None,
            witness: None,
            probed: false,
        }
    }

    /// Whether the silence has lasted longer than `allowed` at `now`.
    fn silent_beyond(&self, allowed: Duration, now: Instant) -> bool {
        now.saturating_duration_since(self.last) > allowed
    }

    /// The standby that saw the market change after the primary last did:
    /// by exchange time if both have one, else by arrival a margin after
    /// the silence began.
    fn witnessed(&self) -> Option<usize> {
        let witness = self.witness?;
        let later = match (witness.seen.exchange_ts, self.changed_ts) {
            (Some(sighting), Some(change)) => sighting > change,
            _ => witness.seen.at > self.last + RECEIVE_MARGIN,
        };
        later.then_some(witness.standby)
    }

    /// The rates decayed to `now` over the short and long `windows`, in
    /// seconds, with the silence counted from the time it was before.
    fn decayed_to(self, now: Instant, windows: (f32, f32)) -> Self {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f32();
        let decay = |rate: f32, window: f32| rate * (-elapsed / window).exp();
        Self {
            short: decay(self.short, windows.0),
            long: decay(self.long, windows.1),
            ..self
        }
    }

    /// Decays the rates to `now` over the short and long `windows`, in
    /// seconds, and counts the silence from `now`.
    fn decay_to(&mut self, now: Instant, windows: (f32, f32)) {
        *self = Self {
            last: now,
            ..self.decayed_to(now, windows)
        };
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// Checks over a 60 s short window and a 3600 s long one, watching
    /// markets of at least 2 changes a minute.
    fn checks() -> StaleChecks {
        StaleChecks::default()
    }

    /// Judging on `evidence`, without backoff.
    fn on(evidence: Evidence) -> Judging {
        Judging {
            evidence,
            backoff: 0,
        }
    }

    /// Arrived at `at`, with no exchange time.
    fn arrived(at: Instant) -> Seen {
        Seen {
            at,
            exchange_ts: None,
        }
    }

    /// Arrived at `at`, made at the exchange `millis` into a fixed time.
    fn made(at: Instant, millis: i64) -> Seen {
        let epoch = DateTime::UNIX_EPOCH + TimeDelta::days(20_000);
        Seen {
            at,
            exchange_ts: Some(epoch + TimeDelta::milliseconds(millis)),
        }
    }

    /// A tracker that saw `symbol` change `count` times, `every` apart,
    /// the last at the returned instant, made at the exchange `count - 1`
    /// milliseconds into a fixed time.
    fn changed(symbol: &str, count: u32, every: Duration) -> (ActivityTracker, Instant) {
        let mut tracker = ActivityTracker::new(&checks());
        let start = Instant::now();
        let symbol = Arc::from(symbol);
        for index in 0..count {
            tracker.note_change(&symbol, made(start + every * index, i64::from(index)));
        }
        (tracker, start + every * count.saturating_sub(1))
    }

    /// An hour of A changing every ten seconds, the last at the returned
    /// instant, made at the exchange 359 ms into a fixed time.
    fn busy_a() -> (ActivityTracker, Instant) {
        changed("A", 360, SECOND * 10)
    }

    /// The markets `tracker` finds silent at `at` on `evidence`.
    fn silent_at(tracker: &ActivityTracker, at: Instant, evidence: Evidence) -> Vec<Silent> {
        tracker.scan(&checks(), at, on(evidence)).silent
    }

    /// A found silent since `since`, seen by `witness`.
    fn silent_a(since: Instant, witness: Option<usize>) -> Silent {
        let symbol = Arc::from("A");
        Silent {
            symbol,
            since,
            witness,
        }
    }

    #[test]
    fn a_market_changing_every_ten_seconds_for_an_hour_is_watched_and_active() {
        let (tracker, last) = busy_a();
        let scan = tracker.scan(&checks(), last, on(Evidence::Silence));
        let activity = MarketActivity {
            watched: 1,
            active: 1,
        };
        assert_eq!((scan.activity, scan.silent.len()), (activity, 0));
    }

    #[test]
    fn a_market_seen_for_a_few_minutes_is_active_but_not_yet_watched() {
        let (tracker, last) = changed("A", 30, SECOND * 10);
        let scan = tracker.scan(&checks(), last, on(Evidence::Silence));
        let expected = MarketActivity {
            watched: 0,
            active: 1,
        };
        assert_eq!(scan.activity, expected);
    }

    #[test]
    fn a_watched_market_is_silent_only_after_two_minutes() {
        let (tracker, last) = busy_a();
        let early = silent_at(&tracker, last + SECOND * 120, Evidence::Silence);
        let late = silent_at(&tracker, last + SECOND * 121, Evidence::Silence);
        assert_eq!((early.len(), late), (0, vec![silent_a(last, None)]));
    }

    #[test]
    fn each_backoff_doubles_the_silence_allowed() {
        let (tracker, last) = busy_a();
        let backed_off = Judging {
            evidence: Evidence::Silence,
            backoff: 1,
        };
        let early = tracker.scan(&checks(), last + SECOND * 240, backed_off);
        let late = tracker.scan(&checks(), last + SECOND * 241, backed_off);
        assert_eq!((early.silent.len(), late.silent.len()), (0, 1));
    }

    #[test]
    fn a_market_silent_for_a_long_time_stops_being_watched() {
        let (tracker, last) = busy_a();
        let scan = tracker.scan(&checks(), last + SECOND * 3600, on(Evidence::Silence));
        assert_eq!((scan.activity.watched, scan.silent.len()), (0, 0));
    }

    #[test]
    fn a_restart_counts_silence_from_then() {
        let (mut tracker, last) = busy_a();
        tracker.restart(last + SECOND * 100);
        let scan = tracker.scan(&checks(), last + SECOND * 200, on(Evidence::Silence));
        assert_eq!((scan.activity.watched, scan.silent.len()), (1, 0));
    }

    #[test]
    fn a_probed_market_is_neither_watched_nor_silent_until_it_changes() {
        let (mut tracker, last) = busy_a();
        tracker.probe(&[Arc::from("A")], last + SECOND * 130);
        let probed = tracker.scan(&checks(), last + SECOND * 400, on(Evidence::Silence));
        tracker.note_change(&Arc::from("A"), arrived(last + SECOND * 401));
        let changed = silent_at(&tracker, last + SECOND * 522, Evidence::Silence);
        let watched = (probed.activity.watched, probed.silent.len());
        assert_eq!((watched, changed.len()), ((0, 0), 1));
    }

    #[test]
    fn cleared_probes_make_a_market_eligible_again() {
        let (mut tracker, last) = busy_a();
        tracker.probe(&[Arc::from("A")], last + SECOND * 130);
        tracker.clear_probes();
        let silent = silent_at(&tracker, last + SECOND * 251, Evidence::Silence);
        assert_eq!(silent, vec![silent_a(last + SECOND * 130, None)]);
    }

    #[test]
    fn a_standby_change_ends_the_probe() {
        let (mut tracker, last) = busy_a();
        tracker.probe(&[Arc::from("A")], last + SECOND * 130);
        tracker.note_standby_change("A", 1, made(last + SECOND * 140, 400));
        let silent = silent_at(&tracker, last + SECOND * 251, Evidence::Silence);
        assert_eq!(silent, vec![silent_a(last + SECOND * 130, Some(1))]);
    }

    #[test]
    fn the_standbys_copy_of_the_primarys_last_change_does_not_end_the_probe() {
        let (mut tracker, last) = busy_a();
        tracker.probe(&[Arc::from("A")], last + SECOND * 130);
        tracker.note_standby_change("A", 1, made(last + SECOND * 140, 359));
        let scan = tracker.scan(&checks(), last + SECOND * 251, on(Evidence::Silence));
        assert_eq!((scan.activity.watched, scan.silent.len()), (0, 0));
    }

    #[test]
    fn with_a_standby_a_market_is_silent_once_the_standby_saw_a_later_change() {
        let (mut tracker, last) = busy_a();
        let at = last + SECOND * 121;
        let unseen = silent_at(&tracker, at, Evidence::Standby);
        tracker.note_standby_change("A", 1, made(last + SECOND, 360));
        let seen = silent_at(&tracker, at, Evidence::Standby);
        assert_eq!((unseen, seen), (vec![], vec![silent_a(last, Some(1))]));
    }

    #[test]
    fn the_standbys_late_copy_of_the_primarys_last_change_is_no_evidence() {
        let (mut tracker, last) = busy_a();
        tracker.note_standby_change("A", 1, made(last + SECOND * 10, 359));
        let silent = silent_at(&tracker, last + SECOND * 121, Evidence::Standby);
        assert!(silent.is_empty());
    }

    #[test]
    fn without_exchange_times_a_sighting_must_come_a_margin_after_the_silence_began() {
        let (mut tracker, last) = busy_a();
        let at = last + SECOND * 121;
        tracker.note_standby_change("A", 1, arrived(last + RECEIVE_MARGIN));
        let within = silent_at(&tracker, at, Evidence::Standby);
        let later = arrived(last + RECEIVE_MARGIN + SECOND);
        tracker.note_standby_change("A", 1, later);
        let beyond = silent_at(&tracker, at, Evidence::Standby);
        assert_eq!((within.len(), beyond.len()), (0, 1));
    }

    #[test]
    fn a_restart_forgets_what_the_standbys_saw() {
        let (mut tracker, last) = busy_a();
        tracker.note_standby_change("A", 1, made(last + SECOND, 360));
        tracker.restart(last + SECOND * 2);
        let silent = silent_at(&tracker, last + SECOND * 200, Evidence::Standby);
        assert!(silent.is_empty());
    }

    #[test]
    fn a_forgotten_market_is_not_tracked() {
        let (mut tracker, last) = busy_a();
        tracker.forget("A");
        let scan = tracker.scan(&checks(), last, on(Evidence::Silence));
        assert_eq!(scan, Scan::default());
    }
}
