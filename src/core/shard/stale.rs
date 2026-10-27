//! [`StaleWatch`]: which of a shard's busy markets have gone silent on its
//! primary, and whether to resubscribe them or treat the primary's socket
//! as dead.
//!
//! A market is watched once its change rate over the long window reaches
//! [`StaleChecks::min_changes_per_minute`]. A watched market is silent once
//! it has gone [`StaleChecks::silence_intervals`] of its mean intervals
//! without a change on the primary, and at least
//! [`StaleChecks::min_silence`]; while a warm standby delivers level
//! changes, only if a standby saw it change since, as [`Evidence::Standby`]
//! says. When enough of the watched markets are silent at once, the socket
//! itself is taken to be dead; otherwise the silent markets are
//! resubscribed, the longest silent first, at most
//! [`StaleChecks::resubscribe_limit`] per
//! [`StaleChecks::resubscribe_window`]. Either way the silent markets acted
//! on are probed: they are not watched again until they change, so a market
//! that is only quiet gets one resubscribe per quiet spell, and the same
//! socket is not taken to be dead again over the same markets.
//!
//! A new primary socket after one taken to be dead may be dead too, so it
//! clears every probe. Each socket taken to be dead in a row doubles the
//! silence a market must keep before it counts, up to
//! [`StaleChecks::max_silence`], until one of the markets the last was
//! taken to be dead over changes on the primary, or none of them is busy
//! enough to be watched any more, having closed, left the shard or decayed.
//! A change of one of them still in flight from the old socket ends the
//! backoff too, as intended: the market is not dead, only slow. Markets that
//! are only quiet thus cost a reconnect at ever longer intervals rather than
//! every few minutes, and stop costing any once their rates decay out of
//! the watched ones, an hour or more at the defaults, longer for busier
//! markets.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::activity::{ActivityTracker, Judging, Silent};
use crate::core::stats::MarketActivity;

pub(super) use super::activity::{Evidence, Seen};

/// When a shard resubscribes the markets that have gone silent on its
/// primary, and when it treats the primary's socket as dead instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleChecks {
    /// How often the markets are checked.
    pub interval: Duration,
    /// The time constant of each market's short-window change rate.
    pub short_window: Duration,
    /// The time constant of each market's long-window change rate.
    pub long_window: Duration,
    /// The fewest changes a minute, averaged over the long window, for a
    /// market to be watched for silence; also the rate over the short
    /// window that counts a market as active on the stats line.
    pub min_changes_per_minute: u32,
    /// How many of its mean intervals between changes a watched market may
    /// go without a change.
    pub silence_intervals: u32,
    /// The shortest silence that counts, whatever the market's rate.
    pub min_silence: Duration,
    /// The longest silence the backoff after sockets taken to be dead may
    /// require.
    pub max_silence: Duration,
    /// The most markets resubscribed within one resubscribe window.
    pub resubscribe_limit: usize,
    /// The span the resubscribe limit applies to.
    pub resubscribe_window: Duration,
    /// The share of the watched markets, in percent, that must be silent
    /// at once, beyond which the primary's socket is treated as dead.
    pub dead_socket_percent: u32,
    /// The fewest silent markets that may make a socket dead, so a shard
    /// with only a few busy markets does not drop its socket over one.
    pub dead_socket_min_markets: usize,
}

impl Default for StaleChecks {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            short_window: Duration::from_secs(60),
            long_window: Duration::from_secs(3600),
            min_changes_per_minute: 2,
            silence_intervals: 3,
            min_silence: Duration::from_secs(120),
            max_silence: Duration::from_secs(1800),
            resubscribe_limit: 20,
            resubscribe_window: Duration::from_secs(60),
            dead_socket_percent: 30,
            dead_socket_min_markets: 5,
        }
    }
}

impl StaleChecks {
    /// [`StaleChecks::min_changes_per_minute`] per second.
    pub(super) fn min_rate_per_second(&self) -> f32 {
        self.min_changes_per_minute as f32 / 60.0
    }

    /// The longest silence normal for a market changing `rate` times a
    /// second: [`StaleChecks::silence_intervals`] of its mean interval, but
    /// never less than [`StaleChecks::min_silence`], doubled `backoff`
    /// times up to [`StaleChecks::max_silence`].
    pub(super) fn allowed_silence(&self, rate: f32, backoff: u32) -> Duration {
        let mean = Duration::try_from_secs_f32(1.0 / rate).unwrap_or(Duration::MAX);
        let silence = mean.saturating_mul(self.silence_intervals);
        let normal = silence.max(self.min_silence);
        let doubled = normal.saturating_mul(2_u32.saturating_pow(backoff));
        normal.max(doubled.min(self.max_silence))
    }

    /// Whether `silent` markets out of `watched` going quiet at once means
    /// the socket is dead.
    fn is_dead_socket(&self, silent: usize, watched: usize) -> bool {
        let share = silent * 100 > watched * self.dead_socket_percent as usize;
        share && silent >= self.dead_socket_min_markets
    }
}

/// What a check calls for.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Finding {
    /// Nothing: no watched market is silent, or the limit leaves no room.
    Nothing,
    /// Resubscribe these markets on the primary.
    Resubscribe(Vec<Arc<str>>),
    /// So many watched markets are silent that the socket is dead.
    DeadSocket(DeadSocket),
}

/// A primary socket taken to be dead.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct DeadSocket {
    /// The silent markets.
    pub(super) silent: usize,
    /// The watched markets.
    pub(super) watched: usize,
    /// The standby that saw most of the silent markets change, when
    /// standbys were the evidence.
    pub(super) witness: Option<usize>,
}

/// What one check found.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Check {
    /// How many markets are busy.
    pub(super) activity: MarketActivity,
    /// What the silent markets call for.
    pub(super) finding: Finding,
}

/// Watches one shard's markets for silence on its primary.
#[derive(Debug)]
pub(super) struct StaleWatch {
    checks: StaleChecks,
    tracker: ActivityTracker,
    /// When each market resubscribed within the last window was.
    resubscribed_at: VecDeque<Instant>,
    next_check: Instant,
    /// Sockets taken to be dead in a row, with none of the markets the
    /// last was taken to be dead over changing on the primary since.
    dead_sockets: u32,
    /// The markets the last socket was taken to be dead over, while the
    /// backoff lasts.
    dead_over: HashSet<Arc<str>>,
    /// Set when a socket is taken to be dead, until the next primary
    /// socket clears every probe.
    clear_on_restart: bool,
}

impl StaleWatch {
    /// A watch on `checks`, with nothing seen yet and its first check an
    /// interval from now.
    pub(super) fn new(checks: StaleChecks) -> Self {
        Self {
            tracker: ActivityTracker::new(&checks),
            resubscribed_at: VecDeque::new(),
            next_check: Instant::now() + checks.interval,
            dead_sockets: 0,
            dead_over: HashSet::new(),
            clear_on_restart: false,
            checks,
        }
    }

    /// When the next check is due.
    pub(super) fn next_check(&self) -> Instant {
        self.next_check
    }

    /// `symbol` changed on the primary, as `seen`. A change of a market the
    /// last socket was taken to be dead over ends the backoff.
    pub(super) fn note_change(&mut self, symbol: &Arc<str>, seen: Seen) {
        self.tracker.note_change(symbol, seen);
        if self.dead_over.contains(symbol) {
            self.dead_over.clear();
            self.dead_sockets = 0;
        }
    }

    /// Standby `standby` saw `symbol` change, as `seen`.
    pub(super) fn note_standby_change(&mut self, symbol: &str, standby: usize, seen: Seen) {
        self.tracker.note_standby_change(symbol, standby, seen);
    }

    /// Stops watching `symbol`, which the shard no longer records.
    pub(super) fn forget(&mut self, symbol: &str) {
        self.tracker.forget(symbol);
        self.dead_over.remove(symbol);
    }

    /// The primary is a new socket at `now`: every market's silence counts
    /// from then. After a socket taken to be dead, every market may be
    /// judged again, as the new socket may be dead too.
    pub(super) fn restart(&mut self, now: Instant) {
        self.tracker.restart(now);
        if std::mem::take(&mut self.clear_on_restart) {
            self.tracker.clear_probes();
        }
    }

    /// Skips the check due, at `now`, while the primary cannot be judged.
    pub(super) fn postpone(&mut self, now: Instant) {
        self.next_check = now + self.checks.interval;
    }

    /// Checks the markets at `now`, on `evidence`, and schedules the next
    /// check. The markets acted on are probed.
    pub(super) fn check(&mut self, now: Instant, evidence: Evidence) -> Check {
        self.postpone(now);
        self.end_backoff_once_idle(now);
        let backoff = self.dead_sockets;
        let judging = Judging { evidence, backoff };
        let scan = self.tracker.scan(&self.checks, now, judging);
        let watched = scan.activity.watched;
        let finding = if scan.silent.is_empty() {
            Finding::Nothing
        } else if self.checks.is_dead_socket(scan.silent.len(), watched) {
            Finding::DeadSocket(self.declare_dead(scan.silent, watched, now))
        } else {
            self.pick_resubscribes(scan.silent, now)
        };
        Check {
            activity: scan.activity,
            finding,
        }
    }

    /// Ends the backoff once none of the markets the last socket was taken
    /// to be dead over is busy enough at `now` to be watched, by rate
    /// alone: they have closed, left the shard or decayed, so no change of
    /// theirs will end it.
    fn end_backoff_once_idle(&mut self, now: Instant) {
        if !self.tracker.any_busy(&self.dead_over, &self.checks, now) {
            self.dead_over.clear();
            self.dead_sockets = 0;
        }
    }

    /// Probes every one of `silent`, out of `watched`, at `now`, as the
    /// socket is taken to be dead over them, backs off further, and has
    /// the next socket clear the probes.
    fn declare_dead(&mut self, silent: Vec<Silent>, watched: usize, now: Instant) -> DeadSocket {
        let witness = most_common_witness(&silent);
        let symbols: Vec<Arc<str>> = silent.into_iter().map(|market| market.symbol).collect();
        self.tracker.probe(&symbols, now);
        self.dead_sockets = self.dead_sockets.saturating_add(1);
        self.dead_over = symbols.iter().cloned().collect();
        self.clear_on_restart = true;
        DeadSocket {
            silent: symbols.len(),
            watched,
            witness,
        }
    }

    /// The longest silent of `silent` that the limit leaves room for at
    /// `now`, noted as resubscribed and probed then.
    fn pick_resubscribes(&mut self, mut silent: Vec<Silent>, now: Instant) -> Finding {
        let window = self.checks.resubscribe_window;
        let recent = |at: &Instant| now.saturating_duration_since(*at) < window;
        self.resubscribed_at.retain(recent);
        let limit = self.checks.resubscribe_limit;
        let room = limit.saturating_sub(self.resubscribed_at.len());
        silent.sort_unstable_by(|a, b| a.since.cmp(&b.since).then(a.symbol.cmp(&b.symbol)));
        let picked: Vec<Arc<str>> = silent.into_iter().take(room).map(|m| m.symbol).collect();
        self.resubscribed_at.extend(picked.iter().map(|_| now));
        self.tracker.probe(&picked, now);
        if picked.is_empty() {
            Finding::Nothing
        } else {
            Finding::Resubscribe(picked)
        }
    }
}

/// The standby that saw the most of `silent` change, the lowest index on a
/// tie; none if no standby saw any.
fn most_common_witness(silent: &[Silent]) -> Option<usize> {
    let mut sightings: BTreeMap<usize, usize> = BTreeMap::new();
    for standby in silent.iter().filter_map(|market| market.witness) {
        *sightings.entry(standby).or_default() += 1;
    }
    let rank = |&(standby, count): &(usize, usize)| (count, Reverse(standby));
    let most = sightings.into_iter().max_by_key(rank);
    most.map(|(standby, _)| standby)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// A tenth of a second.
    const TICK: Duration = Duration::from_millis(100);

    /// Checks over windows of a second and ten, under which a market with a
    /// burst of changes is watched at once, and silent after five seconds.
    fn quick_checks() -> StaleChecks {
        StaleChecks {
            short_window: SECOND,
            long_window: SECOND * 10,
            silence_intervals: 1,
            min_silence: SECOND * 5,
            ..StaleChecks::default()
        }
    }

    fn names(count: usize) -> Vec<Arc<str>> {
        (0..count).map(|n| Arc::from(format!("M{n:03}"))).collect()
    }

    /// Arrived at `at`, made at the exchange `millis` into a fixed time.
    fn seen(at: Instant, millis: i64) -> Seen {
        let epoch = chrono::DateTime::UNIX_EPOCH + chrono::TimeDelta::days(20_000);
        Seen {
            at,
            exchange_ts: Some(epoch + chrono::TimeDelta::milliseconds(millis)),
        }
    }

    /// Notes a burst of ten changes of each of `symbols` at `at`, made at
    /// the exchange at the fixed time.
    fn burst(watch: &mut StaleWatch, symbols: &[Arc<str>], at: Instant) {
        for symbol in symbols {
            (0..10).for_each(|_| watch.note_change(symbol, seen(at, 0)));
        }
    }

    /// A watch on [`quick_checks`] that saw a burst of `quiet` and, a
    /// second later, at the returned instant, of `active`.
    fn watching(active: &[Arc<str>], quiet: &[Arc<str>]) -> (StaleWatch, Instant) {
        let mut watch = StaleWatch::new(quick_checks());
        let start = Instant::now();
        burst(&mut watch, quiet, start);
        let now = start + SECOND;
        burst(&mut watch, active, now);
        (watch, now)
    }

    /// A check `after` the active markets' burst at `now`, when only the
    /// quiet ones have been silent for more than five seconds.
    fn check_after(watch: &mut StaleWatch, now: Instant, after: Duration) -> Finding {
        watch.check(now + after, Evidence::Silence).finding
    }

    /// The finding of a check at `at`, just after a burst of `active`.
    fn check_with_active(watch: &mut StaleWatch, active: &[Arc<str>], at: Instant) -> Finding {
        burst(watch, active, at);
        watch.check(at, Evidence::Silence).finding
    }

    fn dead(silent: usize, watched: usize, witness: Option<usize>) -> Finding {
        Finding::DeadSocket(DeadSocket {
            silent,
            watched,
            witness,
        })
    }

    /// Four and a half seconds after the active burst.
    const QUIET_ONLY: Duration = Duration::from_millis(4500);

    #[test]
    fn a_silent_market_among_busy_ones_is_resubscribed() {
        let markets = names(10);
        let (mut watch, now) = watching(&markets[1..], &markets[..1]);
        let finding = check_after(&mut watch, now, QUIET_ONLY);
        assert_eq!(finding, Finding::Resubscribe(markets[..1].to_vec()));
    }

    #[test]
    fn a_resubscribed_market_counts_its_silence_afresh() {
        let markets = names(10);
        let (mut watch, now) = watching(&markets[1..], &markets[..1]);
        check_after(&mut watch, now, QUIET_ONLY);
        let again = check_after(&mut watch, now, QUIET_ONLY + TICK * 4);
        assert_eq!(again, Finding::Nothing);
    }

    #[test]
    fn at_most_the_limit_is_resubscribed_per_window_longest_silent_first() {
        let markets = names(100);
        let (mut watch, now) = watching(&markets[25..], &markets[..25]);
        let first = check_after(&mut watch, now, QUIET_ONLY);
        let second = check_after(&mut watch, now, QUIET_ONLY + TICK * 4);
        let expected = Finding::Resubscribe(markets[..20].to_vec());
        assert_eq!((first, second), (expected, Finding::Nothing));
    }

    #[test]
    fn the_limit_frees_up_once_the_window_has_passed() {
        let markets = names(100);
        let checks = StaleChecks {
            resubscribe_window: SECOND * 2,
            ..quick_checks()
        };
        let mut watch = StaleWatch::new(checks);
        let start = Instant::now();
        burst(&mut watch, &markets[..25], start);
        burst(&mut watch, &markets[25..], start + SECOND);
        watch.check(start + SECOND * 5 + TICK, Evidence::Silence);
        let later = start + SECOND * 7 + TICK;
        let finding = check_with_active(&mut watch, &markets[25..], later);
        assert_eq!(finding, Finding::Resubscribe(markets[20..25].to_vec()));
    }

    #[test]
    fn a_quiet_market_is_resubscribed_once_per_quiet_spell() {
        let markets = names(10);
        let (mut watch, now) = watching(&markets[1..], &markets[..1]);
        check_after(&mut watch, now, QUIET_ONLY);
        let later = check_with_active(&mut watch, &markets[1..], now + SECOND * 20);
        assert_eq!(later, Finding::Nothing);
    }

    #[test]
    fn more_than_the_share_silent_at_once_is_a_dead_socket() {
        let markets = names(20);
        let (mut watch, now) = watching(&markets[7..], &markets[..7]);
        let finding = check_after(&mut watch, now, QUIET_ONLY);
        assert_eq!(finding, dead(7, 20, None));
    }

    #[test]
    fn a_socket_is_not_taken_to_be_dead_again_over_the_same_markets() {
        let markets = names(20);
        let (mut watch, now) = watching(&markets[7..], &markets[..7]);
        check_after(&mut watch, now, QUIET_ONLY);
        let later = check_with_active(&mut watch, &markets[7..], now + SECOND * 20);
        assert_eq!(later, Finding::Nothing);
    }

    #[test]
    fn with_a_standby_markets_it_did_not_see_change_are_only_quiet() {
        let markets = names(20);
        let (mut watch, now) = watching(&markets[7..], &markets[..7]);
        let finding = watch.check(now + QUIET_ONLY, Evidence::Standby).finding;
        assert_eq!(finding, Finding::Nothing);
    }

    #[test]
    fn the_standby_that_saw_most_silent_markets_change_is_the_witness() {
        let markets = names(20);
        let (mut watch, now) = watching(&markets[7..], &markets[..7]);
        for (index, symbol) in markets[..7].iter().enumerate() {
            let standby = if index < 2 { 2 } else { 1 };
            watch.note_standby_change(symbol, standby, seen(now + TICK, 1));
        }
        let finding = watch.check(now + QUIET_ONLY, Evidence::Standby).finding;
        assert_eq!(finding, dead(7, 20, Some(1)));
    }

    #[test]
    fn exactly_the_share_silent_is_resubscribed() {
        let markets = names(20);
        let (mut watch, now) = watching(&markets[6..], &markets[..6]);
        let finding = check_after(&mut watch, now, QUIET_ONLY);
        assert_eq!(finding, Finding::Resubscribe(markets[..6].to_vec()));
    }

    #[test]
    fn too_few_silent_markets_are_never_a_dead_socket() {
        let markets = names(4);
        let (mut watch, now) = watching(&[], &markets);
        let finding = check_after(&mut watch, now, QUIET_ONLY);
        assert_eq!(finding, Finding::Resubscribe(markets));
    }

    #[test]
    fn a_check_reports_the_busy_markets() {
        let markets = names(3);
        let (mut watch, now) = watching(&markets, &[]);
        let activity = watch.check(now, Evidence::Silence).activity;
        let expected = MarketActivity {
            watched: 3,
            active: 3,
        };
        assert_eq!(activity, expected);
    }

    /// A watch whose 20 markets, all busy, went silent together, taken to
    /// be dead over them at the returned instant's check, the primary then
    /// a new socket.
    fn dead_then_restarted() -> (StaleWatch, Instant) {
        let markets = names(20);
        let (mut watch, now) = watching(&[], &markets);
        check_after(&mut watch, now, QUIET_ONLY);
        let restarted = now + SECOND * 5;
        watch.restart(restarted);
        (watch, restarted)
    }

    #[test]
    fn a_new_socket_after_a_dead_one_is_judged_again_after_twice_the_silence() {
        let (mut watch, restarted) = dead_then_restarted();
        let early = watch.check(restarted + SECOND * 10 - TICK, Evidence::Silence);
        let late = watch.check(restarted + SECOND * 10 + TICK, Evidence::Silence);
        let findings = (early.finding, late.finding);
        assert_eq!(findings, (Finding::Nothing, dead(20, 20, None)));
    }

    #[test]
    fn a_change_on_the_primary_ends_the_backoff() {
        let (mut watch, restarted) = dead_then_restarted();
        watch.note_change(&names(1)[0], seen(restarted + SECOND * 2, 1));
        let finding = watch.check(restarted + SECOND * 5 + TICK, Evidence::Silence);
        assert_eq!(finding.finding, dead(19, 20, None));
    }

    #[test]
    fn markets_still_quiet_on_the_next_socket_count_only_after_a_backoff() {
        let markets = names(20);
        let (active, quiet) = (&markets[7..], &markets[..7]);
        let (mut watch, now) = watching(active, quiet);
        check_after(&mut watch, now, QUIET_ONLY);
        let restarted = now + SECOND * 5;
        watch.restart(restarted);
        let early = check_with_active(&mut watch, active, restarted + SECOND * 5 + TICK);
        let late = check_with_active(&mut watch, active, restarted + SECOND * 10 + TICK);
        assert_eq!((early, late), (Finding::Nothing, dead(7, 20, None)));
    }

    #[test]
    fn a_change_queued_before_the_new_socket_still_has_it_clear_the_probes() {
        let markets = names(20);
        let (mut watch, now) = watching(&[], &markets);
        check_after(&mut watch, now, QUIET_ONLY);
        watch.note_change(&markets[0], seen(now + SECOND * 5, 1));
        let restarted = now + SECOND * 5 + TICK;
        watch.restart(restarted);
        let finding = watch.check(restarted + SECOND * 5 + TICK, Evidence::Silence);
        assert_eq!(finding.finding, dead(20, 20, None));
    }

    #[test]
    fn the_backoff_ends_once_the_markets_it_was_over_leave_the_shard() {
        let (mut watch, restarted) = dead_then_restarted();
        let markets = names(40);
        markets[..20].iter().for_each(|symbol| watch.forget(symbol));
        burst(&mut watch, &markets[20..], restarted);
        let finding = watch.check(restarted + SECOND * 5 + TICK, Evidence::Silence);
        assert_eq!(finding.finding, dead(20, 20, None));
    }

    #[test]
    fn the_backoff_ends_once_the_markets_it_was_over_decay_out_of_watched() {
        let (mut watch, restarted) = dead_then_restarted();
        let fresh = &names(21)[20..];
        let later = restarted + SECOND * 40;
        burst(&mut watch, fresh, later);
        let finding = watch.check(later + SECOND * 5 + TICK, Evidence::Silence);
        assert_eq!(finding.finding, Finding::Resubscribe(fresh.to_vec()));
    }

    #[test]
    fn a_slow_market_may_be_silent_for_three_of_its_mean_intervals() {
        let silence = StaleChecks::default().allowed_silence(1.0 / 50.0, 0);
        assert_eq!(silence.as_secs(), 150);
    }

    #[test]
    fn a_busy_market_may_be_silent_for_the_minimum() {
        let silence = StaleChecks::default().allowed_silence(1.0, 0);
        assert_eq!(silence, SECOND * 120);
    }

    #[test]
    fn the_backoff_doubles_the_silence_up_to_the_maximum() {
        let checks = StaleChecks::default();
        let silences = [3, 4].map(|backoff| checks.allowed_silence(1.0, backoff));
        assert_eq!(silences, [SECOND * 960, SECOND * 1800]);
    }
}
