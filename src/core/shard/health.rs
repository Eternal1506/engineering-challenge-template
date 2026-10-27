//! [`HealthMonitor`]: when a warm standby should take over from a primary
//! that is connected but silent, lagging, or stuck in a resync, and the
//! cooldown after any takeover from a connected primary.
//!
//! Only level changes count. Snapshots arrive when asked for, on subscribe
//! or in a resync, so they say nothing about how a socket keeps up with
//! the market.

use std::time::Duration;

use tokio::time::Instant;

use super::HealthChecks;
use crate::core::stats::FailoverCause;

/// One connected replica's recent traffic, as of a check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Reading {
    /// The replica's index.
    pub(super) replica: usize,
    /// How long its socket has been up.
    pub(super) uptime: Duration,
    /// The level changes its socket delivered over the window.
    pub(super) level_changes: u64,
    /// The median feed lag of those level changes, if sampled.
    pub(super) median_lag_ms: Option<i64>,
}

/// What a check compares: the primary's reading while it is connected, and
/// the reading of each warm standby not asked to reconnect.
#[derive(Debug, Default)]
pub(super) struct Readings {
    pub(super) primary: Option<Reading>,
    pub(super) standbys: Vec<Reading>,
}

/// A standby that should take over from the primary, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Takeover {
    pub(super) standby: usize,
    pub(super) cause: FailoverCause,
}

/// How many checks in a row one primary's lag has trailed a standby's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LagCount {
    primary: usize,
    checks: u32,
}

/// Runs a shard's health checks on [`HealthChecks`].
#[derive(Debug)]
pub(super) struct HealthMonitor {
    checks: HealthChecks,
    next_check: Instant,
    /// The lag count of the current primary, while it trails.
    lagging: Option<LagCount>,
    /// Until when no standby may take over from a connected primary.
    cooldown_until: Option<Instant>,
}

impl HealthMonitor {
    /// A monitor whose first check is one tick from now.
    pub(super) fn new(checks: HealthChecks) -> Self {
        Self {
            next_check: Instant::now() + checks.tick,
            checks,
            lagging: None,
            cooldown_until: None,
        }
    }

    /// When the next check is due.
    pub(super) fn next_check(&self) -> Instant {
        self.next_check
    }

    /// Compares `readings`, taken at `now`, and schedules the next check.
    /// Returns the standby that should take over from a silent or lagging
    /// primary; none does while the primary is down or during the cooldown.
    pub(super) fn check(&mut self, readings: &Readings, now: Instant) -> Option<Takeover> {
        self.next_check = now + self.checks.tick;
        let primary = readings.primary.filter(|_| self.may_take_over(now));
        let Some(primary) = primary else {
            self.lagging = None;
            return None;
        };
        let lagging = self.count_lag(primary, &readings.standbys);
        let silent = self.outlasting_silence(primary, &readings.standbys);
        let (standby, cause) = match (silent, lagging) {
            (Some(standby), _) => (standby, FailoverCause::Silent),
            (None, Some(standby)) => (standby, FailoverCause::Lagging),
            (None, None) => return None,
        };
        Some(Takeover { standby, cause })
    }

    /// The standby that should take over, at `now`, from a primary whose
    /// resync has left a live market unanswered: the one that delivered the
    /// most level changes over the window, if any. None during the
    /// cooldown.
    pub(super) fn delivering_standby(&self, readings: &Readings, now: Instant) -> Option<usize> {
        let busiest = busiest(&readings.standbys).filter(|_| self.may_take_over(now))?;
        (busiest.level_changes > 0).then_some(busiest.replica)
    }

    /// A standby took over from a connected primary at `now`: the cooldown
    /// starts.
    pub(super) fn note_takeover(&mut self, now: Instant) {
        self.cooldown_until = Some(now + self.checks.cooldown);
    }

    /// Whether a standby may take over from a connected primary at `now`:
    /// false during the cooldown.
    pub(super) fn may_take_over(&self, now: Instant) -> bool {
        self.cooldown_until.is_none_or(|until| now >= until)
    }

    /// The standby that delivered the most, if at least the minimum, over a
    /// window in which `primary`, connected throughout, delivered nothing.
    fn outlasting_silence(&self, primary: Reading, standbys: &[Reading]) -> Option<usize> {
        let silent = primary.level_changes == 0 && self.spans_window(&primary);
        let busiest = busiest(standbys)?;
        let active = busiest.level_changes >= self.checks.silent_standby_events;
        (silent && active).then_some(busiest.replica)
    }

    /// Counts this check if `primary` trails a standby's lag by more than
    /// the threshold, or starts the count over; returns that standby once
    /// the count reaches [`HealthChecks::lag_ticks`]. A new primary's count
    /// starts afresh.
    fn count_lag(&mut self, primary: Reading, standbys: &[Reading]) -> Option<usize> {
        let Some(leader) = self.leader_by_lag(primary, standbys) else {
            self.lagging = None;
            return None;
        };
        let same_primary = |count: &LagCount| count.primary == primary.replica;
        let earlier = self.lagging.filter(same_primary);
        let checks = earlier.map_or(1, |count| count.checks + 1);
        self.lagging = Some(LagCount {
            primary: primary.replica,
            checks,
        });
        (checks >= self.checks.lag_ticks).then_some(leader)
    }

    /// The standby with the least lag, if `primary`'s trails it by more
    /// than the threshold while its own is within it. Only sockets up for
    /// the whole window are compared, so both lags cover the same span.
    fn leader_by_lag(&self, primary: Reading, standbys: &[Reading]) -> Option<usize> {
        let primary_lag = self.full_window_lag(&primary)?;
        let sampled = |standby: &Reading| Some((standby.replica, self.full_window_lag(standby)?));
        let lags = standbys.iter().filter_map(sampled);
        let (leader, lag) = lags.min_by_key(|&(_, lag)| lag)?;
        let threshold = i64::try_from(self.checks.lag_threshold.as_millis()).unwrap_or(i64::MAX);
        let trails = primary_lag.saturating_sub(lag) > threshold;
        (trails && lag < threshold).then_some(leader)
    }

    /// `reading`'s median lag, if its socket has been up for the whole
    /// window.
    fn full_window_lag(&self, reading: &Reading) -> Option<i64> {
        reading.median_lag_ms.filter(|_| self.spans_window(reading))
    }

    /// Whether `reading`'s socket has been up for the whole window.
    fn spans_window(&self, reading: &Reading) -> bool {
        reading.uptime >= self.checks.window()
    }
}

/// The standby that delivered the most level changes over the window.
fn busiest(standbys: &[Reading]) -> Option<&Reading> {
    standbys.iter().max_by_key(|standby| standby.level_changes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(10);

    fn monitor() -> HealthMonitor {
        HealthMonitor::new(HealthChecks::default())
    }

    /// Replica `replica`, up for a window, with `level_changes` and `lag`.
    fn reading(replica: usize, level_changes: u64, lag: Option<i64>) -> Reading {
        Reading {
            replica,
            uptime: WINDOW,
            level_changes,
            median_lag_ms: lag,
        }
    }

    fn readings(primary: Reading, standbys: &[Reading]) -> Readings {
        Readings {
            primary: Some(primary),
            standbys: standbys.to_vec(),
        }
    }

    fn takeover(standby: usize, cause: FailoverCause) -> Option<Takeover> {
        Some(Takeover { standby, cause })
    }

    /// Checks `readings` `times` times, a second apart, returning whether
    /// each check handed over.
    fn check_times(monitor: &mut HealthMonitor, readings: &Readings, times: u32) -> Vec<bool> {
        let start = Instant::now();
        let at = |tick| start + Duration::from_secs(u64::from(tick));
        let checks = (0..times).map(|tick| monitor.check(readings, at(tick)).is_some());
        checks.collect()
    }

    #[test]
    fn a_silent_primary_hands_over_to_the_busiest_active_standby() {
        let standbys = [reading(1, 20, None), reading(2, 30, None)];
        let seen = readings(reading(0, 0, None), &standbys);
        let outcome = monitor().check(&seen, Instant::now());
        assert_eq!(outcome, takeover(2, FailoverCause::Silent));
    }

    #[test]
    fn a_standby_below_the_minimum_does_not_take_over_from_a_silent_primary() {
        let seen = readings(reading(0, 0, None), &[reading(1, 19, None)]);
        assert_eq!(monitor().check(&seen, Instant::now()), None);
    }

    #[test]
    fn a_primary_up_for_less_than_the_window_is_not_silent() {
        let primary = Reading {
            uptime: WINDOW - Duration::from_millis(1),
            ..reading(0, 0, None)
        };
        let seen = readings(primary, &[reading(1, 50, None)]);
        assert_eq!(monitor().check(&seen, Instant::now()), None);
    }

    #[test]
    fn a_lagging_primary_hands_over_on_the_third_check_in_a_row() {
        let seen = readings(reading(0, 50, Some(700)), &[reading(1, 50, Some(100))]);
        let outcomes = check_times(&mut monitor(), &seen, 3);
        assert_eq!(outcomes, [false, false, true]);
    }

    #[test]
    fn a_lag_within_the_threshold_starts_the_count_over() {
        let mut monitor = monitor();
        let lagging = readings(reading(0, 50, Some(700)), &[reading(1, 50, Some(100))]);
        let close = readings(reading(0, 50, Some(600)), &[reading(1, 50, Some(100))]);
        check_times(&mut monitor, &lagging, 2);
        check_times(&mut monitor, &close, 1);
        assert_eq!(check_times(&mut monitor, &lagging, 3), [false, false, true]);
    }

    #[test]
    fn the_lag_count_starts_over_for_a_new_primary() {
        let mut monitor = monitor();
        let first = readings(reading(0, 50, Some(700)), &[reading(1, 50, Some(100))]);
        let second = readings(reading(1, 50, Some(700)), &[reading(2, 50, Some(100))]);
        check_times(&mut monitor, &first, 2);
        assert_eq!(check_times(&mut monitor, &second, 3), [false, false, true]);
    }

    #[test]
    fn a_lag_is_only_compared_when_both_were_sampled() {
        let seen = readings(reading(0, 50, Some(5000)), &[reading(1, 50, None)]);
        assert_eq!(check_times(&mut monitor(), &seen, 5), [false; 5]);
    }

    #[test]
    fn a_standby_up_for_less_than_the_window_is_not_compared_for_lag() {
        let standby = Reading {
            uptime: WINDOW - Duration::from_millis(1),
            ..reading(1, 50, Some(0))
        };
        let seen = readings(reading(0, 50, Some(2000)), &[standby]);
        assert_eq!(check_times(&mut monitor(), &seen, 5), [false; 5]);
    }

    #[test]
    fn a_standby_lagging_beyond_the_threshold_itself_does_not_take_over() {
        let seen = readings(reading(0, 50, Some(3000)), &[reading(1, 50, Some(1500))]);
        assert_eq!(check_times(&mut monitor(), &seen, 5), [false; 5]);
    }

    #[test]
    fn nothing_takes_over_during_the_cooldown() {
        let mut monitor = monitor();
        let now = Instant::now();
        monitor.note_takeover(now);
        let seen = readings(reading(0, 0, None), &[reading(1, 50, None)]);
        let during = monitor.check(&seen, now + Duration::from_secs(59));
        let after = monitor.check(&seen, now + Duration::from_secs(60));
        assert_eq!((during, after), (None, takeover(1, FailoverCause::Silent)));
    }

    #[test]
    fn a_primary_that_is_down_is_left_to_the_disconnect_failover() {
        let seen = Readings {
            primary: None,
            standbys: vec![reading(1, 50, None)],
        };
        assert_eq!(monitor().check(&seen, Instant::now()), None);
    }

    #[test]
    fn a_resync_hands_over_to_the_busiest_delivering_standby() {
        let standbys = [reading(1, 3, None), reading(2, 5, None)];
        let seen = readings(reading(0, 50, None), &standbys);
        assert_eq!(monitor().delivering_standby(&seen, Instant::now()), Some(2));
    }

    #[test]
    fn a_resync_does_not_hand_over_to_a_standby_delivering_nothing() {
        let seen = readings(reading(0, 50, None), &[reading(1, 0, None)]);
        assert_eq!(monitor().delivering_standby(&seen, Instant::now()), None);
    }

    #[test]
    fn a_resync_does_not_hand_over_during_the_cooldown() {
        let mut monitor = monitor();
        let now = Instant::now();
        monitor.note_takeover(now);
        let seen = readings(reading(0, 50, None), &[reading(1, 50, None)]);
        assert_eq!(monitor.delivering_standby(&seen, now), None);
    }

    #[test]
    fn each_check_schedules_the_next_a_tick_later() {
        let mut monitor = monitor();
        let now = Instant::now();
        monitor.check(&Readings::default(), now);
        assert_eq!(monitor.next_check(), now + Duration::from_secs(1));
    }
}
