//! [`Backoff`]: jittered exponential delays between reconnects.

use std::time::Duration;

/// Reconnect delays whose ceiling doubles from `min` up to `max`. Each delay
/// is a random point between zero and its ceiling ("full jitter"), so
/// connections that drop together do not reconnect together.
#[derive(Debug)]
pub(super) struct Backoff {
    min: Duration,
    max: Duration,
    reset_after: Duration,
    failures: u32,
}

impl Backoff {
    /// Delays with a ceiling from `min` to `max`, starting over after a
    /// connection that stayed up for `reset_after`.
    pub(super) fn new(min: Duration, max: Duration, reset_after: Duration) -> Self {
        Self {
            min,
            max,
            reset_after,
            failures: 0,
        }
    }

    /// Records how long the last connection stayed up; one that lasted at
    /// least `reset_after` starts the ceiling over from `min`.
    pub(super) fn note_uptime(&mut self, uptime: Duration) {
        if uptime >= self.reset_after {
            self.failures = 0;
        }
    }

    /// The delay before the next reconnect.
    pub(super) fn next_delay(&mut self) -> Duration {
        let ceiling = self.ceiling();
        self.failures = self.failures.saturating_add(1);
        ceiling.mul_f64(rand::random_range(0.0..=1.0))
    }

    fn ceiling(&self) -> Duration {
        let factor = 2_u32.saturating_pow(self.failures);
        self.min.saturating_mul(factor).min(self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(10);
    const RESET_AFTER: Duration = Duration::from_secs(60);

    fn backoff() -> Backoff {
        Backoff::new(MIN, MAX, RESET_AFTER)
    }

    fn ceilings(backoff: &mut Backoff, count: usize) -> Vec<Duration> {
        (0..count)
            .map(|_| {
                let ceiling = backoff.ceiling();
                backoff.next_delay();
                ceiling
            })
            .collect()
    }

    #[test]
    fn the_ceiling_doubles_up_to_the_maximum() {
        let millis: Vec<u128> = ceilings(&mut backoff(), 8)
            .iter()
            .map(Duration::as_millis)
            .collect();
        assert_eq!(millis, [250, 500, 1000, 2000, 4000, 8000, 10000, 10000]);
    }

    #[test]
    fn each_delay_is_at_most_its_ceiling() {
        let mut backoff = backoff();
        for _ in 0..200 {
            let ceiling = backoff.ceiling();
            let delay = backoff.next_delay();
            assert!(delay <= ceiling, "{delay:?} vs {ceiling:?}");
        }
    }

    #[test]
    fn delays_are_spread_below_the_ceiling() {
        let mut backoff = backoff();
        backoff.failures = 10;
        let delays: Vec<Duration> = (0..200).map(|_| backoff.next_delay()).collect();
        assert!(delays.iter().any(|delay| *delay < MAX / 2));
    }

    #[test]
    fn many_failures_stay_at_the_maximum() {
        let mut backoff = backoff();
        backoff.failures = u32::MAX;
        assert_eq!(backoff.ceiling(), MAX);
    }

    #[test]
    fn a_long_uptime_starts_over_from_the_minimum() {
        let mut backoff = backoff();
        ceilings(&mut backoff, 5);
        backoff.note_uptime(RESET_AFTER);
        assert_eq!(backoff.ceiling(), MIN);
    }

    #[test]
    fn a_short_uptime_keeps_the_ceiling() {
        let mut backoff = backoff();
        ceilings(&mut backoff, 3);
        backoff.note_uptime(RESET_AFTER - Duration::from_millis(1));
        assert_eq!(backoff.ceiling(), MIN * 8);
    }
}
