//! [`Timers`]: the coordinator's periodic work.

use std::time::Duration;

use tokio::time::{Instant, Interval, MissedTickBehavior};

use super::LifecycleTimings;

/// How often the coordinator samples the writer channel and checks whether
/// the stats line is due.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// A piece of periodic work that is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Tick {
    /// Sample the writer channel and log the stats line if due.
    Sample,
    /// Sweep for markets whose open or close time has passed.
    Sweep,
    /// Reconcile the markets with the catalog.
    Reconcile,
}

/// The intervals behind each [`Tick`]. The sample is due at once; a sweep
/// and a reconcile first after one period.
#[derive(Debug)]
pub(super) struct Timers {
    samples: Interval,
    sweeps: Interval,
    reconciles: Interval,
}

impl Timers {
    /// Timers for `timings`, starting now.
    pub(super) fn new(timings: &LifecycleTimings) -> Self {
        Self {
            samples: tokio::time::interval(SAMPLE_INTERVAL),
            sweeps: every(timings.sweep_interval),
            reconciles: every(timings.reconcile_interval),
        }
    }

    /// The next tick that falls due.
    pub(super) async fn next(&mut self) -> Tick {
        tokio::select! {
            _ = self.samples.tick() => Tick::Sample,
            _ = self.sweeps.tick() => Tick::Sweep,
            _ = self.reconciles.tick() => Tick::Reconcile,
        }
    }
}

/// An interval of `period` whose first tick is one period away, and which
/// waits a full period after a tick that came late.
fn every(period: Duration) -> Interval {
    let mut interval = tokio::time::interval_at(Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}
