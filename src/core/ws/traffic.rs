//! [`TrafficWindow`]: a socket's recent message rate, byte rate and feed
//! lag, kept cheaply in one-second buckets for diagnostics.

use std::time::Duration;

use tokio::time::Instant;

/// How many one-second buckets the window keeps.
const BUCKETS: usize = 10;

/// The most lag samples one bucket keeps; later ones in the same second are
/// left out.
const LAG_SAMPLES_PER_BUCKET: usize = 256;

/// The shortest span rates are averaged over, so a socket's first instant
/// does not divide by zero.
const MIN_SPAN: Duration = Duration::from_millis(1);

/// Counts of the last [`BUCKETS`] seconds of a socket's traffic.
#[derive(Debug)]
pub(super) struct TrafficWindow {
    started: Instant,
    buckets: [Bucket; BUCKETS],
}

#[derive(Debug, Default)]
struct Bucket {
    /// The second since the window started that the bucket counts.
    second: u64,
    frames: u64,
    bytes: u64,
    lags_ms: Vec<i64>,
}

/// The window's averages over its seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct TrafficSummary {
    /// Frames per second.
    pub(super) frames_per_sec: f64,
    /// Frame bytes per second.
    pub(super) bytes_per_sec: f64,
    /// The median of the sampled feed lags, if any.
    pub(super) median_lag_ms: Option<i64>,
}

impl TrafficWindow {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            started: now,
            buckets: Default::default(),
        }
    }

    /// Counts one frame of `bytes` received at `now`.
    pub(super) fn note_frame(&mut self, now: Instant, bytes: usize) {
        let bucket = self.bucket(now);
        bucket.frames += 1;
        bucket.bytes += bytes as u64;
    }

    /// Samples one feed lag, in milliseconds, seen at `now`.
    pub(super) fn note_lag(&mut self, now: Instant, lag_ms: i64) {
        let bucket = self.bucket(now);
        if bucket.lags_ms.len() < LAG_SAMPLES_PER_BUCKET {
            bucket.lags_ms.push(lag_ms);
        }
    }

    /// The averages over the window's seconds up to `now`.
    pub(super) fn summary(&self, now: Instant) -> TrafficSummary {
        let current = self.second(now);
        let recent = self
            .buckets
            .iter()
            .filter(|bucket| current.saturating_sub(bucket.second) < BUCKETS as u64);
        let (mut frames, mut bytes, mut lags) = (0, 0, Vec::new());
        for bucket in recent {
            frames += bucket.frames;
            bytes += bucket.bytes;
            lags.extend_from_slice(&bucket.lags_ms);
        }
        let span = self.span_secs(now);
        TrafficSummary {
            frames_per_sec: frames as f64 / span,
            bytes_per_sec: bytes as f64 / span,
            median_lag_ms: median(&mut lags),
        }
    }

    /// The bucket for `now`, emptied if it last counted an older second.
    fn bucket(&mut self, now: Instant) -> &mut Bucket {
        let second = self.second(now);
        let bucket = &mut self.buckets[(second % BUCKETS as u64) as usize];
        if bucket.second != second {
            bucket.second = second;
            bucket.frames = 0;
            bucket.bytes = 0;
            bucket.lags_ms.clear();
        }
        bucket
    }

    fn second(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.started).as_secs()
    }

    /// The seconds the counted buckets cover at `now`: from the start of the
    /// oldest second in the window to `now`, at least [`MIN_SPAN`].
    fn span_secs(&self, now: Instant) -> f64 {
        let oldest = self.second(now).saturating_sub(BUCKETS as u64 - 1);
        let window_start = self.started + Duration::from_secs(oldest);
        let span = now.saturating_duration_since(window_start);
        span.max(MIN_SPAN).as_secs_f64()
    }
}

fn median(values: &mut [i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let middle = values.len() / 2;
    Some(*values.select_nth_unstable(middle).1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn after(start: Instant, millis: u64) -> Instant {
        start + Duration::from_millis(millis)
    }

    #[test]
    fn rates_average_over_the_seconds_the_window_covers() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start);
        for second in 0..20 {
            window.note_frame(after(start, second * 1000), 100);
        }
        let summary = window.summary(after(start, 19_500));
        assert_eq!(
            (summary.frames_per_sec, summary.bytes_per_sec),
            (10.0 / 9.5, 1000.0 / 9.5)
        );
    }

    #[test]
    fn a_young_socket_averages_over_its_age() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start);
        for millis in [0, 500, 1000, 1500] {
            window.note_frame(after(start, millis), 10);
        }
        let summary = window.summary(after(start, 2000));
        assert_eq!(summary.frames_per_sec, 2.0);
    }

    #[test]
    fn the_lag_is_the_median_of_the_recent_samples() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start);
        window.note_frame(start, 1);
        for lag in [50, 10, 30, 20, 40] {
            window.note_lag(start, lag);
        }
        assert_eq!(window.summary(start).median_lag_ms, Some(30));
    }

    #[test]
    fn samples_older_than_the_window_are_left_out() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start);
        window.note_frame(start, 1);
        window.note_lag(start, 999);
        window.note_frame(after(start, 12_000), 1);
        window.note_lag(after(start, 12_000), 5);
        assert_eq!(window.summary(after(start, 12_000)).median_lag_ms, Some(5));
    }
}
