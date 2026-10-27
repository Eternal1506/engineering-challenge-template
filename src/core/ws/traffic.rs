//! [`TrafficWindow`]: a socket's recent message rate, byte rate and feed
//! lag, kept cheaply in fixed-length buckets.

use std::time::Duration;

use tokio::time::Instant;

/// How many buckets the window keeps.
pub(crate) const BUCKETS: u32 = 10;

/// The bucket length of a socket's diagnostic window: ten seconds in all.
pub(super) const DIAGNOSTIC_BUCKET: Duration = Duration::from_secs(1);

/// The most lag samples one bucket keeps; later ones in the same bucket are
/// left out.
const LAG_SAMPLES_PER_BUCKET: usize = 256;

/// The shortest span rates are averaged over, so a socket's first instant
/// does not divide by zero, and the shortest bucket.
const MIN_SPAN: Duration = Duration::from_millis(1);

/// Counts of the last [`BUCKETS`] buckets of a socket's traffic.
#[derive(Debug)]
pub(crate) struct TrafficWindow {
    started: Instant,
    /// How long each bucket counts for.
    bucket: Duration,
    buckets: [Bucket; BUCKETS as usize],
}

#[derive(Debug, Default)]
struct Bucket {
    /// The bucket since the window started that this one counts.
    index: u64,
    /// Frames or events counted.
    count: u64,
    bytes: u64,
    lags_ms: Vec<i64>,
}

/// The window's averages over its buckets.
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
    /// An empty window starting at `now`, counting in buckets of `bucket`,
    /// at least a millisecond.
    pub(crate) fn new(now: Instant, bucket: Duration) -> Self {
        Self {
            started: now,
            bucket: bucket.max(MIN_SPAN),
            buckets: Default::default(),
        }
    }

    /// Empties the window, which starts again at `now`.
    pub(crate) fn restart(&mut self, now: Instant) {
        self.started = now;
        self.buckets = Default::default();
    }

    /// Counts one frame of `bytes` received at `now`.
    pub(crate) fn note_frame(&mut self, now: Instant, bytes: usize) {
        let bucket = self.bucket_at(now);
        bucket.count += 1;
        bucket.bytes += bytes as u64;
    }

    /// Counts one message seen at `now` whose size does not matter, such as
    /// a decoded event.
    pub(crate) fn note_event(&mut self, now: Instant) {
        self.bucket_at(now).count += 1;
    }

    /// Samples one feed lag, in milliseconds, seen at `now`.
    pub(crate) fn note_lag(&mut self, now: Instant, lag_ms: i64) {
        let bucket = self.bucket_at(now);
        if bucket.lags_ms.len() < LAG_SAMPLES_PER_BUCKET {
            bucket.lags_ms.push(lag_ms);
        }
    }

    /// The frames or events counted in the window's buckets up to `now`.
    pub(crate) fn count(&self, now: Instant) -> u64 {
        self.recent(now).map(|bucket| bucket.count).sum()
    }

    /// The median of the lags sampled in the window's buckets up to `now`,
    /// if any.
    pub(crate) fn median_lag_ms(&self, now: Instant) -> Option<i64> {
        let samples = self.recent(now).flat_map(|bucket| bucket.lags_ms.iter());
        let mut lags: Vec<i64> = samples.copied().collect();
        median(&mut lags)
    }

    /// The averages over the window's buckets up to `now`.
    pub(super) fn summary(&self, now: Instant) -> TrafficSummary {
        let bytes: u64 = self.recent(now).map(|bucket| bucket.bytes).sum();
        let span = self.span_secs(now);
        TrafficSummary {
            frames_per_sec: self.count(now) as f64 / span,
            bytes_per_sec: bytes as f64 / span,
            median_lag_ms: self.median_lag_ms(now),
        }
    }

    /// The buckets that count towards the window at `now`.
    fn recent(&self, now: Instant) -> impl Iterator<Item = &Bucket> {
        let current = self.index(now);
        let age = move |bucket: &&Bucket| current.saturating_sub(bucket.index);
        self.buckets
            .iter()
            .filter(move |bucket| age(bucket) < u64::from(BUCKETS))
    }

    /// The bucket for `now`, emptied if it last counted an older one.
    fn bucket_at(&mut self, now: Instant) -> &mut Bucket {
        let index = self.index(now);
        let bucket = &mut self.buckets[(index % u64::from(BUCKETS)) as usize];
        if bucket.index != index {
            bucket.index = index;
            bucket.count = 0;
            bucket.bytes = 0;
            bucket.lags_ms.clear();
        }
        bucket
    }

    /// Which bucket since the window started `now` falls in.
    fn index(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.started);
        (elapsed.as_nanos() / self.bucket.as_nanos()) as u64
    }

    /// The seconds the counted buckets cover at `now`: from the start of the
    /// oldest bucket in the window to `now`, at least [`MIN_SPAN`].
    fn span_secs(&self, now: Instant) -> f64 {
        let oldest = self.index(now).saturating_sub(u64::from(BUCKETS) - 1);
        let window_start = self.started + self.bucket.mul_f64(oldest as f64);
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

    const SECOND: Duration = Duration::from_secs(1);

    fn after(start: Instant, millis: u64) -> Instant {
        start + Duration::from_millis(millis)
    }

    #[test]
    fn rates_average_over_the_seconds_the_window_covers() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start, SECOND);
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
        let mut window = TrafficWindow::new(start, SECOND);
        for millis in [0, 500, 1000, 1500] {
            window.note_frame(after(start, millis), 10);
        }
        let summary = window.summary(after(start, 2000));
        assert_eq!(summary.frames_per_sec, 2.0);
    }

    #[test]
    fn the_lag_is_the_median_of_the_recent_samples() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start, SECOND);
        window.note_frame(start, 1);
        for lag in [50, 10, 30, 20, 40] {
            window.note_lag(start, lag);
        }
        assert_eq!(window.summary(start).median_lag_ms, Some(30));
    }

    #[test]
    fn samples_older_than_the_window_are_left_out() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start, SECOND);
        window.note_frame(start, 1);
        window.note_lag(start, 999);
        window.note_frame(after(start, 12_000), 1);
        window.note_lag(after(start, 12_000), 5);
        assert_eq!(window.summary(after(start, 12_000)).median_lag_ms, Some(5));
    }

    #[test]
    fn short_buckets_make_a_short_window() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start, Duration::from_millis(100));
        window.note_frame(start, 1);
        window.note_frame(after(start, 950), 1);
        let count_at = |millis| window.count(after(start, millis));
        assert_eq!((count_at(990), count_at(1000)), (2, 1));
    }

    #[test]
    fn a_restart_forgets_everything_counted() {
        let start = Instant::now();
        let mut window = TrafficWindow::new(start, SECOND);
        window.note_event(start);
        window.note_lag(start, 7);
        window.restart(after(start, 100));
        let now = after(start, 200);
        assert_eq!((window.count(now), window.median_lag_ms(now)), (0, None));
    }
}
