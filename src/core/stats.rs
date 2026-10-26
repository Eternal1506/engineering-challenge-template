//! Running counters shared by the recorder's tasks, and the periodic stats
//! line built from them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::core::writer::{WriterMessage, WriterProgress};

/// The default time between two stats lines.
pub const DEFAULT_STATS_INTERVAL: Duration = Duration::from_secs(30);

/// Counts the shard tasks keep as they run, read by the stats line.
#[derive(Debug, Default)]
pub struct RecorderCounters {
    events_forwarded: AtomicU64,
    connections_up: AtomicUsize,
    disconnects: AtomicU64,
    gaps: AtomicU64,
    resync_drops: AtomicU64,
    resnapshot_requests: AtomicU64,
}

/// One reading of every [`RecorderCounters`] count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CounterValues {
    /// Feed events sent on to the writer.
    pub events_forwarded: u64,
    /// Connections whose subscription the exchange has named.
    pub connections_up: usize,
    /// Connected sockets that were lost.
    pub disconnects: u64,
    /// Sequence gaps seen.
    pub gaps: u64,
    /// Level changes dropped while resyncing after a gap.
    pub resync_drops: u64,
    /// Fresh snapshots requested for inconsistent books.
    pub resnapshot_requests: u64,
}

impl RecorderCounters {
    /// One feed event went to the writer.
    pub fn note_forwarded(&self) {
        self.events_forwarded.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection came up.
    pub fn note_connected(&self) {
        self.connections_up.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection that was up lost its socket.
    pub fn note_disconnected(&self) {
        self.disconnects.fetch_add(1, Ordering::Relaxed);
        self.note_closed();
    }

    /// A connection that was up shut down.
    pub fn note_closed(&self) {
        self.connections_up.fetch_sub(1, Ordering::Relaxed);
    }

    /// A connection saw a sequence gap.
    pub fn note_gap(&self) {
        self.gaps.fetch_add(1, Ordering::Relaxed);
    }

    /// A level change was dropped while resyncing after a gap.
    pub fn note_resync_drop(&self) {
        self.resync_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// A fresh snapshot was requested for an inconsistent book.
    pub fn note_resnapshot_request(&self) {
        self.resnapshot_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// The current counts.
    pub fn read(&self) -> CounterValues {
        CounterValues {
            events_forwarded: self.events_forwarded.load(Ordering::Relaxed),
            connections_up: self.connections_up.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            resync_drops: self.resync_drops.load(Ordering::Relaxed),
            resnapshot_requests: self.resnapshot_requests.load(Ordering::Relaxed),
        }
    }
}

/// Samples the writer channel's depth and logs the stats line at info,
/// once per interval.
///
/// The line has the shard count, connections up and down, events forwarded
/// per second since the last line, snapshots and updates written, the writer
/// channel's depth and its peak since startup, and the resnapshot request,
/// gap, resync drop and disconnect totals. A sample above half the
/// channel's capacity is logged at warn when the depth crosses that mark.
#[derive(Debug)]
pub struct StatsReporter {
    counters: Arc<RecorderCounters>,
    progress: Arc<WriterProgress>,
    interval: Duration,
    last_line_at: Instant,
    forwarded_at_last_line: u64,
    peak_depth: usize,
    over_half: bool,
}

impl StatsReporter {
    /// A reporter over `counters` and the writer's `progress` that logs a
    /// line every `interval`.
    pub fn new(
        counters: Arc<RecorderCounters>,
        progress: Arc<WriterProgress>,
        interval: Duration,
    ) -> Self {
        Self {
            counters,
            progress,
            interval,
            last_line_at: Instant::now(),
            forwarded_at_last_line: 0,
            peak_depth: 0,
            over_half: false,
        }
    }

    /// Samples the depth of `writer`'s channel, and logs the stats line if
    /// one is due, with `shards` as the shard count.
    pub fn sample(&mut self, writer: &mpsc::Sender<WriterMessage>, shards: usize) {
        let capacity = writer.max_capacity();
        let depth = capacity - writer.capacity();
        self.peak_depth = self.peak_depth.max(depth);
        self.warn_when_over_half(depth, capacity);
        if self.last_line_at.elapsed() >= self.interval {
            self.log_line(depth, shards);
        }
    }

    /// Logs the stats line now, with the writer channel's current depth.
    pub fn log_now(&mut self, writer: &mpsc::Sender<WriterMessage>, shards: usize) {
        let depth = writer.max_capacity() - writer.capacity();
        self.peak_depth = self.peak_depth.max(depth);
        self.log_line(depth, shards);
    }

    fn warn_when_over_half(&mut self, depth: usize, capacity: usize) {
        let over_half = depth * 2 > capacity;
        if over_half && !self.over_half {
            tracing::warn!(depth, capacity, "the writer channel is over half full");
        }
        self.over_half = over_half;
    }

    fn log_line(&mut self, depth: usize, shards: usize) {
        let counts = self.counters.read();
        let events_per_sec = self.events_per_second(counts.events_forwarded);
        tracing::info!(
            shards,
            up = counts.connections_up,
            down = shards.saturating_sub(counts.connections_up),
            events_per_sec,
            snapshots = self.progress.snapshots_written(),
            updates = self.progress.updates_written(),
            writer_depth = depth,
            writer_peak = self.peak_depth,
            resnapshots = counts.resnapshot_requests,
            gaps = counts.gaps,
            resync_drops = counts.resync_drops,
            disconnects = counts.disconnects,
            "stats"
        );
    }

    /// Events forwarded per second since the last line, rounded; starts the
    /// next period.
    fn events_per_second(&mut self, forwarded: u64) -> u64 {
        let elapsed = self.last_line_at.elapsed().as_secs_f64();
        let events = forwarded - self.forwarded_at_last_line;
        self.last_line_at = Instant::now();
        self.forwarded_at_last_line = forwarded;
        if elapsed > 0.0 {
            (events as f64 / elapsed).round() as u64
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lost_connection_counts_as_down_and_as_a_disconnect() {
        let counters = RecorderCounters::default();
        counters.note_connected();
        counters.note_connected();
        counters.note_disconnected();
        let counts = counters.read();
        assert_eq!((counts.connections_up, counts.disconnects), (1, 1));
    }

    #[test]
    fn a_closed_connection_counts_as_down_but_not_as_a_disconnect() {
        let counters = RecorderCounters::default();
        counters.note_connected();
        counters.note_closed();
        let counts = counters.read();
        assert_eq!((counts.connections_up, counts.disconnects), (0, 0));
    }

    #[tokio::test]
    async fn the_peak_depth_is_kept_after_the_channel_drains() {
        let (writer, mut receiver) = mpsc::channel(8);
        let progress = Arc::new(WriterProgress::default());
        let counters = Arc::new(RecorderCounters::default());
        let mut stats = StatsReporter::new(counters, progress, Duration::from_secs(3600));
        for symbol in ["A", "B", "C"] {
            writer
                .try_send(WriterMessage::Track(symbol.into()))
                .unwrap();
        }
        stats.sample(&writer, 1);
        while receiver.try_recv().is_ok() {}
        stats.sample(&writer, 1);
        assert_eq!(stats.peak_depth, 3);
    }
}
