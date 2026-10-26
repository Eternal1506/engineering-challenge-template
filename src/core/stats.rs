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

/// What made the coordinator start or stop recording a market after
/// startup. The markets the startup listing records are not counted by
/// cause; the stats line's market count includes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeCause {
    /// A lifecycle event.
    Lifecycle,
    /// The sweep, as an open or close time passed.
    Sweep,
    /// A reconcile with the catalog.
    Reconcile,
}

/// Counts the shard tasks and the coordinator keep as they run, read by the
/// stats line.
#[derive(Debug, Default)]
pub struct RecorderCounters {
    events_forwarded: AtomicU64,
    connections_up: AtomicUsize,
    disconnects: AtomicU64,
    gaps: AtomicU64,
    resyncs_completed: AtomicU64,
    resyncs_overdue: AtomicU64,
    resync_reconnects: AtomicU64,
    resync_drops: AtomicU64,
    resnapshot_requests: AtomicU64,
    lifecycle_events: AtomicU64,
    added: CauseCounters,
    removed: CauseCounters,
    reconciles: AtomicU64,
}

/// One count per [`ChangeCause`].
#[derive(Debug, Default)]
struct CauseCounters {
    lifecycle: AtomicU64,
    sweep: AtomicU64,
    reconcile: AtomicU64,
}

/// One reading of a count per [`ChangeCause`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CauseCounts {
    /// Changes made for lifecycle events.
    pub lifecycle: u64,
    /// Changes made by the sweep.
    pub sweep: u64,
    /// Changes made by reconciles.
    pub reconcile: u64,
}

impl CauseCounters {
    fn note(&self, cause: ChangeCause) {
        let counter = match cause {
            ChangeCause::Lifecycle => &self.lifecycle,
            ChangeCause::Sweep => &self.sweep,
            ChangeCause::Reconcile => &self.reconcile,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn read(&self) -> CauseCounts {
        CauseCounts {
            lifecycle: self.lifecycle.load(Ordering::Relaxed),
            sweep: self.sweep.load(Ordering::Relaxed),
            reconcile: self.reconcile.load(Ordering::Relaxed),
        }
    }
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
    /// Sequence gaps seen, each starting a resync. A resync completes,
    /// becomes overdue, is restarted by another gap, or is ended by a new
    /// socket.
    pub gaps: u64,
    /// Resyncs that got a snapshot of every symbol before their deadline.
    pub resyncs_completed: u64,
    /// Resyncs whose deadline passed with symbols still unanswered.
    pub resyncs_overdue: u64,
    /// Overdue resyncs that reconnected because an unanswered symbol
    /// changed.
    pub resync_reconnects: u64,
    /// Level changes dropped while their symbol was resyncing.
    pub resync_drops: u64,
    /// Fresh snapshots requested for inconsistent books.
    pub resnapshot_requests: u64,
    /// Market lifecycle events received.
    pub lifecycle_events: u64,
    /// Markets the coordinator started recording, by cause.
    pub added: CauseCounts,
    /// Markets the coordinator stopped recording, by cause.
    pub removed: CauseCounts,
    /// Reconciles with the catalog that finished, failed or not.
    pub reconciles: u64,
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

    /// A resync got a snapshot of every symbol in time.
    pub fn note_resync_completed(&self) {
        self.resyncs_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// A resync's deadline passed with symbols unanswered.
    pub fn note_resync_overdue(&self) {
        self.resyncs_overdue.fetch_add(1, Ordering::Relaxed);
    }

    /// An overdue resync reconnected.
    pub fn note_resync_reconnect(&self) {
        self.resync_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    /// A level change was dropped while its symbol was resyncing.
    pub fn note_resync_drop(&self) {
        self.resync_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// A fresh snapshot was requested for an inconsistent book.
    pub fn note_resnapshot_request(&self) {
        self.resnapshot_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// A market lifecycle event arrived.
    pub fn note_lifecycle_event(&self) {
        self.lifecycle_events.fetch_add(1, Ordering::Relaxed);
    }

    /// The coordinator started recording a market because of `cause`.
    pub fn note_added(&self, cause: ChangeCause) {
        self.added.note(cause);
    }

    /// The coordinator stopped recording a market because of `cause`.
    pub fn note_removed(&self, cause: ChangeCause) {
        self.removed.note(cause);
    }

    /// A reconcile with the catalog finished.
    pub fn note_reconcile(&self) {
        self.reconciles.fetch_add(1, Ordering::Relaxed);
    }

    /// The current counts.
    pub fn read(&self) -> CounterValues {
        CounterValues {
            events_forwarded: self.events_forwarded.load(Ordering::Relaxed),
            connections_up: self.connections_up.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            resyncs_completed: self.resyncs_completed.load(Ordering::Relaxed),
            resyncs_overdue: self.resyncs_overdue.load(Ordering::Relaxed),
            resync_reconnects: self.resync_reconnects.load(Ordering::Relaxed),
            resync_drops: self.resync_drops.load(Ordering::Relaxed),
            resnapshot_requests: self.resnapshot_requests.load(Ordering::Relaxed),
            lifecycle_events: self.lifecycle_events.load(Ordering::Relaxed),
            added: self.added.read(),
            removed: self.removed.read(),
            reconciles: self.reconciles.load(Ordering::Relaxed),
        }
    }
}

/// How many shards and markets the coordinator has, for the stats line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Census {
    /// Shards open.
    pub shards: usize,
    /// Markets recorded.
    pub markets: usize,
    /// Markets known, recorded or not: waiting to open, or ended but not
    /// yet forgotten.
    pub known: usize,
}

/// Samples the writer channel's depth and logs the stats line at info,
/// once per interval.
///
/// The line has the shard count, the markets recorded and known,
/// connections up and down, events forwarded per second since the last
/// line, snapshots and updates written, the writer channel's depth and its
/// peak since startup, and the totals of resnapshot requests, gaps, resyncs
/// (completed, overdue, reconnect requested), resync drops, disconnects,
/// lifecycle events, markets added and removed by each [`ChangeCause`], and
/// reconciles. A sample above half the channel's capacity is logged at warn
/// when the depth crosses that mark.
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
    /// one is due, with the coordinator's `census`.
    pub fn sample(&mut self, writer: &mpsc::Sender<WriterMessage>, census: Census) {
        let capacity = writer.max_capacity();
        let depth = capacity - writer.capacity();
        self.peak_depth = self.peak_depth.max(depth);
        self.warn_when_over_half(depth, capacity);
        if self.last_line_at.elapsed() >= self.interval {
            self.log_line(depth, census);
        }
    }

    /// Logs the stats line now, with the writer channel's current depth.
    pub fn log_now(&mut self, writer: &mpsc::Sender<WriterMessage>, census: Census) {
        let depth = writer.max_capacity() - writer.capacity();
        self.peak_depth = self.peak_depth.max(depth);
        self.log_line(depth, census);
    }

    fn warn_when_over_half(&mut self, depth: usize, capacity: usize) {
        let over_half = depth * 2 > capacity;
        if over_half && !self.over_half {
            tracing::warn!(depth, capacity, "the writer channel is over half full");
        }
        self.over_half = over_half;
    }

    fn log_line(&mut self, depth: usize, census: Census) {
        let counts = self.counters.read();
        let events_per_sec = self.events_per_second(counts.events_forwarded);
        let (added, removed) = (counts.added, counts.removed);
        tracing::info!(
            shards = census.shards,
            markets = census.markets,
            known = census.known,
            up = counts.connections_up,
            down = census.shards.saturating_sub(counts.connections_up),
            events_per_sec,
            snapshots = self.progress.snapshots_written(),
            updates = self.progress.updates_written(),
            writer_depth = depth,
            writer_peak = self.peak_depth,
            resnapshots = counts.resnapshot_requests,
            gaps = counts.gaps,
            resyncs_completed = counts.resyncs_completed,
            resyncs_overdue = counts.resyncs_overdue,
            resync_reconnects = counts.resync_reconnects,
            resync_drops = counts.resync_drops,
            disconnects = counts.disconnects,
            lifecycle_events = counts.lifecycle_events,
            added_lifecycle = added.lifecycle,
            removed_lifecycle = removed.lifecycle,
            added_sweep = added.sweep,
            removed_sweep = removed.sweep,
            added_reconcile = added.reconcile,
            removed_reconcile = removed.reconcile,
            reconciles = counts.reconciles,
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

    #[test]
    fn changes_are_counted_by_cause() {
        let counters = RecorderCounters::default();
        counters.note_added(ChangeCause::Lifecycle);
        counters.note_added(ChangeCause::Lifecycle);
        counters.note_removed(ChangeCause::Sweep);
        let counts = counters.read();
        let added = CauseCounts {
            lifecycle: 2,
            ..CauseCounts::default()
        };
        let removed = CauseCounts {
            sweep: 1,
            ..CauseCounts::default()
        };
        assert_eq!((counts.added, counts.removed), (added, removed));
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
        stats.sample(&writer, Census::default());
        while receiver.try_recv().is_ok() {}
        stats.sample(&writer, Census::default());
        assert_eq!(stats.peak_depth, 3);
    }
}
