//! Running counters shared by the recorder's tasks, and the periodic stats
//! line built from them.

mod lag;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::core::writer::{WriterMessage, WriterProgress};
use lag::LagHistogram;

pub use lag::LagQuantiles;

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

/// The part a connection plays in its shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    /// The connection whose events reach the writer.
    Primary,
    /// A connection kept ready to take over from the primary.
    Standby,
}

/// What made a shard promote a standby to primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverCause {
    /// The primary's socket was lost.
    Disconnect,
    /// The primary delivered nothing while a standby kept delivering.
    Silent,
    /// The primary's feed lagged a standby's.
    Lagging,
    /// The primary left a live market unanswered past its resync deadline.
    Resync,
    /// The primary went silent on too many of its busy markets at once.
    Stale,
}

/// Counts the shard tasks and the coordinator keep as they run, read by the
/// stats line.
#[derive(Debug, Default)]
pub struct RecorderCounters {
    events_forwarded: AtomicU64,
    feed_lag: LagHistogram,
    monitoring: MonitorCounters,
    primaries_up: AtomicUsize,
    standbys_up: AtomicUsize,
    sockets: AtomicUsize,
    disconnects: AtomicU64,
    failovers: FailoverCounters,
    unreplicated_shards: AtomicU64,
    gaps: AtomicU64,
    resyncs_completed: AtomicU64,
    resyncs_overdue: AtomicU64,
    resync_reconnects: AtomicU64,
    resync_drops: AtomicU64,
    resync_resnapshots: AtomicU64,
    resnapshot_requests: AtomicU64,
    lifecycle_events: AtomicU64,
    added: CauseCounters,
    removed: CauseCounters,
    reconciles: AtomicU64,
}

/// The counts the shards' and the coordinator's monitoring keeps.
#[derive(Debug, Default)]
struct MonitorCounters {
    resubscribes: AtomicU64,
    stale_escalations: AtomicU64,
    overflows: AtomicU64,
    splits: AtomicU64,
    splits_skipped: AtomicU64,
    markets_watched: AtomicUsize,
    markets_active: AtomicUsize,
}

/// One reading of the monitoring counts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MonitorCounts {
    /// Silent markets resubscribed on their primary.
    pub resubscribes: u64,
    /// Checks that found so many busy markets silent at once that the
    /// primary's socket was treated as dead.
    pub stale_escalations: u64,
    /// Overflow errors, on any connection of any shard.
    pub overflows: u64,
    /// Shards split in two after repeated overflows.
    pub splits: u64,
    /// Splits skipped for want of room in the connection budget.
    pub splits_skipped: u64,
    /// Markets busy enough over the long window to be watched for silence,
    /// as of each shard's last check.
    pub markets_watched: usize,
    /// Markets busy enough over the short window, as of each shard's last
    /// check.
    pub markets_active: usize,
}

/// How many of one shard's markets are busy, as one check found them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MarketActivity {
    /// Markets busy enough over the long window to be watched for silence.
    pub watched: usize,
    /// Markets busy enough over the short window.
    pub active: usize,
}

/// One shard's share of the busy-market totals on the stats line. The
/// share it shows is taken back out when it is dropped.
#[derive(Debug)]
pub struct ActivityGauge {
    counters: Arc<RecorderCounters>,
    shown: MarketActivity,
}

impl ActivityGauge {
    /// A gauge adding to `counters`, showing nothing yet.
    pub fn new(counters: Arc<RecorderCounters>) -> Self {
        Self {
            counters,
            shown: MarketActivity::default(),
        }
    }

    /// Shows `activity` in place of what the gauge showed before.
    pub fn show(&mut self, activity: MarketActivity) {
        let monitoring = &self.counters.monitoring;
        let (before, after) = (self.shown, activity);
        shift(&monitoring.markets_watched, before.watched, after.watched);
        shift(&monitoring.markets_active, before.active, after.active);
        self.shown = activity;
    }
}

impl Drop for ActivityGauge {
    fn drop(&mut self) {
        self.show(MarketActivity::default());
    }
}

/// Moves `total` by the change from `before` to `after`.
fn shift(total: &AtomicUsize, before: usize, after: usize) {
    if after >= before {
        total.fetch_add(after - before, Ordering::Relaxed);
    } else {
        total.fetch_sub(before - after, Ordering::Relaxed);
    }
}

impl MonitorCounters {
    fn read(&self) -> MonitorCounts {
        MonitorCounts {
            resubscribes: self.resubscribes.load(Ordering::Relaxed),
            stale_escalations: self.stale_escalations.load(Ordering::Relaxed),
            overflows: self.overflows.load(Ordering::Relaxed),
            splits: self.splits.load(Ordering::Relaxed),
            splits_skipped: self.splits_skipped.load(Ordering::Relaxed),
            markets_watched: self.markets_watched.load(Ordering::Relaxed),
            markets_active: self.markets_active.load(Ordering::Relaxed),
        }
    }
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

/// One failover count per [`FailoverCause`].
#[derive(Debug, Default)]
struct FailoverCounters {
    disconnect: AtomicU64,
    silent: AtomicU64,
    lagging: AtomicU64,
    resync: AtomicU64,
    stale: AtomicU64,
}

/// One reading of the failover count per [`FailoverCause`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FailoverCounts {
    /// Failovers from a primary whose socket was lost.
    pub disconnect: u64,
    /// Failovers from a primary that went silent while a standby did not.
    pub silent: u64,
    /// Failovers from a primary whose feed lagged a standby's.
    pub lagging: u64,
    /// Failovers from a primary that left a live market unanswered past its
    /// resync deadline.
    pub resync: u64,
    /// Failovers from a primary silent on too many busy markets at once.
    pub stale: u64,
}

impl FailoverCounters {
    fn note(&self, cause: FailoverCause) {
        let counter = match cause {
            FailoverCause::Disconnect => &self.disconnect,
            FailoverCause::Silent => &self.silent,
            FailoverCause::Lagging => &self.lagging,
            FailoverCause::Resync => &self.resync,
            FailoverCause::Stale => &self.stale,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn read(&self) -> FailoverCounts {
        FailoverCounts {
            disconnect: self.disconnect.load(Ordering::Relaxed),
            silent: self.silent.load(Ordering::Relaxed),
            lagging: self.lagging.load(Ordering::Relaxed),
            resync: self.resync.load(Ordering::Relaxed),
            stale: self.stale.load(Ordering::Relaxed),
        }
    }
}

/// One reading of every [`RecorderCounters`] count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CounterValues {
    /// Feed events sent on to the writer.
    pub events_forwarded: u64,
    /// Primary connections whose subscription the exchange has named.
    pub primaries_up: usize,
    /// Standby connections whose subscription the exchange has named.
    pub standbys_up: usize,
    /// Book connections the shards run, connected or reconnecting.
    pub sockets: usize,
    /// Connected sockets that were lost, primaries' and standbys'.
    pub disconnects: u64,
    /// Standbys promoted to primary, by cause.
    pub failovers: FailoverCounts,
    /// Shards opened with one connection instead of the configured
    /// replicas, to keep within the connection budget.
    pub unreplicated_shards: u64,
    /// Sequence gaps seen, each starting a resync. A resync completes,
    /// becomes overdue, is restarted by another gap, or is ended by a new
    /// socket.
    pub gaps: u64,
    /// Resyncs that got a snapshot of every symbol before their deadline.
    pub resyncs_completed: u64,
    /// Resyncs whose deadline passed with symbols still unanswered.
    pub resyncs_overdue: u64,
    /// Overdue resyncs that requested a reconnect because an unanswered
    /// symbol changed.
    pub resync_reconnects: u64,
    /// Level changes dropped while their symbol was resyncing.
    pub resync_drops: u64,
    /// Snapshots asked for again for markets that changed while still
    /// unanswered after the resync deadline.
    pub resync_resnapshots: u64,
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
    /// Resubscribes, overflows, splits and busy markets.
    pub monitoring: MonitorCounts,
}

impl RecorderCounters {
    /// One feed event went to the writer.
    pub fn note_forwarded(&self) {
        self.events_forwarded.fetch_add(1, Ordering::Relaxed);
    }

    /// A level change went to the writer `lag_ms` milliseconds after the
    /// exchange sent it.
    pub fn note_feed_lag(&self, lag_ms: i64) {
        self.feed_lag.note(lag_ms);
    }

    /// The feed lag quantiles since the last call, which starts them over.
    pub fn take_feed_lag(&self) -> LagQuantiles {
        self.feed_lag.take_quantiles()
    }

    /// `count` silent markets were resubscribed.
    pub fn note_resubscribes(&self, count: usize) {
        let resubscribes = &self.monitoring.resubscribes;
        resubscribes.fetch_add(count as u64, Ordering::Relaxed);
    }

    /// A check found so many busy markets silent that the primary's socket
    /// was treated as dead.
    pub fn note_stale_escalation(&self) {
        let escalations = &self.monitoring.stale_escalations;
        escalations.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection reported an overflow.
    pub fn note_overflow(&self) {
        self.monitoring.overflows.fetch_add(1, Ordering::Relaxed);
    }

    /// A shard was split in two.
    pub fn note_split(&self) {
        self.monitoring.splits.fetch_add(1, Ordering::Relaxed);
    }

    /// A split was skipped for want of room in the connection budget.
    pub fn note_split_skipped(&self) {
        let skipped = &self.monitoring.splits_skipped;
        skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection playing `role` came up.
    pub fn note_connected(&self, role: ReplicaRole) {
        self.up(role).fetch_add(1, Ordering::Relaxed);
    }

    /// A connection playing `role` that was up lost its socket.
    pub fn note_disconnected(&self, role: ReplicaRole) {
        self.disconnects.fetch_add(1, Ordering::Relaxed);
        self.note_closed(role);
    }

    /// A connection playing `role` that was up shut down.
    pub fn note_closed(&self, role: ReplicaRole) {
        self.up(role).fetch_sub(1, Ordering::Relaxed);
    }

    /// A connected standby became its shard's primary, in place of one that
    /// was down, because of `cause`.
    pub fn note_promoted(&self, cause: FailoverCause) {
        self.note_failover(cause);
        self.note_closed(ReplicaRole::Standby);
        self.note_connected(ReplicaRole::Primary);
    }

    /// A standby took over from the primary because of `cause`.
    pub fn note_failover(&self, cause: FailoverCause) {
        self.failovers.note(cause);
    }

    /// A shard started a book connection.
    pub fn note_socket_started(&self) {
        self.sockets.fetch_add(1, Ordering::Relaxed);
    }

    /// A shard's book connection ended.
    pub fn note_socket_ended(&self) {
        self.sockets.fetch_sub(1, Ordering::Relaxed);
    }

    /// A shard opened with one connection to keep within the budget.
    pub fn note_unreplicated_shard(&self) {
        self.unreplicated_shards.fetch_add(1, Ordering::Relaxed);
    }

    fn up(&self, role: ReplicaRole) -> &AtomicUsize {
        match role {
            ReplicaRole::Primary => &self.primaries_up,
            ReplicaRole::Standby => &self.standbys_up,
        }
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

    /// An overdue resync requested a reconnect.
    pub fn note_resync_reconnect(&self) {
        self.resync_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    /// A level change was dropped while its symbol was resyncing.
    pub fn note_resync_drop(&self) {
        self.resync_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// A held market's snapshot was asked for again after the deadline.
    pub fn note_resync_resnapshot(&self) {
        self.resync_resnapshots.fetch_add(1, Ordering::Relaxed);
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
        let values = self.read_markets();
        let values = self.read_recovery(values);
        self.read_connections(values)
    }

    /// `values` with the connection and throughput counts read.
    fn read_connections(&self, values: CounterValues) -> CounterValues {
        CounterValues {
            events_forwarded: self.events_forwarded.load(Ordering::Relaxed),
            primaries_up: self.primaries_up.load(Ordering::Relaxed),
            standbys_up: self.standbys_up.load(Ordering::Relaxed),
            sockets: self.sockets.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            failovers: self.failovers.read(),
            unreplicated_shards: self.unreplicated_shards.load(Ordering::Relaxed),
            ..values
        }
    }

    /// `values` with the gap, resync and resnapshot counts read.
    fn read_recovery(&self, values: CounterValues) -> CounterValues {
        CounterValues {
            gaps: self.gaps.load(Ordering::Relaxed),
            resyncs_completed: self.resyncs_completed.load(Ordering::Relaxed),
            resyncs_overdue: self.resyncs_overdue.load(Ordering::Relaxed),
            resync_reconnects: self.resync_reconnects.load(Ordering::Relaxed),
            resync_drops: self.resync_drops.load(Ordering::Relaxed),
            resync_resnapshots: self.resync_resnapshots.load(Ordering::Relaxed),
            resnapshot_requests: self.resnapshot_requests.load(Ordering::Relaxed),
            ..values
        }
    }

    /// The market lifecycle and monitoring counts, every other count zero.
    fn read_markets(&self) -> CounterValues {
        CounterValues {
            lifecycle_events: self.lifecycle_events.load(Ordering::Relaxed),
            added: self.added.read(),
            removed: self.removed.read(),
            reconciles: self.reconciles.load(Ordering::Relaxed),
            monitoring: self.monitoring.read(),
            ..CounterValues::default()
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

/// Samples the writer channel's depth and logs the stats lines at info,
/// once per interval.
///
/// - `stats`: the shard count, the markets recorded and known, the markets
///   busy over the last minute and those watched for silence, primaries up
///   and down, standbys up, book connections running, events forwarded per
///   second and the median and 99th percentile feed lag since the last
///   line, snapshots and updates written, books audited and the share that
///   matched, and the writer channel's depth and its peak since startup.
/// - `stats: recovery`: the totals of disconnects, failovers by
///   [`FailoverCause`], shards left unreplicated, resnapshot requests,
///   gaps, resyncs (completed, overdue, reconnect requested), resync drops,
///   held markets asked for again, silent markets resubscribed, sockets
///   found dead by their silence, overflows, and shard splits made and
///   skipped.
/// - `stats: markets`: the totals of lifecycle events, markets added and
///   removed by each [`ChangeCause`], and reconciles.
///
/// A sample above half the channel's capacity is logged at warn when the
/// depth crosses that mark.
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
        self.log_flow(&counts, depth, census);
        log_recovery(&counts);
        log_market_changes(&counts);
    }

    /// Logs the shards, markets, connections, throughput, feed lag, audits
    /// and writer depth.
    fn log_flow(&mut self, counts: &CounterValues, depth: usize, census: Census) {
        let events_per_sec = self.events_per_second(counts.events_forwarded);
        let lag = self.counters.take_feed_lag();
        let audits = self.progress.audits();
        tracing::info!(
            shards = census.shards,
            markets = census.markets,
            known = census.known,
            markets_active = counts.monitoring.markets_active,
            markets_watched = counts.monitoring.markets_watched,
            up = counts.primaries_up,
            down = census.shards.saturating_sub(counts.primaries_up),
            standbys_up = counts.standbys_up,
            sockets = counts.sockets,
            events_per_sec,
            feed_lag_p50_ms = lag.p50,
            feed_lag_p99_ms = lag.p99,
            snapshots = self.progress.snapshots_written(),
            updates = self.progress.updates_written(),
            audits = audits.total(),
            audit_match_pct = audits.match_percent(),
            writer_depth = depth,
            writer_peak = self.peak_depth,
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

/// Logs the disconnect, failover, gap, resync, resnapshot, resubscribe,
/// overflow and split totals.
fn log_recovery(counts: &CounterValues) {
    let monitoring = &counts.monitoring;
    tracing::info!(
        disconnects = counts.disconnects,
        failovers_disconnect = counts.failovers.disconnect,
        failovers_silent = counts.failovers.silent,
        failovers_lagging = counts.failovers.lagging,
        failovers_resync = counts.failovers.resync,
        failovers_stale = counts.failovers.stale,
        unreplicated_shards = counts.unreplicated_shards,
        resnapshots = counts.resnapshot_requests,
        gaps = counts.gaps,
        resyncs_completed = counts.resyncs_completed,
        resyncs_overdue = counts.resyncs_overdue,
        resync_reconnects = counts.resync_reconnects,
        resync_drops = counts.resync_drops,
        resync_resnapshots = counts.resync_resnapshots,
        resubscribes = monitoring.resubscribes,
        stale_escalations = monitoring.stale_escalations,
        overflows = monitoring.overflows,
        splits = monitoring.splits,
        splits_skipped = monitoring.splits_skipped,
        "stats: recovery"
    );
}

/// Logs the lifecycle events and the markets added and removed by cause.
fn log_market_changes(counts: &CounterValues) {
    let (added, removed) = (counts.added, counts.removed);
    tracing::info!(
        lifecycle_events = counts.lifecycle_events,
        added_lifecycle = added.lifecycle,
        removed_lifecycle = removed.lifecycle,
        added_sweep = added.sweep,
        removed_sweep = removed.sweep,
        added_reconcile = added.reconcile,
        removed_reconcile = removed.reconcile,
        reconciles = counts.reconciles,
        "stats: markets"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lost_connection_counts_as_down_and_as_a_disconnect() {
        let counters = RecorderCounters::default();
        counters.note_connected(ReplicaRole::Primary);
        counters.note_connected(ReplicaRole::Primary);
        counters.note_disconnected(ReplicaRole::Primary);
        let counts = counters.read();
        assert_eq!((counts.primaries_up, counts.disconnects), (1, 1));
    }

    #[test]
    fn a_closed_connection_counts_as_down_but_not_as_a_disconnect() {
        let counters = RecorderCounters::default();
        counters.note_connected(ReplicaRole::Standby);
        counters.note_closed(ReplicaRole::Standby);
        let counts = counters.read();
        assert_eq!((counts.standbys_up, counts.disconnects), (0, 0));
    }

    #[test]
    fn a_promoted_standby_counts_as_a_primary_and_a_failover() {
        let counters = RecorderCounters::default();
        counters.note_connected(ReplicaRole::Standby);
        counters.note_promoted(FailoverCause::Disconnect);
        let counts = counters.read();
        let up = (counts.primaries_up, counts.standbys_up);
        assert_eq!((up, counts.failovers.disconnect), ((1, 0), 1));
    }

    #[test]
    fn failovers_are_counted_by_cause() {
        let counters = RecorderCounters::default();
        counters.note_failover(FailoverCause::Silent);
        counters.note_failover(FailoverCause::Resync);
        counters.note_failover(FailoverCause::Resync);
        let expected = FailoverCounts {
            silent: 1,
            resync: 2,
            ..FailoverCounts::default()
        };
        assert_eq!(counters.read().failovers, expected);
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

    /// The stats lines logged with `counters` and `progress`.
    fn stats_lines(counters: Arc<RecorderCounters>, progress: Arc<WriterProgress>) -> String {
        let (writer, _receiver) = mpsc::channel(8);
        let mut stats = StatsReporter::new(counters, progress, Duration::from_secs(3600));
        crate::core::test_logs::logs_of(|| stats.log_now(&writer, Census::default()))
    }

    #[test]
    fn the_stats_line_shows_the_audit_match_rate() {
        let progress = Arc::new(WriterProgress::default());
        (0..3).for_each(|_| progress.note_audit_match());
        progress.note_audit_mismatch();
        let logs = stats_lines(Arc::default(), progress);
        assert!(logs.contains("audits=4 audit_match_pct=75.0"), "{logs}");
    }

    #[test]
    fn the_stats_line_shows_the_feed_lag_since_the_last_line() {
        let counters = Arc::new(RecorderCounters::default());
        let lags = std::iter::repeat_n(30, 98).chain([700, 700]);
        lags.for_each(|lag| counters.note_feed_lag(lag));
        let logs = stats_lines(counters.clone(), Arc::default());
        let expected = "feed_lag_p50_ms=30 feed_lag_p99_ms=672";
        assert!(logs.contains(expected), "{logs}");
        assert_eq!(counters.take_feed_lag(), LagQuantiles::default());
    }

    #[test]
    fn each_shards_busy_markets_count_until_it_drops_its_gauge() {
        let counters = Arc::new(RecorderCounters::default());
        let busy = |watched, active| MarketActivity { watched, active };
        let mut first = ActivityGauge::new(counters.clone());
        let mut second = ActivityGauge::new(counters.clone());
        first.show(busy(5, 9));
        second.show(busy(2, 1));
        first.show(busy(4, 3));
        let both = counters.read().monitoring;
        drop(second);
        let one = counters.read().monitoring;
        let totals = |counts: MonitorCounts| (counts.markets_watched, counts.markets_active);
        assert_eq!((totals(both), totals(one)), ((6, 4), (4, 3)));
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
