//! The writer thread: it owns every [`Book`] and the SBE output, turns feed
//! events into `Snapshot` and `PriceLevelUpdate` messages, and snapshots every
//! book once per cycle, with the books staggered across the cycle.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::{Instant, Interval};

use crate::core::book::Book;
use crate::core::events::{FeedBody, FeedEvent, LevelChange};
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// The output chunk size. `AlignedSbeReader` reads 64 MiB chunks, so the
/// writer must write the same size.
pub const CHUNK_SIZE: usize = 64 << 20;

/// The default capacity of the writer's input channel.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 262_144;

/// The default time between two snapshot ticks.
pub const DEFAULT_SNAPSHOT_TICK: Duration = Duration::from_millis(250);

/// The default number of snapshot slots. With the default tick, each book is
/// snapshotted once every 240 × 250 ms = 60 s.
pub const DEFAULT_SNAPSHOT_SLOTS: usize = 240;

/// The most messages the writer takes from its channel at once.
const BATCH_SIZE: usize = 1024;

/// What the writer thread is asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriterMessage {
    /// Apply a feed event to its book and write it.
    Event(FeedEvent),
    /// Write a final snapshot of the symbol's book, then drop the book, for a
    /// market that has stopped trading.
    Forget(Arc<str>),
}

/// How to run the writer thread.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// The file to write SBE output to, such as `/dev/stdout`.
    pub output: PathBuf,
    /// The capacity of the input channel.
    pub channel_capacity: usize,
    /// The time between two snapshot ticks.
    pub snapshot_tick: Duration,
    /// How many ticks one snapshot cycle has. Each book is snapshotted once
    /// per cycle, on its own slot.
    pub snapshot_slots: usize,
    /// Flush the current chunk at least this often, if set. Each flush pads
    /// the chunk to its full size.
    pub flush_interval: Option<Duration>,
    /// Where to send the symbol of a book that a level change left
    /// inconsistent, so that a fresh snapshot can be requested for it.
    pub resnapshot_requests: mpsc::Sender<Arc<str>>,
}

impl WriterConfig {
    /// A configuration with the default channel capacity and snapshot
    /// schedule, and no periodic flush.
    pub fn new(output: impl Into<PathBuf>, resnapshot_requests: mpsc::Sender<Arc<str>>) -> Self {
        Self {
            output: output.into(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            snapshot_tick: DEFAULT_SNAPSHOT_TICK,
            snapshot_slots: DEFAULT_SNAPSHOT_SLOTS,
            flush_interval: None,
            resnapshot_requests,
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.channel_capacity > 0, "channel capacity is 0");
        anyhow::ensure!(self.snapshot_slots > 0, "snapshot slot count is 0");
        anyhow::ensure!(!self.snapshot_tick.is_zero(), "snapshot tick is 0");
        let flush_interval = self.flush_interval.unwrap_or(Duration::MAX);
        anyhow::ensure!(!flush_interval.is_zero(), "flush interval is 0");
        Ok(())
    }
}

/// What the writer thread did, returned when it finishes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriterStats {
    /// Feed events received, including dropped ones.
    pub events: u64,
    /// `Snapshot` messages written.
    pub snapshots_written: u64,
    /// `PriceLevelUpdate` messages written.
    pub updates_written: u64,
    /// Level changes dropped because their symbol had no snapshot yet.
    pub dropped_before_snapshot: u64,
    /// Level changes that left their book inconsistent.
    pub inconsistent: u64,
    /// Resnapshot requests lost because the request channel was full or
    /// closed.
    pub resnapshot_requests_dropped: u64,
}

/// The running writer thread.
#[derive(Debug)]
pub struct WriterHandle {
    thread: JoinHandle<anyhow::Result<WriterStats>>,
}

impl WriterHandle {
    /// Whether the thread has stopped, after finishing or failing.
    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }

    /// Waits for the thread to finish, which it does once every sender has
    /// been dropped and the channel is drained. Blocks the calling thread.
    ///
    /// Fails if the thread failed to write or panicked.
    pub fn join(self) -> anyhow::Result<WriterStats> {
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("the writer thread panicked"))?
    }
}

/// Creates the output file and starts the writer thread, named `writer`.
///
/// Returns the sender for the thread's input channel. The thread flushes and
/// closes the output once every clone of the sender is dropped. An output
/// error stops the thread, which then reports it from
/// [`WriterHandle::join`].
pub fn spawn_writer(
    config: WriterConfig,
) -> anyhow::Result<(mpsc::Sender<WriterMessage>, WriterHandle)> {
    config.validate()?;
    let output = AlignedSbeWriter::create(&config.output, CHUNK_SIZE)
        .with_context(|| format!("creating {}", config.output.display()))?;
    let (sender, receiver) = mpsc::channel(config.channel_capacity);
    let thread = std::thread::Builder::new()
        .name("writer".to_owned())
        .spawn(move || run(config, output, receiver))
        .context("spawning the writer thread")?;
    Ok((sender, WriterHandle { thread }))
}

/// The writer thread's body: a single-threaded runtime driving the event
/// loop, so timers need no busy-waiting.
fn run(
    config: WriterConfig,
    output: AlignedSbeWriter,
    receiver: mpsc::Receiver<WriterMessage>,
) -> anyhow::Result<WriterStats> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .context("building the writer runtime")?;
    let timing = Timing {
        snapshot_tick: config.snapshot_tick,
        flush_interval: config.flush_interval,
    };
    let writer = BookWriter::new(output, &config);
    runtime.block_on(writer.run(receiver, timing))
}

/// The writer's timers.
struct Timing {
    snapshot_tick: Duration,
    flush_interval: Option<Duration>,
}

/// The state the writer thread owns.
struct BookWriter {
    output: SbeOutput,
    books: HashMap<Arc<str>, TrackedBook>,
    schedule: SnapshotSchedule,
    resnapshot_requests: mpsc::Sender<Arc<str>>,
    stats: WriterStats,
}

/// A book with its snapshot slot.
struct TrackedBook {
    book: Book,
    slot: usize,
    /// Whether a resnapshot has been requested since the last feed snapshot.
    resnapshot_requested: bool,
}

impl BookWriter {
    fn new(output: AlignedSbeWriter, config: &WriterConfig) -> Self {
        Self {
            output: SbeOutput::new(output),
            books: HashMap::new(),
            schedule: SnapshotSchedule::new(config.snapshot_slots),
            resnapshot_requests: config.resnapshot_requests.clone(),
            stats: WriterStats::default(),
        }
    }

    /// Handles messages and timer ticks until every sender is dropped and the
    /// channel is drained, then flushes the output.
    async fn run(
        mut self,
        mut receiver: mpsc::Receiver<WriterMessage>,
        timing: Timing,
    ) -> anyhow::Result<WriterStats> {
        let mut snapshot_timer = periodic(timing.snapshot_tick);
        let mut flush_timer = timing.flush_interval.map(periodic);
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        loop {
            tokio::select! {
                received = receiver.recv_many(&mut batch, BATCH_SIZE) => {
                    if received == 0 {
                        break;
                    }
                    self.handle_batch(&mut batch)?;
                }
                _ = snapshot_timer.tick() => self.write_due_snapshots()?,
                _ = next_tick(&mut flush_timer) => self.output.flush()?,
            }
        }
        self.finish()
    }

    fn handle_batch(&mut self, batch: &mut Vec<WriterMessage>) -> anyhow::Result<()> {
        batch.drain(..).try_for_each(|message| self.handle(message))
    }

    fn handle(&mut self, message: WriterMessage) -> anyhow::Result<()> {
        match message {
            WriterMessage::Event(event) => self.handle_event(&event),
            WriterMessage::Forget(symbol) => self.forget(&symbol),
        }
    }

    fn handle_event(&mut self, event: &FeedEvent) -> anyhow::Result<()> {
        self.stats.events += 1;
        match &event.body {
            FeedBody::Snapshot { bids, asks } => self.apply_snapshot(event, bids, asks),
            FeedBody::Level(change) => self.apply_level(event, *change),
        }
    }

    /// Replaces the symbol's book, creating it with a snapshot slot if it is
    /// new, and writes the book as a snapshot.
    fn apply_snapshot(
        &mut self,
        event: &FeedEvent,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> anyhow::Result<()> {
        let tracked = self
            .books
            .entry(event.symbol.clone())
            .or_insert_with(|| TrackedBook {
                book: Book::default(),
                slot: self.schedule.add(event.symbol.clone()),
                resnapshot_requested: false,
            });
        let (bids, asks) = (bids.iter().copied(), asks.iter().copied());
        tracked.book.replace(bids, asks, event.exchange_ts);
        tracked.resnapshot_requested = false;
        let snapshot = tracked.book.to_snapshot(&event.symbol, event.received_at);
        self.output.write_snapshot(&snapshot)
    }

    /// Applies a level change and writes the level's absolute quantity, or
    /// drops the change if its symbol has no snapshot yet. The update's
    /// exchange time is the event's, else the book's latest, else the
    /// receive time.
    fn apply_level(&mut self, event: &FeedEvent, change: LevelChange) -> anyhow::Result<()> {
        let Some(tracked) = self.books.get_mut(&event.symbol) else {
            self.drop_before_snapshot(&event.symbol);
            return Ok(());
        };
        let applied = tracked.book.apply(change, event.exchange_ts);
        let exchange_ts = event.exchange_ts.or(tracked.book.last_exchange_ts());
        if !applied.consistent {
            self.report_inconsistent(&event.symbol, change.price);
        }
        self.output.write_update(&PriceLevelUpdateDomain {
            backtest_timestamp: event.received_at,
            exchange_timestamp: exchange_ts.unwrap_or(event.received_at),
            price_level: change.price,
            quantity: applied.quantity,
            side: change.side,
            symbol: event.symbol.to_string(),
        })
    }

    fn drop_before_snapshot(&mut self, symbol: &str) {
        self.stats.dropped_before_snapshot += 1;
        tracing::debug!(symbol, "dropped a level change before the first snapshot");
    }

    /// Counts and logs an inconsistent book, and asks for a fresh snapshot
    /// unless one is already pending.
    fn report_inconsistent(&mut self, symbol: &Arc<str>, price: i64) {
        self.stats.inconsistent += 1;
        tracing::warn!(%symbol, price, "level change left the book inconsistent");
        let Some(tracked) = self.books.get_mut(symbol) else {
            return;
        };
        if tracked.resnapshot_requested {
            return;
        }
        match self.resnapshot_requests.try_send(symbol.clone()) {
            Ok(()) => tracked.resnapshot_requested = true,
            Err(error) => {
                self.stats.resnapshot_requests_dropped += 1;
                tracing::warn!(%symbol, %error, "dropped a resnapshot request");
            }
        }
    }

    /// Writes a final snapshot of the symbol's book and drops the book.
    fn forget(&mut self, symbol: &Arc<str>) -> anyhow::Result<()> {
        let Some(tracked) = self.books.remove(symbol) else {
            tracing::debug!(%symbol, "asked to forget a symbol with no book");
            return Ok(());
        };
        self.schedule.remove(symbol, tracked.slot);
        self.output
            .write_snapshot(&tracked.book.to_snapshot(symbol, Utc::now()))
    }

    /// Snapshots every book whose slot is due on this tick.
    fn write_due_snapshots(&mut self) -> anyhow::Result<()> {
        let now = Utc::now();
        for symbol in self.schedule.next_due() {
            if let Some(tracked) = self.books.get(symbol) {
                self.output
                    .write_snapshot(&tracked.book.to_snapshot(symbol, now))?;
            }
        }
        Ok(())
    }

    /// Flushes the final chunk, closes the output and reports the stats.
    fn finish(self) -> anyhow::Result<WriterStats> {
        let (snapshots_written, updates_written) = self.output.close()?;
        Ok(WriterStats {
            snapshots_written,
            updates_written,
            ..self.stats
        })
    }
}

/// An interval whose first tick is one period from now.
fn periodic(period: Duration) -> Interval {
    tokio::time::interval_at(Instant::now() + period, period)
}

/// The next tick of `timer`, or never if there is no timer.
async fn next_tick(timer: &mut Option<Interval>) {
    match timer {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// The SBE output with counts of what was written to it.
struct SbeOutput {
    writer: AlignedSbeWriter,
    snapshots_written: u64,
    updates_written: u64,
}

impl SbeOutput {
    fn new(writer: AlignedSbeWriter) -> Self {
        Self {
            writer,
            snapshots_written: 0,
            updates_written: 0,
        }
    }

    fn write_snapshot(&mut self, snapshot: &SnapshotDomain) -> anyhow::Result<()> {
        self.writer
            .write_msg(snapshot)
            .with_context(|| format!("writing a snapshot of {}", snapshot.symbol))?;
        self.snapshots_written += 1;
        Ok(())
    }

    fn write_update(&mut self, update: &PriceLevelUpdateDomain) -> anyhow::Result<()> {
        self.writer
            .write_msg(update)
            .with_context(|| format!("writing an update of {}", update.symbol))?;
        self.updates_written += 1;
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush().context("flushing the output")
    }

    /// Flushes and closes the output, returning the snapshot and update
    /// counts.
    fn close(mut self) -> anyhow::Result<(u64, u64)> {
        self.flush()?;
        Ok((self.snapshots_written, self.updates_written))
    }
}

/// Which books are snapshotted on which tick. Each book gets one of a fixed
/// number of slots, assigned round-robin, and the ticks walk the slots in
/// order, so each book is snapshotted once per cycle of ticks.
struct SnapshotSchedule {
    slots: Vec<Vec<Arc<str>>>,
    next_assigned: usize,
    next_due: usize,
}

impl SnapshotSchedule {
    fn new(slot_count: usize) -> Self {
        Self {
            slots: vec![Vec::new(); slot_count],
            next_assigned: 0,
            next_due: 0,
        }
    }

    /// Puts `symbol` on the next slot in turn and returns that slot.
    fn add(&mut self, symbol: Arc<str>) -> usize {
        let slot = self.next_assigned;
        self.slots[slot].push(symbol);
        self.next_assigned = (slot + 1) % self.slots.len();
        slot
    }

    /// Takes `symbol` off `slot`.
    fn remove(&mut self, symbol: &str, slot: usize) {
        let symbols = &mut self.slots[slot];
        if let Some(index) = symbols.iter().position(|listed| &**listed == symbol) {
            symbols.swap_remove(index);
        }
    }

    /// The symbols due on this tick; moves on to the next slot.
    fn next_due(&mut self) -> &[Arc<str>] {
        let slot = self.next_due;
        self.next_due = (slot + 1) % self.slots.len();
        &self.slots[slot]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn schedule_with(slot_count: usize, symbols: &[&str]) -> SnapshotSchedule {
        let mut schedule = SnapshotSchedule::new(slot_count);
        for symbol in symbols {
            schedule.add(Arc::from(*symbol));
        }
        schedule
    }

    /// How often each symbol is due over `ticks` ticks.
    fn due_counts(schedule: &mut SnapshotSchedule, ticks: usize) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for _ in 0..ticks {
            for symbol in schedule.next_due() {
                *counts.entry(symbol.to_string()).or_insert(0) += 1;
            }
        }
        counts
    }

    #[test]
    fn every_book_is_due_once_per_cycle() {
        let symbols = ["A", "B", "C", "D", "E", "F", "G"];
        let mut schedule = schedule_with(3, &symbols);
        let counts = due_counts(&mut schedule, 3);
        let expected: BTreeMap<_, _> = symbols.iter().map(|s| (s.to_string(), 1)).collect();
        assert_eq!(counts, expected);
    }

    #[test]
    fn slots_are_assigned_round_robin() {
        let mut schedule = SnapshotSchedule::new(3);
        let slots: Vec<_> = ["A", "B", "C", "D"]
            .into_iter()
            .map(|symbol| schedule.add(Arc::from(symbol)))
            .collect();
        assert_eq!(slots, [0, 1, 2, 0]);
    }

    #[test]
    fn a_removed_book_is_never_due() {
        let mut schedule = schedule_with(2, &["A", "B", "C"]);
        schedule.remove("C", 0);
        let counts = due_counts(&mut schedule, 4);
        let expected = BTreeMap::from([("A".to_owned(), 2), ("B".to_owned(), 2)]);
        assert_eq!(counts, expected);
    }

    #[test]
    fn zero_slots_are_rejected() {
        let (requests, _) = mpsc::channel(1);
        let config = WriterConfig {
            snapshot_slots: 0,
            ..WriterConfig::new("/dev/null", requests)
        };
        assert!(spawn_writer(config).is_err());
    }
}
