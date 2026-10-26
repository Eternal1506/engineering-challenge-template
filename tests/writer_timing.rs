//! The writer's timers: periodic flushes, staggered snapshots, and snapshots
//! interleaved with a flood of events. Ticks follow the wall clock, but every
//! assertion is on the output's stream times.

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use common::{
    Recorded, feed_level, feed_snapshot, forget, level, read_recording, run_live, snapshot_sides,
    stamped, track, verify_clean,
};
use engineering_challenge::Side;
use engineering_challenge::core::events::FeedBody;
use engineering_challenge::core::events::QuantityChange::{Absolute, Delta};
use engineering_challenge::core::writer::{CHUNK_SIZE, WriterConfig, WriterMessage};
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotDomain,
};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;

const UNIT: i64 = 1_000_000;

/// A writer configuration change: snapshot ticks of `tick_ms` and `slots`
/// slots per cycle.
fn ticks(tick_ms: u64, slots: usize) -> impl FnOnce(&mut WriterConfig) {
    move |config| {
        config.snapshot_tick = Duration::from_millis(tick_ms);
        config.snapshot_slots = slots;
    }
}

#[test]
fn flush_interval_writes_the_chunk_while_running() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let with_flush = |config: &mut WriterConfig| {
        config.flush_interval = Some(Duration::from_millis(20));
    };
    let mut length_while_running = 0;
    run_live(output.path(), with_flush, |sender| {
        sender.blocking_send(stamped(feed_snapshot("A", &[(1, 1)], &[]), 1, None))?;
        length_while_running = wait_for_length(output.path(), CHUNK_SIZE as u64)?;
        Ok(())
    })?;
    assert_eq!(length_while_running, CHUNK_SIZE as u64);
    Ok(())
}

/// Polls the length of the file at `path` until it reaches `expected`, for
/// up to 10 s, and returns the last length seen. Writing a 64 MiB chunk
/// takes a while in a debug build.
fn wait_for_length(path: &Path, expected: u64) -> anyhow::Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let length = std::fs::metadata(path)?.len();
        if length >= expected || Instant::now() > deadline {
            return Ok(length);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Each symbol's snapshot receive times, in output order.
fn snapshot_times(messages: &[Recorded]) -> BTreeMap<String, Vec<DateTime<Utc>>> {
    let mut times: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for message in messages {
        if let Recorded::Snapshot(snapshot) = message {
            let symbol = snapshot.symbol.clone();
            times.entry(symbol).or_default().push(snapshot.received_at);
        }
    }
    times
}

/// Sends a snapshot of each symbol, then for `run_for` keeps the stream
/// clock moving with dropped level changes stamped with the elapsed time.
fn send_with_moving_clock(
    sender: &mpsc::Sender<WriterMessage>,
    symbols: &[&str],
    run_for: Duration,
) -> anyhow::Result<()> {
    for symbol in symbols {
        sender.blocking_send(stamped(feed_snapshot(symbol, &[(1, 1)], &[]), 0, None))?;
    }
    let started = Instant::now();
    while started.elapsed() < run_for {
        let elapsed = started.elapsed().as_millis() as i64;
        let tick = feed_level("CLOCK", level(Side::Buy, 1, Delta(1)));
        sender.blocking_send(stamped(tick, elapsed, None))?;
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

#[test]
fn staggered_snapshots_cover_every_book_each_cycle_in_stream_time() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let symbols = ["A", "B", "C", "D", "E", "F", "G", "H"];
    run_live(output.path(), ticks(10, 4), |sender| {
        send_with_moving_clock(sender, &symbols, Duration::from_secs(1))
    })?;
    let times = snapshot_times(&read_recording(output.path())?);
    let counts: Vec<_> = times.values().map(Vec::len).collect();
    let (fewest, most) = (counts.iter().min(), counts.iter().max());
    assert_eq!(times.len(), symbols.len(), "{times:?}");
    assert!(fewest >= Some(&5) && most <= fewest.map(|n| n + 1).as_ref());
    let widest_gap = times
        .values()
        .flat_map(|times| times.windows(2).map(|pair| pair[1] - pair[0]))
        .max();
    let cycle_with_tolerance = Some(chrono::TimeDelta::milliseconds(40 + 160));
    assert!(widest_gap <= cycle_with_tolerance, "{widest_gap:?}");
    Ok(())
}

#[test]
fn snapshots_amid_a_flood_of_queued_events_keep_time_moving_forward() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let symbols: Vec<String> = (0..20).map(|n| format!("S{n}")).collect();
    let (stats, _) = run_live(output.path(), ticks(1, 1), |sender| {
        send_flood(sender, &symbols)
    })?;
    verify_clean(output.path())?;
    let feed_snapshots = symbols.len() as u64;
    assert!(stats.snapshots_written > 2 * feed_snapshots, "{stats:?}");
    Ok(())
}

/// A snapshot of each symbol, then 20,000 level changes across them. Each
/// symbol's receive times rise, but symbol `n` lags the others by `3n` ms,
/// as if it arrived on a slower connection, so many events arrive stamped
/// before the writer's clock.
fn send_flood(sender: &mpsc::Sender<WriterMessage>, symbols: &[String]) -> anyhow::Result<()> {
    for symbol in symbols {
        let snapshot = feed_snapshot(symbol, &[(500_000, UNIT)], &[]);
        sender.blocking_send(stamped(snapshot, 0, None))?;
    }
    for index in 0..20_000_i64 {
        let lag = index % symbols.len() as i64;
        let symbol = &symbols[lag as usize];
        let change = feed_level(symbol, level(Side::Buy, 500_000, Delta(1)));
        sender.blocking_send(stamped(change, 100 + index - 3 * lag, None))?;
        if index % 500 == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    Ok(())
}

/// SplitMix64: a small, seeded, deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..bound`.
    fn below(&mut self, bound: u64) -> i64 {
        (self.next() % bound) as i64
    }
}

const SYMBOLS: [&str; 4] = ["A", "B", "C", "D"];

fn random_side(rng: &mut Rng) -> Side {
    if rng.below(2) == 0 {
        Side::Buy
    } else {
        Side::Sell
    }
}

fn random_levels(rng: &mut Rng) -> Vec<(i64, i64)> {
    (0..rng.below(6))
        .map(|_| ((1 + rng.below(10)) * 50_000, rng.below(5) * UNIT))
        .collect()
}

/// One random message for event number `index`: mostly level changes, with
/// some snapshots, forgets and tracks.
fn random_message(rng: &mut Rng, index: i64) -> WriterMessage {
    let symbol = SYMBOLS[rng.below(SYMBOLS.len() as u64) as usize];
    let exchange = (rng.below(2) == 0).then_some(index);
    let (side, price) = (random_side(rng), (1 + rng.below(10)) * 50_000);
    let event = match rng.below(100) {
        0..3 => return forget(symbol),
        3..6 => return track(symbol),
        6..11 => feed_snapshot(symbol, &random_levels(rng), &random_levels(rng)),
        11..18 => feed_level(symbol, level(side, price, Absolute(rng.below(5) * UNIT))),
        _ => feed_level(symbol, level(side, price, Delta((rng.below(7) - 3) * UNIT))),
    };
    stamped(event, index, exchange)
}

/// One side of a book: quantity by price.
type PriceLevels = BTreeMap<i64, i64>;

/// Each symbol's book as rebuilt from the output messages alone.
#[derive(Default)]
struct RebuiltBooks(HashMap<String, (PriceLevels, PriceLevels)>);

impl RebuiltBooks {
    fn replace(&mut self, snapshot: &SnapshotDomain) {
        let symbol = snapshot.symbol.clone();
        self.0.insert(symbol, levels_by_price(snapshot));
    }

    fn matches(&self, snapshot: &SnapshotDomain) -> bool {
        self.0.get(&snapshot.symbol) == Some(&levels_by_price(snapshot))
    }

    fn apply(&mut self, update: &PriceLevelUpdateDomain) {
        let (bids, asks) = self.0.entry(update.symbol.clone()).or_default();
        let levels = if update.side == Side::Buy { bids } else { asks };
        if update.quantity == 0 {
            levels.remove(&update.price_level);
        } else {
            levels.insert(update.price_level, update.quantity);
        }
    }
}

fn levels_by_price(snapshot: &SnapshotDomain) -> (PriceLevels, PriceLevels) {
    let (bids, asks) = snapshot_sides(snapshot);
    (bids.into_iter().collect(), asks.into_iter().collect())
}

/// The random input: `count` messages, then a forget of every symbol.
fn random_messages(count: i64) -> Vec<WriterMessage> {
    let mut rng = Rng(7);
    let mut messages: Vec<_> = (0..count).map(|i| random_message(&mut rng, i)).collect();
    messages.extend(SYMBOLS.map(forget));
    messages
}

/// The symbol and receive time of every feed snapshot in `messages`. Feed
/// snapshots come in at distinct times; a writer-made snapshot never comes
/// before the feed snapshot it may share a time with.
fn feed_snapshot_keys(messages: &[WriterMessage]) -> HashSet<(String, DateTime<Utc>)> {
    let keys = messages.iter().filter_map(|message| match message {
        WriterMessage::Event(event) if matches!(event.body, FeedBody::Snapshot { .. }) => {
            Some((event.symbol.to_string(), event.received_at))
        }
        _ => None,
    });
    keys.collect()
}

/// Replays the output, checking every writer-made snapshot against the book
/// rebuilt from the messages before it. Returns how many were checked.
fn check_against_rebuilt_books(
    output: &[Recorded],
    mut feed_snapshots: HashSet<(String, DateTime<Utc>)>,
) -> usize {
    let mut books = RebuiltBooks::default();
    let mut checked = 0;
    for message in output {
        match message {
            Recorded::Snapshot(snapshot) => {
                let key = (snapshot.symbol.clone(), snapshot.received_at);
                if feed_snapshots.remove(&key) {
                    books.replace(snapshot);
                } else {
                    assert!(books.matches(snapshot), "mismatch: {snapshot:?}");
                    checked += 1;
                }
            }
            Recorded::Update(update) => books.apply(update),
        }
    }
    checked
}

#[test]
fn writer_made_snapshots_match_books_rebuilt_from_the_output() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let messages = random_messages(3000);
    let feed_snapshots = feed_snapshot_keys(&messages);
    run_live(output.path(), ticks(1, 3), |sender| {
        for (index, message) in messages.into_iter().enumerate() {
            sender.blocking_send(message)?;
            if index % 25 == 0 {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        Ok(())
    })?;
    let recorded = read_recording(output.path())?;
    let checked = check_against_rebuilt_books(&recorded, feed_snapshots);
    assert!(checked > 50, "only {checked} snapshots were checked");
    verify_clean(output.path())?;
    Ok(())
}
