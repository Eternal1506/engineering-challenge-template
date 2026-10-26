//! The writer thread, end to end: messages in, the SBE file read back.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{
    Recorded, ask, at, bid, feed_level, feed_snapshot, level, read_recording, run_writer,
    snapshot_message, snapshot_sides, stamped, update_message,
};
use engineering_challenge::Side;
use engineering_challenge::core::events::QuantityChange::{Absolute, Delta};
use engineering_challenge::core::writer::{
    CHUNK_SIZE, WriterConfig, WriterMessage, WriterStats, spawn_writer,
};
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotDomain,
};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;

const UNIT: i64 = 1_000_000;

fn forget(symbol: &str) -> WriterMessage {
    WriterMessage::Forget(Arc::from(symbol))
}

/// The snapshots in `messages` that the writer made itself, rather than
/// from a feed snapshot: those received at or after `since`.
fn own_snapshots(messages: &[Recorded], since: DateTime<Utc>) -> Vec<&SnapshotDomain> {
    messages
        .iter()
        .filter_map(|message| match message {
            Recorded::Snapshot(snapshot) if snapshot.received_at >= since => Some(snapshot),
            _ => None,
        })
        .collect()
}

#[test]
fn events_are_written_as_snapshots_and_absolute_updates() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let a_bids = [(400_000, 5 * UNIT), (300_000, UNIT)];
    let messages = vec![
        stamped(
            feed_snapshot("A", &a_bids, &[(600_000, 2 * UNIT)]),
            1000,
            Some(990),
        ),
        stamped(
            feed_level("A", bid(400_000, Delta(2 * UNIT))),
            1001,
            Some(995),
        ),
        stamped(feed_level("A", ask(600_000, Delta(-2 * UNIT))), 1002, None),
        stamped(feed_snapshot("B", &[], &[(700_000, UNIT)]), 1003, None),
        stamped(
            feed_level("B", ask(650_000, Absolute(3 * UNIT))),
            1004,
            Some(1002),
        ),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let expected = vec![
        snapshot_message("A", (1000, 990), (&a_bids, &[(600_000, 2 * UNIT)])),
        update_message("A", (1001, 995), (Side::Buy, 400_000, 7 * UNIT)),
        update_message("A", (1002, 995), (Side::Sell, 600_000, 0)),
        snapshot_message("B", (1003, 1003), (&[], &[(700_000, UNIT)])),
        update_message("B", (1004, 1002), (Side::Sell, 650_000, 3 * UNIT)),
    ];
    assert_eq!(run.messages, expected);
    let stats = WriterStats {
        events: 5,
        snapshots_written: 2,
        updates_written: 3,
        ..WriterStats::default()
    };
    assert_eq!(run.stats, stats);
    Ok(())
}

#[test]
fn a_level_change_before_the_first_snapshot_is_dropped_and_counted() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let messages = vec![
        stamped(feed_level("A", bid(400_000, Delta(UNIT))), 1, Some(1)),
        stamped(feed_snapshot("A", &[(400_000, UNIT)], &[]), 2, Some(2)),
        stamped(feed_level("A", bid(400_000, Delta(UNIT))), 3, Some(3)),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let expected = vec![
        snapshot_message("A", (2, 2), (&[(400_000, UNIT)], &[])),
        update_message("A", (3, 3), (Side::Buy, 400_000, 2 * UNIT)),
    ];
    assert_eq!(run.messages, expected);
    let stats = run.stats;
    assert_eq!((stats.events, stats.dropped_before_snapshot), (3, 1));
    Ok(())
}

#[test]
fn an_inconsistent_change_requests_one_resnapshot_until_the_next_snapshot() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let snapshot = feed_snapshot("A", &[(400_000, UNIT)], &[]);
    let overdraw = |price| feed_level("A", bid(price, Delta(-2 * UNIT)));
    let messages = vec![
        stamped(snapshot.clone(), 1, None),
        stamped(overdraw(400_000), 2, None),
        stamped(overdraw(300_000), 3, None),
        stamped(snapshot, 4, None),
        stamped(overdraw(400_000), 5, None),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let last = run.messages.last().cloned();
    let emptied = update_message("A", (5, 5), (Side::Buy, 400_000, 0));
    assert_eq!(last, Some(emptied));
    let requested: Vec<&str> = run.resnapshot_requests.iter().map(|s| &**s).collect();
    assert_eq!(requested, ["A", "A"]);
    assert_eq!(run.stats.inconsistent, 3);
    Ok(())
}

#[test]
fn a_full_resnapshot_channel_counts_the_dropped_request() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let (requests, _receiver) = mpsc::channel(1);
    requests.try_send(Arc::from("X"))?;
    let overdraw = feed_level("A", bid(400_000, Delta(-2 * UNIT)));
    let messages = vec![
        stamped(feed_snapshot("A", &[(400_000, UNIT)], &[]), 1, None),
        stamped(overdraw, 2, None),
    ];
    let run = run_writer(output.path(), messages, |config| {
        config.resnapshot_requests = requests;
    })?;
    let dropped = run.stats.resnapshot_requests_dropped;
    assert_eq!((run.stats.inconsistent, dropped), (1, 1));
    Ok(())
}

#[test]
fn forget_writes_a_final_snapshot_and_drops_the_book() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let started = Utc::now();
    let snapshot = feed_snapshot("A", &[(400_000, UNIT)], &[(600_000, UNIT)]);
    let messages = vec![
        stamped(snapshot, 1, Some(1)),
        stamped(feed_level("A", bid(450_000, Delta(UNIT))), 2, Some(2)),
        forget("A"),
        stamped(feed_level("A", bid(450_000, Delta(UNIT))), 3, Some(3)),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let finals = own_snapshots(&run.messages, started);
    let [last] = finals.as_slice() else {
        anyhow::bail!("expected one final snapshot, got {finals:?}");
    };
    let bids = vec![(450_000, UNIT), (400_000, UNIT)];
    assert_eq!(snapshot_sides(last), (bids, vec![(600_000, UNIT)]));
    assert_eq!(last.exchange_timestamp, at(2));
    let dropped = run.stats.dropped_before_snapshot;
    assert_eq!((run.messages.len(), dropped), (3, 1));
    Ok(())
}

#[test]
fn forgetting_an_unknown_symbol_writes_nothing() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let run = run_writer(output.path(), vec![forget("A")], |_| {})?;
    assert_eq!((run.messages, run.stats), (vec![], WriterStats::default()));
    Ok(())
}

#[test]
fn shutdown_flushes_the_final_chunk() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let snapshot = feed_snapshot("A", &[(400_000, UNIT)], &[]);
    let run = run_writer(output.path(), vec![stamped(snapshot, 1, None)], |_| {})?;
    let length = std::fs::metadata(output.path())?.len();
    assert_eq!((length, run.messages.len()), (CHUNK_SIZE as u64, 1));
    Ok(())
}

#[test]
fn output_spanning_two_chunks_reads_back_completely() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let bids: Vec<_> = (1..=10_000).map(|price| (price, UNIT)).collect();
    let messages: Vec<_> = (0..450)
        .map(|index| stamped(feed_snapshot("A", &bids, &[]), index, None))
        .collect();
    let run = run_writer(output.path(), messages, |_| {})?;
    let length = std::fs::metadata(output.path())?.len();
    assert_eq!(length, 2 * CHUNK_SIZE as u64);
    let written = run.stats.snapshots_written;
    assert_eq!((run.messages.len(), written), (450, 450));
    Ok(())
}

#[test]
fn flush_interval_writes_the_chunk_while_running() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let (requests, _receiver) = mpsc::channel(1);
    let mut config = WriterConfig::new(output.path(), requests);
    config.flush_interval = Some(Duration::from_millis(20));
    let (sender, handle) = spawn_writer(config)?;
    sender.blocking_send(stamped(feed_snapshot("A", &[(1, 1)], &[]), 1, None))?;
    let length_while_running = wait_for_length(output.path(), CHUNK_SIZE as u64)?;
    drop(sender);
    handle.join()?;
    assert_eq!(length_while_running, CHUNK_SIZE as u64);
    Ok(())
}

/// Polls the length of the file at `path` until it reaches `expected`, for
/// up to 10 s, and returns the last length seen. Writing a 64 MiB chunk
/// takes a while in a debug build.
fn wait_for_length(path: &std::path::Path, expected: u64) -> anyhow::Result<u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let length = std::fs::metadata(path)?.len();
        if length >= expected || std::time::Instant::now() > deadline {
            return Ok(length);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn an_output_write_error_fails_the_thread() -> anyhow::Result<()> {
    let (requests, _receiver) = mpsc::channel(1);
    let (sender, handle) = spawn_writer(WriterConfig::new("/dev/full", requests))?;
    sender.blocking_send(stamped(feed_snapshot("A", &[(1, 1)], &[]), 1, None))?;
    drop(sender);
    let error = format!("{:#}", handle.join().expect_err("the write succeeded"));
    assert!(error.contains("flushing the output"), "{error}");
    Ok(())
}

#[test]
fn an_uncreatable_output_fails_to_spawn() {
    let (requests, _receiver) = mpsc::channel(1);
    let config = WriterConfig::new("/nonexistent-directory/out.sbe", requests);
    assert!(spawn_writer(config).is_err());
}

#[test]
fn staggered_snapshots_cover_every_book_each_cycle() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let started = Utc::now();
    let symbols = ["A", "B", "C", "D", "E", "F", "G", "H"];
    let messages = run_staggered(output.path(), &symbols)?;
    let counts = count_by_symbol(&own_snapshots(&messages, started));
    let fewest = counts.values().copied().min().unwrap_or(0);
    let most = counts.values().copied().max().unwrap_or(0);
    assert_eq!(counts.len(), symbols.len(), "{counts:?}");
    assert!(fewest >= 5 && most <= fewest + 1, "{counts:?}");
    Ok(())
}

/// Starts the writer with a 20 ms tick and 4 slots, gives it a snapshot of
/// each symbol, lets it run for 50 ticks and returns its output.
fn run_staggered(output: &std::path::Path, symbols: &[&str]) -> anyhow::Result<Vec<Recorded>> {
    let (requests, _receiver) = mpsc::channel(1);
    let mut config = WriterConfig::new(output, requests);
    config.snapshot_tick = Duration::from_millis(20);
    config.snapshot_slots = 4;
    let (sender, handle) = spawn_writer(config)?;
    for symbol in symbols {
        sender.blocking_send(stamped(feed_snapshot(symbol, &[(1, 1)], &[]), 0, None))?;
    }
    std::thread::sleep(Duration::from_millis(1000));
    drop(sender);
    handle.join()?;
    read_recording(output)
}

fn count_by_symbol(snapshots: &[&SnapshotDomain]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for snapshot in snapshots {
        *counts.entry(snapshot.symbol.clone()).or_insert(0) += 1;
    }
    counts
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

/// One random message for event number `index`.
fn random_message(rng: &mut Rng, index: i64) -> WriterMessage {
    let symbol = SYMBOLS[rng.below(SYMBOLS.len() as u64) as usize];
    let exchange = (rng.below(2) == 0).then_some(index);
    let (side, price) = (random_side(rng), (1 + rng.below(10)) * 50_000);
    let event = match rng.below(100) {
        0..3 => return forget(symbol),
        3..8 => feed_snapshot(symbol, &random_levels(rng), &random_levels(rng)),
        8..15 => feed_level(symbol, level(side, price, Absolute(rng.below(5) * UNIT))),
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

/// Sends `count` random messages, pausing now and then so periodic
/// snapshots interleave with them, then forgets every symbol.
fn send_random_messages(sender: &mpsc::Sender<WriterMessage>, count: i64) -> anyhow::Result<()> {
    let mut rng = Rng(7);
    for index in 0..count {
        sender.blocking_send(random_message(&mut rng, index))?;
        if index % 25 == 0 {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    for symbol in SYMBOLS {
        sender.blocking_send(forget(symbol))?;
    }
    Ok(())
}

#[test]
fn periodic_and_final_snapshots_match_books_rebuilt_from_the_output() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let started = Utc::now();
    let (requests, _receiver) = mpsc::channel(4096);
    let mut config = WriterConfig::new(output.path(), requests);
    config.snapshot_tick = Duration::from_millis(1);
    config.snapshot_slots = 3;
    let (sender, handle) = spawn_writer(config)?;
    send_random_messages(&sender, 3000)?;
    drop(sender);
    handle.join()?;
    let mut books = RebuiltBooks::default();
    let mut checked = 0;
    for message in read_recording(output.path())? {
        match message {
            Recorded::Snapshot(snapshot) if snapshot.received_at >= started => {
                assert!(books.matches(&snapshot), "mismatch: {snapshot:?}");
                checked += 1;
            }
            Recorded::Snapshot(snapshot) => books.replace(&snapshot),
            Recorded::Update(update) => books.apply(&update),
        }
    }
    assert!(checked > 50, "only {checked} snapshots were checked");
    Ok(())
}
