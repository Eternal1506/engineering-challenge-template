//! The writer thread, end to end: messages in, the SBE file read back. The
//! tests here do not depend on timer ticks; see `writer_timing.rs` for those
//! that do.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    ask, at, bid, feed_level, feed_snapshot, forget, run_live, run_writer, snapshot_message,
    stamped, track, update_message,
};
use engineering_challenge::Side;
use engineering_challenge::core::events::QuantityChange::{Absolute, Delta};
use engineering_challenge::core::writer::{CHUNK_SIZE, WriterConfig, WriterStats, spawn_writer};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;

const UNIT: i64 = 1_000_000;

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
fn a_symbols_times_never_go_backwards() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let messages = vec![
        stamped(feed_snapshot("A", &[(400_000, UNIT)], &[]), 10, Some(100)),
        stamped(feed_level("A", bid(400_000, Delta(UNIT))), 5, Some(50)),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let late = update_message("A", (10, 100), (Side::Buy, 400_000, 2 * UNIT));
    assert_eq!(run.messages.last(), Some(&late));
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

/// The book may never have started because its snapshot was lost, so the
/// writer asks for one, but only once within the rate limit.
#[test]
fn level_changes_with_no_book_ask_for_one_snapshot_within_the_rate_limit() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let change = |received| stamped(feed_level("A", bid(400_000, Delta(UNIT))), received, None);
    let run = run_writer(output.path(), vec![change(1), change(2), change(3)], |_| {})?;
    let requested: Vec<&str> = run.resnapshot_requests.iter().map(|s| &**s).collect();
    let asked = run.stats.bookless_resnapshot_requests;
    let dropped = run.stats.dropped_before_snapshot;
    assert_eq!((requested, asked, dropped), (vec!["A"], 1, 3));
    Ok(())
}

#[test]
fn a_level_change_for_a_forgotten_symbol_asks_for_nothing() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let change = stamped(feed_level("A", bid(400_000, Delta(UNIT))), 1, None);
    let run = run_writer(output.path(), vec![forget("A"), change], |_| {})?;
    let stats = &run.stats;
    assert_eq!(
        (run.resnapshot_requests.len(), stats.dropped_forgotten),
        (0, 1)
    );
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
    let snapshot = feed_snapshot("A", &[(400_000, UNIT)], &[(600_000, UNIT)]);
    let messages = vec![
        stamped(snapshot, 1, Some(1)),
        stamped(feed_level("A", bid(450_000, Delta(UNIT))), 2, Some(2)),
        forget("A"),
        stamped(feed_level("A", bid(450_000, Delta(UNIT))), 3, Some(3)),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let final_bids = [(450_000, UNIT), (400_000, UNIT)];
    let expected = vec![
        snapshot_message("A", (1, 1), (&[(400_000, UNIT)], &[(600_000, UNIT)])),
        update_message("A", (2, 2), (Side::Buy, 450_000, UNIT)),
        snapshot_message("A", (2, 2), (&final_bids, &[(600_000, UNIT)])),
    ];
    assert_eq!(run.messages, expected);
    assert_eq!(run.stats.dropped_forgotten, 1);
    Ok(())
}

#[test]
fn the_final_snapshot_is_stamped_the_margin_behind_the_writer_clock() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let messages = vec![
        stamped(feed_snapshot("A", &[(400_000, UNIT)], &[]), 0, None),
        stamped(feed_snapshot("B", &[(400_000, UNIT)], &[]), 5000, None),
        forget("A"),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let final_snapshot = snapshot_message("A", (4000, 4000), (&[(400_000, UNIT)], &[]));
    assert_eq!(run.messages.last(), Some(&final_snapshot));
    Ok(())
}

#[test]
fn a_late_snapshot_after_forget_is_dropped_and_never_snapshotted() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let snapshot = feed_snapshot("A", &[(400_000, UNIT)], &[]);
    let messages = [
        stamped(snapshot.clone(), 1, None),
        forget("A"),
        stamped(snapshot, 3, None),
        stamped(feed_level("A", bid(400_000, Delta(UNIT))), 4, None),
    ];
    let fast_ticks = |config: &mut WriterConfig| {
        config.snapshot_tick = Duration::from_millis(1);
        config.snapshot_slots = 1;
    };
    let (stats, _) = run_live(output.path(), fast_ticks, |sender| {
        for message in messages {
            sender.blocking_send(message)?;
        }
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    })?;
    let recorded = common::read_recording(output.path())?;
    let latest = recorded.iter().map(common::received_at).max();
    assert_eq!((latest, stats.dropped_forgotten), (Some(at(1)), 2));
    Ok(())
}

#[test]
fn track_after_forget_restarts_the_book_from_its_next_snapshot() -> anyhow::Result<()> {
    let output = NamedTempFile::new()?;
    let snapshot = feed_snapshot("A", &[(400_000, UNIT)], &[]);
    let messages = vec![
        stamped(snapshot.clone(), 1, None),
        forget("A"),
        track("A"),
        stamped(feed_level("A", bid(400_000, Delta(UNIT))), 3, None),
        stamped(snapshot, 4, None),
        stamped(feed_level("A", bid(400_000, Delta(UNIT))), 5, None),
    ];
    let run = run_writer(output.path(), messages, |_| {})?;
    let expected = vec![
        snapshot_message("A", (1, 1), (&[(400_000, UNIT)], &[])),
        snapshot_message("A", (1, 1), (&[(400_000, UNIT)], &[])),
        snapshot_message("A", (4, 4), (&[(400_000, UNIT)], &[])),
        update_message("A", (5, 5), (Side::Buy, 400_000, 2 * UNIT)),
    ];
    assert_eq!(run.messages, expected);
    let dropped = run.stats.dropped_before_snapshot;
    assert_eq!((dropped, run.stats.dropped_forgotten), (1, 0));
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
