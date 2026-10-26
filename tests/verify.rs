//! `verify`: recordings from the writer pass, and each crafted bad recording
//! fails with its own violation kind.

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use common::{
    Recorded, ask, bid, feed_level, feed_snapshot, run_writer, snapshot_message, stamped,
    update_message, write_recording,
};
use engineering_challenge::Side;
use engineering_challenge::core::events::QuantityChange::Delta;
use engineering_challenge::core::verify::{
    VerifyLimits, VerifyReport, ViolationKind, verify_files,
};
use engineering_challenge::core::writer::WriterMessage;
use tempfile::NamedTempFile;

const UNIT: i64 = 1_000_000;

/// The messages of one recorder run starting at `start` milliseconds: a
/// snapshot of each of two books, then updates to both.
fn run_messages(start: i64) -> Vec<WriterMessage> {
    let events = [
        feed_snapshot("A", &[(400_000, UNIT)], &[(600_000, UNIT)]),
        feed_snapshot("B", &[], &[(700_000, UNIT)]),
        feed_level("A", bid(400_000, Delta(UNIT))),
        feed_level("B", ask(700_000, Delta(-UNIT))),
        feed_level("A", ask(650_000, Delta(UNIT))),
    ];
    let exchange_times = [None, Some(start), Some(start + 1), None, None];
    (start..)
        .zip(events.into_iter().zip(exchange_times))
        .map(|(received, (event, exchange))| stamped(event, received, exchange))
        .collect()
}

/// A recording written by the writer for one run starting at `start`.
fn writer_recording(start: i64) -> anyhow::Result<NamedTempFile> {
    let file = NamedTempFile::new()?;
    run_writer(file.path(), run_messages(start), |_| {})?;
    Ok(file)
}

/// A recording of exactly `messages`.
fn crafted(messages: &[Recorded]) -> anyhow::Result<NamedTempFile> {
    let file = NamedTempFile::new()?;
    write_recording(file.path(), messages)?;
    Ok(file)
}

fn verify(files: &[&NamedTempFile]) -> anyhow::Result<VerifyReport> {
    let paths: Vec<PathBuf> = files.iter().map(|file| file.path().to_owned()).collect();
    verify_files(&paths, &VerifyLimits::default())
}

fn kinds(report: &VerifyReport) -> Vec<(ViolationKind, u64)> {
    let tallies = report.violations.iter();
    tallies.map(|(&kind, tally)| (kind, tally.count)).collect()
}

fn snapshot(symbol: &str, at: i64) -> Recorded {
    snapshot_message(symbol, (at, at), (&[(400_000, UNIT)], &[(600_000, UNIT)]))
}

fn update(symbol: &str, at: i64, price: i64, quantity: i64) -> Recorded {
    update_message(symbol, (at, at), (Side::Buy, price, quantity))
}

#[test]
fn a_writer_recording_verifies_clean() -> anyhow::Result<()> {
    let file = writer_recording(0)?;
    let report = verify(&[&file])?;
    assert!(report.is_clean(), "{report}");
    let counts = (report.snapshots, report.updates, report.symbols);
    assert_eq!((report.files, counts), (1, (2, 3, 2)));
    assert_eq!(report.violation_count(), 0);
    Ok(())
}

#[test]
fn two_consecutive_runs_verify_clean_as_two_paths() -> anyhow::Result<()> {
    let (first, second) = (writer_recording(0)?, writer_recording(30_000)?);
    let report = verify(&[&first, &second])?;
    assert!(report.is_clean(), "{report}");
    assert_eq!((report.files, report.snapshots, report.updates), (2, 4, 6));
    Ok(())
}

#[test]
fn two_consecutive_runs_verify_clean_concatenated() -> anyhow::Result<()> {
    let (first, second) = (writer_recording(0)?, writer_recording(30_000)?);
    let report = verify(&[&concatenated(&[&first, &second])?])?;
    assert!(report.is_clean(), "{report}");
    assert_eq!((report.files, report.snapshots, report.updates), (1, 4, 6));
    Ok(())
}

/// The bytes of `files` joined into one file, as `cat` would.
fn concatenated(files: &[&NamedTempFile]) -> anyhow::Result<NamedTempFile> {
    let joined = NamedTempFile::new()?;
    let mut output = joined.reopen()?;
    for file in files {
        output.write_all(&std::fs::read(file.path())?)?;
    }
    Ok(joined)
}

#[test]
fn a_restart_after_70_s_passes_as_two_paths_but_is_a_gap_concatenated() -> anyhow::Result<()> {
    let (first, restarted) = (writer_recording(0)?, writer_recording(70_000)?);
    assert!(verify(&[&first, &restarted])?.is_clean());
    let report = verify(&[&concatenated(&[&first, &restarted])?])?;
    assert_eq!(kinds(&report), [(ViolationKind::SnapshotGap, 2)]);
    Ok(())
}

#[test]
fn an_update_before_any_snapshot_is_reported() -> anyhow::Result<()> {
    let file = crafted(&[update("A", 0, 400_000, UNIT), snapshot("A", 1)])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::UpdateBeforeSnapshot, 1)]);
    Ok(())
}

#[test]
fn each_file_must_start_a_symbol_with_a_snapshot() -> anyhow::Result<()> {
    let first = crafted(&[snapshot("A", 0)])?;
    let second = crafted(&[update("A", 10, 400_000, UNIT)])?;
    let report = verify(&[&first, &second])?;
    assert_eq!(kinds(&report), [(ViolationKind::UpdateBeforeSnapshot, 1)]);
    Ok(())
}

#[test]
fn snapshots_too_far_apart_are_reported() -> anyhow::Result<()> {
    let file = crafted(&[snapshot("A", 0), snapshot("A", 65_001)])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::SnapshotGap, 1)]);
    Ok(())
}

#[test]
fn snapshots_exactly_the_limit_apart_pass() -> anyhow::Result<()> {
    let file = crafted(&[snapshot("A", 0), snapshot("A", 65_000)])?;
    assert!(verify(&[&file])?.is_clean());
    Ok(())
}

#[test]
fn updates_running_past_the_gap_without_a_snapshot_are_reported() -> anyhow::Result<()> {
    let file = crafted(&[
        snapshot("A", 0),
        update("A", 10, 400_000, UNIT),
        snapshot("B", 70_000),
    ])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::SnapshotGap, 1)]);
    Ok(())
}

#[test]
fn a_symbol_that_ends_with_its_final_snapshot_has_no_gap() -> anyhow::Result<()> {
    let file = crafted(&[
        snapshot("A", 0),
        snapshot("B", 0),
        update("A", 10, 400_000, UNIT),
        snapshot("A", 20),
        snapshot("B", 60_000),
        snapshot("B", 120_000),
    ])?;
    let report = verify(&[&file])?;
    assert!(report.is_clean(), "{report}");
    Ok(())
}

#[test]
fn the_gap_limit_is_configurable() -> anyhow::Result<()> {
    let file = crafted(&[snapshot("A", 0), snapshot("A", 20_000)])?;
    let limits = VerifyLimits {
        max_snapshot_gap: std::time::Duration::from_secs(10),
        ..VerifyLimits::default()
    };
    let report = verify_files(&[file.path().to_owned()], &limits)?;
    assert_eq!(kinds(&report), [(ViolationKind::SnapshotGap, 1)]);
    Ok(())
}

#[test]
fn time_going_backwards_is_reported() -> anyhow::Result<()> {
    let file = crafted(&[snapshot("A", 10), update("A", 5, 400_000, UNIT)])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::TimeWentBackwards, 1)]);
    Ok(())
}

#[test]
fn the_report_counts_symbols_going_backwards_and_the_largest_step() -> anyhow::Result<()> {
    let file = crafted(&[
        snapshot("A", 10),
        update("A", 5, 400_000, UNIT),
        update("A", 4, 400_000, UNIT),
        snapshot("B", 100),
        update("B", 98, 400_000, UNIT),
    ])?;
    let report = verify(&[&file])?;
    let count = report.count(ViolationKind::TimeWentBackwards);
    let largest_step = report.largest_step_back.num_milliseconds();
    let summary = (report.symbols_going_backwards, largest_step);
    assert_eq!((count, summary), (3, (2, 6)));
    let summary_line = "(2 symbols, largest step back 0.006 s)";
    assert!(report.to_string().contains(summary_line), "{report}");
    Ok(())
}

#[test]
fn time_going_backwards_across_files_is_reported() -> anyhow::Result<()> {
    let first = crafted(&[snapshot("A", 100)])?;
    let second = crafted(&[snapshot("A", 50)])?;
    let report = verify(&[&first, &second])?;
    assert_eq!(kinds(&report), [(ViolationKind::TimeWentBackwards, 1)]);
    Ok(())
}

#[test]
fn a_negative_quantity_is_reported() -> anyhow::Result<()> {
    let file = crafted(&[snapshot("A", 0), update("A", 1, 400_000, -1)])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::NegativeQuantity, 1)]);
    Ok(())
}

#[test]
fn prices_outside_the_range_are_reported() -> anyhow::Result<()> {
    let bad_bid = snapshot_message("A", (0, 0), (&[(-1, UNIT)], &[]));
    let file = crafted(&[
        bad_bid,
        update("A", 1, 1_000_001, UNIT),
        update("A", 2, 0, UNIT),
    ])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::PriceOutOfRange, 3)]);
    Ok(())
}

#[test]
fn prices_just_inside_the_range_pass() -> anyhow::Result<()> {
    let file = crafted(&[
        snapshot("A", 0),
        update("A", 1, 1, UNIT),
        update("A", 2, 1_000_000, UNIT),
    ])?;
    assert!(verify(&[&file])?.is_clean());
    Ok(())
}

#[test]
fn an_empty_recording_is_an_error() -> anyhow::Result<()> {
    let empty = NamedTempFile::new()?;
    assert!(verify(&[&empty]).is_err());
    Ok(())
}

#[test]
fn unsorted_snapshot_sides_are_reported() -> anyhow::Result<()> {
    let bids = [(300_000, UNIT), (400_000, UNIT)];
    let asks = [(700_000, UNIT), (700_000, UNIT)];
    let file = crafted(&[snapshot_message("A", (0, 0), (&bids, &asks))])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::UnsortedSnapshotSide, 2)]);
    Ok(())
}

#[test]
fn a_zero_quantity_snapshot_level_is_reported() -> anyhow::Result<()> {
    let file = crafted(&[snapshot_message("A", (0, 0), (&[(400_000, 0)], &[]))])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::ZeroQuantityLevel, 1)]);
    Ok(())
}

#[test]
fn a_partial_trailing_chunk_is_reported() -> anyhow::Result<()> {
    let file = writer_recording(0)?;
    let mut appended = std::fs::OpenOptions::new().append(true).open(file.path())?;
    appended.write_all(&[1; 10])?;
    let report = verify(&[&file])?;
    assert_eq!(kinds(&report), [(ViolationKind::PartialChunk, 1)]);
    Ok(())
}

#[test]
fn the_report_keeps_five_examples_per_kind() -> anyhow::Result<()> {
    let updates: Vec<_> = (0..7).map(|at| update("A", at, 400_000, UNIT)).collect();
    let report = verify(&[&crafted(&updates)?])?;
    let tally = &report.violations[&ViolationKind::UpdateBeforeSnapshot];
    assert_eq!((tally.count, tally.examples.len()), (7, 5));
    Ok(())
}

#[test]
fn the_summary_names_counts_and_examples() -> anyhow::Result<()> {
    let report = verify(&[&crafted(&[update("KX-1", 0, 400_000, UNIT)])?])?;
    let summary = report.to_string();
    let expected = [
        "0 snapshots, 1 updates, 1 symbols",
        "violations: 1",
        "update before snapshot: 1",
        "KX-1 at",
    ];
    let missing: Vec<_> = expected
        .iter()
        .filter(|text| !summary.contains(*text))
        .collect();
    assert!(missing.is_empty(), "{missing:?} missing from:\n{summary}");
    Ok(())
}

/// Runs the binary's `verify` on `paths`; returns whether it succeeded and
/// its stderr.
fn run_verify_command(paths: &[&Path]) -> anyhow::Result<(bool, String)> {
    let output = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
        .arg("verify")
        .args(paths)
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    Ok((output.status.success(), stderr))
}

#[test]
fn the_verify_command_succeeds_on_a_clean_recording() -> anyhow::Result<()> {
    let (first, second) = (writer_recording(0)?, writer_recording(30_000)?);
    let (succeeded, stderr) = run_verify_command(&[first.path(), second.path()])?;
    assert!(succeeded && stderr.contains("violations: none"), "{stderr}");
    Ok(())
}

#[test]
fn the_verify_command_fails_on_a_violation() -> anyhow::Result<()> {
    let file = crafted(&[update("A", 0, 400_000, UNIT)])?;
    let (succeeded, stderr) = run_verify_command(&[file.path()])?;
    let reported = stderr.contains("update before snapshot: 1");
    assert!(!succeeded && reported, "{stderr}");
    Ok(())
}
