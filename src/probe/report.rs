//! Prints the probe's findings to stderr.

use std::time::Duration;

use super::books::Comparison;
use super::findings::{Conn, SecondTotals, TypeStats};
use super::session::{GET_SNAPSHOT_ID, Observations, SUBSCRIBE_ID};

/// Prints every finding, one section per question the probe answers.
pub fn print(seen: &Observations, duration: Duration) {
    eprintln!("\n===== Kalshi live feed findings =====");
    eprintln!(
        "tickers subscribed: {}, run length: {duration:?}",
        seen.markets.open.len()
    );
    print_connection(seen);
    print_subscription(seen);
    print_ordering(seen);
    print_books(seen);
    print_get_snapshot(seen);
    print_rates(seen, duration);
    print_latency(seen);
    print_unopened(seen);
    print_lifecycle(seen);
    print_anomalies(seen);
    eprintln!(
        "fixture kinds seen: {:?}",
        seen.fixtures.kinds().collect::<Vec<_>>()
    );
}

fn print_connection(seen: &Observations) {
    let findings = &seen.findings;
    eprintln!("\n-- connection --");
    for (conn, handshake) in &findings.handshakes {
        eprintln!("{conn:?}: signed handshake accepted in {handshake:?}");
    }
    eprintln!("pings received: {}", findings.pings);
    for event in &findings.connection_events {
        eprintln!("event: {event}");
    }
}

fn print_subscription(seen: &Observations) {
    eprintln!("\n-- subscription --");
    for (id, sids) in &seen.sids_by_command {
        eprintln!("command {id}: subscribed sids {sids:?}");
    }
    eprintln!(
        "order book sids from subscribe {SUBSCRIBE_ID}: {}",
        seen.book_subs.len()
    );
    for ack in &seen.acks {
        eprintln!(
            "ok: reply to {:?} sid {} seq {:?}: {:?} tickers, {:?} not in the first subscribe, {:?} of it missing",
            ack.reply_to, ack.sub.0, ack.seq, ack.symbol_count, ack.extra, ack.missing
        );
    }
    let (received, last) = seen.books.initial_snapshots(&seen.markets.open);
    eprintln!(
        "initial snapshots: {received} of {} tickers; last arrived {last:?} after subscribe",
        seen.markets.open.len()
    );
}

fn print_ordering(seen: &Observations) {
    let stats = &seen.books.stats;
    eprintln!("\n-- clock and seq --");
    eprintln!(
        "snapshots with clock: {} of {}",
        stats.snapshots_with_clock, stats.snapshots
    );
    eprintln!(
        "deltas with clock: {} of {}",
        stats.deltas_with_clock, stats.deltas
    );
    eprintln!(
        "deltas with clock <= their book's snapshot clock: {}",
        stats.deltas_covered_by_snapshot
    );
    eprintln!("per-market clock regressions: {}", stats.clock_regressions);
    for ((conn, sid), track) in &seen.findings.seq {
        eprintln!(
            "{conn:?} sid {sid}: seq {}..={} over {} messages, gaps {}",
            track.first, track.last, track.messages, track.gaps
        );
    }
    for example in seen.findings.gap_examples.iter().take(10) {
        eprintln!("gap: {example}");
    }
    for ((conn, kind), stats) in &seen.findings.by_type {
        eprintln!(
            "{conn:?} {kind}: {} frames, {} with seq, {} with sending_ts_ms",
            stats.frames, stats.with_seq, stats.with_sending_ts
        );
    }
}

fn print_books(seen: &Observations) {
    let stats = &seen.books.stats;
    eprintln!("\n-- books --");
    eprintln!(
        "snapshots with sending_ts_ms: {} of {}",
        stats.snapshots_with_exchange_ts, stats.snapshots
    );
    eprintln!(
        "deltas with an exchange time: {} of {}",
        stats.deltas_with_exchange_ts, stats.deltas
    );
    eprintln!("negative-quantity results: {}", stats.negative_results);
    eprintln!(
        "deltas before any snapshot: {}",
        stats.deltas_before_snapshot
    );
    eprintln!(
        "crossed snapshots (best bid >= best ask): {}",
        stats.crossed_snapshots
    );
    eprintln!(
        "crossed local books at the end: {}",
        seen.books.crossed_books()
    );
    eprintln!(
        "feed events on other subscriptions: {}",
        seen.other_sub_events
    );
}

fn print_get_snapshot(seen: &Observations) {
    eprintln!("\n-- get_snapshot (command {GET_SNAPSHOT_ID}) --");
    for round in &seen.snapshot_rounds {
        eprintln!(
            "sent after {:?} for {} tickers; all replies by {:?}",
            round.sent_after,
            round.symbols.len(),
            round.completed_after,
        );
    }
    eprintln!(
        "replies still outstanding: {}",
        seen.books.replies_outstanding()
    );
    let comparisons = &seen.books.comparisons;
    let matched = comparisons.iter().filter(|c| c.matched).count();
    eprintln!(
        "replies matching the local book: {matched} of {}",
        comparisons.len()
    );
    comparisons.iter().for_each(print_comparison);
}

fn print_comparison(comparison: &Comparison) {
    eprintln!(
        "  {}: matched {} local clock {:?} reply clock {:?} levels (local, reply) {:?}",
        comparison.symbol,
        comparison.matched,
        comparison.local_clock,
        comparison.snapshot_clock,
        comparison.level_counts
    );
    if comparison.matched {
        return;
    }
    eprintln!("    differing levels: {:?}", comparison.differences);
    eprintln!("    deltas that followed: {:?}", comparison.followups);
    eprintln!(
        "    pre-reply book equalled the reply after {:?} more deltas",
        comparison.caught_up_after
    );
}

/// The first whole second after the last initial snapshot arrived.
fn settled_from(seen: &Observations) -> usize {
    let (_, last) = seen.books.initial_snapshots(&seen.markets.open);
    last.map_or(0, |last| last.as_secs() as usize + 1)
}

fn print_rates(seen: &Observations, duration: Duration) {
    let seconds = &seen.findings.per_second;
    eprintln!("\n-- order book connection rates --");
    print_window("whole run", seconds, duration.as_secs_f64());
    let settled_from = settled_from(seen);
    let whole_seconds = duration.as_secs() as usize;
    if settled_from < whole_seconds.min(seconds.len()) {
        let window = &seconds[settled_from..whole_seconds.min(seconds.len())];
        print_window("after initial snapshots", window, window.len() as f64);
    }
    let peak = seconds.iter().map(|second| second.bytes).max().unwrap_or(0);
    eprintln!("peak one-second bytes: {peak}");
}

fn print_window(label: &str, seconds: &[SecondTotals], length: f64) {
    let total = |field: fn(&SecondTotals) -> u64| seconds.iter().map(field).sum::<u64>() as f64;
    eprintln!(
        "{label}: {:.1} msgs/s, {:.0} bytes/s, {:.1} deltas/s",
        total(|s| s.frames) / length,
        total(|s| s.bytes) / length,
        total(|s| s.deltas) / length
    );
}

fn print_latency(seen: &Observations) {
    let findings = &seen.findings;
    eprintln!("\n-- timing --");
    for ((conn, kind), stats) in &findings.by_type {
        print_decode_time(*conn, kind, stats);
    }
    let settled_from = settled_from(seen) as u64;
    print_lag(
        "whole run",
        findings.feed_lag_ms.iter().map(|&(_, lag)| lag),
    );
    let settled = findings
        .feed_lag_ms
        .iter()
        .filter(|&&(second, _)| second >= settled_from);
    print_lag("after initial snapshots", settled.map(|&(_, lag)| lag));
    let mut change_to_send = findings.change_to_send_ms.clone();
    eprintln!(
        "delta sending_ts_ms - ts_ms (ms): p50 {:?} p99 {:?}",
        percentile(&mut change_to_send, 50.0),
        percentile(&mut change_to_send, 99.0)
    );
}

fn print_lag(label: &str, lags: impl Iterator<Item = i64>) {
    let mut lags: Vec<i64> = lags.collect();
    eprintln!(
        "feed lag received_at - sending_ts_ms, {label} (ms): p50 {:?} p90 {:?} p99 {:?} max {:?}",
        percentile(&mut lags, 50.0),
        percentile(&mut lags, 90.0),
        percentile(&mut lags, 99.0),
        percentile(&mut lags, 100.0)
    );
}

fn print_decode_time(conn: Conn, kind: &str, stats: &TypeStats) {
    let mut nanos = stats.decode_nanos.clone();
    let micros = |value: Option<u64>| value.map(|nanos| nanos as f64 / 1000.0);
    eprintln!(
        "decode {conn:?} {kind}: p50 {:?} us, p99 {:?} us, mean size {} bytes",
        micros(percentile(&mut nanos, 50.0)),
        micros(percentile(&mut nanos, 99.0)),
        stats.bytes / stats.frames.max(1)
    );
}

fn print_unopened(seen: &Observations) {
    eprintln!("\n-- unopened ticker {:?} --", seen.markets.unopened);
    for reply in &seen.unopened_replies {
        eprintln!("  {reply}");
    }
}

fn print_lifecycle(seen: &Observations) {
    eprintln!("\n-- lifecycle --");
    eprintln!(
        "raw event_type counts: {:?}",
        seen.findings.lifecycle_event_types
    );
    eprintln!("decoded lifecycle events: {:?}", seen.lifecycle_decoded);
    eprintln!("frames decoded as Ignored: {}", seen.ignored);
}

fn print_anomalies(seen: &Observations) {
    let findings = &seen.findings;
    eprintln!("\n-- anomalies --");
    eprintln!("unknown message types: {:?}", findings.unknown_types);
    eprintln!("unknown fields: {:?}", findings.unknown_fields);
    eprintln!("error-25 overflows: {}", seen.overflow_errors);
    for error in &findings.error_frames {
        eprintln!("error frame: {error}");
    }
    for error in &seen.decoded_errors {
        eprintln!("decoded error: {error}");
    }
    eprintln!("decode failures: {}", findings.decode_errors.len());
    for failure in findings.decode_errors.iter().take(10) {
        eprintln!("decode failure: {failure}");
    }
}

/// The `p`th percentile of `values` (sorted in place), or `None` if empty.
fn percentile<T: Ord + Copy>(values: &mut [T], p: f64) -> Option<T> {
    values.sort_unstable();
    let last = values.len().checked_sub(1)?;
    let index = ((last as f64) * p / 100.0).round() as usize;
    values.get(index).copied()
}
