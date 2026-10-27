//! When a shard's standbys start, and when one takes over: after the
//! warmup, during a gap resync, and never when the shard itself reconnected
//! its primary. Also the resync deadline's drain on a paired shard.

use engineering_challenge::core::shard::ResyncTimings;
use engineering_challenge::exchange::MarketStatus;

use super::failover::{
    Pair, QUIET, accept_pair, accept_unnamed, answer, answer_unnamed_close, fail_over, paired,
    raise, recording_a_pair, recording_a_pair_with, resynced_then_raised, stop,
};
use super::mock_exchange::market;
use super::resync::{
    GENEROUS_TIMEOUT, WELL_PAST, block_on_the_writer, gated_pipeline, leave_b_overdue, open_gap,
    overdue_timings, resync_counts,
};
use super::*;

/// Paired shards of `shard_size` whose failover runs on `failover`.
fn paired_with(shard_size: usize, failover: FailoverTimings) -> CoordinatorConfig {
    CoordinatorConfig {
        failover,
        ..paired(shard_size)
    }
}

/// Failover timings with `standby_delay` and `warmup`.
fn timings(standby_delay: Duration, warmup: Duration) -> FailoverTimings {
    FailoverTimings {
        standby_delay,
        warmup,
        ..FailoverTimings::default()
    }
}

/// The standby's connection waits for the standby delay after the primary
/// connects.
#[tokio::test]
async fn standbys_start_once_the_standby_delay_has_passed() {
    let server = MockServer::start().await;
    let delay = Duration::from_millis(400);
    let config = paired_with(2, timings(delay, Duration::ZERO));
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B"]);
    let mut primary = Shard::accept(&server, 1).await;
    let connected_at = Instant::now();
    primary.send_books().await;
    let early = timeout(delay / 2, server.accept()).await;
    let standby = Shard::accept(&server, 2).await;
    let waited = connected_at.elapsed();
    recorder.output.wait_for(2, 0).await;
    stop(recorder, vec![primary, standby]).await;
    assert!(early.is_err(), "the standby started before the delay");
    assert!(waited >= delay * 3 / 4, "started after {waited:?}");
}

/// No standby starts while the primary has never connected; once it does,
/// the standby follows.
#[tokio::test]
async fn standbys_wait_for_the_primary_to_connect() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start_with(&server, paired(2));
    recorder.list_open(&["A", "B"]);
    let (client, subscribe) = accept_unnamed(&server).await;
    let early = timeout(QUIET, server.accept()).await;
    let mut primary = super::failover::name(client, subscribe, 1).await;
    let standby = Shard::accept(&server, 2).await;
    primary.send_books().await;
    recorder.output.wait_for(2, 0).await;
    stop(recorder, vec![primary, standby]).await;
    assert!(early.is_err(), "a standby started too early");
}

/// The primary drops 100 ms after the standby connects, inside its 500 ms
/// warmup, and its own reconnect is never named. The standby takes over once
/// warm: it is asked for snapshots, and its answers and change are written.
#[tokio::test]
async fn a_standby_warming_up_takes_over_once_warm() {
    let server = MockServer::start().await;
    let warmup = Duration::from_millis(500);
    let config = paired_with(2, timings(Duration::ZERO, warmup));
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B"]);
    let Pair {
        mut primary,
        mut standby,
    } = accept_pair(&server).await;
    let standby_connected_at = Instant::now();
    primary.send_books().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    primary.client.close().await;
    let (reconnect, _) = accept_unnamed(&server).await;
    let request = standby.client.next_command().await;
    let waited = standby_connected_at.elapsed();
    answer(&mut standby, &request, "A").await;
    answer(&mut standby, &request, "B").await;
    raise(&mut standby, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let reconnect_closed = answer_unnamed_close(reconnect);
    let finished = stop(recorder, vec![standby]).await;
    reconnect_closed.await.unwrap();
    assert!(waited >= warmup * 4 / 5, "took over after {waited:?}");
    let failovers = counters.read().failovers.disconnect;
    let outcome = (request.cmd.as_str(), finished.of("A"), failovers);
    assert_eq!(outcome, ("snapshot", resynced_then_raised(), 1));
}

/// The primary drops while the standby warms up but is back before the
/// warmup ends: it keeps its role, and the standby is never asked for
/// anything.
#[tokio::test]
async fn a_primary_back_before_the_warmup_keeps_its_role() {
    let server = MockServer::start().await;
    let warmup = Duration::from_millis(500);
    let config = paired_with(2, timings(Duration::ZERO, warmup));
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B"]);
    let Pair {
        mut primary,
        mut standby,
    } = accept_pair(&server).await;
    primary.send_books().await;
    primary.client.close().await;
    let mut back = Shard::accept(&server, 3).await;
    back.send_books().await;
    standby.client.assert_no_command(warmup * 2).await;
    raise(&mut back, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![back, standby]).await;
    let failovers = counters.read().failovers.disconnect;
    assert_eq!((finished.of("A"), failovers), (resynced_then_raised(), 0));
}

/// The shard reconnects its primary for a market held past the ceiling,
/// and the new socket is never named. Once the reconnect grace has passed,
/// the warm standby takes over: it is asked for snapshots, and its answers
/// and change are written.
#[tokio::test]
async fn a_requested_reconnect_that_never_comes_back_fails_over_after_the_grace() {
    let server = MockServer::start().await;
    let grace = Duration::from_millis(500);
    let config = CoordinatorConfig {
        resync: overdue_timings(Duration::from_millis(800), Duration::ZERO),
        failover: FailoverTimings {
            reconnect_grace: grace,
            ..paired(2).failover
        },
        ..paired(2)
    };
    let (recorder, mut pair) = recording_a_pair_with(&server, config).await;
    leave_b_overdue(&mut pair.primary).await;
    raise(&mut pair.primary, "B").await;
    let reconnect_asked = Instant::now();
    let (reconnect, _) = accept_unnamed(&server).await;
    let request = pair.standby.client.next_command().await;
    let waited = reconnect_asked.elapsed();
    answer(&mut pair.standby, &request, "A").await;
    answer(&mut pair.standby, &request, "B").await;
    raise(&mut pair.standby, "B").await;
    recorder.output.wait_for(5, 1).await;
    let counters = recorder.counters.clone();
    let reconnect_closed = answer_unnamed_close(reconnect);
    let finished = stop(recorder, vec![pair.standby]).await;
    reconnect_closed.await.unwrap();
    assert!(waited >= grace * 4 / 5, "took over after {waited:?}");
    let counts = counters.read();
    let outcome = (counts.failovers.disconnect, counts.resync_reconnects);
    let expected = (resynced_then_raised(), (1, 1));
    assert_eq!((finished.of("B"), outcome), expected);
}

/// A market held past the ceiling reconnects the primary, as on a lone
/// shard. The shard does not fail over to the standby, which would only be
/// asked for snapshots the way that already failed: the primary's new
/// socket's subscribe snapshots release B.
#[tokio::test]
async fn a_market_held_past_the_ceiling_reconnects_the_primary_without_failing_over() {
    let server = MockServer::start().await;
    let config = CoordinatorConfig {
        resync: overdue_timings(Duration::from_millis(800), Duration::ZERO),
        ..paired(2)
    };
    let (recorder, mut pair) = recording_a_pair_with(&server, config).await;
    leave_b_overdue(&mut pair.primary).await;
    raise(&mut pair.primary, "B").await;
    let mut reconnected = Shard::accept(&server, 3).await;
    pair.standby.client.assert_no_command(QUIET).await;
    reconnected.send_books().await;
    raise(&mut reconnected, "B").await;
    recorder.output.wait_for(5, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![reconnected, pair.standby]).await;
    let counts = counters.read();
    let outcome = (counts.failovers.disconnect, counts.resync_reconnects);
    let expected = (resynced_then_raised(), (0, 1));
    assert_eq!((finished.of("B"), outcome), expected);
}

/// The primary drops while a gap resync waits for its snapshots: the
/// resync restarts on the standby, whose answers complete it in time.
#[tokio::test]
async fn a_failover_during_a_gap_resync_restarts_it_on_the_standby() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair(&server).await;
    open_gap(&mut pair.primary, "A").await;
    let request = fail_over(&mut pair).await;
    answer(&mut pair.standby, &request, "A").await;
    answer(&mut pair.standby, &request, "B").await;
    raise(&mut pair.standby, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![pair.standby]).await;
    let counts = counters.read();
    let failed_over = (counts.gaps, counts.failovers.disconnect);
    let totals = (failed_over, resync_counts(&counters));
    let expected = (resynced_then_raised(), ((1, 1), (1, 0, 0)));
    assert_eq!((finished.of("A"), totals), expected);
}

/// On a paired shard, the answers to a gap's request and the standby's
/// books wait behind a writer held until well past the deadline. The
/// queued events of both connections are handled before the deadline is,
/// so the resync completes in time.
#[tokio::test]
async fn answers_queued_on_a_paired_shard_when_the_deadline_passes_complete_the_resync() {
    let server = MockServer::start().await;
    let config = CoordinatorConfig {
        resync: ResyncTimings {
            timeout: GENEROUS_TIMEOUT,
            ..ResyncTimings::default()
        },
        ..paired(3)
    };
    let (mut pipeline, gate) = gated_pipeline(&server, config);
    let markets = ["A", "B"].map(|symbol| market(symbol, MarketStatus::Open));
    pipeline.coordinator.add_markets(markets.to_vec()).await;
    let mut pair = accept_pair(&server).await;
    pair.primary.send_books().await;
    pipeline.output.wait_for(2, 0).await;
    let request = open_gap(&mut pair.primary, "A").await;
    block_on_the_writer(&mut pipeline, &mut pair.primary, &gate).await;
    pair.standby.send_books().await;
    answer(&mut pair.primary, &request, "A").await;
    answer(&mut pair.primary, &request, "B").await;
    tokio::time::sleep(GENEROUS_TIMEOUT + WELL_PAST).await;
    gate.send_replace(true);
    pipeline.output.wait_for(5, 2).await;
    let counters = pipeline.coordinator.counters();
    let closes = answer_closes(vec![pair.primary, pair.standby]);
    pipeline.coordinator.shutdown().await;
    closed(closes).await;
    pipeline.output.finish().await;
    let outcome = (server.handshake_attempts(), resync_counts(&counters));
    assert_eq!(outcome, (2, (1, 0, 0)));
}
