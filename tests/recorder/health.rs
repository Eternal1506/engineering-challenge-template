//! A warm standby taking over from a primary that is still connected: one
//! that goes silent, one whose feed lags, and one that leaves a live market
//! unanswered past its resync deadline. Also what holds a takeover back: a
//! healthy or merely quiet primary, lags over unequal windows, a link where
//! everything lags, a market that never answers or changes, the cooldown
//! after a takeover, and a standby still warming up.

use chrono::Utc;
use engineering_challenge::core::shard::{HealthChecks, ResyncTimings};
use engineering_challenge::core::stats::FailoverCounts;

use super::failover::{
    Pair, QUIET, answer, paired, raise, recording_a_pair_with, resynced_then_raised, stop,
};
use super::mock_exchange::SentCommand;
use super::resync::{WELL_PAST, open_gap};
use super::*;

/// How often the tests' shards check their replicas' health, and the
/// length of each bucket of the window checked.
const TICK: Duration = Duration::from_millis(100);

/// The window the tests' health checks look at.
const WINDOW: Duration = Duration::from_secs(1);

/// The time between two changes played on one connection.
const PACE: Duration = Duration::from_millis(20);

/// How long a test waits for a takeover that must happen.
const PATIENCE: Duration = Duration::from_secs(4);

/// A paired shard whose health is checked every [`TICK`] over a
/// [`WINDOW`], whose standbys may take over once connected for `warmup`.
fn watched(warmup: Duration) -> CoordinatorConfig {
    let paired = paired(2);
    let health = HealthChecks {
        tick: TICK,
        bucket: TICK,
        ..HealthChecks::default()
    };
    CoordinatorConfig {
        failover: FailoverTimings {
            warmup,
            health,
            ..paired.failover.clone()
        },
        ..paired
    }
}

/// What a test plays on each of a pair's connections: changes whose
/// exchange time trails the real time by the milliseconds given, or
/// nothing.
#[derive(Debug, Clone, Copy)]
struct Play {
    primary: Option<i64>,
    standby: Option<i64>,
}

/// The primary silent, the standby busy and on time.
const STANDBY_ONLY: Play = Play {
    primary: None,
    standby: Some(0),
};

/// Plays `play` on `pair` until the standby is sent a command, returning
/// it with how long that took, or until `limit` has passed.
async fn play_until_asked(
    pair: &mut Pair,
    play: Play,
    limit: Duration,
) -> Option<(SentCommand, Duration)> {
    let started = Instant::now();
    while started.elapsed() < limit {
        play_once(pair, play).await;
        if let Ok(command) = timeout(PACE, pair.standby.client.next_command()).await {
            return Some((command, started.elapsed()));
        }
    }
    None
}

/// Sends one change on each connection `play` keeps busy.
async fn play_once(pair: &mut Pair, play: Play) {
    if let Some(behind_ms) = play.primary {
        send_stamped(&mut pair.primary, behind_ms).await;
    }
    if let Some(behind_ms) = play.standby {
        send_stamped(&mut pair.standby, behind_ms).await;
    }
}

/// Sends `shard` a +10 change to A's 0.40 bid, stamped `behind_ms` before
/// now. A change to a socket the recorder dropped as it handed over is let
/// go.
async fn send_stamped(shard: &mut Shard, behind_ms: i64) {
    let seq = shard.next_seq();
    let change = frames::level(shard.sub, seq, "A", ("buy", 400_000, 10));
    let ts = Utc::now().timestamp_millis() - behind_ms;
    let _ = shard.client.try_send(frames::stamped(change, ts)).await;
}

/// Plays `play` until the standby takes over, then waits for the old
/// primary's socket to go and accepts its new one, naming it 3. Returns the
/// snapshot request the standby got, how long it took, and the new socket.
async fn take_over(
    server: &MockServer,
    pair: &mut Pair,
    play: Play,
) -> (SentCommand, Duration, Shard) {
    let asked = play_until_asked(pair, play, PATIENCE).await;
    let (request, waited) = asked.expect("the standby did not take over");
    pair.primary.client.expect_gone().await;
    let reconnected = Shard::accept(server, 3).await;
    (request, waited, reconnected)
}

/// Answers `request` on `shard` with the books of A and B.
async fn answer_both(shard: &mut Shard, request: &SentCommand) {
    answer(shard, request, "A").await;
    answer(shard, request, "B").await;
}

/// The primary's books arrive, then only the standby delivers. Once the
/// books are older than the window, the standby takes over: it is asked
/// for snapshots, its answers and its next change are written, and the old
/// primary reconnects.
#[tokio::test]
async fn a_silent_primary_fails_over_to_an_active_standby_within_the_window() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, watched(Duration::ZERO)).await;
    let (request, waited, reconnected) = take_over(&server, &mut pair, STANDBY_ONLY).await;
    answer_both(&mut pair.standby, &request).await;
    raise(&mut pair.standby, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![pair.standby, reconnected]).await;
    assert!(waited < WINDOW * 2, "took over after {waited:?}");
    let failovers = FailoverCounts {
        silent: 1,
        ..FailoverCounts::default()
    };
    let asked = (request.cmd.as_str(), request.sub);
    let outcome = (asked, finished.of("A"), counters.read().failovers);
    let expected = (("snapshot", Some(2)), resynced_then_raised(), failovers);
    assert_eq!(outcome, expected);
}

/// Once both sockets have been up for a window, both deliver, but the
/// primary's changes were sent 2 s before the standby's. The standby takes
/// over after three checks in a row, and the old primary is told to
/// reconnect: its socket goes and a new one connects.
#[tokio::test]
async fn a_lagging_primary_fails_over_after_three_ticks_and_reconnects() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, watched(Duration::ZERO)).await;
    tokio::time::sleep(WINDOW).await;
    let lagging = Play {
        primary: Some(2000),
        standby: Some(0),
    };
    let (request, waited, reconnected) = take_over(&server, &mut pair, lagging).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.standby, reconnected]).await;
    assert!(waited >= TICK * 2, "took over after {waited:?}");
    let failovers = FailoverCounts {
        lagging: 1,
        ..FailoverCounts::default()
    };
    let outcome = (request.cmd.as_str(), counters.read().failovers);
    assert_eq!(outcome, ("snapshot", failovers));
}

/// Both connections deliver the same changes, on time, for three seconds:
/// nothing fails over.
#[tokio::test]
async fn a_healthy_pair_with_identical_traffic_never_fails_over() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, watched(Duration::ZERO)).await;
    let healthy = Play {
        primary: Some(0),
        standby: Some(0),
    };
    let asked = play_until_asked(&mut pair, healthy, Duration::from_secs(3)).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, pair.standby]).await;
    let outcome = (asked.map(|(command, _)| command), counters.read().failovers);
    assert_eq!(outcome, (None, FailoverCounts::default()));
}

/// The standby takes over from a silent primary. Then the new primary goes
/// silent while the old one's new socket delivers, inside the cooldown:
/// nothing more fails over.
#[tokio::test]
async fn a_second_trigger_inside_the_cooldown_does_nothing() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, watched(Duration::ZERO)).await;
    let (request, _, reconnected) = take_over(&server, &mut pair, STANDBY_ONLY).await;
    answer_both(&mut pair.standby, &request).await;
    let mut swapped = Pair {
        primary: pair.standby,
        standby: reconnected,
    };
    let asked = play_until_asked(&mut swapped, STANDBY_ONLY, WINDOW * 3).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![swapped.primary, swapped.standby]).await;
    let failovers = FailoverCounts {
        silent: 1,
        ..FailoverCounts::default()
    };
    let outcome = (asked.map(|(command, _)| command), counters.read().failovers);
    assert_eq!(outcome, (None, failovers));
}

/// The resync deadline of the tests that let it pass.
const RESYNC_DEADLINE: Duration = Duration::from_millis(300);

/// Paired shards whose resyncs are overdue after [`RESYNC_DEADLINE`].
fn quick_resyncs() -> CoordinatorConfig {
    CoordinatorConfig {
        resync: ResyncTimings {
            timeout: RESYNC_DEADLINE,
            ..ResyncTimings::default()
        },
        ..paired(2)
    }
}

/// The standby delivers a change, then a gap's snapshot request goes
/// unanswered past the deadline, which alone hands nothing over. Held A
/// then changes on the primary, so it is live: the delivering standby takes
/// over. It is asked for every market, the old primary reconnects, and the
/// standby's answers and next change are written.
#[tokio::test]
async fn a_live_market_held_past_the_deadline_hands_over_to_a_delivering_standby() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, quick_resyncs()).await;
    raise(&mut pair.standby, "A").await;
    open_gap(&mut pair.primary, "A").await;
    let quiet = RESYNC_DEADLINE + WELL_PAST;
    pair.standby.client.assert_no_command(quiet).await;
    raise(&mut pair.primary, "A").await;
    let request = pair.standby.client.next_command().await;
    pair.primary.client.expect_gone().await;
    let reconnected = Shard::accept(&server, 3).await;
    answer_both(&mut pair.standby, &request).await;
    raise(&mut pair.standby, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![pair.standby, reconnected]).await;
    let counts = counters.read();
    let asked = (request.cmd.as_str(), request.sub, request.symbols.len());
    let resync = (counts.failovers.resync, counts.resyncs_overdue);
    let outcome = (asked, finished.of("A"), resync);
    let expected = (("snapshot", Some(2), 2), resynced_then_raised(), (1, 1));
    assert_eq!(outcome, expected);
}

/// B never answers and never changes, like a market the exchange dropped.
/// Its resync passes the deadline while the standby delivers, and the
/// shard stays on its primary.
#[tokio::test]
async fn a_market_that_never_answers_or_changes_hands_nothing_over() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, quick_resyncs()).await;
    raise(&mut pair.standby, "A").await;
    let request = open_gap(&mut pair.primary, "A").await;
    answer(&mut pair.primary, &request, "A").await;
    let quiet = RESYNC_DEADLINE + WELL_PAST;
    pair.standby.client.assert_no_command(quiet).await;
    raise(&mut pair.primary, "A").await;
    recorder.output.wait_for(3, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![pair.primary, pair.standby]).await;
    let counts = counters.read();
    let outcome = (finished.of("A"), counts.failovers, counts.resyncs_overdue);
    let expected = (resynced_then_raised(), FailoverCounts::default(), 1);
    assert_eq!(outcome, expected);
}

/// The standby takes over from a silent primary, and the resync that
/// starts on it goes unanswered past its deadline. Held A then changes
/// inside the cooldown, while the old primary's new socket delivers: A's
/// snapshot is asked for again on the new primary instead of the shard
/// being handed over again.
#[tokio::test]
async fn a_live_held_market_inside_the_cooldown_is_asked_for_again() {
    let server = MockServer::start().await;
    let deadline = Duration::from_millis(800);
    let config = CoordinatorConfig {
        resync: ResyncTimings {
            timeout: deadline,
            ..ResyncTimings::default()
        },
        ..watched(Duration::ZERO)
    };
    let (recorder, mut pair) = recording_a_pair_with(&server, config).await;
    let (_, _, mut reconnected) = take_over(&server, &mut pair, STANDBY_ONLY).await;
    reconnected.client.assert_no_command(deadline * 2).await;
    raise(&mut reconnected, "A").await;
    tokio::time::sleep(TICK).await;
    raise(&mut pair.standby, "A").await;
    let again = pair.standby.client.next_command().await;
    reconnected.client.assert_no_command(QUIET).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.standby, reconnected]).await;
    let counts = counters.read();
    let failovers = FailoverCounts {
        silent: 1,
        ..FailoverCounts::default()
    };
    let asked = (again.cmd.as_str(), again.symbols);
    let outcome = (asked, counts.failovers, counts.resync_resnapshots);
    let expected = (("snapshot", vec!["A".to_owned()]), failovers, 1);
    assert_eq!(outcome, expected);
}

/// A quiet shard: no level change on either connection for a window. Then
/// the standby's subscribe snapshots arrive. Snapshots come when asked for,
/// so they do not make the primary look silent.
#[tokio::test]
async fn subscribe_snapshots_on_the_standby_do_not_make_a_quiet_primary_silent() {
    let server = MockServer::start().await;
    let mut config = watched(Duration::ZERO);
    config.failover.health.silent_standby_events = 2;
    let (recorder, mut pair) = recording_a_pair_with(&server, config).await;
    tokio::time::sleep(WINDOW + TICK * 2).await;
    pair.standby.send_books().await;
    pair.standby.client.assert_no_command(WINDOW * 2).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, pair.standby]).await;
    assert_eq!(counters.read().failovers, FailoverCounts::default());
}

/// Both sockets lag 2 s in a burst, then the standby reconnects and both
/// run on time. The standby's new window lacks the burst while the
/// primary's still holds it, so their lags are compared only once the
/// standby has been up for a whole window: nothing fails over.
#[tokio::test]
async fn a_standby_window_shorter_than_the_primarys_is_not_compared_for_lag() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, watched(Duration::ZERO)).await;
    let burst = Play {
        primary: Some(2000),
        standby: Some(2000),
    };
    let during = play_until_asked(&mut pair, burst, Duration::from_millis(600)).await;
    pair.standby.client.close().await;
    let standby = Shard::accept(&server, 3).await;
    let mut pair = Pair {
        primary: pair.primary,
        standby,
    };
    let on_time = Play {
        primary: Some(0),
        standby: Some(0),
    };
    let after = play_until_asked(&mut pair, on_time, WINDOW * 2).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, pair.standby]).await;
    let asked = (during.is_some(), after.is_some());
    let outcome = (asked, counters.read().failovers);
    assert_eq!(outcome, ((false, false), FailoverCounts::default()));
}

/// On a saturated link every socket lags: the primary's changes 3 s, the
/// standby's 1.5 s. The standby is beyond the threshold itself, so it does
/// not take over.
#[tokio::test]
async fn a_link_where_every_socket_lags_never_hands_over() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair_with(&server, watched(Duration::ZERO)).await;
    tokio::time::sleep(WINDOW).await;
    let saturated = Play {
        primary: Some(3000),
        standby: Some(1500),
    };
    let asked = play_until_asked(&mut pair, saturated, WINDOW * 2).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, pair.standby]).await;
    let outcome = (asked.map(|(command, _)| command), counters.read().failovers);
    assert_eq!(outcome, (None, FailoverCounts::default()));
}

/// The primary is silent while a standby still inside its warmup delivers:
/// the standby does not take over.
#[tokio::test]
async fn a_standby_inside_its_warmup_is_not_promoted_for_a_silent_primary() {
    let server = MockServer::start().await;
    let config = watched(Duration::from_secs(3600));
    let (recorder, mut pair) = recording_a_pair_with(&server, config).await;
    let asked = play_until_asked(&mut pair, STANDBY_ONLY, WINDOW * 3).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, pair.standby]).await;
    let outcome = (asked.map(|(command, _)| command), counters.read().failovers);
    assert_eq!(outcome, (None, FailoverCounts::default()));
}
