//! Resyncing a shard's markets after a sequence gap.

use engineering_challenge::core::writer::WriterMessage;
use engineering_challenge::exchange::MarketStatus;
use tokio::sync::watch;

use super::mock_exchange::{SentCommand, market};
use super::recovery::recording_two_shards;
use super::*;

/// A recorder with one shard over A and B, both books recorded, whose
/// resyncs time out after `resync_timeout`.
async fn recording_one_shard(server: &MockServer, resync_timeout: Duration) -> (Recorder, Shard) {
    let config = CoordinatorConfig {
        resync_timeout,
        ..config(2)
    };
    let mut recorder = Recorder::start_with(server, config);
    recorder.list_open(&["A", "B"]);
    let mut shard = Shard::accept(server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(2, 0).await;
    (recorder, shard)
}

/// Shards of `shard_size` whose resyncs time out after `resync_timeout`.
fn resync_config(shard_size: usize, resync_timeout: Duration) -> CoordinatorConfig {
    CoordinatorConfig {
        resync_timeout,
        ..config(shard_size)
    }
}

/// Has `pipeline`'s coordinator, driven by the test, record A and B on one
/// shard, and returns that shard with both books recorded.
async fn record_a_and_b(server: &MockServer, pipeline: &mut Pipeline) -> Shard {
    let markets = ["A", "B"].map(|symbol| market(symbol, MarketStatus::Open));
    pipeline.coordinator.add_markets(markets.to_vec()).await;
    let mut shard = Shard::accept(server, 1).await;
    shard.send_books().await;
    pipeline.output.wait_for(2, 0).await;
    shard
}

/// A pipeline for `server` whose writer takes its messages through a gate,
/// open until the test closes it with the returned sender.
fn gated_pipeline(
    server: &MockServer,
    config: CoordinatorConfig,
) -> (Pipeline, watch::Sender<bool>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recording.sbe");
    let (mut link, writer) = start_writer(&path, |_| {});
    let (messages, gate) = gated_relay(link.messages);
    link.messages = messages;
    let output = Output {
        progress: link.progress.clone(),
        writer,
        path,
        _dir: dir,
    };
    let coordinator = Coordinator::new(mock_feed(server), link, config);
    let pipeline = Pipeline {
        coordinator,
        output,
    };
    (pipeline, gate)
}

/// Passes messages on to `writer` through a channel of one while the
/// returned gate is open; a closed gate holds the next message.
fn gated_relay(
    writer: mpsc::Sender<WriterMessage>,
) -> (mpsc::Sender<WriterMessage>, watch::Sender<bool>) {
    let (sender, mut receiver) = mpsc::channel(1);
    let (gate, mut open) = watch::channel(true);
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            open.wait_for(|open| *open).await.ok();
            if writer.send(message).await.is_err() {
                return;
            }
        }
    });
    (sender, gate)
}

/// Adds C to the shard during a resync of A and B, closes `gate`, and
/// sends C's book and two changes. The relay holds the first, its channel
/// the second, and the shard waits to hand over the third, so whatever the
/// shard's connection delivers next stays queued until the gate opens.
async fn block_on_the_writer(
    pipeline: &mut Pipeline,
    shard: &mut Shard,
    gate: &watch::Sender<bool>,
) {
    let markets = vec![market("C", MarketStatus::Open)];
    pipeline.coordinator.add_markets(markets).await;
    let add = shard.client.next_command().await;
    assert_eq!(add.cmd, "add");
    gate.send_replace(false);
    send_book(shard, "C").await;
    raise(shard, "C").await;
    raise(shard, "C").await;
}

/// Shuts `pipeline` down, answering `shard`'s close, and checks the
/// recording.
async fn shut_down(pipeline: Pipeline, shard: Shard) -> Finished {
    let closes = answer_closes(vec![shard]);
    pipeline.coordinator.shutdown().await;
    closed(closes).await;
    pipeline.output.finish().await
}

/// Skips two sequence numbers on `shard` with a +10 bid change for
/// `symbol`, and returns the snapshot request the gap brings.
async fn open_gap(shard: &mut Shard, symbol: &str) -> SentCommand {
    shard.seq += 2;
    raise(shard, symbol).await;
    shard.client.next_command().await
}

/// Sends a +10 bid change for `symbol` at 0.40, where every sent book has
/// 100.
async fn raise(shard: &mut Shard, symbol: &str) {
    shard.send_level(symbol, ("buy", 400_000, 10)).await;
}

/// Answers `request` with the standard book for `symbol`.
async fn answer(shard: &mut Shard, request: &SentCommand, symbol: &str) {
    let seq = shard.next_seq();
    let reply = frames::book(shard.sub, seq, symbol, (BIDS, ASKS));
    shard.client.reply(request, reply).await;
}

/// Sends the standard book for `symbol` unprompted.
async fn send_book(shard: &mut Shard, symbol: &str) {
    let seq = shard.next_seq();
    let frame = frames::book(shard.sub, seq, symbol, (BIDS, ASKS));
    shard.client.send(frame).await;
}

/// A book as sent, then a fresh copy of it, then the +10 change on it.
fn resynced_then_raised() -> Vec<Shape> {
    vec![
        sent_book(),
        sent_book(),
        Shape::Update(Side::Buy, 400_000, 110),
    ]
}

/// The resync counts: completed in time, overdue, and reconnected.
fn resync_counts(counters: &RecorderCounters) -> (u64, u64, u64) {
    let counts = counters.read();
    let (completed, overdue) = (counts.resyncs_completed, counts.resyncs_overdue);
    (completed, overdue, counts.resync_reconnects)
}

#[tokio::test]
async fn a_gap_requests_a_snapshot_of_every_market_of_its_shard() {
    let server = MockServer::start().await;
    let (recorder, mut shards) = recording_two_shards(&server).await;
    let gapped = owner_of(&mut shards, "A");
    let request = open_gap(gapped, "A").await;
    let (sub, mut owned) = (gapped.sub, gapped.symbols.clone());
    let closes = answer_closes(shards);
    recorder.stop().await;
    closed(closes).await;
    owned.sort();
    let asked = (request.cmd.as_str(), request.sub, request.symbols);
    assert_eq!(asked, ("snapshot", Some(sub), owned));
}

#[tokio::test]
async fn a_gap_sends_nothing_to_the_other_shard() {
    let server = MockServer::start().await;
    let (recorder, mut shards) = recording_two_shards(&server).await;
    open_gap(owner_of(&mut shards, "A"), "A").await;
    let other = shards.iter_mut().find(|shard| !shard.owns("A")).unwrap();
    other
        .client
        .assert_no_command(Duration::from_millis(300))
        .await;
    let closes = answer_closes(shards);
    recorder.stop().await;
    closed(closes).await;
}

/// The change that reveals the gap and the one before A's snapshot are
/// dropped; the one after it applies to the fresh book.
#[tokio::test]
async fn a_markets_changes_are_dropped_from_a_gap_until_its_snapshot() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, Duration::from_secs(5)).await;
    let request = open_gap(&mut shard, "A").await;
    raise(&mut shard, "A").await;
    answer(&mut shard, &request, "A").await;
    raise(&mut shard, "A").await;
    recorder.output.wait_for(3, 1).await;
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![shard]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let dropped = counters.read().resync_drops;
    assert_eq!((finished.of("A"), dropped), (resynced_then_raised(), 2));
}

/// B's change after A's snapshot is still dropped, and A's applied; B's
/// change after its own snapshot is applied.
#[tokio::test]
async fn each_market_leaves_the_resync_on_its_own_snapshot() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, Duration::from_secs(5)).await;
    let request = open_gap(&mut shard, "A").await;
    answer(&mut shard, &request, "A").await;
    raise(&mut shard, "B").await;
    raise(&mut shard, "A").await;
    answer(&mut shard, &request, "B").await;
    raise(&mut shard, "B").await;
    recorder.output.wait_for(4, 2).await;
    let closes = answer_closes(vec![shard]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let both = (finished.of("A"), finished.of("B"));
    assert_eq!(both, (resynced_then_raised(), resynced_then_raised()));
}

/// How long tests whose answers must beat the deadline give them.
const GENEROUS_TIMEOUT: Duration = Duration::from_secs(2);

/// How long tests that let the deadline pass wait beyond it.
const WELL_PAST: Duration = Duration::from_millis(1200);

/// The deadline of tests that let it pass.
const SHORT_TIMEOUT: Duration = Duration::from_millis(300);

/// Every snapshot arrives well before the deadline, which then passes
/// without a reconnect.
#[tokio::test]
async fn a_resync_answered_in_time_keeps_the_socket() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, GENEROUS_TIMEOUT).await;
    let request = open_gap(&mut shard, "A").await;
    answer(&mut shard, &request, "A").await;
    answer(&mut shard, &request, "B").await;
    let quiet = GENEROUS_TIMEOUT + WELL_PAST;
    shard.client.assert_no_command(quiet).await;
    let handshakes = server.handshake_attempts();
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![shard]);
    recorder.stop().await;
    closed(closes).await;
    assert_eq!((handshakes, resync_counts(&counters)), (1, (1, 0, 0)));
}

/// B never answers and never changes, like a market the exchange dropped:
/// the deadline passes without a reconnect, and A keeps recording.
#[tokio::test]
async fn a_silent_market_past_the_deadline_is_held_without_a_reconnect() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, SHORT_TIMEOUT).await;
    let request = open_gap(&mut shard, "A").await;
    answer(&mut shard, &request, "A").await;
    let quiet = SHORT_TIMEOUT + WELL_PAST;
    shard.client.assert_no_command(quiet).await;
    raise(&mut shard, "A").await;
    recorder.output.wait_for(3, 1).await;
    let handshakes = server.handshake_attempts();
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![shard]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let outcome = (finished.of("A"), handshakes, resync_counts(&counters));
    assert_eq!(outcome, (resynced_then_raised(), 1, (0, 1, 0)));
}

/// B changes after the deadline: that change is dropped and the shard
/// reconnects. B's subscribe snapshot on the new socket comes before any
/// later change of B is written.
#[tokio::test]
async fn a_change_to_a_market_past_the_deadline_reconnects_the_shard() {
    let server = MockServer::start().await;
    let (recorder, mut gapped) = recording_one_shard(&server, SHORT_TIMEOUT).await;
    let request = open_gap(&mut gapped, "A").await;
    answer(&mut gapped, &request, "A").await;
    let quiet = SHORT_TIMEOUT + WELL_PAST;
    gapped.client.assert_no_command(quiet).await;
    raise(&mut gapped, "B").await;
    let mut reconnected = Shard::accept(&server, 2).await;
    reconnected.send_books().await;
    raise(&mut reconnected, "B").await;
    recorder.output.wait_for(5, 1).await;
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![reconnected]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let outcome = (finished.of("B"), resync_counts(&counters));
    assert_eq!(outcome, (resynced_then_raised(), (0, 1, 1)));
}

/// The second gap comes before the first deadline and the replies after
/// it, but before the second deadline: the resync completes in time.
#[tokio::test]
async fn a_second_gap_during_a_resync_restarts_it() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, Duration::from_secs(3)).await;
    let first = open_gap(&mut shard, "A").await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    let second = open_gap(&mut shard, "B").await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    answer(&mut shard, &second, "A").await;
    answer(&mut shard, &second, "B").await;
    raise(&mut shard, "A").await;
    recorder.output.wait_for(4, 1).await;
    let handshakes = server.handshake_attempts();
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![shard]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    assert_ne!(first.id, second.id);
    let outcome = (finished.of("A"), handshakes, resync_counts(&counters));
    assert_eq!(outcome, (resynced_then_raised(), 1, (1, 0, 0)));
}

/// A server close mid-resync: the new socket's subscribe snapshots release
/// both markets long before the deadline.
#[tokio::test]
async fn a_new_socket_releases_the_held_markets_before_the_deadline() {
    let server = MockServer::start().await;
    let (recorder, mut gapped) = recording_one_shard(&server, Duration::from_secs(10)).await;
    open_gap(&mut gapped, "A").await;
    gapped.client.close().await;
    let mut reconnected = Shard::accept(&server, 2).await;
    reconnected.send_books().await;
    raise(&mut reconnected, "A").await;
    raise(&mut reconnected, "B").await;
    recorder.output.wait_for(4, 2).await;
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![reconnected]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let both = (finished.of("A"), finished.of("B"));
    let expected = (resynced_then_raised(), resynced_then_raised());
    assert_eq!((both, resync_counts(&counters)), (expected, (0, 0, 0)));
}

/// The socket drops mid-resync and stays down past the deadline. That stops
/// the resync clock, so nothing counts as overdue, and the new socket
/// releases both markets.
#[tokio::test]
async fn a_deadline_passing_while_the_socket_is_down_is_not_overdue() {
    let server = MockServer::start().await;
    let (recorder, mut gapped) = recording_one_shard(&server, SHORT_TIMEOUT).await;
    open_gap(&mut gapped, "A").await;
    gapped.client.close().await;
    tokio::time::sleep(SHORT_TIMEOUT + WELL_PAST).await;
    let mut reconnected = Shard::accept(&server, 2).await;
    reconnected.send_books().await;
    raise(&mut reconnected, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![reconnected]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let outcome = (finished.of("A"), resync_counts(&counters));
    assert_eq!(outcome, (resynced_then_raised(), (0, 0, 0)));
}

/// A answers and B is removed: nothing is left to wait for, so the resync
/// completes and the deadline passes without a reconnect.
#[tokio::test]
async fn removing_the_last_unanswered_market_completes_the_resync() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, resync_config(2, GENEROUS_TIMEOUT));
    let mut shard = record_a_and_b(&server, &mut pipeline).await;
    let request = open_gap(&mut shard, "A").await;
    answer(&mut shard, &request, "A").await;
    pipeline.coordinator.remove_market("B".into()).await;
    let remove = shard.client.next_command().await;
    let quiet = GENEROUS_TIMEOUT + WELL_PAST;
    shard.client.assert_no_command(quiet).await;
    let handshakes = server.handshake_attempts();
    let counters = pipeline.coordinator.counters();
    shut_down(pipeline, shard).await;
    let outcome = (remove.cmd.as_str(), handshakes, resync_counts(&counters));
    assert_eq!(outcome, ("remove", 1, (1, 0, 0)));
}

/// C joins the shard during a resync of A and B. The resync completes on
/// A's and B's snapshots alone, and C records from its subscribe snapshot.
#[tokio::test]
async fn a_market_added_during_a_resync_is_not_held() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, resync_config(3, Duration::from_secs(5)));
    let mut shard = record_a_and_b(&server, &mut pipeline).await;
    let request = open_gap(&mut shard, "A").await;
    let markets = vec![market("C", MarketStatus::Open)];
    pipeline.coordinator.add_markets(markets).await;
    let add = shard.client.next_command().await;
    answer(&mut shard, &request, "A").await;
    answer(&mut shard, &request, "B").await;
    pipeline.output.wait_for(4, 0).await;
    let completed = pipeline.coordinator.counters().read().resyncs_completed;
    let ack = frames::ack(shard.sub, shard.next_seq());
    shard.client.reply(&add, ack).await;
    send_book(&mut shard, "C").await;
    raise(&mut shard, "C").await;
    pipeline.output.wait_for(5, 1).await;
    let finished = shut_down(pipeline, shard).await;
    let raised = vec![sent_book(), Shape::Update(Side::Buy, 400_000, 110)];
    let outcome = (add.symbols, completed, finished.of("C"));
    assert_eq!(outcome, (vec!["C".to_owned()], 1, raised));
}

/// A's and B's answers arrive in time but wait behind a writer that is
/// held until well past the deadline. They are handled before the deadline
/// is, so the resync completes in time and nothing is overdue.
#[tokio::test]
async fn answers_queued_when_the_deadline_passes_complete_the_resync() {
    let server = MockServer::start().await;
    let config = resync_config(3, GENEROUS_TIMEOUT);
    let (mut pipeline, gate) = gated_pipeline(&server, config);
    let mut shard = record_a_and_b(&server, &mut pipeline).await;
    let request = open_gap(&mut shard, "A").await;
    block_on_the_writer(&mut pipeline, &mut shard, &gate).await;
    answer(&mut shard, &request, "A").await;
    answer(&mut shard, &request, "B").await;
    tokio::time::sleep(GENEROUS_TIMEOUT + WELL_PAST).await;
    gate.send_replace(true);
    pipeline.output.wait_for(5, 2).await;
    let handshakes = server.handshake_attempts();
    let counters = pipeline.coordinator.counters();
    shut_down(pipeline, shard).await;
    assert_eq!((handshakes, resync_counts(&counters)), (1, (1, 0, 0)));
}

/// A second gap waits behind a writer that is held until well past the
/// first deadline. It is handled before the deadline is and restarts the
/// resync of A, B and now C, so the first deadline no longer stands; the
/// answers to the second request then complete the resync in time.
#[tokio::test]
async fn a_gap_queued_when_the_deadline_passes_restarts_the_resync() {
    let server = MockServer::start().await;
    let config = resync_config(3, GENEROUS_TIMEOUT);
    let (mut pipeline, gate) = gated_pipeline(&server, config);
    let mut shard = record_a_and_b(&server, &mut pipeline).await;
    open_gap(&mut shard, "A").await;
    block_on_the_writer(&mut pipeline, &mut shard, &gate).await;
    shard.seq += 2;
    raise(&mut shard, "A").await;
    tokio::time::sleep(GENEROUS_TIMEOUT + WELL_PAST).await;
    gate.send_replace(true);
    let second = shard.client.next_command().await;
    for symbol in ["A", "B", "C"] {
        answer(&mut shard, &second, symbol).await;
    }
    pipeline.output.wait_for(6, 2).await;
    let handshakes = server.handshake_attempts();
    let counters = pipeline.coordinator.counters();
    shut_down(pipeline, shard).await;
    let asked = (second.cmd.as_str(), second.symbols.len());
    let outcome = (asked, handshakes, resync_counts(&counters));
    assert_eq!(outcome, (("snapshot", 3), 1, (1, 0, 0)));
}
