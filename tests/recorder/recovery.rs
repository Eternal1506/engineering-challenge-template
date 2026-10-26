//! Recovering from lost messages and inconsistent books.

use super::*;

/// A recorder with shards of two over A to D, each book recorded.
async fn recording_two_shards(server: &MockServer) -> (Recorder, Vec<Shard>) {
    let mut recorder = Recorder::start(server, 2);
    recorder.list_open(&["A", "B", "C", "D"]);
    let mut shards = Shard::accept_all(server, 2).await;
    for shard in &mut shards {
        shard.send_books().await;
    }
    recorder.output.wait_for(4, 0).await;
    (recorder, shards)
}

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

/// Skips two sequence numbers on `shard` with a +10 bid change for
/// `symbol`, and returns the snapshot request the gap brings.
async fn open_gap(shard: &mut Shard, symbol: &str) -> mock_exchange::SentCommand {
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
async fn answer(shard: &mut Shard, request: &mock_exchange::SentCommand, symbol: &str) {
    let seq = shard.next_seq();
    let reply = frames::book(shard.sub, seq, symbol, (BIDS, ASKS));
    shard.client.reply(request, reply).await;
}

/// A book as sent, then a fresh copy of it, then the +10 change on it.
fn resynced_then_raised() -> Vec<Shape> {
    vec![
        sent_book(),
        sent_book(),
        Shape::Update(Side::Buy, 400_000, 110),
    ]
}

/// The resync counts: started, completed in time, and reconnected.
fn resync_counts(counters: &RecorderCounters) -> (u64, u64, u64) {
    let counts = counters.read();
    let (started, completed) = (counts.resyncs_started, counts.resyncs_completed);
    (started, completed, counts.resync_reconnects)
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

/// Every snapshot arrives well before the deadline, which then passes
/// without a reconnect.
#[tokio::test]
async fn a_resync_answered_in_time_keeps_the_socket() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, Duration::from_millis(500)).await;
    let request = open_gap(&mut shard, "A").await;
    answer(&mut shard, &request, "A").await;
    answer(&mut shard, &request, "B").await;
    shard.client.assert_no_command(Duration::from_secs(1)).await;
    let handshakes = server.handshake_attempts();
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![shard]);
    recorder.stop().await;
    closed(closes).await;
    assert_eq!((handshakes, resync_counts(&counters)), (1, (1, 1, 0)));
}

/// Only A answers. Once the deadline passes the shard reconnects, and B's
/// subscribe snapshot on the new socket releases it.
#[tokio::test]
async fn a_resync_past_its_deadline_reconnects_and_resubscribes() {
    let server = MockServer::start().await;
    let (recorder, mut gapped) = recording_one_shard(&server, Duration::from_millis(300)).await;
    let request = open_gap(&mut gapped, "A").await;
    answer(&mut gapped, &request, "A").await;
    let mut reconnected = Shard::accept(&server, 2).await;
    reconnected.send_books().await;
    raise(&mut reconnected, "B").await;
    recorder.output.wait_for(5, 1).await;
    let counters = recorder.counters.clone();
    let closes = answer_closes(vec![reconnected]);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let reconnects = counters.read().resync_reconnects;
    assert_eq!((finished.of("B"), reconnects), (resynced_then_raised(), 1));
}

/// The second gap comes before the first deadline and the replies after
/// it, but before the second deadline: no reconnect.
#[tokio::test]
async fn a_second_gap_during_a_resync_restarts_it() {
    let server = MockServer::start().await;
    let (recorder, mut shard) = recording_one_shard(&server, Duration::from_secs(2)).await;
    let first = open_gap(&mut shard, "A").await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let second = open_gap(&mut shard, "B").await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
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
    assert_eq!(outcome, (resynced_then_raised(), 1, (2, 1, 0)));
}

/// Sends a −1000 bid change for C, which has only 100 at that price, and
/// returns the snapshot request the exchange gets for it.
async fn make_c_inconsistent(shards: &mut [Shard]) -> SnapshotRequest {
    let owner = owner_of(shards, "C");
    owner.send_level("C", ("buy", 400_000, -1000)).await;
    let command = owner.client.next_command().await;
    SnapshotRequest {
        sub: owner.sub,
        command,
    }
}

/// The command the exchange received after an inconsistent book, and the
/// subscription of the shard it came from.
struct SnapshotRequest {
    sub: u64,
    command: mock_exchange::SentCommand,
}

#[tokio::test]
async fn an_inconsistent_book_requests_a_snapshot_of_that_market_on_its_shard() {
    let server = MockServer::start().await;
    let (recorder, mut shards) = recording_two_shards(&server).await;
    let seen = make_c_inconsistent(&mut shards).await;
    let closes = answer_closes(shards);
    recorder.stop().await;
    closed(closes).await;
    let command = seen.command;
    assert_eq!(
        (command.cmd.as_str(), command.sub, command.symbols),
        ("snapshot", Some(seen.sub), vec!["C".to_owned()])
    );
}

#[tokio::test]
async fn an_inconsistent_book_sends_nothing_to_the_other_shards() {
    let server = MockServer::start().await;
    let (recorder, mut shards) = recording_two_shards(&server).await;
    make_c_inconsistent(&mut shards).await;
    let other = shards.iter_mut().find(|shard| !shard.owns("C")).unwrap();
    other
        .client
        .assert_no_command(Duration::from_millis(300))
        .await;
    let closes = answer_closes(shards);
    recorder.stop().await;
    closed(closes).await;
}

#[tokio::test]
async fn the_snapshot_that_answers_the_request_is_recorded() {
    let server = MockServer::start().await;
    let (recorder, mut shards) = recording_two_shards(&server).await;
    let seen = make_c_inconsistent(&mut shards).await;
    let owner = owner_of(&mut shards, "C");
    let seq = owner.next_seq();
    let reply = frames::book(seen.sub, seq, "C", (BIDS, ASKS));
    owner.client.reply(&seen.command, reply).await;
    recorder.output.wait_for(5, 1).await;
    let closes = answer_closes(shards);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let expected = vec![
        sent_book(),
        Shape::Update(Side::Buy, 400_000, 0),
        sent_book(),
    ];
    assert_eq!(finished.of("C"), expected);
}
