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

/// After a gap on the shard holding A, what the exchange saw.
struct AfterGap {
    recorder: Recorder,
    /// The shard with the gap, before it reconnected.
    gapped: Shard,
    /// The same shard's new connection, its books and one change sent.
    reconnected: Shard,
    /// The shard without the gap.
    other: Shard,
}

/// Skips two sequence numbers on A's shard with a +10 bid change for A,
/// then serves the reconnect that follows with fresh books and another +10
/// bid change for A.
async fn gap_on_the_shard_of_a(server: &MockServer) -> AfterGap {
    let (recorder, mut shards) = recording_two_shards(server).await;
    let index = shards.iter().position(|shard| shard.owns("A")).unwrap();
    let mut gapped = shards.remove(index);
    gapped.seq += 2;
    gapped.send_level("A", ("buy", 400_000, 10)).await;
    let mut reconnected = Shard::accept(server, 3).await;
    reconnected.send_books().await;
    reconnected.send_level("A", ("buy", 400_000, 10)).await;
    recorder.output.wait_for(6, 1).await;
    let other = shards.pop().unwrap();
    AfterGap {
        recorder,
        gapped,
        reconnected,
        other,
    }
}

#[tokio::test]
async fn a_gap_resubscribes_the_shards_markets_with_fresh_snapshots() {
    let server = MockServer::start().await;
    let after = gap_on_the_shard_of_a(&server).await;
    let resubscribed = after.reconnected.symbols.clone();
    let closes = answer_closes(vec![after.reconnected, after.other]);
    after.recorder.stop().await;
    closed(closes).await;
    assert_eq!(resubscribed, after.gapped.symbols);
}

/// The change after the gap is dropped, since A's book missed messages;
/// the one after the reconnect applies to the fresh book.
#[tokio::test]
async fn level_changes_are_dropped_from_a_gap_until_the_reconnect() {
    let server = MockServer::start().await;
    let after = gap_on_the_shard_of_a(&server).await;
    let closes = answer_closes(vec![after.reconnected, after.other]);
    let counters = after.recorder.counters.clone();
    let (_, finished) = after.recorder.stop().await;
    closed(closes).await;
    let expected = vec![
        sent_book(),
        sent_book(),
        Shape::Update(Side::Buy, 400_000, 110),
    ];
    let dropped = counters.read().resync_drops;
    assert_eq!((finished.of("A"), dropped), (expected, 1));
}

#[tokio::test]
async fn a_gap_leaves_the_other_shards_connected() {
    let server = MockServer::start().await;
    let mut after = gap_on_the_shard_of_a(&server).await;
    after
        .other
        .client
        .assert_no_command(Duration::from_millis(300))
        .await;
    let handshakes = server.handshake_attempts();
    let closes = answer_closes(vec![after.reconnected, after.other]);
    after.recorder.stop().await;
    closed(closes).await;
    assert_eq!(handshakes, 3);
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
