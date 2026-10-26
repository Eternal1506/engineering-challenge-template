//! Recovering inconsistent books.

use super::*;

/// A recorder with shards of two over A to D, each book recorded.
pub(super) async fn recording_two_shards(server: &MockServer) -> (Recorder, Vec<Shard>) {
    let mut recorder = Recorder::start(server, 2);
    recorder.list_open(&["A", "B", "C", "D"]);
    let mut shards = Shard::accept_all(server, 2).await;
    for shard in &mut shards {
        shard.send_books().await;
    }
    recorder.output.wait_for(4, 0).await;
    (recorder, shards)
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
