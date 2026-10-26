//! Shutting the whole pipeline down on cancel.

use super::*;

const SEVEN: [&str; 7] = ["A", "B", "C", "D", "E", "F", "G"];

/// A recorder with `config` over seven markets, each book recorded.
async fn recording_seven_with(
    server: &MockServer,
    config: CoordinatorConfig,
) -> (Recorder, Vec<Shard>) {
    let shard_count = SEVEN.len().div_ceil(config.shard_size) as u64;
    let mut recorder = Recorder::start_with(server, config);
    recorder.list_open(&SEVEN);
    let mut shards = Shard::accept_all(server, shard_count).await;
    for shard in &mut shards {
        shard.send_books().await;
    }
    recorder.output.wait_for(7, 0).await;
    (recorder, shards)
}

/// A recorder with shards of three over seven markets, each book recorded.
async fn recording_seven(server: &MockServer) -> (Recorder, Vec<Shard>) {
    recording_seven_with(server, config(3)).await
}

#[tokio::test]
async fn cancel_stops_the_whole_pipeline_within_two_seconds() {
    let server = MockServer::start().await;
    let (recorder, shards) = recording_seven(&server).await;
    let closes = answer_closes(shards);
    let (took, _) = recorder.stop().await;
    closed(closes).await;
    assert!(took < Duration::from_secs(2), "{took:?}");
}

#[tokio::test]
async fn the_recording_is_complete_after_cancel() {
    let server = MockServer::start().await;
    let (recorder, shards) = recording_seven(&server).await;
    let closes = answer_closes(shards);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    assert_eq!(
        (finished.messages.len(), finished.stats.snapshots_written),
        (7, 7)
    );
}

#[tokio::test]
async fn cancel_is_bounded_by_the_close_timeout_when_the_exchange_never_answers() {
    let mut config = config(3);
    config.connection.close_timeout = Duration::from_millis(200);
    let server = MockServer::start().await;
    let (recorder, _silent_shards) = recording_seven_with(&server, config).await;
    let (took, _) = recorder.stop().await;
    assert!(took < Duration::from_millis(1500), "{took:?}");
}

#[tokio::test]
async fn cancel_during_the_listing_stops_within_two_seconds() {
    let server = MockServer::start().await;
    let recorder = Recorder::start(&server, 3);
    recorder.pages().send_open(&["A", "B", "C"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(3, 0).await;
    let close = shard.answer_close();
    let (took, _) = recorder.stop().await;
    closed(vec![close]).await;
    assert!(took < Duration::from_secs(2), "{took:?}");
}
