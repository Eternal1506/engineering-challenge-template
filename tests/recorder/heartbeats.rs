//! The heartbeats the watchdog reads: the coordinator and each running
//! shard supervisor keep beating, even behind a slow writer, and a closed
//! shard is no longer read.

use engineering_challenge::core::watchdog::Heartbeats;
use engineering_challenge::exchange::MarketStatus;
use tokio::sync::watch;

use super::mock_exchange::market;
use super::resync::gated_pipeline;
use super::*;

/// The source name of the first shard's heartbeat.
const FIRST_SHARD: &str = "shard 0";

/// How long each test watches a heartbeat; at one beat a second, it beats
/// at least twice meanwhile.
const WATCH: Duration = Duration::from_millis(2_500);

/// How many times `source` has beaten, if it is registered.
fn beats_of(heartbeats: &Heartbeats, source: &str) -> Option<u64> {
    let reading = heartbeats.read().into_iter().find(|r| &*r.source == source);
    reading.map(|reading| reading.count)
}

/// How many times `source` beats over [`WATCH`].
async fn beats_over_watch(heartbeats: &Heartbeats, source: &str) -> u64 {
    let before = beats_of(heartbeats, source).expect("no such heartbeat");
    tokio::time::sleep(WATCH).await;
    let after = beats_of(heartbeats, source).expect("no such heartbeat");
    after - before
}

/// Has `pipeline`'s coordinator, driven by the test, record market A on
/// one shard, and returns that shard with A's book written.
async fn record_a(server: &MockServer, pipeline: &mut Pipeline) -> Shard {
    let markets = vec![market("A", MarketStatus::Open)];
    pipeline.coordinator.add_markets(markets).await;
    let mut shard = Shard::accept(server, 1).await;
    shard.send_books().await;
    pipeline.output.wait_for(1, 0).await;
    shard
}

/// Shuts `pipeline` down, answering `shard`'s close, and checks the
/// recording.
async fn shut_down(pipeline: Pipeline, shard: Shard) {
    let close = shard.answer_close();
    pipeline.coordinator.shutdown().await;
    closed(vec![close]).await;
    pipeline.output.finish().await;
}

#[tokio::test]
async fn a_running_shard_supervisor_keeps_beating() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, config(3));
    let shard = record_a(&server, &mut pipeline).await;
    let heartbeats = pipeline.coordinator.heartbeats();
    let beats = beats_over_watch(&heartbeats, FIRST_SHARD).await;
    shut_down(pipeline, shard).await;
    assert!(beats >= 2, "{beats} beats");
}

/// Closes `gate` and sends four changes to A on `shard`. The writer takes
/// nothing: the relay holds the first change, its channel the second, and
/// the shard waits to hand over the third.
async fn block_the_writer(shard: &mut Shard, gate: &watch::Sender<bool>) {
    gate.send_replace(false);
    for _ in 0..4 {
        shard.send_level("A", ("buy", 400_000, 10)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn a_shard_waiting_on_a_slow_writer_keeps_beating() {
    let server = MockServer::start().await;
    let (mut pipeline, gate) = gated_pipeline(&server, config(3));
    let mut shard = record_a(&server, &mut pipeline).await;
    block_the_writer(&mut shard, &gate).await;
    let heartbeats = pipeline.coordinator.heartbeats();
    let beats = beats_over_watch(&heartbeats, FIRST_SHARD).await;
    gate.send_replace(true);
    pipeline.output.wait_for(1, 4).await;
    shut_down(pipeline, shard).await;
    assert!(beats >= 2, "{beats} beats");
}

/// Removing A has the coordinator tell the blocked writer to forget it,
/// and it beats as it waits.
#[tokio::test]
async fn the_coordinator_waiting_on_a_slow_writer_keeps_beating() {
    let server = MockServer::start().await;
    let (mut pipeline, gate) = gated_pipeline(&server, config(3));
    let mut shard = record_a(&server, &mut pipeline).await;
    block_the_writer(&mut shard, &gate).await;
    let heartbeats = pipeline.coordinator.heartbeats();
    let Pipeline {
        mut coordinator,
        output,
    } = pipeline;
    let removing = tokio::spawn(async move {
        coordinator.remove_market("A".into()).await;
        coordinator
    });
    let beats = beats_over_watch(&heartbeats, "coordinator").await;
    gate.send_replace(true);
    let coordinator = removing.await.unwrap();
    shard.client.expect_close().await;
    coordinator.shutdown().await;
    output.finish().await;
    assert!(beats >= 2, "{beats} beats");
}

#[tokio::test]
async fn the_coordinator_keeps_beating_while_it_serves() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start(&server, 3);
    recorder.list_open(&["A"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(1, 0).await;
    let beats = beats_over_watch(&recorder.heartbeats, "coordinator").await;
    let close = shard.answer_close();
    recorder.stop().await;
    closed(vec![close]).await;
    assert!(beats >= 2, "{beats} beats");
}

#[tokio::test]
async fn a_closed_shard_supervisor_is_no_longer_read() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, config(3));
    let mut shard = record_a(&server, &mut pipeline).await;
    let heartbeats = pipeline.coordinator.heartbeats();
    pipeline.coordinator.remove_market("A".into()).await;
    shard.client.expect_close().await;
    let deregistered = async {
        while beats_of(&heartbeats, FIRST_SHARD).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let gone = timeout(STEP_TIMEOUT, deregistered).await;
    pipeline.coordinator.shutdown().await;
    pipeline.output.finish().await;
    assert!(gone.is_ok(), "the closed shard's heartbeat is still read");
}
