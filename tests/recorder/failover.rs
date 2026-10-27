//! Replicas: standbys whose events never reach the writer, the connection
//! budget, and failing over to a standby when the primary disconnects. The
//! helpers here serve [`super::takeover`] too.

use engineering_challenge::exchange::MarketStatus;

use super::mock_exchange::{SentCommand, market};
use super::*;

/// How long a test waits to be sure something does not happen.
pub(super) const QUIET: Duration = Duration::from_millis(300);

/// Shards of `shard_size` on a primary and a standby each, the standby
/// starting as soon as the primary connects and able to take over at once.
pub(super) fn paired(shard_size: usize) -> CoordinatorConfig {
    CoordinatorConfig {
        replicas: NonZeroUsize::new(2).unwrap(),
        ..config(shard_size)
    }
}

/// The exchange's side of one shard's two connections.
pub(super) struct Pair {
    pub(super) primary: Shard,
    pub(super) standby: Shard,
}

/// Accepts a shard's primary, naming its subscription 1, then its standby,
/// which starts only once the primary has connected, naming it 2.
pub(super) async fn accept_pair(server: &MockServer) -> Pair {
    let primary = Shard::accept(server, 1).await;
    let standby = Shard::accept(server, 2).await;
    Pair { primary, standby }
}

/// A recorder with one paired shard over A and B, both books recorded from
/// the primary, using `config`.
pub(super) async fn recording_a_pair_with(
    server: &MockServer,
    config: CoordinatorConfig,
) -> (Recorder, Pair) {
    let mut recorder = Recorder::start_with(server, config);
    recorder.list_open(&["A", "B"]);
    let mut pair = accept_pair(server).await;
    pair.primary.send_books().await;
    recorder.output.wait_for(2, 0).await;
    (recorder, pair)
}

pub(super) async fn recording_a_pair(server: &MockServer) -> (Recorder, Pair) {
    recording_a_pair_with(server, paired(2)).await
}

/// Accepts the next connection and reads its subscribe without answering
/// it, so the connection stays down.
pub(super) async fn accept_unnamed(server: &MockServer) -> (MockClient, SentCommand) {
    let mut client = server.accept().await;
    let command = client.next_command().await;
    assert_eq!(command.cmd, "subscribe");
    (client, command)
}

/// Names the subscription `command` asked for on `client`, as `sub`.
pub(super) async fn name(mut client: MockClient, command: SentCommand, sub: u64) -> Shard {
    client.reply(&command, frames::subscribed(sub)).await;
    Shard {
        client,
        sub,
        symbols: command.symbols,
        seq: 0,
    }
}

/// Answers `client`'s close frame in the background.
pub(super) fn answer_unnamed_close(mut client: MockClient) -> JoinHandle<()> {
    tokio::spawn(async move { client.expect_close().await })
}

/// Sends a +10 bid change for `symbol` at 0.40, where every sent book has
/// 100.
pub(super) async fn raise(shard: &mut Shard, symbol: &str) {
    shard.send_level(symbol, ("buy", 400_000, 10)).await;
}

/// Answers `request` with a book for `symbol` whose 0.40 bid is `bid`.
pub(super) async fn answer_with_bid(
    shard: &mut Shard,
    request: &SentCommand,
    symbol: &str,
    bid: i64,
) {
    let seq = shard.next_seq();
    let bids = [(400_000, bid)];
    let reply = frames::book(shard.sub, seq, symbol, (&bids, ASKS));
    shard.client.reply(request, reply).await;
}

/// Answers `request` with the standard book for `symbol`.
pub(super) async fn answer(shard: &mut Shard, request: &SentCommand, symbol: &str) {
    answer_with_bid(shard, request, symbol, BIDS[0].1).await;
}

/// The book the mock exchange sent, with its 0.40 bid at `bid`.
pub(super) fn book_with_bid(bid: i64) -> Shape {
    Shape::Snapshot(vec![(400_000, bid)], ASKS.to_vec())
}

/// The 0.40 bid became `qty`.
pub(super) fn bid_at(qty: i64) -> Shape {
    Shape::Update(Side::Buy, 400_000, qty)
}

/// A book as sent, then a fresh copy of it, then the +10 change on it.
pub(super) fn resynced_then_raised() -> Vec<Shape> {
    vec![sent_book(), sent_book(), bid_at(110)]
}

/// Closes the primary's socket and returns the snapshot request the
/// standby gets as it takes over.
pub(super) async fn fail_over(pair: &mut Pair) -> SentCommand {
    pair.primary.client.close().await;
    pair.standby.client.next_command().await
}

/// Waits until `holds` is true, failing the test if it takes too long.
pub(super) async fn wait_until(holds: impl Fn() -> bool) {
    let waiting = async {
        while !holds() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    timeout(STEP_TIMEOUT, waiting)
        .await
        .expect("the condition did not hold in time");
}

/// Stops `recorder`, answering the close of each of `shards`, and returns
/// the recording.
pub(super) async fn stop(recorder: Recorder, shards: Vec<Shard>) -> Finished {
    let closes = answer_closes(shards);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    finished
}

#[tokio::test]
async fn a_standbys_books_and_changes_never_reach_the_writer() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair(&server).await;
    pair.standby.send_books().await;
    raise(&mut pair.standby, "A").await;
    raise(&mut pair.primary, "B").await;
    recorder.output.wait_for(2, 1).await;
    tokio::time::sleep(QUIET).await;
    let counters = recorder.counters.clone();
    let up = counters.read();
    let finished = stop(recorder, vec![pair.primary, pair.standby]).await;
    let written = (finished.of("A"), finished.of("B"));
    let expected = (vec![sent_book()], vec![sent_book(), bid_at(110)]);
    assert_eq!(written, expected);
    let forwarded = (finished.stats.events, counters.read().events_forwarded);
    let roles_up = (up.primaries_up, up.standbys_up, up.sockets);
    assert_eq!((forwarded, roles_up), ((3, 3), (1, 1, 2)));
}

/// Every connection counted up while recording is counted down by the
/// time the recorder has stopped.
#[tokio::test]
async fn the_connection_counts_return_to_zero_after_a_stop() {
    let server = MockServer::start().await;
    let (recorder, pair) = recording_a_pair(&server).await;
    let counters = recorder.counters.clone();
    wait_until(|| counters.read().standbys_up == 1).await;
    stop(recorder, vec![pair.primary, pair.standby]).await;
    let counts = counters.read();
    let up = (counts.primaries_up, counts.standbys_up, counts.sockets);
    assert_eq!(up, (0, 0, 0));
}

#[tokio::test]
async fn an_added_market_is_subscribed_on_both_connections() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, paired(2));
    let markets = vec![market("A", MarketStatus::Open)];
    pipeline.coordinator.add_markets(markets).await;
    let mut pair = accept_pair(&server).await;
    pair.primary.send_books().await;
    pipeline.output.wait_for(1, 0).await;
    pipeline
        .coordinator
        .add_markets(vec![market("B", MarketStatus::Open)])
        .await;
    let added = [
        pair.primary.client.next_command().await,
        pair.standby.client.next_command().await,
    ];
    let closes = answer_closes(vec![pair.primary, pair.standby]);
    pipeline.coordinator.shutdown().await;
    closed(closes).await;
    pipeline.output.finish().await;
    let asked = added.map(|command| (command.cmd, command.sub, command.symbols));
    let add_b = |sub| ("add".to_owned(), Some(sub), vec!["B".to_owned()]);
    assert_eq!(asked, [add_b(1), add_b(2)]);
}

#[tokio::test]
async fn a_gap_on_the_standby_requests_nothing_and_holds_nothing() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair(&server).await;
    pair.standby.send_books().await;
    pair.standby.seq += 2;
    raise(&mut pair.standby, "A").await;
    pair.standby.client.assert_no_command(QUIET).await;
    pair.primary.client.assert_no_command(QUIET).await;
    raise(&mut pair.primary, "A").await;
    recorder.output.wait_for(2, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![pair.primary, pair.standby]).await;
    let counts = counters.read();
    let outcome = (finished.of("A"), counts.gaps, counts.resync_drops);
    assert_eq!(outcome, (vec![sent_book(), bid_at(110)], 0, 0));
}

/// Shards of one market each, two connections per shard, and a budget of
/// five. Two paired shards and the lifecycle connection's reserve use it
/// up, so the third shard gets one connection rather than none.
#[tokio::test]
async fn a_shard_beyond_the_connection_budget_gets_one_connection() {
    let server = MockServer::start().await;
    let config = CoordinatorConfig {
        max_connections: 5,
        ..paired(1)
    };
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B", "C"]);
    let mut shards = Vec::new();
    for sub in 1..=5 {
        shards.push(Shard::accept(&server, sub).await);
    }
    let extra = timeout(QUIET, server.accept()).await;
    for shard in &mut shards {
        shard.send_books().await;
    }
    recorder.output.wait_for(3, 0).await;
    let counters = recorder.counters.clone();
    wait_until(|| counters.read().standbys_up == 2).await;
    let counts = counters.read();
    stop(recorder, shards).await;
    assert!(extra.is_err(), "a shard opened a connection too many");
    let up = (counts.primaries_up, counts.standbys_up, counts.sockets);
    let opened = (up, counts.unreplicated_shards);
    assert_eq!((opened, server.handshake_attempts()), (((3, 2, 5), 1), 5));
}

/// Room for one pair beside the lifecycle reserve. B opens a new shard in
/// the same flush that closes A's, so A's pair is not counted and B's shard
/// gets a pair too.
#[tokio::test]
async fn a_shard_closing_in_the_same_flush_leaves_room_for_a_new_pair() {
    let server = MockServer::start().await;
    let config = CoordinatorConfig {
        max_connections: 3,
        ..paired(1)
    };
    let mut pipeline = Pipeline::new(&server, config);
    let a = vec![market("A", MarketStatus::Open)];
    pipeline.coordinator.add_markets(a).await;
    let mut old = accept_pair(&server).await;
    old.primary.send_books().await;
    pipeline.output.wait_for(1, 0).await;
    let b_opens_a_closes = vec![
        market("B", MarketStatus::Open),
        market("A", MarketStatus::Closed),
    ];
    pipeline.coordinator.add_markets(b_opens_a_closes).await;
    let old_closed = answer_closes(vec![old.primary, old.standby]);
    let mut new = accept_pair(&server).await;
    new.primary.send_books().await;
    pipeline.output.wait_for(3, 0).await;
    let unreplicated = pipeline.coordinator.counters().read().unreplicated_shards;
    let new_closed = answer_closes(vec![new.primary, new.standby]);
    pipeline.coordinator.shutdown().await;
    closed(old_closed).await;
    closed(new_closed).await;
    let finished = pipeline.output.finish().await;
    let outcome = (finished.of("B"), unreplicated, server.handshake_attempts());
    assert_eq!(outcome, (vec![sent_book()], 0, 4));
}

/// The standby is asked for a snapshot of every market as it takes over.
/// Its change to A before A's snapshot is dropped; the one after is
/// written on the fresh book.
#[tokio::test]
async fn a_primary_disconnect_fails_over_to_the_standby_with_fresh_snapshots() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair(&server).await;
    let request = fail_over(&mut pair).await;
    let standby = &mut pair.standby;
    raise(standby, "A").await;
    answer(standby, &request, "A").await;
    answer(standby, &request, "B").await;
    raise(standby, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![pair.standby]).await;
    let asked = (request.cmd.as_str(), request.sub, request.symbols);
    let every_market = vec!["A".to_owned(), "B".to_owned()];
    assert_eq!(asked, ("snapshot", Some(2), every_market));
    let counts = counters.read();
    let written = (finished.of("A"), finished.of("B"));
    let expected = (resynced_then_raised(), vec![sent_book(), sent_book()]);
    let failed_over = (counts.failovers.disconnect, counts.resync_drops);
    assert_eq!((written, failed_over), (expected, (1, 1)));
}

/// After the failover, B's change is dropped until B's own snapshot even
/// though A's has arrived.
#[tokio::test]
async fn each_market_waits_for_its_own_snapshot_from_the_new_primary() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair(&server).await;
    let request = fail_over(&mut pair).await;
    let standby = &mut pair.standby;
    answer(standby, &request, "A").await;
    raise(standby, "B").await;
    raise(standby, "A").await;
    answer(standby, &request, "B").await;
    raise(standby, "B").await;
    recorder.output.wait_for(4, 2).await;
    let finished = stop(recorder, vec![pair.standby]).await;
    let written = (finished.of("A"), finished.of("B"));
    assert_eq!(written, (resynced_then_raised(), resynced_then_raised()));
}

/// Both connections carry change d1, +10 to 110; the standby delivers it
/// only after it has taken over, before its snapshot of 110. Then d2, +10,
/// arrives on the new primary. Each change is written once: the book is
/// never 130, and 120 is not missed. The old primary's new socket, now a
/// standby, writes nothing.
#[tokio::test]
async fn no_change_is_applied_twice_or_lost_across_a_failover() {
    let server = MockServer::start().await;
    let (recorder, mut pair) = recording_a_pair(&server).await;
    raise(&mut pair.primary, "A").await;
    recorder.output.wait_for(2, 1).await;
    let request = fail_over(&mut pair).await;
    let standby = &mut pair.standby;
    raise(standby, "A").await;
    answer_with_bid(standby, &request, "A", 110).await;
    raise(standby, "A").await;
    recorder.output.wait_for(3, 2).await;
    let mut old_primary = Shard::accept(&server, 3).await;
    old_primary.send_books().await;
    raise(&mut old_primary, "A").await;
    old_primary.client.assert_no_command(QUIET).await;
    let finished = stop(recorder, vec![pair.standby, old_primary]).await;
    let expected = vec![sent_book(), bid_at(110), book_with_bid(110), bid_at(120)];
    assert_eq!(finished.of("A"), expected);
}

/// Drops the standby's socket and accepts its reconnect without naming it,
/// so the standby stays down, then closes the primary's socket.
async fn lose_both(server: &MockServer, pair: Pair) -> (MockClient, SentCommand) {
    let Pair {
        mut primary,
        mut standby,
    } = pair;
    standby.client.close().await;
    let reconnect = accept_unnamed(server).await;
    primary.client.close().await;
    reconnect
}

/// With the standby down, the primary reconnects as before: its resubscribe
/// snapshots are written, and nothing fails over.
#[tokio::test]
async fn a_primary_disconnect_with_the_standby_down_resubscribes_the_primary() {
    let server = MockServer::start().await;
    let (recorder, pair) = recording_a_pair(&server).await;
    let (standby, _) = lose_both(&server, pair).await;
    let mut primary = Shard::accept(&server, 3).await;
    primary.send_books().await;
    raise(&mut primary, "A").await;
    recorder.output.wait_for(4, 1).await;
    let counters = recorder.counters.clone();
    let standby_closed = answer_unnamed_close(standby);
    let finished = stop(recorder, vec![primary]).await;
    standby_closed.await.unwrap();
    let failovers = counters.read().failovers.disconnect;
    assert_eq!((finished.of("A"), failovers), (resynced_then_raised(), 0));
}

/// The primary is down when the standby's new socket connects: the standby
/// becomes the primary and its subscribe snapshots are written. The old
/// primary's new socket is then a standby and writes nothing.
#[tokio::test]
async fn a_standby_that_connects_while_the_primary_is_down_takes_over() {
    let server = MockServer::start().await;
    let (recorder, pair) = recording_a_pair(&server).await;
    let (client, subscribe) = lose_both(&server, pair).await;
    let mut standby = name(client, subscribe, 3).await;
    standby.send_books().await;
    raise(&mut standby, "A").await;
    recorder.output.wait_for(4, 1).await;
    let mut old_primary = Shard::accept(&server, 4).await;
    old_primary.send_books().await;
    old_primary.client.assert_no_command(QUIET).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![standby, old_primary]).await;
    let failovers = counters.read().failovers.disconnect;
    assert_eq!((finished.of("A"), failovers), (resynced_then_raised(), 1));
}

/// A standby connected for less than the warmup does not take over: the
/// primary resubscribes on its own.
#[tokio::test]
async fn a_standby_still_warming_up_does_not_take_over() {
    let server = MockServer::start().await;
    let config = CoordinatorConfig {
        failover: FailoverTimings {
            warmup: Duration::from_secs(3600),
            ..paired(2).failover
        },
        ..paired(2)
    };
    let (recorder, mut pair) = recording_a_pair_with(&server, config).await;
    pair.primary.client.close().await;
    pair.standby.client.assert_no_command(QUIET).await;
    let mut primary = Shard::accept(&server, 3).await;
    primary.send_books().await;
    recorder.output.wait_for(4, 0).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![primary, pair.standby]).await;
    let failovers = counters.read().failovers.disconnect;
    let expected = vec![sent_book(), sent_book()];
    assert_eq!((finished.of("A"), failovers), (expected, 0));
}
