//! Spreading markets over shards: filling, streaming the listing, filtering
//! by status, and removing markets.

use engineering_challenge::exchange::{MarketInfo, MarketStatus};

use super::mock_exchange::market;
use super::*;

const SEVEN: [&str; 7] = ["A", "B", "C", "D", "E", "F", "G"];

fn open_markets(names: &[&str]) -> Vec<MarketInfo> {
    names
        .iter()
        .map(|&name| market(name, MarketStatus::Open))
        .collect()
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|&name| name.to_owned()).collect()
}

/// Sends each shard's books, then a bid change of +10 and an ask change of
/// −20 for every market.
async fn send_books_and_changes(shards: &mut [Shard]) {
    for shard in shards {
        shard.send_books().await;
        for symbol in shard.symbols.clone() {
            shard.send_level(&symbol, ("buy", 400_000, 10)).await;
            shard.send_level(&symbol, ("sell", 600_000, -20)).await;
        }
    }
}

#[tokio::test]
async fn seven_markets_on_shards_of_three_use_three_connections() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start(&server, 3);
    recorder.list_open(&SEVEN);
    let mut shards = Shard::accept_all(&server, 3).await;
    send_books_and_changes(&mut shards).await;
    recorder.output.wait_for(7, 14).await;
    let mut sizes: Vec<usize> = shards.iter().map(|shard| shard.symbols.len()).collect();
    sizes.sort_unstable();
    let closes = answer_closes(shards);
    recorder.stop().await;
    closed(closes).await;
    assert_eq!((sizes, server.handshake_attempts()), (vec![1, 3, 3], 3));
}

#[tokio::test]
async fn every_market_is_recorded_as_its_book_then_absolute_level_quantities() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start(&server, 3);
    recorder.list_open(&SEVEN);
    let mut shards = Shard::accept_all(&server, 3).await;
    send_books_and_changes(&mut shards).await;
    recorder.output.wait_for(7, 14).await;
    let closes = answer_closes(shards);
    let (_, finished) = recorder.stop().await;
    closed(closes).await;
    let expected = vec![
        sent_book(),
        Shape::Update(Side::Buy, 400_000, 110),
        Shape::Update(Side::Sell, 600_000, 30),
    ];
    for symbol in SEVEN {
        assert_eq!(finished.of(symbol), expected, "{symbol}");
    }
}

#[tokio::test]
async fn the_first_shard_subscribes_before_the_last_page_is_served() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start(&server, 3);
    recorder.pages().send_open(&["A", "B", "C"]);
    let mut first = Shard::accept(&server, 1).await;
    let subscribed_first = first.symbols.clone();
    recorder.pages().send_open(&["D"]);
    recorder.end_listing();
    let mut second = Shard::accept(&server, 2).await;
    first.send_books().await;
    second.send_books().await;
    recorder.output.wait_for(4, 0).await;
    let closes = answer_closes(vec![first, second]);
    recorder.stop().await;
    closed(closes).await;
    assert_eq!(subscribed_first, names(&["A", "B", "C"]));
}

#[tokio::test]
async fn only_open_and_paused_markets_are_subscribed() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start(&server, 10);
    recorder.pages().send(&[
        ("A", MarketStatus::Open),
        ("B", MarketStatus::Paused),
        ("C", MarketStatus::Closed),
        ("D", MarketStatus::Settled),
        ("E", MarketStatus::Unopened),
    ]);
    recorder.end_listing();
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(2, 0).await;
    let subscribed = shard.symbols.clone();
    let close = shard.answer_close();
    recorder.stop().await;
    closed(vec![close]).await;
    assert_eq!(subscribed, names(&["A", "B"]));
}

/// A coordinator with shards of three recording A to D: A, B and C on one
/// shard, D alone on another, each with its book recorded.
async fn two_shards(server: &MockServer) -> (Pipeline, Vec<Shard>) {
    let mut pipeline = Pipeline::new(server, config(3));
    let markets = open_markets(&["A", "B", "C", "D"]);
    pipeline.coordinator.add_markets(markets).await;
    let mut shards = Shard::accept_all(server, 2).await;
    for shard in &mut shards {
        shard.send_books().await;
    }
    pipeline.output.wait_for(4, 0).await;
    (pipeline, shards)
}

/// Shuts the coordinator down, answering the shards' closes, and checks
/// the recording.
async fn shut_down(pipeline: Pipeline, shards: Vec<Shard>) -> Finished {
    let closes = answer_closes(shards);
    pipeline.coordinator.shutdown().await;
    closed(closes).await;
    pipeline.output.finish().await
}

#[tokio::test]
async fn removing_the_last_market_of_a_shard_closes_that_shard() {
    let server = MockServer::start().await;
    let (mut pipeline, mut shards) = two_shards(&server).await;
    let lone = shards.iter().position(|shard| shard.owns("D")).unwrap();
    let mut lone = shards.remove(lone);
    pipeline.coordinator.remove_market("D".into()).await;
    lone.client.expect_close().await;
    let remaining = pipeline.coordinator.shard_count();
    shut_down(pipeline, shards).await;
    assert_eq!((lone.symbols, remaining), (names(&["D"]), 1));
}

#[tokio::test]
async fn removing_a_market_from_a_shard_with_others_unsubscribes_it() {
    let server = MockServer::start().await;
    let (mut pipeline, mut shards) = two_shards(&server).await;
    pipeline.coordinator.remove_market("A".into()).await;
    let command = owner_of(&mut shards, "B").client.next_command().await;
    shut_down(pipeline, shards).await;
    assert_eq!(
        (command.cmd, command.symbols),
        ("remove".to_owned(), names(&["A"]))
    );
}

/// The next `count` commands the exchange gets on `shard`, as `(cmd,
/// symbols)`.
async fn next_commands(shard: &mut Shard, count: usize) -> Vec<(String, Vec<String>)> {
    let mut commands = Vec::new();
    for _ in 0..count {
        let command = shard.client.next_command().await;
        commands.push((command.cmd, command.symbols));
    }
    commands
}

#[tokio::test]
async fn re_adding_a_market_on_its_old_shard_requests_a_fresh_snapshot() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, config(3));
    pipeline
        .coordinator
        .add_markets(open_markets(&["A", "B", "C"]))
        .await;
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    pipeline.output.wait_for(3, 0).await;
    pipeline.coordinator.remove_market("A".into()).await;
    pipeline.coordinator.add_markets(open_markets(&["A"])).await;
    let commands = next_commands(&mut shard, 3).await;
    shut_down(pipeline, vec![shard]).await;
    let a = names(&["A"]);
    let expected = [("remove", a.clone()), ("add", a.clone()), ("snapshot", a)];
    assert_eq!(
        commands,
        expected.map(|(cmd, symbols)| (cmd.to_owned(), symbols))
    );
}

/// A's old shard holds B and C and D's shard has more room, yet A goes back
/// to its old shard, so its old subscription's last messages come before
/// its new snapshot on one connection.
#[tokio::test]
async fn a_re_added_market_goes_back_to_its_previous_shard() {
    let server = MockServer::start().await;
    let (mut pipeline, mut shards) = two_shards(&server).await;
    pipeline.coordinator.remove_market("A".into()).await;
    pipeline.coordinator.add_markets(open_markets(&["A"])).await;
    let old = next_commands(owner_of(&mut shards, "B"), 3).await;
    let other = owner_of(&mut shards, "D");
    other
        .client
        .assert_no_command(Duration::from_millis(300))
        .await;
    shut_down(pipeline, shards).await;
    let a = names(&["A"]);
    let expected = [("remove", a.clone()), ("add", a.clone()), ("snapshot", a)];
    let expected = expected.map(|(cmd, symbols)| (cmd.to_owned(), symbols));
    assert_eq!(old, expected);
}

/// With shards of two, A and B on one and C on the other: A is removed and
/// X fills its slot, so A comes back on C's shard.
#[tokio::test]
async fn a_re_added_market_whose_old_shard_is_full_records_its_new_book_on_another() {
    let server = MockServer::start().await;
    let mut pipeline = Pipeline::new(&server, config(2));
    let markets = open_markets(&["A", "B", "C"]);
    pipeline.coordinator.add_markets(markets).await;
    let mut shards = Shard::accept_all(&server, 2).await;
    for shard in &mut shards {
        shard.send_books().await;
    }
    pipeline.output.wait_for(3, 0).await;
    pipeline.coordinator.remove_market("A".into()).await;
    pipeline.coordinator.add_markets(open_markets(&["X"])).await;
    pipeline.coordinator.add_markets(open_markets(&["A"])).await;
    let other = owner_of(&mut shards, "C");
    let add = other.client.next_command().await;
    let frame = frames::book(other.sub, other.next_seq(), "A", (BIDS, ASKS));
    other.client.reply(&add, frame).await;
    pipeline.output.wait_for(5, 0).await;
    let finished = shut_down(pipeline, shards).await;
    assert_eq!(
        (add.cmd.as_str(), add.symbols, finished.of("A")),
        ("add", names(&["A"]), vec![sent_book(); 3])
    );
}

#[tokio::test]
async fn a_removed_market_gets_a_final_snapshot() {
    let server = MockServer::start().await;
    let (mut pipeline, shards) = two_shards(&server).await;
    pipeline.coordinator.remove_market("A".into()).await;
    pipeline.output.wait_for(5, 0).await;
    let finished = shut_down(pipeline, shards).await;
    assert_eq!(finished.of("A"), vec![sent_book(), sent_book()]);
}
