//! permessage-deflate: negotiated when on, absent when off, and messages
//! intact either way, against a server that compresses.

use engineering_challenge::core::events::FeedBody;
use mock_exchange::{DeflateClient, DeflateServer, Terms};
use yawc::Options;

use super::*;

/// A book side of `count` levels, large and repetitive enough to compress
/// well and to take several deflate blocks.
fn levels(count: i64) -> Vec<(i64, i64)> {
    (0..count).map(|n| (n * 100, n % 7 + 1)).collect()
}

/// Answers the client's subscribe by naming subscription 1, and checks
/// that the owner hears `Connected` and then the reply.
async fn subscribe_compressing(client: &mut DeflateClient, running: &mut Running) {
    let command = client.next_command().await;
    assert_eq!(command.cmd, "subscribe");
    client.reply(&command, frames::subscribed(1)).await;
    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    let reply = running.next_frame().await;
    assert_eq!(reply.message, Inbound::Subscribed { sub: SubId(1) });
}

#[tokio::test]
async fn compression_is_negotiated_and_messages_arrive_intact() {
    let server = DeflateServer::start().await;
    let mut running = Setup::new(books(&["A"])).start_at(server.endpoint());
    let mut client = server.accept().await;
    subscribe_compressing(&mut client, &mut running).await;
    let (bids, asks) = (levels(20_000), levels(15_000));
    client.send(frames::book(1, 1, "A", (&bids, &asks))).await;
    for seq in 2..=200 {
        client.send(frames::delta(1, seq, "A")).await;
    }
    let Inbound::Feed(book) = running.next_frame().await.message else {
        panic!("expected the book");
    };
    for expected in 2..=200 {
        assert_eq!(seq(&running.next_frame().await), expected);
    }
    running.request(Request::Add(symbols(&["B", "C"])));
    let add = client.next_command().await;

    let deflate = Some("permessage-deflate".to_owned());
    let agreed = Terms {
        offered: deflate.clone(),
        agreed: deflate,
    };
    assert_eq!(client.terms(), &agreed);
    assert_eq!(book.body, FeedBody::Snapshot { bids, asks });
    assert_eq!((add.cmd.as_str(), add.symbols), ("add", names(&["B", "C"])));
}

#[tokio::test]
async fn compression_off_offers_no_extension_and_still_works() {
    let config = ConnectionConfig {
        compression: false,
        ..fast_config()
    };
    let server = DeflateServer::start().await;
    let setup = Setup::new(books(&["A"])).config(config);
    let mut running = setup.start_at(server.endpoint());
    let mut client = server.accept().await;
    subscribe_compressing(&mut client, &mut running).await;
    client.send(frames::delta(1, 1, "A")).await;

    assert_eq!(client.terms(), &Terms::default());
    assert_eq!(seq(&running.next_frame().await), 1);
}

/// The server inflates each of our messages from a fresh context, so they
/// arrive intact only if our compressor resets after each one too.
#[tokio::test]
async fn commands_arrive_intact_when_the_server_wants_our_context_reset() {
    let resetting = Options::default()
        .with_balanced_compression()
        .client_no_context_takeover();
    let server = DeflateServer::start_with(resetting).await;
    let mut running = Setup::new(books(&["A"])).start_at(server.endpoint());
    let mut client = server.accept().await;
    subscribe_compressing(&mut client, &mut running).await;
    running.request(Request::Add(symbols(&["B"])));
    running.request(Request::Add(symbols(&["C"])));
    running.request(Request::Remove(symbols(&["A"])));
    let mut commands = Vec::new();
    for _ in 0..3 {
        let command = client.next_command().await;
        commands.push((command.cmd, command.symbols));
    }

    let agreed = Some("permessage-deflate; client_no_context_takeover".to_owned());
    assert_eq!(client.terms().agreed, agreed);
    let expected = [("add", ["B"]), ("add", ["C"]), ("remove", ["A"])];
    let expected = expected.map(|(cmd, symbols)| (cmd.to_owned(), names(&symbols)));
    assert_eq!(commands, expected);
}

/// A message small on the wire but over the limit once inflated ends the
/// socket with a read error, and the connection comes back.
#[tokio::test]
async fn a_message_over_the_limit_once_inflated_reconnects() {
    let config = ConnectionConfig {
        max_message_bytes: 64 << 10,
        ..fast_config()
    };
    let server = DeflateServer::start().await;
    let mut running = Setup::new(books(&["A"]))
        .config(config)
        .start_at(server.endpoint());
    let mut first = server.accept().await;
    subscribe_compressing(&mut first, &mut running).await;
    let flat = vec![(100, 1); 100_000];
    first.send(frames::book(1, 1, "A", (&flat, &flat))).await;
    let reason = running.next_disconnect_reason().await;
    let mut second = server.accept().await;
    subscribe_compressing(&mut second, &mut running).await;

    assert!(reason.contains("read error"), "{reason}");
}
