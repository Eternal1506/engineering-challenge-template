//! Subscribe batches, later changes, and rejected subscribes.

use std::collections::BTreeSet;

use engineering_challenge::core::events::FeedErrorKind;

use super::*;

#[tokio::test]
async fn subscribes_in_batches_named_by_the_first_reply_and_acked_after() {
    let server = MockServer::start().await;
    let mut running = Setup::new(numbered_books(1200)).start(&server);
    let mut client = server.accept().await;
    let first = client.next_command().await;
    let first_at = Instant::now();
    client.reply(&first, frames::subscribed(1)).await;
    let second = client.next_command().await;
    let spacing = first_at.elapsed();
    client.reply(&second, frames::ack(1, 1)).await;
    let third = client.next_command().await;
    client.reply(&third, frames::ack(1, 2)).await;

    let batches = [&first, &second, &third];
    assert!(batches.iter().all(|command| command.cmd == "subscribe"));
    let sizes: Vec<usize> = batches.iter().map(|c| c.symbols.len()).collect();
    assert_eq!(sizes, [500, 500, 200]);
    let distinct: BTreeSet<&String> = batches.iter().flat_map(|c| &c.symbols).collect();
    assert_eq!(distinct.len(), 1200);
    assert!(
        spacing >= Duration::from_millis(80),
        "batches {spacing:?} apart"
    );

    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    let replies = [
        running.next_frame().await,
        running.next_frame().await,
        running.next_frame().await,
    ];
    let reply_ids: Vec<Option<u64>> = replies.iter().map(|reply| reply.reply_to).collect();
    assert_eq!(reply_ids, [Some(first.id), Some(second.id), Some(third.id)]);
    assert!(matches!(replies[1].message, Inbound::Ack { .. }));
    client.assert_no_command(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn an_add_during_the_batches_joins_the_next_subscribe() {
    let server = MockServer::start().await;
    let running = Setup::new(numbered_books(600)).start(&server);
    let mut client = server.accept().await;
    let first = client.next_command().await;
    running.request(Request::Add(symbols(&["NEW"])));
    let second = client.next_command().await;
    let sizes = (first.symbols.len(), second.symbols.len());
    assert_eq!((second.cmd.as_str(), sizes), ("subscribe", (500, 101)));
    assert!(second.symbols.contains(&"NEW".to_owned()));
}

#[tokio::test]
async fn an_add_before_the_subscription_is_named_is_another_subscribe() {
    let server = MockServer::start().await;
    let running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    client.next_command().await;
    running.request(Request::Add(symbols(&["B"])));
    let next = client.next_command().await;
    assert_eq!(
        (next.cmd.as_str(), next.sub, next.symbols),
        ("subscribe", None, names(&["B"]))
    );
}

#[tokio::test]
async fn changes_after_the_subscription_is_named_are_updates_on_it() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 7).await;
    running.request(Request::Add(symbols(&["B"])));
    let add = client.next_command().await;
    running.request(Request::Remove(symbols(&["A"])));
    let remove = client.next_command().await;
    assert_eq!(
        (add.cmd.as_str(), add.sub, add.symbols),
        ("add", Some(7), names(&["B"]))
    );
    assert_eq!(
        (remove.cmd.as_str(), remove.sub, remove.symbols),
        ("remove", Some(7), names(&["A"]))
    );
}

#[tokio::test]
async fn removing_every_unsent_symbol_ends_the_batches_and_sends_the_remove() {
    let server = MockServer::start().await;
    let mut running = Setup::new(numbered_books(600)).start(&server);
    let mut client = server.accept().await;
    let first = client.next_command().await;
    let unsent_and_one_sent = (500..600).chain([0]).map(numbered);
    let removed = unsent_and_one_sent.map(Arc::from).collect();
    running.request(Request::Remove(removed));
    client.reply(&first, frames::subscribed(3)).await;
    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    let next = client.next_command().await;
    assert_eq!(
        (next.cmd.as_str(), next.sub, next.symbols),
        ("remove", Some(3), names(&["S0000"]))
    );
}

#[tokio::test]
async fn an_error_reply_to_a_later_batch_is_reported_as_rejected() {
    let server = MockServer::start().await;
    let mut running = Setup::new(numbered_books(600)).start(&server);
    let mut client = server.accept().await;
    let first = client.next_command().await;
    client.reply(&first, frames::subscribed(1)).await;
    let second = client.next_command().await;
    client.reply(&second, frames::error(17)).await;
    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    running.next_frame().await;
    let error = running.next_frame().await;
    let rejected = running.next_event().await;

    let ConnectionEvent::Rejected { symbols, kind } = rejected else {
        panic!("expected a rejection, got {rejected:?}");
    };
    let rejected: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
    assert_eq!(rejected, second.symbols);
    assert!(matches!(kind, FeedErrorKind::Other { code: 17, .. }));
    assert!(matches!(error.message, Inbound::Error { .. }));
    running.assert_no_event(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn an_error_reply_to_the_first_subscribe_reconnects() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut first = server.accept().await;
    let command = first.next_command().await;
    first.reply(&command, frames::error(17)).await;
    running.next_frame().await;
    let rejected = running.next_event().await;
    let mut second = server.accept().await;
    subscribe(&mut second, &mut running, 1).await;
    assert!(
        matches!(&rejected, ConnectionEvent::Rejected { symbols: s, .. } if *s == symbols(&["A"])),
        "{rejected:?}"
    );
}

#[tokio::test]
async fn a_lifecycle_connection_sends_one_lifecycle_subscribe() {
    let server = MockServer::start().await;
    let mut running = Setup::new(Subscription::Lifecycle).start(&server);
    let mut client = server.accept().await;
    let command = client.next_command().await;
    client.reply(&command, frames::subscribed(3)).await;
    assert_eq!(command.cmd, "subscribe_lifecycle");
    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    client.assert_no_command(Duration::from_millis(300)).await;
}
