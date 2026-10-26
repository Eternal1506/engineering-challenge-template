//! Snapshot requests and their tagged replies.

use super::*;

fn snapshot_of(tag: u64, names: &[&str]) -> Request {
    Request::RequestSnapshot {
        tag,
        symbols: symbols(names),
    }
}

#[tokio::test]
async fn snapshot_replies_carry_the_request_tag() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A", "B"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 4).await;
    running.request(snapshot_of(42, &["A", "B"]));
    let request = client.next_command().await;
    client.reply(&request, frames::snapshot(4, 1, "A")).await;
    client.reply(&request, frames::snapshot(4, 2, "B")).await;
    client.send(frames::delta(4, 3, "A")).await;
    let replies = [
        running.next_tagged_frame().await,
        running.next_tagged_frame().await,
    ];
    let (_, delta_tag) = running.next_tagged_frame().await;
    assert_eq!(
        (request.cmd.as_str(), request.sub, request.symbols),
        ("snapshot", Some(4), names(&["A", "B"]))
    );
    assert!(
        replies
            .iter()
            .all(|(reply, tag)| reply.reply_to == Some(request.id) && *tag == Some(42))
    );
    assert_eq!(delta_tag, None);
}

#[tokio::test]
async fn a_snapshot_request_names_only_symbols_subscribed_on_this_socket() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A", "B"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 4).await;
    running.request(snapshot_of(1, &["A", "Z"]));
    let request = client.next_command().await;
    assert_eq!(request.symbols, names(&["A"]));
}

#[tokio::test]
async fn a_snapshot_request_before_the_subscription_is_named_is_dropped() {
    let server = MockServer::start().await;
    let running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    client.next_command().await;
    running.request(snapshot_of(1, &["A"]));
    client.assert_no_command(Duration::from_millis(300)).await;
}
