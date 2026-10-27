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

/// Five symbols in batches of two, 200 ms apart: three commands, the later
/// ones paced.
#[tokio::test]
async fn a_large_snapshot_request_goes_out_in_paced_batches() {
    let server = MockServer::start().await;
    let interval = Duration::from_millis(200);
    let config = ConnectionConfig {
        snapshot_batch_size: 2,
        snapshot_batch_interval: interval,
        ..fast_config()
    };
    let all = ["A", "B", "C", "D", "E"];
    let mut running = Setup::new(books(&all)).config(config).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 4).await;
    running.request(snapshot_of(9, &all));
    let mut batches = Vec::new();
    for _ in 0..3 {
        let command = client.next_command().await;
        batches.push((Instant::now(), command.cmd, command.symbols));
    }
    let spaced = |pair: &[(Instant, String, Vec<String>)]| pair[1].0 - pair[0].0 >= interval / 2;
    let paced = batches.windows(2).all(spaced);
    let without_times = |(_, cmd, symbols)| (cmd, symbols);
    let sent: Vec<_> = batches.into_iter().map(without_times).collect();
    let snapshot = |list: &[&str]| ("snapshot".to_owned(), names(list));
    let expected = [
        snapshot(&["A", "B"]),
        snapshot(&["C", "D"]),
        snapshot(&["E"]),
    ];
    assert_eq!((sent, paced), (expected.to_vec(), true));
}

/// B is removed while its batch waits: only A's snapshot is asked for.
#[tokio::test]
async fn a_symbol_removed_while_its_batch_waits_is_left_out() {
    let server = MockServer::start().await;
    let config = ConnectionConfig {
        snapshot_batch_size: 1,
        snapshot_batch_interval: Duration::from_millis(300),
        ..fast_config()
    };
    let mut running = Setup::new(books(&["A", "B"])).config(config).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 4).await;
    running.request(snapshot_of(1, &["A", "B"]));
    let first = client.next_command().await;
    running.request(Request::Remove(symbols(&["B"])));
    let remove = client.next_command().await;
    client.assert_no_command(Duration::from_millis(600)).await;
    let sent = [(first.cmd, first.symbols), (remove.cmd, remove.symbols)];
    let expected = [("snapshot", names(&["A"])), ("remove", names(&["B"]))];
    assert_eq!(sent, expected.map(|(cmd, list)| (cmd.to_owned(), list)));
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
