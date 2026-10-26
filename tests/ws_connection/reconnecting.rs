//! Losing a socket, the backoff, and resubscribing on the next one.

use super::*;

/// A config whose liveness timeout is `millis`.
fn liveness(millis: u64) -> ConnectionConfig {
    ConnectionConfig {
        liveness_timeout: Duration::from_millis(millis),
        ..fast_config()
    }
}

#[tokio::test]
async fn a_server_close_reconnects_and_resubscribes_the_current_set() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A", "B"])).start(&server);
    let mut first = server.accept().await;
    subscribe(&mut first, &mut running, 1).await;
    first.send(frames::delta(1, 1, "A")).await;
    first.send(frames::delta(1, 2, "A")).await;
    running.request(Request::Add(symbols(&["C"])));
    running.request(Request::Remove(symbols(&["A"])));
    first.next_command().await;
    first.next_command().await;
    first.close().await;
    assert_eq!(seq(&running.next_frame().await), 1);
    assert_eq!(seq(&running.next_frame().await), 2);

    let reason = running.next_disconnect_reason().await;
    let mut second = server.accept().await;
    let resubscribe = subscribe(&mut second, &mut running, 1).await;
    second.send(frames::delta(1, 1, "B")).await;

    assert!(reason.contains("closed"), "{reason}");
    assert_eq!(
        (first.auth(), second.auth()),
        (Some("attempt-1"), Some("attempt-2"))
    );
    assert_eq!(resubscribe.symbols, names(&["B", "C"]));
    assert_eq!(seq(&running.next_frame().await), 1, "the baseline restarts");
}

#[tokio::test]
async fn changes_requested_while_disconnected_shape_the_resubscribe() {
    let config = ConnectionConfig {
        backoff_min: Duration::from_millis(500),
        backoff_max: Duration::from_millis(500),
        ..fast_config()
    };
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A", "B"])).config(config).start(&server);
    let mut first = server.accept().await;
    subscribe(&mut first, &mut running, 1).await;
    first.close().await;
    running.next_disconnect_reason().await;
    running.request(Request::Add(symbols(&["C"])));
    running.request(Request::Remove(symbols(&["A"])));
    running.request(Request::RequestSnapshot {
        tag: 1,
        symbols: symbols(&["B"]),
    });
    let mut second = server.accept().await;
    let resubscribe = subscribe(&mut second, &mut running, 1).await;
    assert_eq!(resubscribe.symbols, names(&["B", "C"]));
    second.assert_no_command(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn a_socket_lost_before_its_subscribe_reply_reconnects_without_reporting() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut first = server.accept().await;
    first.next_command().await;
    first.close().await;
    let mut second = server.accept().await;
    subscribe(&mut second, &mut running, 1).await;
}

/// Twelve quick failures raise the backoff ceiling to 41 s; a connection
/// that then stays up past `backoff_reset_after` brings it back to 10 ms.
/// Without the reset, the reconnect lands within 10 ms only about 0.02% of
/// the time. Runs on tokio's paused clock, so the delays cost no real time
/// and are exact.
///
/// The lasting socket ends by `Request::Reconnect`, not a server close: on
/// the paused clock, a close frame can sit unread while the clock jumps to
/// the next timer, whereas the dropped sockets below are seen at once.
#[tokio::test(start_paused = true)]
async fn the_backoff_starts_over_after_a_long_enough_connection() {
    let config = ConnectionConfig {
        backoff_min: Duration::from_millis(10),
        backoff_max: Duration::from_secs(3600),
        backoff_reset_after: Duration::from_secs(1),
        ..ConnectionConfig::default()
    };
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).config(config).start(&server);
    for _ in 0..12 {
        let mut client = server.accept_within(Duration::from_secs(60)).await;
        client.next_command().await;
        drop(client);
    }
    let mut lasting = server.accept_within(Duration::from_secs(60)).await;
    subscribe(&mut lasting, &mut running, 1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let ended_at = Instant::now();
    running.request(Request::Reconnect);
    server.accept_within(Duration::from_secs(60)).await;
    let reconnect_after = ended_at.elapsed();
    assert!(
        reconnect_after <= Duration::from_millis(10),
        "{reconnect_after:?}"
    );
}

#[tokio::test]
async fn a_reconnect_request_before_the_subscription_is_named_is_ignored() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    let command = client.next_command().await;
    running.request(Request::Reconnect);
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.reply(&command, frames::subscribed(1)).await;
    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    assert_eq!(server.handshake_attempts(), 1);
}

#[tokio::test]
async fn a_handshake_that_never_finishes_is_retried() {
    let server = MockServer::start().await;
    let setup = Setup::new(books(&["A"])).config(liveness(300));
    let mut running = setup.start(&server);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let attempts = server.handshake_attempts();
    running.assert_no_event(Duration::ZERO).await;
    assert!(attempts >= 2, "{attempts} handshake attempts");
}

#[tokio::test]
async fn a_subscription_not_named_within_the_liveness_timeout_reconnects() {
    let server = MockServer::start().await;
    let setup = Setup::new(books(&["A"])).config(liveness(300));
    let _running = setup.start(&server);
    let mut first = server.accept().await;
    first.next_command().await;
    let started = Instant::now();
    let keep_pinging = async {
        while first.ping().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        std::future::pending::<()>().await;
    };
    tokio::select! {
        _second = server.accept() => {}
        () = keep_pinging => {}
    }
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(250), "{waited:?}");
}

/// Building and starting the 40 MB write takes a debug build over a second
/// before the write's timeout can run out, and several seconds under load,
/// so the reconnect gets far more than the usual step timeout. A write
/// that never times out still never reconnects.
#[tokio::test]
async fn a_write_the_server_never_drains_reconnects() {
    let server = MockServer::start().await;
    let setup = Setup::new(bulky_books()).config(one_batch(liveness(300)));
    let _running = setup.start(&server);
    let _stalled = server.accept().await;
    let started = Instant::now();
    let _second = server.accept_within(Duration::from_secs(30)).await;
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(250), "{waited:?}");
}

#[tokio::test]
async fn silence_past_the_liveness_timeout_reconnects() {
    let server = MockServer::start().await;
    let setup = Setup::new(books(&["A"])).config(liveness(300));
    let mut running = setup.start(&server);
    let mut first = server.accept().await;
    let before_last_frame = Instant::now();
    subscribe(&mut first, &mut running, 1).await;
    let reason = running.next_disconnect_reason().await;
    let silent_for = before_last_frame.elapsed();
    let mut second = server.accept().await;
    subscribe(&mut second, &mut running, 2).await;
    assert!(reason.contains("no frame"), "{reason}");
    assert!(silent_for >= Duration::from_millis(300), "{silent_for:?}");
}

/// The same check at the default 30 s timeout, with tokio's clock paused so
/// the wait takes no real time.
#[tokio::test(start_paused = true)]
async fn thirty_seconds_of_silence_reconnects_with_time_paused() {
    let server = MockServer::start().await;
    let config = ConnectionConfig::default();
    let mut running = Setup::new(books(&["A"])).config(config).start(&server);
    let mut first = server.accept().await;
    let before_last_frame = Instant::now();
    subscribe(&mut first, &mut running, 1).await;
    let event = running.next_event_within(Duration::from_secs(60)).await;
    let silent_for = before_last_frame.elapsed();
    let mut second = server.accept().await;
    subscribe(&mut second, &mut running, 2).await;
    assert!(
        matches!(event, ConnectionEvent::Disconnected { .. }),
        "{event:?}"
    );
    assert!(
        silent_for >= Duration::from_secs(30) && silent_for < Duration::from_secs(31),
        "{silent_for:?}"
    );
}

/// Pings 100 ms apart keep the connection up for twice its liveness
/// timeout. The timeout is long enough that a stall of the test's thread
/// under load does not open a real silence between two pings.
#[tokio::test]
async fn pings_keep_a_quiet_connection_alive() {
    let server = MockServer::start().await;
    let setup = Setup::new(books(&["A"])).config(liveness(1500));
    let mut running = setup.start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    for _ in 0..30 {
        client.ping().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    running.assert_no_event(Duration::ZERO).await;
}

/// The owner stops reading for longer than the liveness timeout while the
/// connection holds a frame for it. Once the owner catches up, the socket is
/// not mistaken for a silent one.
#[tokio::test]
async fn a_slow_owner_is_not_mistaken_for_a_silent_socket() {
    let server = MockServer::start().await;
    let setup = Setup::new(books(&["A"])).config(liveness(300));
    let mut running = setup.event_capacity(1).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    client.send(frames::delta(1, 1, "A")).await;
    client.send(frames::delta(1, 2, "A")).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(seq(&running.next_frame().await), 1);
    assert_eq!(seq(&running.next_frame().await), 2);
    running.assert_no_event(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn an_overflow_error_reconnects() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut first = server.accept().await;
    subscribe(&mut first, &mut running, 1).await;
    first.send(frames::error(25)).await;
    let error = running.next_frame().await;
    let reason = running.next_disconnect_reason().await;
    let mut second = server.accept().await;
    subscribe(&mut second, &mut running, 1).await;
    assert!(matches!(error.message, Inbound::Error { .. }));
    assert!(reason.contains("overflow"), "{reason}");
}

#[tokio::test]
async fn a_reconnect_request_drops_the_socket_and_resubscribes() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut first = server.accept().await;
    subscribe(&mut first, &mut running, 1).await;
    running.request(Request::Reconnect);
    let reason = running.next_disconnect_reason().await;
    let mut second = server.accept().await;
    let resubscribe = subscribe(&mut second, &mut running, 2).await;
    assert_eq!(
        (reason.as_str(), resubscribe.symbols),
        ("requested", names(&["A"]))
    );
}
