//! Shutting down: by cancel, or by the owner dropping a channel.

use super::*;

#[tokio::test]
async fn cancel_sends_a_close_frame_and_returns() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    let stopping = tokio::spawn(running.stop());
    client.expect_close().await;
    let took = stopping.await.unwrap();
    assert!(took < Duration::from_secs(2), "{took:?}");
}

#[tokio::test]
async fn cancel_gives_up_on_a_server_that_never_answers_the_close() {
    let config = ConnectionConfig {
        close_timeout: Duration::from_millis(300),
        ..fast_config()
    };
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).config(config).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    let took = running.stop().await;
    assert!(
        took >= Duration::from_millis(300) && took < Duration::from_secs(2),
        "{took:?}"
    );
}

#[tokio::test]
async fn cancel_is_not_held_up_by_an_owner_that_stopped_reading() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).event_capacity(1).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    for seq in 1..=5 {
        client.send(frames::delta(1, seq, "A")).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stopping = tokio::spawn(running.stop());
    client.expect_close().await;
    let took = stopping.await.unwrap();
    assert!(took < Duration::from_secs(2), "{took:?}");
}

/// The default 30 s liveness timeout with a short close timeout, sending
/// the whole subscription as one subscribe.
fn stalling_config() -> ConnectionConfig {
    one_batch(ConnectionConfig {
        close_timeout: Duration::from_millis(300),
        ..ConnectionConfig::default()
    })
}

#[tokio::test]
async fn cancel_ends_a_stalled_write() {
    let server = MockServer::start().await;
    let setup = Setup::new(bulky_books()).config(stalling_config());
    let running = setup.start(&server);
    let _stalled = server.accept().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let took = running.stop().await;
    assert!(took < Duration::from_secs(2), "{took:?}");
}

#[tokio::test]
async fn dropping_the_event_receiver_ends_a_stalled_write() {
    let server = MockServer::start().await;
    let setup = Setup::new(bulky_books()).config(stalling_config());
    let Running {
        events,
        task,
        requests: _requests,
        ..
    } = setup.start(&server);
    let _stalled = server.accept().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(events);
    finish(task).await;
}

#[tokio::test]
async fn dropping_the_request_sender_shuts_down() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    let Running {
        requests,
        task,
        events: _events,
        ..
    } = running;
    drop(requests);
    client.expect_close().await;
    finish(task).await;
}

#[tokio::test]
async fn dropping_the_event_receiver_shuts_down() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    let Running {
        events,
        task,
        requests: _requests,
        ..
    } = running;
    drop(events);
    client.expect_close().await;
    finish(task).await;
}

#[tokio::test]
async fn dropping_the_event_receiver_while_reconnecting_shuts_down() {
    let config = ConnectionConfig {
        backoff_min: Duration::from_secs(10),
        backoff_max: Duration::from_secs(10),
        ..fast_config()
    };
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).config(config).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    client.close().await;
    running.next_disconnect_reason().await;
    let Running {
        events,
        task,
        requests: _requests,
        ..
    } = running;
    drop(events);
    finish(task).await;
}
