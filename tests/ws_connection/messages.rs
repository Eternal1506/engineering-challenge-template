//! Forwarding messages: sequence gaps and receive timestamps.

use super::*;

fn gap(sub: u64, expected: u64, got: u64) -> ConnectionEvent {
    ConnectionEvent::Gap {
        sub: SubId(sub),
        expected,
        got,
    }
}

#[tokio::test]
async fn a_seq_jump_emits_a_gap_and_rebaselines() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    for seq in [1, 2, 5, 6] {
        client.send(frames::delta(1, seq, "A")).await;
    }
    assert_eq!(seq(&running.next_frame().await), 1);
    assert_eq!(seq(&running.next_frame().await), 2);
    assert_eq!(running.next_event().await, gap(1, 3, 5));
    assert_eq!(seq(&running.next_frame().await), 5);
    assert_eq!(seq(&running.next_frame().await), 6);
}

#[tokio::test]
async fn a_frame_that_fails_to_decode_shows_as_a_gap_on_the_next_frame() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    client.send(frames::delta(1, 1, "A")).await;
    client.send_text("{not json").await;
    client.send(frames::delta(1, 3, "A")).await;
    assert_eq!(seq(&running.next_frame().await), 1);
    assert_eq!(running.next_event().await, gap(1, 2, 3));
    assert_eq!(seq(&running.next_frame().await), 3);
}

#[tokio::test]
async fn received_at_is_stamped_before_decoding() {
    let server = MockServer::start().await;
    let mut running = Setup::new(books(&["A"])).start(&server);
    let mut client = server.accept().await;
    subscribe(&mut client, &mut running, 1).await;
    client.send(frames::delta(1, 1, "A")).await;
    let Inbound::Feed(event) = running.next_frame().await.message else {
        panic!("expected a feed event");
    };
    let decode_started = *running.codec.decode_times().last().unwrap();
    assert!(
        event.received_at <= decode_started,
        "{} > {decode_started}",
        event.received_at
    );
}
