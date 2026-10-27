//! The generic websocket [`Connection`], driven against the mock exchange
//! over real localhost sockets. Timings are real but short, set through
//! [`ConnectionConfig`], except where tokio's paused clock is noted.

#[path = "mock_exchange/mod.rs"]
mod mock_exchange;

#[path = "ws_connection/compression.rs"]
mod compression;
#[path = "ws_connection/messages.rs"]
mod messages;
#[path = "ws_connection/reconnecting.rs"]
mod reconnecting;
#[path = "ws_connection/shutdown.rs"]
mod shutdown;
#[path = "ws_connection/snapshots.rs"]
mod snapshots;
#[path = "ws_connection/subscribing.rs"]
mod subscribing;

use std::sync::Arc;
use std::time::Duration;

use engineering_challenge::core::events::{Decoded, Inbound, SubId};
use engineering_challenge::core::ws::{
    Connection, ConnectionConfig, ConnectionEvent, Request, Subscription,
};
use mock_exchange::{MockClient, MockCodec, MockEndpoint, MockServer, SentCommand, frames};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// The default config with quick reconnects.
fn fast_config() -> ConnectionConfig {
    ConnectionConfig {
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(200),
        ..ConnectionConfig::default()
    }
}

fn books(names: &[&str]) -> Subscription {
    Subscription::Books(names.iter().map(|&name| Arc::from(name)).collect())
}

fn numbered_books(count: usize) -> Subscription {
    Subscription::Books((0..count).map(|n| Arc::from(numbered(n))).collect())
}

/// 20,000 symbols of 2,000 characters each: a 40 MB subscribe that a
/// server which never reads cannot drain, so the write stalls.
fn bulky_books() -> Subscription {
    let symbol = |n: usize| Arc::from(format!("{n:0>2000}"));
    Subscription::Books((0..20_000).map(symbol).collect())
}

/// A config that sends the whole subscription as one subscribe.
fn one_batch(config: ConnectionConfig) -> ConnectionConfig {
    ConnectionConfig {
        subscribe_batch_size: 1_000_000,
        ..config
    }
}

fn numbered(n: usize) -> String {
    format!("S{n:04}")
}

fn symbols(names: &[&str]) -> Vec<Arc<str>> {
    names.iter().map(|&name| Arc::from(name)).collect()
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|&name| name.to_owned()).collect()
}

fn seq(frame: &Decoded) -> u64 {
    frame.position.expect("a numbered frame").seq
}

/// How to start a connection under test.
struct Setup {
    subscription: Subscription,
    config: ConnectionConfig,
    event_capacity: usize,
}

impl Setup {
    /// `subscription` with [`fast_config`] and a roomy event channel.
    fn new(subscription: Subscription) -> Self {
        Self {
            subscription,
            config: fast_config(),
            event_capacity: 4096,
        }
    }

    fn config(self, config: ConnectionConfig) -> Self {
        Self { config, ..self }
    }

    fn event_capacity(self, event_capacity: usize) -> Self {
        Self {
            event_capacity,
            ..self
        }
    }

    /// Spawns the connection against `server`.
    fn start(self, server: &MockServer) -> Running {
        self.start_at(server.endpoint())
    }

    /// Spawns the connection against `endpoint`.
    fn start_at(self, endpoint: MockEndpoint) -> Running {
        let codec = MockCodec::default();
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let (event_sender, events) = mpsc::channel(self.event_capacity);
        let cancel = CancellationToken::new();
        let connection =
            Connection::new(endpoint, codec.clone(), self.subscription).with_config(self.config);
        let task = tokio::spawn(connection.run(request_receiver, event_sender, cancel.clone()));
        Running {
            codec,
            requests,
            events,
            cancel,
            task,
        }
    }
}

/// A connection running on its own task, and the owner's ends of its
/// channels.
struct Running {
    codec: MockCodec,
    requests: mpsc::UnboundedSender<Request>,
    events: mpsc::Receiver<ConnectionEvent>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Running {
    async fn next_event(&mut self) -> ConnectionEvent {
        self.next_event_within(STEP_TIMEOUT).await
    }

    async fn next_event_within(&mut self, limit: Duration) -> ConnectionEvent {
        timeout(limit, self.events.recv())
            .await
            .expect("no event in time")
            .expect("the connection stopped")
    }

    async fn next_tagged_frame(&mut self) -> (Decoded, Option<u64>) {
        match self.next_event().await {
            ConnectionEvent::Frame { decoded, tag } => (decoded, tag),
            other => panic!("expected a frame, got {other:?}"),
        }
    }

    async fn next_frame(&mut self) -> Decoded {
        self.next_tagged_frame().await.0
    }

    async fn next_disconnect_reason(&mut self) -> String {
        match self.next_event().await {
            ConnectionEvent::Disconnected { reason } => reason,
            other => panic!("expected a disconnect, got {other:?}"),
        }
    }

    /// Fails if an event arrives within `within`.
    async fn assert_no_event(&mut self, within: Duration) {
        if let Ok(event) = timeout(within, self.events.recv()).await {
            panic!("unexpected event {event:?}");
        }
    }

    fn request(&self, request: Request) {
        self.requests.send(request).unwrap();
    }

    /// Cancels the connection and returns how long its task took to end.
    async fn stop(self) -> Duration {
        let started = Instant::now();
        self.cancel.cancel();
        finish(self.task).await;
        started.elapsed()
    }
}

/// Waits for a connection task to end.
async fn finish(task: JoinHandle<()>) {
    timeout(STEP_TIMEOUT, task)
        .await
        .expect("the task did not end")
        .unwrap();
}

/// Answers the client's next command, a subscribe, by naming subscription
/// `sub`, and checks that the owner hears `Connected` and then the reply.
async fn subscribe(client: &mut MockClient, running: &mut Running, sub: u64) -> SentCommand {
    let command = client.next_command().await;
    assert_eq!(command.cmd, "subscribe");
    client.reply(&command, frames::subscribed(sub)).await;
    assert_eq!(running.next_event().await, ConnectionEvent::Connected);
    let reply = running.next_frame().await;
    assert_eq!(reply.message, Inbound::Subscribed { sub: SubId(sub) });
    command
}
