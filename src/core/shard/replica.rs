//! [`Replica`]: one of a shard's connections as its supervisor holds it,
//! with the [`ReplicaHealth`] the supervisor tracks for it, and the
//! [`ReplicaFactory`] that starts them.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::ShardContext;
use crate::core::events::Inbound;
use crate::core::ws::{Connection, ConnectionConfig, ConnectionEvent, Request, Subscription};
use crate::exchange::{FeedCodec, FeedEndpoint};

/// One of a shard's connections: the channel its requests go through, the
/// events it delivers, its task, and how it is doing.
pub(super) struct Replica {
    requests: mpsc::UnboundedSender<Request>,
    events: mpsc::Receiver<ConnectionEvent>,
    task: JoinHandle<()>,
    health: ReplicaHealth,
    /// Set once the connection's event channel has closed.
    ended: bool,
}

/// What a supervisor has seen of one replica.
#[derive(Debug, Default)]
pub(super) struct ReplicaHealth {
    /// When the current socket connected, while it is up.
    connected_since: Option<Instant>,
    /// Feed events delivered.
    feed_events: u64,
    /// Sequence gaps seen.
    gaps: u64,
}

/// Where a replica's connection runs: the token that shuts it down and its
/// tracing span.
struct ReplicaScope {
    cancel: CancellationToken,
    span: tracing::Span,
}

impl Replica {
    /// Spawns `connection` in `scope`, delivering into a channel of
    /// `capacity` events.
    fn spawn<E, C>(connection: Connection<E, C>, capacity: usize, scope: ReplicaScope) -> Self
    where
        E: FeedEndpoint + Send + Sync + 'static,
        C: FeedCodec + Send + Sync + 'static,
    {
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let (event_sender, events) = mpsc::channel(capacity);
        let run = connection.run(request_receiver, event_sender, scope.cancel);
        Self {
            requests,
            events,
            task: tokio::spawn(run.instrument(scope.span)),
            health: ReplicaHealth::default(),
            ended: false,
        }
    }

    /// Passes `request` to the connection; a stopped one drops it.
    pub(super) fn request(&self, request: Request) {
        if self.requests.send(request).is_err() {
            tracing::debug!("dropped a request for a stopped connection");
        }
    }

    /// The channel the connection delivers its events into.
    pub(super) fn events(&mut self) -> &mut mpsc::Receiver<ConnectionEvent> {
        &mut self.events
    }

    /// How many events wait in the channel.
    pub(super) fn queued(&self) -> usize {
        self.events.len()
    }

    /// Notes what `event` says about the replica's health.
    pub(super) fn observe(&mut self, event: &ConnectionEvent) {
        self.health.observe(event);
    }

    /// Whether the replica's socket is up.
    pub(super) fn is_connected(&self) -> bool {
        self.health.connected_since.is_some()
    }

    /// When the replica's socket connected, while it is up.
    pub(super) fn connected_since(&self) -> Option<Instant> {
        self.health.connected_since
    }

    /// How long the replica's socket has been up at `now`, while it is.
    pub(super) fn uptime(&self, now: Instant) -> Option<Duration> {
        let since = self.health.connected_since?;
        Some(now.saturating_duration_since(since))
    }

    /// What the supervisor has seen of the replica.
    pub(super) fn health(&self) -> &ReplicaHealth {
        &self.health
    }

    /// Whether the connection's event channel has closed.
    pub(super) fn is_ended(&self) -> bool {
        self.ended
    }

    /// The connection's event channel closed, so its socket is gone too.
    /// Returns whether the socket was up.
    pub(super) fn end(&mut self) -> bool {
        self.ended = true;
        self.health.connected_since.take().is_some()
    }

    /// Waits for the connection task to end, logging it if it failed.
    pub(super) async fn join(self) {
        if let Err(error) = self.task.await {
            tracing::error!(%error, "the connection task failed");
        }
    }
}

impl ReplicaHealth {
    /// Feed events delivered.
    pub(super) fn feed_events(&self) -> u64 {
        self.feed_events
    }

    /// Sequence gaps seen.
    pub(super) fn gaps(&self) -> u64 {
        self.gaps
    }

    fn observe(&mut self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => self.connected_since = Some(Instant::now()),
            ConnectionEvent::Disconnected { .. } => self.connected_since = None,
            ConnectionEvent::Frame { decoded, .. } if is_feed(&decoded.message) => {
                self.feed_events += 1;
            }
            ConnectionEvent::Gap { .. } => self.gaps += 1,
            ConnectionEvent::Frame { .. } | ConnectionEvent::Rejected { .. } => {}
        }
    }
}

fn is_feed(message: &Inbound) -> bool {
    matches!(message, Inbound::Feed(_))
}

/// Everything needed to start one of a shard's connections.
struct ConnectionRecipe<E, C> {
    endpoint: Arc<E>,
    codec: Arc<C>,
    config: ConnectionConfig,
    capacity: usize,
    /// The shard's token, the parent of each connection's.
    cancel: CancellationToken,
    /// The shard's span, the parent of each connection's.
    shard: tracing::Span,
}

impl<E, C> ConnectionRecipe<E, C>
where
    E: FeedEndpoint + Send + Sync + 'static,
    C: FeedCodec + Send + Sync + 'static,
{
    /// Starts replica `index`, subscribing `symbols`.
    fn start(&self, index: usize, symbols: BTreeSet<Arc<str>>) -> Replica {
        let subscription = Subscription::Books(symbols);
        let connection = Connection::new(self.endpoint.clone(), self.codec.clone(), subscription)
            .with_config(self.config.clone());
        let scope = ReplicaScope {
            cancel: self.cancel.child_token(),
            span: tracing::info_span!(parent: &self.shard, "replica", index),
        };
        Replica::spawn(connection, self.capacity, scope)
    }
}

/// The type-erased start of one replica: its index and its symbols.
type StartReplica = dyn Fn(usize, BTreeSet<Arc<str>>) -> Replica + Send + Sync;

/// Starts a shard's replicas over the context's endpoint and codec, each in
/// a `replica` span inside the shard's, each cancelled with the shard.
pub(super) struct ReplicaFactory {
    start: Box<StartReplica>,
    cancel: CancellationToken,
}

impl ReplicaFactory {
    /// A factory for the shard whose token is `cancel` and whose span is
    /// `shard`.
    pub(super) fn new<E, C>(
        context: &ShardContext<E, C>,
        cancel: CancellationToken,
        shard: tracing::Span,
    ) -> Self
    where
        E: FeedEndpoint + Send + Sync + 'static,
        C: FeedCodec + Send + Sync + 'static,
    {
        let recipe = ConnectionRecipe {
            endpoint: context.endpoint.clone(),
            codec: context.codec.clone(),
            config: context.connection.clone(),
            capacity: context.event_capacity,
            cancel: cancel.clone(),
            shard,
        };
        let start = move |index, symbols| recipe.start(index, symbols);
        Self {
            start: Box::new(start),
            cancel,
        }
    }

    /// Starts replica `index`, subscribing `symbols`.
    pub(super) fn start(&self, index: usize, symbols: BTreeSet<Arc<str>>) -> Replica {
        (self.start)(index, symbols)
    }

    /// Shuts every replica down.
    pub(super) fn cancel_all(&self) {
        self.cancel.cancel();
    }

    /// Whether the shard is shutting down.
    pub(super) fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}
