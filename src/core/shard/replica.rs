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
use crate::core::events::{FeedBody, FeedEvent, Inbound};
use crate::core::ws::traffic::TrafficWindow;
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
#[derive(Debug)]
pub(super) struct ReplicaHealth {
    /// When the current socket connected, while it is up.
    connected_since: Option<Instant>,
    /// Feed events delivered.
    feed_events: u64,
    /// Sequence gaps seen.
    gaps: u64,
    /// The current socket's recent level changes and their lags. Snapshots
    /// are left out: they come when asked for, so they say nothing about
    /// how a socket keeps up with the market.
    traffic: TrafficWindow,
    /// Set when the shard asks the current socket to reconnect, until a new
    /// socket connects.
    reconnect_asked: bool,
}

/// Where a replica's connection runs: the token that shuts it down, its
/// tracing span, and the capacity of the channel its events go through.
struct ReplicaScope {
    cancel: CancellationToken,
    span: tracing::Span,
    capacity: usize,
}

impl Replica {
    /// Spawns `connection` in `scope`, keeping its `health`.
    fn spawn<E, C>(connection: Connection<E, C>, scope: ReplicaScope, health: ReplicaHealth) -> Self
    where
        E: FeedEndpoint + Send + Sync + 'static,
        C: FeedCodec + Send + Sync + 'static,
    {
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let (event_sender, events) = mpsc::channel(scope.capacity);
        let run = connection.run(request_receiver, event_sender, scope.cancel);
        Self {
            requests,
            events,
            task: tokio::spawn(run.instrument(scope.span)),
            health,
            ended: false,
        }
    }

    /// Passes `request` to the connection; a stopped one drops it.
    pub(super) fn request(&self, request: Request) {
        if self.requests.send(request).is_err() {
            tracing::debug!("dropped a request for a stopped connection");
        }
    }

    /// Asks the connection to drop its socket and reconnect.
    pub(super) fn reconnect(&mut self) {
        self.health.reconnect_asked = true;
        self.request(Request::Reconnect);
    }

    /// Whether the replica's socket is up and is not being replaced, so it
    /// may take over.
    pub(super) fn is_ready(&self) -> bool {
        self.is_connected() && !self.health.reconnect_asked
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
    /// Nothing seen yet; recent traffic is counted in buckets of `bucket`.
    fn new(bucket: Duration) -> Self {
        Self {
            connected_since: None,
            feed_events: 0,
            gaps: 0,
            traffic: TrafficWindow::new(Instant::now(), bucket),
            reconnect_asked: false,
        }
    }

    /// Feed events delivered.
    pub(super) fn feed_events(&self) -> u64 {
        self.feed_events
    }

    /// Sequence gaps seen.
    pub(super) fn gaps(&self) -> u64 {
        self.gaps
    }

    /// The level changes the current socket delivered over the window up to
    /// `now`.
    pub(super) fn recent_level_changes(&self, now: Instant) -> u64 {
        self.traffic.count(now)
    }

    /// The median feed lag of the current socket's level changes over the
    /// window up to `now`, if any said when the exchange sent them.
    pub(super) fn median_lag_ms(&self, now: Instant) -> Option<i64> {
        self.traffic.median_lag_ms(now)
    }

    fn observe(&mut self, event: &ConnectionEvent) {
        let now = Instant::now();
        match event {
            ConnectionEvent::Connected => self.on_connected(now),
            ConnectionEvent::Disconnected { .. } => self.connected_since = None,
            ConnectionEvent::Frame { decoded, .. } => {
                if let Inbound::Feed(event) = &decoded.message {
                    self.note_feed(event, now);
                }
            }
            ConnectionEvent::Gap { .. } => self.gaps += 1,
            ConnectionEvent::Rejected { .. } => {}
        }
    }

    /// A new socket: its recent traffic is counted from `now`.
    fn on_connected(&mut self, now: Instant) {
        self.connected_since = Some(now);
        self.reconnect_asked = false;
        self.traffic.restart(now);
    }

    /// Counts feed `event`, seen at `now`. A level change also counts
    /// towards the window, with its lag sampled: the time from the exchange
    /// sending it to its frame arriving.
    fn note_feed(&mut self, event: &FeedEvent, now: Instant) {
        self.feed_events += 1;
        if !matches!(event.body, FeedBody::Level(_)) {
            return;
        }
        self.traffic.note_event(now);
        if let Some(sent) = event.exchange_ts {
            let lag = event.received_at - sent;
            self.traffic.note_lag(now, lag.num_milliseconds());
        }
    }
}

/// Everything needed to start one of a shard's connections.
struct ConnectionRecipe<E, C> {
    endpoint: Arc<E>,
    codec: Arc<C>,
    config: ConnectionConfig,
    capacity: usize,
    /// The length of the buckets each replica's recent traffic is counted
    /// in.
    bucket: Duration,
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
            capacity: self.capacity,
        };
        Replica::spawn(connection, scope, ReplicaHealth::new(self.bucket))
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
            bucket: context.failover.health.bucket,
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

#[cfg(test)]
impl ReplicaFactory {
    /// A factory of replicas with no connection behind them, whose events
    /// a test feeds to [`Replica::observe`] itself.
    pub(super) fn detached() -> Self {
        let start = |_, _| Replica::detached();
        Self {
            start: Box::new(start),
            cancel: CancellationToken::new(),
        }
    }
}

#[cfg(test)]
impl Replica {
    /// A replica with no connection behind it; needs a tokio runtime.
    fn detached() -> Self {
        let (requests, _) = mpsc::unbounded_channel();
        let (_, events) = mpsc::channel(1);
        Self {
            requests,
            events,
            task: tokio::spawn(async {}),
            health: ReplicaHealth::new(Duration::from_secs(1)),
            ended: false,
        }
    }
}
