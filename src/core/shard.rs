//! A shard: a fixed group of symbols recorded over its own exchange
//! connections, its replicas, with a supervisor task between them and the
//! writer.
//!
//! Every replica subscribes the same symbols. One, the primary, feeds the
//! writer; the others are standbys, whose events only tell the supervisor
//! how they are doing. The primary starts at once and the standbys
//! [`FailoverTimings::standby_delay`] after it first connects.
//!
//! The supervisor sends every feed event of the primary on to the writer,
//! in arrival order, waiting for room rather than dropping. When the shard
//! is cancelled it keeps sending until the connections' last events are
//! delivered; only the context's abandon token makes it give up on events
//! the writer has not taken.
//!
//! When the primary's socket is lost, a standby connected for at least
//! [`FailoverTimings::warmup`] takes over at once: it becomes the primary,
//! and the shard resyncs on it as after a sequence gap, below. Its first
//! forwarded message for each symbol is then a snapshot from its own
//! socket, so nothing the old primary delivered, or failed to, is applied
//! twice or lost. The old primary reconnects as a standby. While the
//! primary is down, a standby still warming up takes over once it is warm,
//! and a standby that connects becomes the primary. A primary the shard
//! itself reconnected, below, is not failed over at once, since its
//! resubscribe is the point; it is failed over only if it is not back within
//! the reconnect grace. A primary whose task ends is replaced like a lost one, and a
//! shard left without any connection stops.
//!
//! It answers a sequence gap with a resync. Every symbol of the shard is
//! held back and one snapshot request for all of them goes out. A held-back
//! symbol's level changes are dropped, since they apply to a book that
//! missed messages, until any snapshot of it arrives; that snapshot is
//! forwarded and releases the symbol, independently of the others. Another
//! gap restarts the resync.
//!
//! Symbols still held back after [`ResyncTimings::timeout`] stay held: they
//! may be markets the exchange dropped, which never answer and never
//! change. A change to one of them after the deadline shows it is live and
//! still stale, so the supervisor asks for its snapshot again, at most once
//! per [`ResyncTimings::resnapshot_interval`]. Only a change once the
//! resync has lasted [`ResyncTimings::reconnect_after`], on a socket that
//! has been the primary for [`ResyncTimings::primary_cooldown`], reconnects
//! the primary, whose resubscribe brings a fresh snapshot of every symbol.
//! Any new primary socket releases every symbol, for the same reason.
//!
//! It also turns the coordinator's [`ShardCommand`]s into connection
//! [`Request`](crate::core::ws::Request)s: adds and removals for every
//! replica, snapshot requests for the primary.

mod failover;
mod inputs;
mod replica;
mod resync;
mod supervisor;

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::core::stats::RecorderCounters;
use crate::core::writer::WriterMessage;
use crate::core::ws::ConnectionConfig;
use crate::exchange::{FeedCodec, FeedEndpoint};
use failover::ReplicaSet;
use replica::ReplicaFactory;
use supervisor::Supervisor;

/// The default capacity of the channel from a shard's connection to its
/// supervisor.
pub const DEFAULT_EVENT_CAPACITY: usize = 65_536;

/// How a shard waits for the snapshots a resync asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncTimings {
    /// How long the snapshots may take before the resync is overdue.
    pub timeout: Duration,
    /// After the deadline, the least time between two requests for one
    /// held market's snapshot, each made when the market changes.
    pub resnapshot_interval: Duration,
    /// How long a resync must have lasted before a change to a held market
    /// reconnects the primary.
    pub reconnect_after: Duration,
    /// How long a socket must have been the primary before a change to a
    /// held market may reconnect it.
    pub primary_cooldown: Duration,
}

impl Default for ResyncTimings {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            resnapshot_interval: Duration::from_secs(10),
            reconnect_after: Duration::from_secs(60),
            primary_cooldown: Duration::from_secs(60),
        }
    }
}

/// The default time from a shard's primary first connecting to its
/// standbys starting.
pub const DEFAULT_STANDBY_DELAY: Duration = Duration::from_secs(30);

/// The default time a standby must have been connected to take over.
pub const DEFAULT_FAILOVER_WARMUP: Duration = Duration::from_secs(5);

/// The default time a primary the shard reconnected has to come back before
/// a standby takes over: the default liveness timeout, within which a
/// healthy reconnect names its subscription.
pub const DEFAULT_RECONNECT_GRACE: Duration = Duration::from_secs(30);

/// When a shard's standbys start and may take over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverTimings {
    /// How long after the primary first connects the standbys start, which
    /// spreads the handshakes and the sockets' lifetimes apart.
    pub standby_delay: Duration,
    /// How long a standby must have been connected to take over from a
    /// primary whose socket was lost.
    pub warmup: Duration,
    /// How long a primary the shard asked to reconnect has to connect again
    /// before a warm standby takes over.
    pub reconnect_grace: Duration,
}

impl Default for FailoverTimings {
    fn default() -> Self {
        Self {
            standby_delay: DEFAULT_STANDBY_DELAY,
            warmup: DEFAULT_FAILOVER_WARMUP,
            reconnect_grace: DEFAULT_RECONNECT_GRACE,
        }
    }
}

/// What a new shard records, and over how many connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardPlan {
    /// The symbols to record.
    pub symbols: BTreeSet<Arc<str>>,
    /// How many connections subscribe them: one primary, the rest
    /// standbys.
    pub replicas: NonZeroUsize,
}

/// A shard's identity, unique for the life of a coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub u64);

/// Something the coordinator asks a shard to do.
#[derive(Debug)]
pub enum ShardCommand {
    /// Record these symbols too.
    Add(Vec<Arc<str>>),
    /// Stop recording these symbols.
    Remove(Vec<Arc<str>>),
    /// Ask the exchange for a fresh snapshot of this symbol.
    Resnapshot(Arc<str>),
}

/// What every shard of one recorder shares.
#[derive(Debug)]
pub struct ShardContext<E, C> {
    /// The exchange's feed endpoint.
    pub endpoint: Arc<E>,
    /// The exchange's feed codec.
    pub codec: Arc<C>,
    /// Timings and limits for each shard's connection.
    pub connection: ConnectionConfig,
    /// The capacity of the channel from a connection to its supervisor.
    pub event_capacity: usize,
    /// How each shard waits for the snapshots a resync asks for.
    pub resync: ResyncTimings,
    /// When each shard's standbys start and may take over.
    pub failover: FailoverTimings,
    /// The writer's input channel.
    pub writer: mpsc::Sender<WriterMessage>,
    /// The counters the shards keep.
    pub counters: Arc<RecorderCounters>,
    /// The parent of every shard's cancellation token.
    pub cancel: CancellationToken,
    /// Fired to make every shard stop waiting for the writer, dropping the
    /// events it has not taken.
    pub abandon: CancellationToken,
}

/// A running shard: its connection and supervisor tasks.
#[derive(Debug)]
pub struct ShardHandle {
    commands: mpsc::UnboundedSender<ShardCommand>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
    replicas: NonZeroUsize,
}

impl ShardHandle {
    /// Starts shard `id`, recording `plan`'s symbols over its primary
    /// connection now and its standbys once the primary has connected.
    pub fn spawn<E, C>(id: ShardId, plan: ShardPlan, context: &ShardContext<E, C>) -> Self
    where
        E: FeedEndpoint + Send + Sync + 'static,
        C: FeedCodec + Send + Sync + 'static,
    {
        let span = tracing::info_span!("shard", id = id.0);
        let cancel = context.cancel.child_token();
        let factory = ReplicaFactory::new(context, cancel.clone(), span.clone());
        let replicas = ReplicaSet::start(factory, &plan, context.failover.clone());
        context.counters.note_socket_started();
        let supervisor = Supervisor::new(replicas, plan.symbols, context);
        let (commands, command_receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(supervisor.run(command_receiver).instrument(span));
        Self {
            commands,
            cancel,
            task,
            replicas: plan.replicas,
        }
    }

    /// How many connections the shard runs once its standbys have started.
    pub fn replicas(&self) -> usize {
        self.replicas.get()
    }

    /// Passes `command` to the shard. A shard that has stopped ignores it.
    pub fn send(&self, command: ShardCommand) {
        if self.commands.send(command).is_err() {
            tracing::debug!("dropped a command for a stopped shard");
        }
    }

    /// Starts closing the shard's connections and returns the task that
    /// ends once they have closed and every event they delivered has gone
    /// to the writer, or the context's abandon token fires.
    pub fn close(self) -> JoinHandle<()> {
        self.cancel.cancel();
        self.task
    }
}
