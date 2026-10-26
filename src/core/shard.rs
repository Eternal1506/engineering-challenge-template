//! A shard: a fixed group of symbols recorded over its own exchange
//! connection, with a supervisor task between that connection and the
//! writer.
//!
//! The supervisor sends every feed event on to the writer, in arrival order,
//! waiting for room rather than dropping. When the shard is cancelled it
//! keeps sending until the connection's last event is delivered; only the
//! context's abandon token makes it give up on events the writer has not
//! taken.
//!
//! It answers a sequence gap by reconnecting the shard's connection, whose
//! resubscribe brings a fresh snapshot of every symbol. From the gap until
//! the connection is back, level changes are dropped, since they apply to
//! books that missed messages; snapshots still pass. It also turns the
//! coordinator's [`ShardCommand`]s into connection [`Request`]s.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::core::events::{Decoded, FeedBody, FeedEvent, Inbound};
use crate::core::stats::RecorderCounters;
use crate::core::writer::WriterMessage;
use crate::core::ws::{Connection, ConnectionConfig, ConnectionEvent, Request, Subscription};
use crate::exchange::{FeedCodec, FeedEndpoint};

/// The default capacity of the channel from a shard's connection to its
/// supervisor.
pub const DEFAULT_EVENT_CAPACITY: usize = 65_536;

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
}

impl ShardHandle {
    /// Starts shard `id`, subscribing `symbols` over a new connection.
    pub fn spawn<E, C>(
        id: ShardId,
        symbols: BTreeSet<Arc<str>>,
        context: &ShardContext<E, C>,
    ) -> Self
    where
        E: FeedEndpoint + Send + Sync + 'static,
        C: FeedCodec + Send + Sync + 'static,
    {
        let span = tracing::info_span!("shard", id = id.0);
        let (link, events) = start_connection(symbols, context, &span);
        let cancel = link.cancel.clone();
        let supervisor = Supervisor::new(link, context);
        let (commands, command_receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(supervisor.run(events, command_receiver).instrument(span));
        Self {
            commands,
            cancel,
            task,
        }
    }

    /// Passes `command` to the shard. A shard that has stopped ignores it.
    pub fn send(&self, command: ShardCommand) {
        if self.commands.send(command).is_err() {
            tracing::debug!("dropped a command for a stopped shard");
        }
    }

    /// Starts closing the shard's connection and returns the task that ends
    /// once the connection has closed and every event it delivered has gone
    /// to the writer, or the context's abandon token fires.
    pub fn close(self) -> JoinHandle<()> {
        self.cancel.cancel();
        self.task
    }
}

/// A supervisor's hold on its connection task.
struct ConnectionLink {
    requests: mpsc::UnboundedSender<Request>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

/// Spawns a connection subscribing `symbols`, in `span`, with a child of the
/// context's cancellation token. Returns the link to it and its events.
fn start_connection<E, C>(
    symbols: BTreeSet<Arc<str>>,
    context: &ShardContext<E, C>,
    span: &tracing::Span,
) -> (ConnectionLink, mpsc::Receiver<ConnectionEvent>)
where
    E: FeedEndpoint + Send + Sync + 'static,
    C: FeedCodec + Send + Sync + 'static,
{
    let cancel = context.cancel.child_token();
    let (requests, request_receiver) = mpsc::unbounded_channel();
    let (event_sender, events) = mpsc::channel(context.event_capacity);
    let run = connection(symbols, context).run(request_receiver, event_sender, cancel.clone());
    let task = tokio::spawn(run.instrument(span.clone()));
    let link = ConnectionLink {
        requests,
        cancel,
        task,
    };
    (link, events)
}

/// A connection over the context's endpoint and codec subscribing
/// `symbols`.
fn connection<E, C>(
    symbols: BTreeSet<Arc<str>>,
    context: &ShardContext<E, C>,
) -> Connection<Arc<E>, Arc<C>>
where
    E: FeedEndpoint,
    C: FeedCodec,
{
    let (endpoint, codec) = (context.endpoint.clone(), context.codec.clone());
    Connection::new(endpoint, codec, Subscription::Books(symbols))
        .with_config(context.connection.clone())
}

/// The next thing a supervisor has to handle.
enum Input {
    /// A command from the coordinator.
    Command(ShardCommand),
    /// The coordinator dropped the shard's handle.
    Uncommanded,
    /// An event from the connection.
    Event(ConnectionEvent),
}

/// Waits for the next command or connection event, commands first; `None`
/// once the connection has ended. Without `commands`, waits for events
/// only.
async fn next_input(
    events: &mut mpsc::Receiver<ConnectionEvent>,
    commands: &mut Option<mpsc::UnboundedReceiver<ShardCommand>>,
) -> Option<Input> {
    let next_command = async {
        match commands {
            Some(commands) => commands.recv().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        command = next_command => Some(command.map_or(Input::Uncommanded, Input::Command)),
        event = events.recv() => event.map(Input::Event),
    }
}

/// The task between a shard's connection and the writer.
struct Supervisor {
    connection: ConnectionLink,
    writer: mpsc::Sender<WriterMessage>,
    counters: Arc<RecorderCounters>,
    abandon: CancellationToken,
    connected: bool,
    /// True from a sequence gap until the connection is back.
    resyncing: bool,
    next_tag: u64,
}

impl Supervisor {
    fn new<E, C>(connection: ConnectionLink, context: &ShardContext<E, C>) -> Self {
        Self {
            connection,
            writer: context.writer.clone(),
            counters: context.counters.clone(),
            abandon: context.abandon.clone(),
            connected: false,
            resyncing: false,
            next_tag: 0,
        }
    }

    /// Handles commands and connection events until the connection ends or
    /// the writer is gone or abandoned, then waits for the connection task.
    /// If the coordinator drops the shard's handle, the connection shuts
    /// down.
    async fn run(
        mut self,
        mut events: mpsc::Receiver<ConnectionEvent>,
        commands: mpsc::UnboundedReceiver<ShardCommand>,
    ) {
        let mut commands = Some(commands);
        while let Some(input) = next_input(&mut events, &mut commands).await {
            if self.on_input(input, &mut commands).await.is_break() {
                break;
            }
        }
        drop(events);
        self.finish().await;
    }

    async fn on_input(
        &mut self,
        input: Input,
        commands: &mut Option<mpsc::UnboundedReceiver<ShardCommand>>,
    ) -> ControlFlow<()> {
        match input {
            Input::Command(command) => self.apply(command),
            Input::Uncommanded => {
                *commands = None;
                self.connection.cancel.cancel();
            }
            Input::Event(event) => return self.handle(event).await,
        }
        ControlFlow::Continue(())
    }

    fn apply(&mut self, command: ShardCommand) {
        let request = match command {
            ShardCommand::Add(symbols) => Request::Add(symbols),
            ShardCommand::Remove(symbols) => Request::Remove(symbols),
            ShardCommand::Resnapshot(symbol) => {
                self.next_tag += 1;
                Request::RequestSnapshot {
                    tag: self.next_tag,
                    symbols: vec![symbol],
                }
            }
        };
        self.request(request);
    }

    fn request(&self, request: Request) {
        if self.connection.requests.send(request).is_err() {
            tracing::debug!("dropped a request for a stopped connection");
        }
    }

    async fn handle(&mut self, event: ConnectionEvent) -> ControlFlow<()> {
        match event {
            ConnectionEvent::Frame { decoded, tag } => {
                return self.on_message(decoded, tag).await;
            }
            ConnectionEvent::Gap { sub, expected, got } => self.on_gap(sub.0, expected, got),
            ConnectionEvent::Rejected { symbols, kind } => {
                let (count, first) = (symbols.len(), symbols.first());
                tracing::debug!(count, ?first, error = ?kind, "the exchange rejected symbols");
            }
            ConnectionEvent::Connected => self.on_connected(),
            ConnectionEvent::Disconnected { reason } => self.on_disconnected(&reason),
        }
        ControlFlow::Continue(())
    }

    /// Forwards feed events. Logs an error the exchange sent unprompted at
    /// warn; the connection already logs errors that answer a command.
    async fn on_message(&mut self, decoded: Decoded, tag: Option<u64>) -> ControlFlow<()> {
        match decoded.message {
            Inbound::Feed(event) => return self.on_feed(event).await,
            Inbound::Error { sub, kind } if decoded.reply_to.is_none() => {
                let sub = sub.map(|sub| sub.0);
                tracing::warn!(sub, error = ?kind, "the exchange reported an error");
            }
            other => tracing::debug!(kind = message_kind(&other), tag, "control message"),
        }
        ControlFlow::Continue(())
    }

    /// Forwards `event`, unless it is a level change during a resync.
    async fn on_feed(&mut self, event: FeedEvent) -> ControlFlow<()> {
        if self.resyncing && matches!(event.body, FeedBody::Level(_)) {
            self.counters.note_resync_drop();
            return ControlFlow::Continue(());
        }
        self.forward(event).await
    }

    /// Sends `event` to the writer, waiting for room until the abandon token
    /// fires. If the writer has gone or is abandoned, shuts the connection
    /// down.
    async fn forward(&mut self, event: FeedEvent) -> ControlFlow<()> {
        let sent = tokio::select! {
            biased;
            sent = self.writer.send(WriterMessage::Event(event)) => sent.is_ok(),
            () = self.abandon.cancelled() => {
                tracing::warn!("abandoned the events the writer has not taken");
                self.connection.cancel.cancel();
                return ControlFlow::Break(());
            }
        };
        if !sent {
            tracing::error!("the writer has stopped, closing the shard");
            self.connection.cancel.cancel();
            return ControlFlow::Break(());
        }
        self.counters.note_forwarded();
        ControlFlow::Continue(())
    }

    fn on_gap(&mut self, sub: u64, expected: u64, got: u64) {
        self.counters.note_gap();
        self.resyncing = true;
        tracing::warn!(sub, expected, got, "sequence gap, reconnecting");
        self.request(Request::Reconnect);
    }

    fn on_connected(&mut self) {
        self.connected = true;
        self.resyncing = false;
        self.counters.note_connected();
        tracing::info!("connected");
    }

    fn on_disconnected(&mut self, reason: &str) {
        self.connected = false;
        self.counters.note_disconnected();
        tracing::info!(reason, "disconnected, reconnecting");
    }

    /// Counts the connection as down if it was up, and waits for its task.
    async fn finish(self) {
        if self.connected {
            self.counters.note_closed();
        }
        if let Err(error) = self.connection.task.await {
            tracing::error!(%error, "the connection task failed");
        }
        tracing::info!("shard stopped");
    }
}

/// A short name for a control message, for logs.
fn message_kind(message: &Inbound) -> &'static str {
    match message {
        Inbound::Feed(_) => "feed",
        Inbound::Subscribed { .. } => "subscribed",
        Inbound::Ack { .. } => "ack",
        Inbound::Unsubscribed { .. } => "unsubscribed",
        Inbound::Error { .. } => "error",
        Inbound::Lifecycle(_) => "lifecycle",
        Inbound::Ignored => "ignored",
    }
}
