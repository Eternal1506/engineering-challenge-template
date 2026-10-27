//! [`Supervisor`]: the task between a shard's connections and the writer.
//! Its replica roles and failovers are in [`roles`], and its takeovers from
//! a connected primary in [`handover`].

mod handover;
mod roles;

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::failover::{ReplicaInput, ReplicaSet};
use super::health::HealthMonitor;
use super::inputs::{Deadlines, Input, Inputs};
use super::resync::{Remedy, Resync};
use super::{ShardCommand, ShardContext};
use crate::core::events::{Decoded, FeedBody, FeedErrorKind, FeedEvent, Inbound};
use crate::core::stats::RecorderCounters;
use crate::core::writer::WriterMessage;
use crate::core::ws::{ConnectionEvent, Request};

/// The task between a shard's connections and the writer.
pub(super) struct Supervisor {
    replicas: ReplicaSet,
    writer: mpsc::Sender<WriterMessage>,
    counters: Arc<RecorderCounters>,
    abandon: CancellationToken,
    /// The symbols the shard records.
    symbols: BTreeSet<Arc<str>>,
    resync: Resync,
    health: HealthMonitor,
    next_tag: u64,
    /// Set when the shard asks its primary to reconnect, until that
    /// primary's socket is lost or a new one connects.
    reconnect_requested: bool,
}

impl Supervisor {
    pub(super) fn new<E, C>(
        replicas: ReplicaSet,
        symbols: BTreeSet<Arc<str>>,
        context: &ShardContext<E, C>,
    ) -> Self {
        Self {
            replicas,
            writer: context.writer.clone(),
            counters: context.counters.clone(),
            abandon: context.abandon.clone(),
            symbols,
            resync: Resync::new(context.resync.clone()),
            health: HealthMonitor::new(context.failover.health.clone()),
            next_tag: 0,
            reconnect_requested: false,
        }
    }

    /// Handles commands, connection events and timers until every
    /// connection ends or the writer is gone or abandoned, then waits for
    /// the connection tasks. If the coordinator drops the shard's handle,
    /// the connections shut down.
    pub(super) async fn run(mut self, commands: mpsc::UnboundedReceiver<ShardCommand>) {
        let mut inputs = Inputs::new(commands);
        loop {
            let due = self.deadlines();
            let Some(input) = inputs.next(&mut self.replicas, due).await else {
                break;
            };
            if self.on_input(input, &mut inputs).await.is_break() {
                break;
            }
        }
        drop(inputs);
        self.finish().await;
    }

    fn deadlines(&self) -> Deadlines {
        Deadlines {
            resync: self.resync.deadline(),
            standbys: self.replicas.standbys_due(),
            promotion: self.replicas.promotion_due(),
            health: self.health_check_due(),
        }
    }

    async fn on_input(&mut self, input: Input, inputs: &mut Inputs) -> ControlFlow<()> {
        match input {
            Input::Command(command) => self.apply(command),
            Input::Uncommanded => {
                inputs.stop_commands();
                self.replicas.cancel_all();
            }
            Input::Replica(input) => return self.on_replica_input(input).await,
            Input::ResyncDeadline => return self.on_resync_deadline().await,
            Input::StandbysDue => self.start_standbys(),
            Input::PromotionDue => self.on_promotion_due(),
            Input::HealthCheck => self.on_health_check(),
        }
        ControlFlow::Continue(())
    }

    async fn on_replica_input(&mut self, input: ReplicaInput) -> ControlFlow<()> {
        match input {
            ReplicaInput::Event { replica, event } => return self.handle(replica, event).await,
            ReplicaInput::Ended {
                replica,
                was_connected,
            } => self.on_replica_ended(replica, was_connected),
        }
        ControlFlow::Continue(())
    }

    /// Starts the standbys on the symbols the shard records now.
    fn start_standbys(&mut self) {
        let started = self.replicas.start_standbys(&self.symbols);
        for _ in 0..started {
            self.counters.note_socket_started();
        }
        if started > 0 {
            tracing::info!(started, "starting the standbys");
        }
    }

    /// Adds and removals go to every replica, so each stays subscribed to
    /// the shard's symbols; snapshot requests go to the primary.
    fn apply(&mut self, command: ShardCommand) {
        match command {
            ShardCommand::Add(symbols) => {
                self.symbols.extend(symbols.iter().cloned());
                self.replicas.request_all(&Request::Add(symbols));
            }
            ShardCommand::Remove(symbols) => {
                self.forget(&symbols);
                self.replicas.request_all(&Request::Remove(symbols));
            }
            ShardCommand::Resnapshot(symbol) => {
                self.request_snapshot(vec![symbol]);
            }
        }
    }

    /// Stops recording `symbols`, which no resync waits for any more.
    fn forget(&mut self, symbols: &[Arc<str>]) {
        for symbol in symbols {
            self.symbols.remove(symbol);
            self.settle(symbol);
        }
    }

    /// Asks the primary for fresh snapshots of `symbols` under a new tag,
    /// and returns the tag.
    fn request_snapshot(&mut self, symbols: Vec<Arc<str>>) -> u64 {
        self.next_tag += 1;
        let tag = self.next_tag;
        self.replicas
            .request_primary(Request::RequestSnapshot { tag, symbols });
        tag
    }

    /// Notes `event`'s bearing on replica `replica`'s health, then handles
    /// it as its role calls for.
    async fn handle(&mut self, replica: usize, event: ConnectionEvent) -> ControlFlow<()> {
        self.replicas.observe(replica, &event);
        if self.replicas.is_primary(replica) {
            return self.handle_primary(event).await;
        }
        self.handle_standby(replica, event);
        ControlFlow::Continue(())
    }

    async fn handle_primary(&mut self, event: ConnectionEvent) -> ControlFlow<()> {
        match event {
            ConnectionEvent::Frame { decoded, tag } => {
                return self.on_message(decoded, tag).await;
            }
            ConnectionEvent::Gap { sub, expected, got } => self.on_gap(sub.0, expected, got),
            ConnectionEvent::Rejected { symbols, kind } => log_rejected(&symbols, &kind),
            ConnectionEvent::Connected => self.on_connected(),
            ConnectionEvent::Disconnected { reason } => self.on_primary_lost(&reason),
        }
        ControlFlow::Continue(())
    }

    /// A standby's events only tell how it is doing: nothing of it reaches
    /// the writer or the resync. A standby that connects while the primary
    /// is down takes over.
    fn handle_standby(&mut self, replica: usize, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => self.on_standby_connected(replica),
            ConnectionEvent::Disconnected { reason } => self.on_standby_lost(replica, &reason),
            ConnectionEvent::Rejected { symbols, kind } => log_rejected(&symbols, &kind),
            ConnectionEvent::Frame { .. } | ConnectionEvent::Gap { .. } => {}
        }
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

    /// Forwards `event`, unless it is a level change of a symbol held back
    /// by a resync. A snapshot releases its symbol.
    async fn on_feed(&mut self, event: FeedEvent) -> ControlFlow<()> {
        match event.body {
            FeedBody::Level(_) if self.resync.holds_back(&event.symbol) => {
                self.drop_held_change(&event.symbol);
                return ControlFlow::Continue(());
            }
            FeedBody::Snapshot { .. } => self.settle(&event.symbol),
            FeedBody::Level(_) => {}
        }
        self.forward(event).await
    }

    /// Drops a level change of `symbol`, which is held back. A change after
    /// the deadline shows the market is live yet still unanswered, so a
    /// standby takes over or its snapshot is asked for again or, once the
    /// resync has gone on too long, the primary reconnects: the new
    /// socket's subscribe snapshots are the sure way to fresh books.
    fn drop_held_change(&mut self, symbol: &Arc<str>) {
        self.counters.note_resync_drop();
        let primary_for = self.replicas.primary_for();
        match self.resync.remedy(symbol, primary_for) {
            Remedy::Wait => {}
            Remedy::Resnapshot => self.on_live_held_market(symbol.clone()),
            Remedy::NewSocket => self.reconnect_primary(symbol),
        }
    }

    /// Asks the primary again for held `symbol`'s snapshot.
    fn resnapshot_held(&mut self, symbol: Arc<str>) {
        self.counters.note_resync_resnapshot();
        let tag = self.request_snapshot(vec![symbol.clone()]);
        tracing::debug!(%symbol, tag, "asking again for a held market's snapshot");
    }

    /// Reconnects the primary because `symbol` is still unanswered. The
    /// loss of its socket that follows is not failed over.
    fn reconnect_primary(&mut self, symbol: &str) {
        self.counters.note_resync_reconnect();
        self.reconnect_requested = true;
        tracing::warn!(%symbol, "a market is still unanswered, reconnecting");
        self.replicas.request_primary(Request::Reconnect);
    }

    /// `symbol` needs no snapshot any more; counts the resync complete if it
    /// was the last one waited for.
    fn settle(&mut self, symbol: &str) {
        if self.resync.settle(symbol) {
            self.counters.note_resync_completed();
            tracing::info!("resynced every market");
        }
    }

    /// Sends `event` to the writer, waiting for room until the abandon token
    /// fires. If the writer has gone or is abandoned, shuts the connections
    /// down.
    async fn forward(&mut self, event: FeedEvent) -> ControlFlow<()> {
        let sent = tokio::select! {
            biased;
            sent = self.writer.send(WriterMessage::Event(event)) => sent.is_ok(),
            () = self.abandon.cancelled() => {
                tracing::warn!("abandoned the events the writer has not taken");
                self.replicas.cancel_all();
                return ControlFlow::Break(());
            }
        };
        if !sent {
            tracing::error!("the writer has stopped, closing the shard");
            self.replicas.cancel_all();
            return ControlFlow::Break(());
        }
        self.counters.note_forwarded();
        ControlFlow::Continue(())
    }

    /// Holds back every symbol and asks for a fresh snapshot of each,
    /// restarting any resync already running.
    fn on_gap(&mut self, sub: u64, expected: u64, got: u64) {
        self.counters.note_gap();
        let tag = self.resync_all();
        let markets = self.symbols.len();
        tracing::warn!(sub, expected, got, tag, markets, "sequence gap, resyncing");
    }

    /// Holds back every symbol until the primary's next snapshot of it and
    /// asks the primary for one of each, restarting any resync already
    /// running. Returns the request's tag.
    fn resync_all(&mut self) -> u64 {
        self.resync.start(&self.symbols);
        let symbols = self.symbols.iter().cloned().collect();
        self.request_snapshot(symbols)
    }

    /// Handles the events already queued when the deadline passed, since
    /// any of them may settle, restart or stop the resync, then marks it
    /// overdue if its deadline still stands.
    async fn on_resync_deadline(&mut self) -> ControlFlow<()> {
        for (replica, queued) in self.replicas.queued().into_iter().enumerate() {
            for _ in 0..queued {
                let Some(event) = self.replicas.try_next(replica) else {
                    break;
                };
                if self.handle(replica, event).await.is_break() {
                    return ControlFlow::Break(());
                }
            }
        }
        let deadline = self.resync.deadline();
        if deadline.is_some_and(|at| at <= Instant::now()) {
            self.mark_resync_overdue();
        }
        ControlFlow::Continue(())
    }

    /// Stops waiting on the clock. The unanswered symbols, which may be
    /// markets the exchange dropped, stay held back until their snapshot,
    /// a change that calls for a new socket, or a new socket.
    fn mark_resync_overdue(&mut self) {
        let unanswered = self.resync.expire();
        self.counters.note_resync_overdue();
        tracing::warn!(unanswered, "the resync timed out, holding the rest");
    }

    /// Counts the connections that were up as down, and waits for their
    /// tasks.
    async fn finish(self) {
        for role in self.replicas.connected_roles() {
            self.counters.note_closed(role);
        }
        let sockets = self.replicas.started_count();
        self.replicas.join().await;
        for _ in 0..sockets {
            self.counters.note_socket_ended();
        }
        tracing::info!("shard stopped");
    }
}

/// Logs the symbols the exchange refused to subscribe.
fn log_rejected(symbols: &[Arc<str>], kind: &FeedErrorKind) {
    let (count, first) = (symbols.len(), symbols.first());
    tracing::debug!(count, ?first, error = ?kind, "the exchange rejected symbols");
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
