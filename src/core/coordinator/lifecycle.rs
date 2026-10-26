//! [`LifecycleLink`]: the coordinator's connection to the exchange's market
//! lifecycle channel.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::core::events::{Decoded, Inbound, LifecycleEvent};
use crate::core::shard::ShardContext;
use crate::core::ws::{Connection, ConnectionEvent, Request, Subscription};
use crate::exchange::{FeedCodec, FeedEndpoint};

/// The capacity of the channel from the lifecycle connection to the
/// coordinator.
const EVENT_CAPACITY: usize = 4_096;

/// What the lifecycle connection tells the coordinator.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum LifecycleNews {
    /// A market's lifecycle changed.
    Changed(LifecycleEvent),
    /// Lifecycle events may have been lost: the connection saw a sequence
    /// gap or came back after a disconnect.
    Missed,
    /// Nothing the coordinator acts on.
    Nothing,
}

/// A running lifecycle connection.
pub(super) struct LifecycleLink {
    events: mpsc::Receiver<ConnectionEvent>,
    /// Held so the connection keeps running; it never gets a request.
    _requests: mpsc::UnboundedSender<Request>,
    task: JoinHandle<()>,
    connected_before: bool,
}

impl LifecycleLink {
    /// Starts a connection to `endpoint` listening for lifecycle changes,
    /// with the codec, connection config and a child of the cancellation
    /// token of `context`.
    pub(super) fn spawn<L, E, C>(endpoint: L, context: &ShardContext<E, C>) -> Self
    where
        L: FeedEndpoint + Send + Sync + 'static,
        C: FeedCodec + Send + Sync + 'static,
    {
        let connection = Connection::new(endpoint, context.codec.clone(), Subscription::Lifecycle)
            .with_config(context.connection.clone());
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let (event_sender, events) = mpsc::channel(EVENT_CAPACITY);
        let run = connection.run(request_receiver, event_sender, context.cancel.child_token());
        let task = tokio::spawn(run.instrument(tracing::info_span!("lifecycle")));
        Self {
            events,
            _requests: requests,
            task,
            connected_before: false,
        }
    }

    /// The connection's next event; `None` once it has stopped.
    pub(super) async fn next(&mut self) -> Option<ConnectionEvent> {
        self.events.recv().await
    }

    /// What `event` means for the coordinator, logging what it should
    /// know.
    pub(super) fn interpret(&mut self, event: ConnectionEvent) -> LifecycleNews {
        match event {
            ConnectionEvent::Frame { decoded, .. } => on_frame(decoded),
            ConnectionEvent::Connected => self.on_connected(),
            ConnectionEvent::Disconnected { reason } => {
                tracing::warn!(reason, "the lifecycle connection was lost, reconnecting");
                LifecycleNews::Nothing
            }
            ConnectionEvent::Gap { expected, got, .. } => {
                tracing::warn!(expected, got, "lifecycle events were lost");
                LifecycleNews::Missed
            }
            ConnectionEvent::Rejected { kind, .. } => {
                tracing::warn!(error = ?kind, "the exchange rejected the lifecycle subscribe");
                LifecycleNews::Nothing
            }
        }
    }

    /// Stops waiting for events and returns the connection's task, which
    /// ends once the connection has closed. Cancel the context's token
    /// first.
    pub(super) fn into_task(self) -> JoinHandle<()> {
        self.task
    }

    fn on_connected(&mut self) -> LifecycleNews {
        tracing::info!("listening for market lifecycle changes");
        if std::mem::replace(&mut self.connected_before, true) {
            return LifecycleNews::Missed;
        }
        LifecycleNews::Nothing
    }
}

/// A lifecycle change, or nothing. Logs an error the exchange sent
/// unprompted; the connection logs errors that answer a command.
fn on_frame(decoded: Decoded) -> LifecycleNews {
    match decoded.message {
        Inbound::Lifecycle(event) => return LifecycleNews::Changed(event),
        Inbound::Error { sub, kind } if decoded.reply_to.is_none() => {
            let sub = sub.map(|sub| sub.0);
            tracing::warn!(sub, error = ?kind, "the exchange reported an error");
        }
        _ => {}
    }
    LifecycleNews::Nothing
}
