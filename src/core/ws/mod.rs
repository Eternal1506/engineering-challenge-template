//! A generic exchange websocket connection, driven by one tokio task.
//!
//! A [`Connection`] owns one websocket to a [`FeedEndpoint`], speaks to it
//! through a [`FeedCodec`], and keeps it subscribed to its [`Subscription`]
//! across disconnects. Its owner changes the subscription with [`Request`]s
//! and receives every decoded message, plus connection state, sequence gaps
//! and rejected symbols, as [`ConnectionEvent`]s.
//!
//! On each socket:
//!
//! - The handshake uses fresh headers from the endpoint, with TCP_NODELAY.
//! - The subscription goes out as `Subscribe` commands of at most
//!   [`ConnectionConfig::subscribe_batch_size`] symbols, spaced by
//!   [`ConnectionConfig::subscribe_batch_interval`]. The exchange names the
//!   subscription in its reply to the first; the later batches join it.
//!   Symbols added before the exchange names the subscription join the
//!   batches.
//! - Once the batches are out and the subscription is named, changes go out
//!   as `AddSymbols` and `RemoveSymbols` on it.
//! - Every numbered message is checked for contiguity per subscription. The
//!   first number seen is the baseline; a jump is reported as
//!   [`ConnectionEvent::Gap`] and the check continues from the new number.
//!
//! The socket is lost, and the connection reconnects after a jittered
//! exponential backoff, on any of:
//!
//! - silence for [`ConnectionConfig::liveness_timeout`], measured only while
//!   waiting on the socket;
//! - no name for the subscription within the liveness timeout of the first
//!   subscribe, or an error in reply to that subscribe;
//! - a read or write error, or a write stalled for the liveness timeout;
//! - a close from the server, or an overflow error;
//! - a [`Request::Reconnect`].
//!
//! A new socket resubscribes the whole subscription, which resets the
//! subscription id and the sequence baselines.

mod backoff;
mod desired;
mod replies;
mod sequence;
mod session;
mod socket;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::core::events::{Decoded, FeedErrorKind, SubId};
use crate::exchange::{FeedCodec, FeedEndpoint};
use backoff::Backoff;
use desired::Desired;
use session::{Session, Stop};
use socket::Socket;

/// What a connection subscribes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscription {
    /// The order books of a set of symbols, as one subscription.
    Books(BTreeSet<Arc<str>>),
    /// Every market's lifecycle changes. Takes no symbols.
    Lifecycle,
}

/// Something the owner asks a [`Connection`] to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Subscribe these symbols too.
    Add(Vec<Arc<str>>),
    /// Stop subscribing these symbols. Removing a symbol the exchange has
    /// already dropped is harmless.
    Remove(Vec<Arc<str>>),
    /// Ask for fresh snapshots of the symbols already subscribed on the
    /// current socket; the others are left out, since their subscribe brings
    /// a snapshot anyway. The replies arrive as [`ConnectionEvent::Frame`]s
    /// carrying `tag`. Dropped while the socket is down or its subscription
    /// not yet named.
    ///
    /// Tagging is best effort: a reply that comes more than
    /// [`ConnectionConfig::reply_timeout`] after the request, or on a later
    /// socket, arrives untagged.
    RequestSnapshot {
        /// The owner's label for the request, echoed on its replies.
        tag: u64,
        /// The markets to snapshot.
        symbols: Vec<Arc<str>>,
    },
    /// Drop the current socket and reconnect, resubscribing everything.
    /// Ignored while reconnecting and on a new socket whose subscription is
    /// not yet named, which is as fresh as a reconnect would make it.
    Reconnect,
}

/// What a [`Connection`] reports to its owner.
///
/// `Connected` and `Disconnected` alternate, starting with `Connected`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionEvent {
    /// The exchange answered the first subscribe on a new socket.
    Connected,
    /// A socket that had reported `Connected` was lost; the connection is
    /// reconnecting.
    Disconnected {
        /// Why the socket was lost.
        reason: String,
    },
    /// One decoded message, in arrival order.
    Frame {
        /// The message.
        decoded: Decoded,
        /// The tag of the [`Request::RequestSnapshot`] it answers, if any.
        tag: Option<u64>,
    },
    /// Messages were lost on subscription `sub`: the next message should have
    /// been `expected` but was `got`. Sent just before the `Frame` for `got`.
    Gap {
        /// The subscription with the gap.
        sub: SubId,
        /// The sequence number that should have come next.
        expected: u64,
        /// The sequence number that came instead.
        got: u64,
    },
    /// The exchange answered a subscribe or add of `symbols` with an error.
    /// The symbols stay wanted until the owner removes them. Sent just after
    /// the error's `Frame`.
    Rejected {
        /// The symbols the rejected command named; empty for a lifecycle
        /// subscribe.
        symbols: Vec<Arc<str>>,
        /// The error.
        kind: FeedErrorKind,
    },
}

/// Timings and limits for a [`Connection`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionConfig {
    /// How long a socket may stay silent, pings included, before it is
    /// treated as dead. Also bounds the handshake, the wait for the
    /// subscription's name, and each write.
    pub liveness_timeout: Duration,
    /// The first reconnect backoff ceiling.
    pub backoff_min: Duration,
    /// The largest reconnect backoff ceiling.
    pub backoff_max: Duration,
    /// How long a socket must stay up for the backoff to start over.
    pub backoff_reset_after: Duration,
    /// The most symbols in one `Subscribe` command.
    pub subscribe_batch_size: usize,
    /// The pause between `Subscribe` commands.
    pub subscribe_batch_interval: Duration,
    /// How long to keep waiting for a command's replies.
    pub reply_timeout: Duration,
    /// How long a clean close on shutdown may take.
    pub close_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            liveness_timeout: Duration::from_secs(30),
            backoff_min: Duration::from_millis(250),
            backoff_max: Duration::from_secs(10),
            backoff_reset_after: Duration::from_secs(60),
            subscribe_batch_size: 500,
            subscribe_batch_interval: Duration::from_millis(100),
            reply_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(2),
        }
    }
}

/// One exchange websocket that stays subscribed across disconnects.
///
/// Built with [`Connection::new`] and driven by [`Connection::run`] on its
/// own task.
pub struct Connection<E, C> {
    endpoint: E,
    codec: C,
    config: ConnectionConfig,
    desired: Desired,
    ids: CommandIds,
}

/// The channels between a connection and its owner.
struct Channels {
    requests: mpsc::UnboundedReceiver<Request>,
    events: mpsc::Sender<ConnectionEvent>,
    cancel: CancellationToken,
}

/// Hands out command ids, unique for the life of a connection.
#[derive(Debug, Default)]
struct CommandIds(u64);

impl CommandIds {
    fn next(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

impl<E: FeedEndpoint, C: FeedCodec> Connection<E, C> {
    /// A connection to `endpoint`, speaking `codec`, that will subscribe to
    /// `subscription`, with the default [`ConnectionConfig`].
    pub fn new(endpoint: E, codec: C, subscription: Subscription) -> Self {
        Self {
            endpoint,
            codec,
            config: ConnectionConfig::default(),
            desired: Desired::new(subscription),
            ids: CommandIds::default(),
        }
    }

    /// The same connection with `config` instead.
    pub fn with_config(self, config: ConnectionConfig) -> Self {
        Self { config, ..self }
    }

    /// Connects, subscribes and forwards events to `events` until `cancel`
    /// fires, `requests` is closed or `events` is closed. Reconnects
    /// whenever the socket is lost. On shutdown it sends a close frame and
    /// waits for the server's, for at most
    /// [`ConnectionConfig::close_timeout`].
    ///
    /// Waits on `events` rather than dropping when the owner falls behind,
    /// except once shutting down.
    pub async fn run(
        mut self,
        requests: mpsc::UnboundedReceiver<Request>,
        events: mpsc::Sender<ConnectionEvent>,
        cancel: CancellationToken,
    ) {
        let mut channels = Channels {
            requests,
            events,
            cancel,
        };
        let config = &self.config;
        let mut backoff = Backoff::new(
            config.backoff_min,
            config.backoff_max,
            config.backoff_reset_after,
        );
        while let Stop::Lost(_) = self.attempt(&mut channels, &mut backoff).await {
            let delay = backoff.next_delay();
            if self.wait(delay, &mut channels).await.is_err() {
                return;
            }
        }
    }

    /// Opens one socket and serves it until it stops.
    async fn attempt(&mut self, channels: &mut Channels, backoff: &mut Backoff) -> Stop {
        let socket = match self.open(channels).await {
            Ok(socket) => socket,
            Err(stop) => return stop,
        };
        let opened_at = Instant::now();
        let stop = self.session(socket, channels).run().await;
        backoff.note_uptime(opened_at.elapsed());
        stop
    }

    async fn open(&self, channels: &Channels) -> Result<Socket, Stop> {
        let handshake =
            tokio::time::timeout(self.config.liveness_timeout, socket::open(&self.endpoint));
        let failure = tokio::select! {
            () = channels.cancel.cancelled() => return Err(Stop::Shutdown),
            () = channels.events.closed() => return Err(Stop::Shutdown),
            result = handshake => match result {
                Ok(Ok(socket)) => return Ok(socket),
                Ok(Err(error)) => format!("{error:#}"),
                Err(_) => "the handshake timed out".to_owned(),
            },
        };
        tracing::warn!(url = self.endpoint.url(), error = %failure, "could not connect");
        Err(Stop::Lost(failure))
    }

    fn session<'c>(&'c mut self, socket: Socket, channels: &'c mut Channels) -> Session<'c, C> {
        let parts = session::Parts {
            codec: &self.codec,
            config: &self.config,
            desired: &mut self.desired,
            ids: &mut self.ids,
            channels,
        };
        Session::new(parts, socket)
    }

    /// Sleeps for `delay` before a reconnect, still applying subscription
    /// changes. Fails if the connection should shut down instead.
    async fn wait(&mut self, delay: Duration, channels: &mut Channels) -> Result<(), Stop> {
        let until = Instant::now() + delay;
        loop {
            tokio::select! {
                biased;
                () = channels.cancel.cancelled() => return Err(Stop::Shutdown),
                () = channels.events.closed() => return Err(Stop::Shutdown),
                request = channels.requests.recv() => {
                    self.apply_offline(request.ok_or(Stop::Shutdown)?);
                }
                () = tokio::time::sleep_until(until) => return Ok(()),
            }
        }
    }

    /// Applies `request` while no socket is open. Snapshot and reconnect
    /// requests are moot then: the next socket resubscribes everything.
    fn apply_offline(&mut self, request: Request) {
        match request {
            Request::Add(symbols) => self.desired.add(symbols),
            Request::Remove(symbols) => self.desired.remove(&symbols),
            Request::RequestSnapshot { .. } | Request::Reconnect => {
                tracing::debug!(?request, "ignored a request while reconnecting");
            }
        }
    }
}
