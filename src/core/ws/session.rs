//! [`Session`]: one socket's life, from the first subscribe to its loss or
//! shutdown.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::{Instant, Sleep};
use yawc::{Frame, OpCode, WebSocketError};

use super::desired::Desired;
use super::replies::{Answer, Replies};
use super::sequence::SequenceCheck;
use super::snapshots::SnapshotQueue;
use super::socket::{self, Socket};
use super::traffic::{DIAGNOSTIC_BUCKET, TrafficWindow};
use super::{Channels, CommandIds, ConnectionConfig, ConnectionEvent, Request};
use crate::core::events::{Command, Decoded, FeedErrorKind, Inbound, SubId};
use crate::exchange::FeedCodec;

/// The longest prefix of an undecodable frame that is logged.
const FRAME_PREVIEW_CHARS: usize = 200;

/// Why a session ended.
#[derive(Debug)]
pub(super) enum Stop {
    /// The socket was lost or never opened; the connection should reconnect.
    Lost(String),
    /// The connection should close and return.
    Shutdown,
}

/// What a session borrows from its connection.
pub(super) struct Parts<'c, C> {
    pub(super) codec: &'c C,
    pub(super) config: &'c ConnectionConfig,
    pub(super) desired: &'c mut Desired,
    pub(super) ids: &'c mut CommandIds,
    pub(super) channels: &'c mut Channels,
}

/// One open socket and the state that lasts only as long as it does.
pub(super) struct Session<'c, C> {
    parts: Parts<'c, C>,
    socket: Socket,
    /// The subscription the exchange named, once it has.
    sub: Option<SubId>,
    /// True while the opening subscribe batches are still going out.
    batching: bool,
    /// The id of the first subscribe sent on this socket.
    opening_id: Option<u64>,
    /// When the subscription must be named by, once a subscribe is out.
    name_by: Instant,
    sequence: SequenceCheck,
    replies: Replies,
    /// When the socket last delivered a message and the owner took it.
    last_frame: Instant,
    silence: Pin<Box<Sleep>>,
    next_batch_at: Instant,
    snapshots: SnapshotQueue,
    decoded: Vec<Decoded>,
    /// Recent traffic, logged if the exchange reports an overflow.
    traffic: TrafficWindow,
}

type Step = Result<(), Stop>;

impl<'c, C: FeedCodec> Session<'c, C> {
    /// A session on a freshly opened `socket`, which has asked for nothing.
    pub(super) fn new(parts: Parts<'c, C>, socket: Socket) -> Self {
        parts.desired.forget_requests();
        let now = Instant::now();
        let liveness = parts.config.liveness_timeout;
        Self {
            batching: parts.desired.has_unrequested(),
            replies: Replies::new(parts.config.reply_timeout),
            parts,
            socket,
            sub: None,
            opening_id: None,
            name_by: now + liveness,
            sequence: SequenceCheck::default(),
            last_frame: now,
            silence: Box::pin(tokio::time::sleep_until(now + liveness)),
            next_batch_at: now,
            snapshots: SnapshotQueue::new(),
            decoded: Vec::new(),
            traffic: TrafficWindow::new(now, DIAGNOSTIC_BUCKET),
        }
    }

    /// Serves the socket until it is lost, reporting the loss to the owner
    /// if the socket had connected, or until shutdown, closing it cleanly.
    pub(super) async fn run(mut self) -> Stop {
        match self.serve().await {
            Stop::Shutdown => {
                self.socket.close(self.parts.config.close_timeout).await;
                Stop::Shutdown
            }
            Stop::Lost(reason) => self.report_lost(reason).await,
        }
    }

    async fn serve(&mut self) -> Stop {
        loop {
            if let Err(stop) = self.step().await {
                return stop;
            }
        }
    }

    /// Waits for and handles one thing: a shutdown, a request, a due
    /// subscribe or snapshot batch, a websocket message, or a timer running
    /// out.
    async fn step(&mut self) -> Step {
        let batch_at = self.batch_due().then_some(self.next_batch_at);
        let snapshot_at = self.snapshots.due_at();
        let awaiting_name = self.sub.is_none() && self.opening_id.is_some();
        let name_by = awaiting_name.then_some(self.name_by);
        tokio::select! {
            biased;
            () = self.parts.channels.cancel.cancelled() => Err(Stop::Shutdown),
            () = self.parts.channels.events.closed() => Err(Stop::Shutdown),
            request = self.parts.channels.requests.recv() => self.on_request(request).await,
            () = sleep_until_some(batch_at) => self.send_batch().await,
            () = sleep_until_some(snapshot_at) => self.send_snapshot_batch().await,
            message = self.socket.next_frame() => self.on_message(message).await,
            () = self.silence.as_mut() => self.on_silence(),
            () = sleep_until_some(name_by) => Err(unnamed()),
        }
    }

    /// Subscribe batches go out while the opening batches last, and again
    /// for symbols added before the exchange names the subscription.
    fn batch_due(&self) -> bool {
        self.parts.desired.has_unrequested() && (self.batching || self.sub.is_none())
    }

    async fn on_request(&mut self, request: Option<Request>) -> Step {
        match request.ok_or(Stop::Shutdown)? {
            Request::Add(symbols) => self.parts.desired.add(symbols),
            Request::Remove(symbols) => self.parts.desired.remove(&symbols),
            Request::RequestSnapshot { tag, symbols } => {
                self.request_snapshot(tag, symbols);
                return Ok(());
            }
            Request::Reconnect => return self.reconnect(),
        }
        self.sync_changes().await
    }

    /// Ends a connected socket on request. A socket whose subscription is
    /// not yet named is new already, and the naming deadline covers one
    /// that never gets named.
    fn reconnect(&self) -> Step {
        if self.sub.is_none() {
            tracing::debug!("ignored a reconnect request on a socket not yet connected");
            return Ok(());
        }
        Err(Stop::Lost("requested".to_owned()))
    }

    async fn send_batch(&mut self) -> Step {
        let config = self.parts.config;
        let limit = config.subscribe_batch_size;
        if let Some(batch) = self.parts.desired.next_subscribe(limit) {
            let id = self.parts.ids.next();
            self.note_opening(id);
            self.send(batch.into_command(id)).await?;
            self.next_batch_at = Instant::now() + config.subscribe_batch_interval;
        }
        self.sync_changes().await
    }

    /// Remembers the first subscribe, which must name the subscription
    /// within the liveness timeout.
    fn note_opening(&mut self, id: u64) {
        if self.opening_id.is_none() {
            self.opening_id = Some(id);
            self.name_by = Instant::now() + self.parts.config.liveness_timeout;
        }
    }

    /// Ends the opening batches once nothing is left to subscribe, then
    /// sends the changes the subscription is behind on.
    async fn sync_changes(&mut self) -> Step {
        if !self.parts.desired.has_unrequested() {
            self.batching = false;
        }
        self.send_changes().await
    }

    /// Sends the removes and adds the subscription is behind on, once the
    /// opening batches are out and the exchange has named the subscription.
    async fn send_changes(&mut self) -> Step {
        let Some(sub) = self.sub.filter(|_| !self.batching) else {
            return Ok(());
        };
        let remove = |id, symbols| Command::RemoveSymbols { id, sub, symbols };
        let add = |id, symbols| Command::AddSymbols { id, sub, symbols };
        let removed = self.parts.desired.take_removed();
        self.send_update(removed, remove).await?;
        let added = self.parts.desired.take_added();
        self.send_update(added, add).await
    }

    /// Sends the command `build` makes for `symbols`, unless there are none.
    async fn send_update(
        &mut self,
        symbols: Vec<Arc<str>>,
        build: impl FnOnce(u64, Vec<Arc<str>>) -> Command,
    ) -> Step {
        if symbols.is_empty() {
            return Ok(());
        }
        let id = self.parts.ids.next();
        self.send(build(id, symbols)).await
    }

    /// Queues the symbols of a snapshot request that this socket has asked
    /// for, to go out in paced batches.
    fn request_snapshot(&mut self, tag: u64, symbols: Vec<Arc<str>>) {
        let symbols = self.parts.desired.asked_for(symbols);
        if self.sub.is_none() || symbols.is_empty() {
            tracing::debug!(tag, "dropped a snapshot request with nothing to ask yet");
            return;
        }
        let size = self.parts.config.snapshot_batch_size;
        self.snapshots.push(tag, &symbols, size);
    }

    /// Sends the next queued snapshot batch, leaving out symbols removed
    /// since it was queued.
    async fn send_snapshot_batch(&mut self) -> Step {
        let interval = self.parts.config.snapshot_batch_interval;
        let (Some(sub), Some(batch)) = (self.sub, self.snapshots.pop(interval)) else {
            return Ok(());
        };
        let symbols = self.parts.desired.asked_for(batch.symbols);
        if symbols.is_empty() {
            return Ok(());
        }
        let id = self.parts.ids.next();
        let command = Command::RequestSnapshot { id, sub, symbols };
        self.send_tagged(command, Some(batch.tag)).await
    }

    async fn send(&mut self, command: Command) -> Step {
        self.send_tagged(command, None).await
    }

    async fn send_tagged(&mut self, command: Command, tag: Option<u64>) -> Step {
        let text = self.parts.codec.encode(&command);
        self.replies.expect(&command, tag);
        self.write(text).await
    }

    /// Writes `text`; see [`guard_write`].
    async fn write(&mut self, text: String) -> Step {
        let limit = self.parts.config.liveness_timeout;
        let write = self.socket.send_text(text);
        guard_write(self.parts.channels, limit, write).await
    }

    /// Writes out the replies the socket has queued, such as a pong, at
    /// once rather than when the next read runs dry; see [`guard_write`].
    async fn send_replies(&mut self) -> Step {
        let limit = self.parts.config.liveness_timeout;
        let write = self.socket.send_replies();
        guard_write(self.parts.channels, limit, write).await
    }

    /// Handles one websocket message. The silence clock restarts only once
    /// the owner has taken everything in it, so a slow owner never looks
    /// like a silent socket.
    async fn on_message(&mut self, message: Result<Frame, WebSocketError>) -> Step {
        let received_at = Utc::now();
        let step = match message {
            Ok(frame) => self.on_frame(frame, received_at).await,
            Err(WebSocketError::ConnectionClosed) => {
                Err(Stop::Lost("the server ended the stream".to_owned()))
            }
            Err(error) => Err(Stop::Lost(format!("read error: {error}"))),
        };
        self.last_frame = Instant::now();
        step
    }

    async fn on_frame(&mut self, frame: Frame, received_at: DateTime<Utc>) -> Step {
        match frame.opcode() {
            OpCode::Text => match std::str::from_utf8(frame.payload()) {
                Ok(text) => self.on_text(text, received_at).await,
                Err(error) => Err(Stop::Lost(format!(
                    "read error: a text frame is not UTF-8 ({error})"
                ))),
            },
            OpCode::Binary => {
                tracing::warn!("ignored a binary frame");
                Ok(())
            }
            OpCode::Close => Err(Stop::Lost(format!(
                "the server closed the socket: {}",
                socket::describe_close(&frame)
            ))),
            OpCode::Ping => self.send_replies().await,
            _ => Ok(()),
        }
    }

    async fn on_text(&mut self, text: &str, received_at: DateTime<Utc>) -> Step {
        let arrived = Instant::now();
        self.traffic.note_frame(arrived, text.len());
        let mut decoded = std::mem::take(&mut self.decoded);
        decoded.clear();
        let result = match self.parts.codec.decode(text, received_at, &mut decoded) {
            Ok(()) => self.deliver_all(&mut decoded, arrived).await,
            Err(error) => {
                log_undecodable(text, &error);
                Ok(())
            }
        };
        self.decoded = decoded;
        result
    }

    /// Delivers the messages of a frame that arrived at `arrived`.
    async fn deliver_all(&mut self, decoded: &mut Vec<Decoded>, arrived: Instant) -> Step {
        for message in decoded.drain(..) {
            self.deliver(message, arrived).await?;
        }
        Ok(())
    }

    /// Checks one message's sequence and replies, then forwards it, followed
    /// by any rejection it carries. Logs the recent traffic on an overflow.
    async fn deliver(&mut self, decoded: Decoded, arrived: Instant) -> Step {
        self.check_sequence(&decoded).await?;
        self.note_lag(&decoded.message, arrived);
        let Answer { tag, rejected } = self.track_reply(&decoded);
        self.note_subscription(&decoded.message).await?;
        let rejection = rejected.zip(error_kind(&decoded.message));
        if is_overflow(&decoded.message) {
            self.log_overflow();
        }
        let loss = self.loss_reason(&decoded);
        self.emit(ConnectionEvent::Frame { decoded, tag }).await?;
        if let Some((symbols, kind)) = rejection {
            self.emit(ConnectionEvent::Rejected { symbols, kind })
                .await?;
        }
        loss.map_or(Ok(()), |reason| Err(Stop::Lost(reason)))
    }

    async fn check_sequence(&mut self, decoded: &Decoded) -> Step {
        match decoded.position.and_then(|at| self.sequence.observe(at)) {
            Some(gap) => self.emit(gap).await,
            None => Ok(()),
        }
    }

    /// Samples the feed lag of a message, from a frame that arrived at
    /// `arrived`, that says when the exchange made or sent it.
    fn note_lag(&mut self, message: &Inbound, arrived: Instant) {
        let Inbound::Feed(event) = message else {
            return;
        };
        if let Some(exchange_ts) = event.exchange_ts {
            let lag_ms = (event.received_at - exchange_ts).num_milliseconds();
            self.traffic.note_lag(arrived, lag_ms);
        }
    }

    fn track_reply(&mut self, decoded: &Decoded) -> Answer {
        match decoded.reply_to {
            Some(id) => self.replies.on_reply(id, &decoded.message),
            None => Answer::default(),
        }
    }

    /// The first subscribe reply names the subscription and makes the
    /// socket connected.
    async fn note_subscription(&mut self, message: &Inbound) -> Step {
        let (Inbound::Subscribed { sub } | Inbound::Ack { sub, .. }) = message else {
            return Ok(());
        };
        match self.sub {
            None => self.name_subscription(*sub).await,
            Some(named) if named != *sub => {
                tracing::warn!(named = named.0, other = sub.0, "a reply named another sid");
                Ok(())
            }
            Some(_) => Ok(()),
        }
    }

    async fn name_subscription(&mut self, sub: SubId) -> Step {
        self.sub = Some(sub);
        let local_port = self.socket.local_port();
        tracing::info!(sub = sub.0, local_port, "subscribed");
        self.emit(ConnectionEvent::Connected).await?;
        self.sync_changes().await
    }

    /// Why `decoded` ends the socket, if it does: an overflow, or an error
    /// in reply to the first subscribe before the subscription is named.
    fn loss_reason(&self, decoded: &Decoded) -> Option<String> {
        let Inbound::Error { kind, .. } = &decoded.message else {
            return None;
        };
        if *kind == FeedErrorKind::Overflow {
            return Some("the exchange reported an overflow".to_owned());
        }
        let opening = decoded.reply_to.is_some() && decoded.reply_to == self.opening_id;
        (opening && self.sub.is_none())
            .then(|| format!("the exchange rejected the first subscribe: {kind:?}"))
    }

    /// Logs the event channel's depth and the socket's traffic over the last
    /// ten seconds: frames and bytes per second and the median feed lag.
    fn log_overflow(&self) {
        let events = &self.parts.channels.events;
        let traffic = self.traffic.summary(Instant::now());
        tracing::warn!(
            event_channel_depth = events.max_capacity() - events.capacity(),
            frames_per_sec = traffic.frames_per_sec.round() as u64,
            bytes_per_sec = traffic.bytes_per_sec.round() as u64,
            median_lag_ms = traffic.median_lag_ms,
            "overflow: the socket's last ten seconds"
        );
    }

    /// The silence timer fired: either the socket has been silent for the
    /// whole liveness timeout, or a frame came since and the timer moves on.
    fn on_silence(&mut self) -> Step {
        let timeout = self.parts.config.liveness_timeout;
        let deadline = self.last_frame + timeout;
        if deadline <= Instant::now() {
            return Err(Stop::Lost(format!("no frame for {timeout:?}")));
        }
        self.silence.as_mut().reset(deadline);
        Ok(())
    }

    async fn report_lost(&mut self, reason: String) -> Stop {
        tracing::warn!(%reason, "lost the connection");
        if self.sub.is_none() {
            return Stop::Lost(reason);
        }
        let event = ConnectionEvent::Disconnected {
            reason: reason.clone(),
        };
        match self.emit(event).await {
            Ok(()) => Stop::Lost(reason),
            Err(stop) => stop,
        }
    }

    /// Sends `event` to the owner, waiting for room; fails if the owner is
    /// gone or the connection is shutting down, dropping the event.
    async fn emit(&mut self, event: ConnectionEvent) -> Step {
        let channels = &*self.parts.channels;
        tokio::select! {
            biased;
            () = channels.cancel.cancelled() => Err(Stop::Shutdown),
            sent = channels.events.send(event) => sent.map_err(|_| Stop::Shutdown),
        }
    }
}

/// Runs `write`, giving up on shutdown, when the owner is gone, or after
/// `limit`, so a socket that stopped draining cannot stall the session.
async fn guard_write(
    channels: &Channels,
    limit: Duration,
    write: impl Future<Output = Result<(), WebSocketError>>,
) -> Step {
    tokio::select! {
        biased;
        () = channels.cancel.cancelled() => Err(Stop::Shutdown),
        () = channels.events.closed() => Err(Stop::Shutdown),
        written = tokio::time::timeout(limit, write) => match written {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Stop::Lost(format!("write error: {error}"))),
            Err(_) => Err(Stop::Lost(format!("a write took over {limit:?}"))),
        },
    }
}

/// Sleeps until `deadline`; never resolves without one.
async fn sleep_until_some(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// The loss of a socket whose subscription the exchange never named.
fn unnamed() -> Stop {
    Stop::Lost("the exchange did not name the subscription in time".to_owned())
}

fn is_overflow(message: &Inbound) -> bool {
    matches!(
        message,
        Inbound::Error {
            kind: FeedErrorKind::Overflow,
            ..
        }
    )
}

fn error_kind(message: &Inbound) -> Option<FeedErrorKind> {
    match message {
        Inbound::Error { kind, .. } => Some(kind.clone()),
        _ => None,
    }
}

/// Logs a frame the codec rejected: the error at warn, and the start of the
/// frame, which can be large, only at debug.
fn log_undecodable(text: &str, error: &anyhow::Error) {
    tracing::warn!(error = %format_args!("{error:#}"), "dropped a frame that failed to decode");
    let preview = text
        .char_indices()
        .nth(FRAME_PREVIEW_CHARS)
        .map_or(text, |(end, _)| &text[..end]);
    tracing::debug!(frame = preview, "the frame that failed to decode");
}
