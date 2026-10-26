//! One probe run: two live connections, scheduled commands, and every frame
//! decoded and applied to local books.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use engineering_challenge::core::events::{
    Command, Decoded, FeedBody, FeedErrorKind, FeedEvent, Inbound, LifecycleEvent, StreamPosition,
    SubId,
};
use engineering_challenge::exchange::FeedCodec;
use engineering_challenge::exchange::kalshi::{KalshiCodec, KalshiEndpoint};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::{self, Message};

use super::books::Books;
use super::findings::{Arrival, Conn, Findings, preview};
use super::fixtures::Fixtures;
use super::markets::ProbeMarkets;
use super::socket::{Socket, SocketSender, connect, send};

pub const SUBSCRIBE_ID: u64 = 1;
const LIFECYCLE_ID: u64 = 2;
const UNOPENED_ID: u64 = 3;
const UNKNOWN_SID_ID: u64 = 4;
pub const GET_SNAPSHOT_ID: u64 = 5;
const REMOVE_UNOPENED_ID: u64 = 6;
const UNKNOWN_SID: u64 = 999_999;
const SNAPSHOT_SAMPLE: usize = 10;
const MAX_UNOPENED_REPLIES: usize = 20;

/// A command the probe sends partway through the run.
#[derive(Debug, Clone, Copy)]
enum Action {
    SubscribeUnopened,
    SnapshotUnknownSid,
    GetSnapshot,
    RemoveUnopened,
}

/// When each scheduled command goes out, in seconds after subscribing.
/// Removing the unopened market after the first `get_snapshot` makes its
/// `ok` list the whole subscription, which shows whether `get_snapshot`
/// changed it. Later `get_snapshot` rounds add comparison samples.
const SCHEDULE: [(u64, Action); 7] = [
    (30, Action::SubscribeUnopened),
    (30, Action::SnapshotUnknownSid),
    (60, Action::GetSnapshot),
    (90, Action::RemoveUnopened),
    (120, Action::GetSnapshot),
    (180, Action::GetSnapshot),
    (240, Action::GetSnapshot),
];

/// An `ok` reply, summarised against the tickers the probe subscribed.
pub struct AckSummary {
    pub reply_to: Option<u64>,
    pub sub: SubId,
    pub seq: Option<u64>,
    pub symbol_count: Option<usize>,
    /// Tickers in the reply that the first subscribe did not include.
    pub extra: Option<usize>,
    /// Tickers the first subscribe included that the reply lacks.
    pub missing: Option<usize>,
}

/// One `get_snapshot` round trip.
pub struct SnapshotRequest {
    pub symbols: Vec<Arc<str>>,
    pub sent_after: Duration,
    pub completed_after: Option<Duration>,
}

/// Everything a run observed, for the report.
pub struct Observations {
    pub markets: ProbeMarkets,
    pub findings: Findings,
    pub books: Books,
    pub fixtures: Fixtures,
    pub book_subs: BTreeSet<SubId>,
    pub sids_by_command: BTreeMap<u64, Vec<SubId>>,
    pub unopened_replies: Vec<String>,
    pub acks: Vec<AckSummary>,
    pub snapshot_rounds: Vec<SnapshotRequest>,
    pub decoded_errors: Vec<String>,
    pub overflow_errors: u64,
    pub lifecycle_decoded: BTreeMap<&'static str, u64>,
    pub ignored: u64,
    pub other_sub_events: u64,
}

/// Where a decoded message came from in the stream.
#[derive(Clone, Copy)]
struct Origin {
    reply_to: Option<u64>,
    position: Option<StreamPosition>,
}

/// Both connections, split into halves.
struct Connections {
    book_sender: SocketSender,
    book_frames: SplitStream<Socket>,
    lifecycle_sender: SocketSender,
    lifecycle_frames: SplitStream<Socket>,
}

/// One probe run's state.
pub struct Session {
    codec: KalshiCodec,
    subscribed_at: Instant,
    decoded: Vec<Decoded>,
    next_action: usize,
    seen: Observations,
}

impl Session {
    /// A session that will subscribe `markets`.
    pub fn new(markets: ProbeMarkets) -> Self {
        Self {
            codec: KalshiCodec,
            subscribed_at: Instant::now(),
            decoded: Vec::new(),
            next_action: 0,
            seen: Observations {
                markets,
                findings: Findings::default(),
                books: Books::default(),
                fixtures: Fixtures::default(),
                book_subs: BTreeSet::new(),
                sids_by_command: BTreeMap::new(),
                unopened_replies: Vec::new(),
                acks: Vec::new(),
                snapshot_rounds: Vec::new(),
                decoded_errors: Vec::new(),
                overflow_errors: 0,
                lifecycle_decoded: BTreeMap::new(),
                ignored: 0,
                other_sub_events: 0,
            },
        }
    }

    /// What the run has observed so far.
    pub fn observations(&self) -> &Observations {
        &self.seen
    }

    /// Connects, subscribes, and watches the feed for `duration`.
    pub async fn run(
        &mut self,
        endpoint: &KalshiEndpoint,
        duration: Duration,
    ) -> anyhow::Result<()> {
        let mut connections = self.connect_and_subscribe(endpoint).await?;
        self.watch(&mut connections, duration).await;
        for sender in [
            &mut connections.book_sender,
            &mut connections.lifecycle_sender,
        ] {
            sender.send(Message::Close(None)).await.ok();
        }
        Ok(())
    }

    async fn connect_and_subscribe(
        &mut self,
        endpoint: &KalshiEndpoint,
    ) -> anyhow::Result<Connections> {
        let (book, book_handshake) = connect(endpoint).await.context("order book connection")?;
        let (lifecycle, lifecycle_handshake) =
            connect(endpoint).await.context("lifecycle connection")?;
        self.seen.findings.handshakes = vec![
            (Conn::Book, book_handshake),
            (Conn::Lifecycle, lifecycle_handshake),
        ];
        let (mut book_sender, book_frames) = book.split();
        let (mut lifecycle_sender, lifecycle_frames) = lifecycle.split();
        let lifecycle_subscribe = Command::SubscribeLifecycle { id: LIFECYCLE_ID };
        send(&mut lifecycle_sender, &self.codec, &lifecycle_subscribe).await?;
        let symbols = self.seen.markets.open.clone();
        let subscribe = Command::Subscribe {
            id: SUBSCRIBE_ID,
            symbols,
        };
        send(&mut book_sender, &self.codec, &subscribe).await?;
        self.subscribed_at = Instant::now();
        Ok(Connections {
            book_sender,
            book_frames,
            lifecycle_sender,
            lifecycle_frames,
        })
    }

    async fn watch(&mut self, connections: &mut Connections, duration: Duration) {
        let deadline = tokio::time::sleep(duration);
        tokio::pin!(deadline);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let (mut book_open, mut lifecycle_open) = (true, true);
        loop {
            tokio::select! {
                frame = connections.book_frames.next(), if book_open => {
                    book_open = self.on_frame(Conn::Book, frame);
                }
                frame = connections.lifecycle_frames.next(), if lifecycle_open => {
                    lifecycle_open = self.on_frame(Conn::Lifecycle, frame);
                }
                _ = tick.tick() => self.run_due_actions(&mut connections.book_sender).await,
                () = &mut deadline => return,
            }
        }
    }

    /// Handles one websocket message; returns whether to keep reading.
    /// Only a close, a read error or the end of the stream stops reading.
    fn on_frame(&mut self, conn: Conn, frame: Option<Result<Message, tungstenite::Error>>) -> bool {
        let event = match frame {
            Some(Ok(Message::Text(text))) => return self.on_text(conn, text.as_str()),
            Some(Ok(Message::Ping(_))) => return self.count_ping(),
            Some(Ok(Message::Close(close))) => format!("closed by the server: {close:?}"),
            Some(Ok(_)) => return true,
            Some(Err(error)) => format!("read error: {error}"),
            None => "stream ended".to_owned(),
        };
        let after = self.subscribed_at.elapsed();
        let note = format!("{conn:?} +{after:?}: {event}");
        self.seen.findings.connection_events.push(note);
        false
    }

    fn count_ping(&mut self) -> bool {
        self.seen.findings.pings += 1;
        true
    }

    fn on_text(&mut self, conn: Conn, text: &str) -> bool {
        let arrival = Arrival {
            conn,
            text,
            received_at: Utc::now(),
            since: self.subscribed_at.elapsed(),
        };
        let frame: Value = match serde_json::from_str(text) {
            Ok(frame) => frame,
            Err(error) => {
                let failure = format!("not JSON: {error}: {}", preview(text));
                self.seen.findings.decode_errors.push(failure);
                return true;
            }
        };
        self.seen.findings.observe(&arrival, &frame);
        self.seen.fixtures.offer(&frame, text);
        self.note_unopened_reply(&arrival, &frame);
        self.decode(&arrival, frame["type"].as_str().unwrap_or("<missing type>"));
        true
    }

    fn decode(&mut self, arrival: &Arrival, kind: &str) {
        let mut decoded = std::mem::take(&mut self.decoded);
        decoded.clear();
        let started = Instant::now();
        let result = self
            .codec
            .decode(arrival.text, arrival.received_at, &mut decoded);
        self.seen
            .findings
            .decode_time(arrival.conn, kind, started.elapsed());
        match result {
            Ok(()) => decoded
                .drain(..)
                .for_each(|message| self.on_decoded(message, arrival.since)),
            Err(error) => {
                let failure = format!("{error:#}: {}", preview(arrival.text));
                self.seen.findings.decode_errors.push(failure);
            }
        }
        self.decoded = decoded;
    }

    fn on_decoded(&mut self, decoded: Decoded, since: Duration) {
        let origin = Origin {
            reply_to: decoded.reply_to,
            position: decoded.position,
        };
        match decoded.message {
            Inbound::Feed(event) => self.on_feed(&event, origin, since),
            Inbound::Subscribed { sub } => self.on_subscribed(origin, sub),
            Inbound::Ack { sub, symbols } => self.on_ack(origin, sub, symbols),
            Inbound::Error { sub, kind } => self.on_error(origin, sub, kind),
            Inbound::Lifecycle(event) => {
                *self
                    .seen
                    .lifecycle_decoded
                    .entry(lifecycle_name(&event))
                    .or_default() += 1;
            }
            Inbound::Unsubscribed { .. } => {}
            Inbound::Ignored => self.seen.ignored += 1,
        }
    }

    fn on_feed(&mut self, event: &FeedEvent, origin: Origin, since: Duration) {
        let sub = origin.position.map(|position| position.sub);
        if !sub.is_some_and(|sub| self.seen.book_subs.contains(&sub)) {
            self.seen.other_sub_events += 1;
            return;
        }
        match &event.body {
            FeedBody::Snapshot { .. } => {
                self.seen.books.on_snapshot(event, since);
                self.note_snapshot_replies_complete(since);
            }
            FeedBody::Level(change) => self.seen.books.on_delta(event, *change),
        }
    }

    fn note_snapshot_replies_complete(&mut self, since: Duration) {
        let outstanding = self.seen.books.replies_outstanding();
        let Some(round) = self.seen.snapshot_rounds.last_mut() else {
            return;
        };
        if round.completed_after.is_none() && outstanding == 0 {
            round.completed_after = Some(since);
        }
    }

    fn on_subscribed(&mut self, origin: Origin, sub: SubId) {
        let Some(id) = origin.reply_to else {
            return;
        };
        self.seen.sids_by_command.entry(id).or_default().push(sub);
        if id == SUBSCRIBE_ID {
            self.seen.book_subs.insert(sub);
        }
    }

    /// Records an `ok` reply, comparing its ticker list with the first
    /// subscribe's.
    fn on_ack(&mut self, origin: Origin, sub: SubId, symbols: Option<Vec<Arc<str>>>) {
        let (extra, missing) = symbols
            .as_deref()
            .map(|symbols| self.compare_with_subscribed(symbols))
            .unzip();
        self.seen.acks.push(AckSummary {
            reply_to: origin.reply_to,
            sub,
            seq: origin.position.map(|position| position.seq),
            symbol_count: symbols.map(|symbols| symbols.len()),
            extra,
            missing,
        });
    }

    fn on_error(&mut self, origin: Origin, sub: Option<SubId>, kind: FeedErrorKind) {
        self.seen.overflow_errors += u64::from(kind == FeedErrorKind::Overflow);
        let note = format!("reply to {:?}, sub {sub:?}: {kind:?}", origin.reply_to);
        self.seen.decoded_errors.push(note);
    }

    /// How many of `symbols` the first subscribe left out, and how many of
    /// its tickers `symbols` lacks.
    fn compare_with_subscribed(&self, symbols: &[Arc<str>]) -> (usize, usize) {
        let subscribed: HashSet<&Arc<str>> = self.seen.markets.open.iter().collect();
        let replied: HashSet<&Arc<str>> = symbols.iter().collect();
        (
            replied.difference(&subscribed).count(),
            subscribed.difference(&replied).count(),
        )
    }

    /// Keeps frames about the unopened market or the commands that touch it.
    fn note_unopened_reply(&mut self, arrival: &Arrival, frame: &Value) {
        let Some(ticker) = self.seen.markets.unopened.as_deref() else {
            return;
        };
        let by_id = matches!(frame["id"].as_u64(), Some(UNOPENED_ID | REMOVE_UNOPENED_ID));
        let by_ticker = frame["msg"]["market_ticker"].as_str() == Some(ticker);
        let replies = &mut self.seen.unopened_replies;
        if (by_id || by_ticker) && replies.len() < MAX_UNOPENED_REPLIES {
            let text = preview(arrival.text);
            replies.push(format!("{:?} +{:?}: {text}", arrival.conn, arrival.since));
        }
    }

    async fn run_due_actions(&mut self, sender: &mut SocketSender) {
        let elapsed = self.subscribed_at.elapsed();
        while let Some(&(at, action)) = SCHEDULE.get(self.next_action) {
            if elapsed < Duration::from_secs(at) {
                return;
            }
            self.next_action += 1;
            let note = match self.command_for(action) {
                None => Some(format!("{action:?} skipped: nothing to send")),
                Some(command) => send(sender, &self.codec, &command)
                    .await
                    .err()
                    .map(|error| format!("{action:?} not sent: {error:#}")),
            };
            self.seen.findings.connection_events.extend(note);
        }
    }

    fn command_for(&mut self, action: Action) -> Option<Command> {
        let markets = &self.seen.markets;
        match action {
            Action::SubscribeUnopened => Some(Command::Subscribe {
                id: UNOPENED_ID,
                symbols: vec![markets.unopened.clone()?],
            }),
            Action::SnapshotUnknownSid => Some(Command::RequestSnapshot {
                id: UNKNOWN_SID_ID,
                sub: SubId(UNKNOWN_SID),
                symbols: vec![markets.open.first()?.clone()],
            }),
            Action::GetSnapshot => self.get_snapshot_command(),
            Action::RemoveUnopened => Some(Command::RemoveSymbols {
                id: REMOVE_UNOPENED_ID,
                sub: *self.seen.book_subs.first()?,
                symbols: vec![markets.unopened.clone()?],
            }),
        }
    }

    fn get_snapshot_command(&mut self) -> Option<Command> {
        let sub = *self.seen.book_subs.first()?;
        let symbols = self
            .seen
            .books
            .await_replies_for_most_active(SNAPSHOT_SAMPLE);
        self.seen.snapshot_rounds.push(SnapshotRequest {
            symbols: symbols.clone(),
            sent_after: self.subscribed_at.elapsed(),
            completed_after: None,
        });
        Some(Command::RequestSnapshot {
            id: GET_SNAPSHOT_ID,
            sub,
            symbols,
        })
    }
}

fn lifecycle_name(event: &LifecycleEvent) -> &'static str {
    match event {
        LifecycleEvent::Created { .. } => "Created",
        LifecycleEvent::Activated { .. } => "Activated",
        LifecycleEvent::Deactivated { .. } => "Deactivated",
        LifecycleEvent::CloseChanged { .. } => "CloseChanged",
        LifecycleEvent::Determined { .. } => "Determined",
        LifecycleEvent::Settled { .. } => "Settled",
    }
}
