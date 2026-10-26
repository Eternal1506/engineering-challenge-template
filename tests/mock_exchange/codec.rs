//! [`MockCodec`]: the mock exchange's JSON wire format.
//!
//! Frames from the exchange are JSON objects with a `kind` and the fields
//! that kind uses:
//!
//! - `subscribed`: `sub`, `reply_to`;
//! - `ack`: `sub`, `seq`, `reply_to`;
//! - `snapshot`: `sub`, `seq`, `symbol`, and `reply_to` when it answers a
//!   snapshot request;
//! - `delta`: `sub`, `seq`, `symbol`, `price`, `qty`;
//! - `error`: `code` (25 is an overflow), and optionally `sub`, `reply_to`.
//!
//! Any other kind, or text that is not JSON, fails to decode. Commands to
//! the exchange are `{"cmd", "id", "sub", "symbols"}`, read back as
//! [`SentCommand`].

use std::sync::{Arc, Mutex, PoisonError};

use anyhow::Context;
use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::core::events::{
    Command, Decoded, FeedBody, FeedErrorKind, FeedEvent, Inbound, LevelChange, QuantityChange,
    StreamPosition, SubId,
};
use engineering_challenge::exchange::FeedCodec;
use serde::Deserialize;
use serde_json::{Value, json};

/// The mock exchange's codec. Clones share a record of when each `decode`
/// call started.
#[derive(Debug, Clone, Default)]
pub struct MockCodec {
    decode_started: Arc<Mutex<Vec<DateTime<Utc>>>>,
}

impl MockCodec {
    /// When each `decode` call started, in call order.
    pub fn decode_times(&self) -> Vec<DateTime<Utc>> {
        let times = self.decode_started.lock();
        times.unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl FeedCodec for MockCodec {
    fn encode(&self, command: &Command) -> String {
        command_json(command).to_string()
    }

    fn decode(
        &self,
        text: &str,
        received_at: DateTime<Utc>,
        out: &mut Vec<Decoded>,
    ) -> anyhow::Result<()> {
        self.decode_started
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Utc::now());
        let frame: Frame = serde_json::from_str(text).context("invalid mock frame")?;
        out.push(frame.into_decoded(received_at)?);
        Ok(())
    }
}

/// A command as the mock exchange received it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SentCommand {
    /// `subscribe`, `subscribe_lifecycle`, `add`, `remove`, `snapshot` or
    /// `unsubscribe`.
    pub cmd: String,
    /// The command's id.
    pub id: u64,
    /// The subscription it changes, if any.
    pub sub: Option<u64>,
    /// The symbols it names, if any.
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Deserialize)]
struct Frame {
    kind: String,
    sub: Option<u64>,
    seq: Option<u64>,
    reply_to: Option<u64>,
    symbol: Option<String>,
    #[serde(default)]
    price: i64,
    #[serde(default)]
    qty: i64,
    code: Option<i64>,
}

impl Frame {
    fn into_decoded(self, received_at: DateTime<Utc>) -> anyhow::Result<Decoded> {
        Ok(Decoded {
            position: self.sub.zip(self.seq).map(|(sub, seq)| StreamPosition {
                sub: SubId(sub),
                seq,
            }),
            reply_to: self.reply_to,
            message: self.message(received_at)?,
        })
    }

    fn message(&self, received_at: DateTime<Utc>) -> anyhow::Result<Inbound> {
        Ok(match self.kind.as_str() {
            "subscribed" => Inbound::Subscribed { sub: self.sub()? },
            "ack" => Inbound::Ack {
                sub: self.sub()?,
                symbols: None,
            },
            "snapshot" => Inbound::Feed(self.feed(received_at, empty_book())?),
            "delta" => Inbound::Feed(self.feed(received_at, self.level())?),
            "error" => Inbound::Error {
                sub: self.sub.map(SubId),
                kind: error_kind(self.code.context("error frame without code")?),
            },
            other => anyhow::bail!("unknown frame kind {other:?}"),
        })
    }

    fn feed(&self, received_at: DateTime<Utc>, body: FeedBody) -> anyhow::Result<FeedEvent> {
        let symbol = self
            .symbol
            .as_deref()
            .context("feed frame without symbol")?;
        Ok(FeedEvent {
            symbol: symbol.into(),
            received_at,
            exchange_ts: None,
            seq_hint: None,
            body,
        })
    }

    fn level(&self) -> FeedBody {
        FeedBody::Level(LevelChange {
            side: Side::Buy,
            price: self.price,
            change: QuantityChange::Delta(self.qty),
        })
    }

    fn sub(&self) -> anyhow::Result<SubId> {
        self.sub.map(SubId).context("frame without sub")
    }
}

fn empty_book() -> FeedBody {
    FeedBody::Snapshot {
        bids: Vec::new(),
        asks: Vec::new(),
    }
}

fn error_kind(code: i64) -> FeedErrorKind {
    match code {
        25 => FeedErrorKind::Overflow,
        code => FeedErrorKind::Other {
            code,
            message: "mock error".to_owned(),
        },
    }
}

fn command_json(command: &Command) -> Value {
    let (cmd, id, sub, symbols): (&str, &u64, Option<&SubId>, &[Arc<str>]) = match command {
        Command::Subscribe { id, symbols } => ("subscribe", id, None, symbols),
        Command::SubscribeLifecycle { id } => ("subscribe_lifecycle", id, None, &[]),
        Command::AddSymbols { id, sub, symbols } => ("add", id, Some(sub), symbols),
        Command::RemoveSymbols { id, sub, symbols } => ("remove", id, Some(sub), symbols),
        Command::RequestSnapshot { id, sub, symbols } => ("snapshot", id, Some(sub), symbols),
        Command::Unsubscribe { id, sub } => ("unsubscribe", id, Some(sub), &[]),
    };
    let sub = sub.map(|sub| sub.0);
    json!({ "cmd": cmd, "id": id, "sub": sub, "symbols": names(symbols) })
}

fn names(symbols: &[Arc<str>]) -> Vec<&str> {
    symbols.iter().map(|symbol| &**symbol).collect()
}

/// Builders for frames the mock exchange sends. A reply gets its
/// `reply_to` from [`MockClient::reply`](super::MockClient::reply).
pub mod frames {
    use serde_json::{Value, json};

    /// Names subscription `sub`.
    pub fn subscribed(sub: u64) -> Value {
        json!({ "kind": "subscribed", "sub": sub })
    }

    /// Confirms a change to subscription `sub`, at `seq`.
    pub fn ack(sub: u64, seq: u64) -> Value {
        json!({ "kind": "ack", "sub": sub, "seq": seq })
    }

    /// An empty book for `symbol` on subscription `sub`, at `seq`.
    pub fn snapshot(sub: u64, seq: u64, symbol: &str) -> Value {
        json!({ "kind": "snapshot", "sub": sub, "seq": seq, "symbol": symbol })
    }

    /// A bid change for `symbol` on subscription `sub`, at `seq`.
    pub fn delta(sub: u64, seq: u64, symbol: &str) -> Value {
        json!({ "kind": "delta", "sub": sub, "seq": seq, "symbol": symbol, "price": 500_000, "qty": 10 })
    }

    /// An error with `code`, not in reply to any command.
    pub fn error(code: i64) -> Value {
        json!({ "kind": "error", "code": code })
    }
}
