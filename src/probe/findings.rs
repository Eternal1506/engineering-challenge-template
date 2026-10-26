//! What the probe observes about raw frames, independent of the codec.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// One text frame as it arrived.
pub struct Arrival<'a> {
    /// The connection it arrived on.
    pub conn: Conn,
    /// The frame's text.
    pub text: &'a str,
    /// When it arrived, stamped before any parsing.
    pub received_at: DateTime<Utc>,
    /// How long after subscribing it arrived.
    pub since: Duration,
}

/// Which of the probe's two connections a frame arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Conn {
    /// The `orderbook_delta` connection.
    Book,
    /// The `market_lifecycle_v2` connection.
    Lifecycle,
}

const ENVELOPE_FIELDS: &[&str] = &["type", "id", "sid", "seq", "sending_ts_ms", "msg"];

const KNOWN_TYPES: &[&str] = &[
    "orderbook_snapshot",
    "orderbook_delta",
    "subscribed",
    "ok",
    "unsubscribed",
    "error",
    "market_lifecycle_v2",
    "event_lifecycle",
    "event_fee_update",
];

const LIFECYCLE_FIELDS: &[&str] = &[
    "market_ticker",
    "event_type",
    "exchange_index",
    "open_ts",
    "close_ts",
    "determination_ts",
    "settled_ts",
    "result",
    "settlement_value",
    "is_deactivated",
    "additional_metadata",
    "price_level_structure",
    "price_ranges",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "custom_strike",
    "yes_sub_title",
];

/// Frame counts and sizes for one message type on one connection.
#[derive(Default)]
pub struct TypeStats {
    pub frames: u64,
    pub bytes: u64,
    pub with_seq: u64,
    pub with_sending_ts: u64,
    pub decode_nanos: Vec<u64>,
}

/// Sequence tracking for one subscription.
pub struct SeqTrack {
    pub first: u64,
    pub last: u64,
    pub messages: u64,
    pub gaps: u64,
}

/// Order book connection totals for one second after subscribing.
#[derive(Default, Clone, Copy)]
pub struct SecondTotals {
    pub frames: u64,
    pub bytes: u64,
    pub deltas: u64,
}

/// Everything the probe records about raw frames.
#[derive(Default)]
pub struct Findings {
    pub handshakes: Vec<(Conn, Duration)>,
    pub by_type: BTreeMap<(Conn, String), TypeStats>,
    pub seq: BTreeMap<(Conn, u64), SeqTrack>,
    pub gap_examples: Vec<String>,
    pub feed_lag_ms: Vec<(u64, i64)>,
    pub change_to_send_ms: Vec<i64>,
    pub per_second: Vec<SecondTotals>,
    pub unknown_types: BTreeSet<String>,
    pub unknown_fields: BTreeSet<String>,
    pub error_frames: Vec<String>,
    pub decode_errors: Vec<String>,
    pub lifecycle_event_types: BTreeMap<String, u64>,
    pub pings: u64,
    pub connection_events: Vec<String>,
}

impl Findings {
    /// Records one text frame's envelope, size and timing.
    pub fn observe(&mut self, arrival: &Arrival, frame: &Value) {
        let (conn, text) = (arrival.conn, arrival.text);
        let kind = frame["type"].as_str().unwrap_or("<missing type>");
        let stats = self.by_type.entry((conn, kind.to_owned())).or_default();
        stats.frames += 1;
        stats.bytes += text.len() as u64;
        stats.with_seq += u64::from(frame.get("seq").is_some());
        stats.with_sending_ts += u64::from(frame.get("sending_ts_ms").is_some());
        self.track_seq(conn, frame);
        self.check_fields(kind, frame);
        self.note_errors_and_lifecycle(kind, frame, text);
        if conn == Conn::Book {
            self.count_second(kind, text.len(), arrival.since);
            self.observe_timing(arrival, frame);
        }
    }

    fn note_errors_and_lifecycle(&mut self, kind: &str, frame: &Value, text: &str) {
        if kind == "error" {
            self.error_frames.push(preview(text));
        }
        if kind == "market_lifecycle_v2" {
            let event_type = frame["msg"]["event_type"].as_str().unwrap_or("<missing>");
            *self
                .lifecycle_event_types
                .entry(event_type.to_owned())
                .or_default() += 1;
        }
    }

    /// Records feed lag, keyed by whole seconds since subscribing, and the
    /// exchange's own change-to-send delay.
    fn observe_timing(&mut self, arrival: &Arrival, frame: &Value) {
        let Some(sent_ms) = frame["sending_ts_ms"].as_i64() else {
            return;
        };
        let lag = arrival.received_at.timestamp_millis() - sent_ms;
        self.feed_lag_ms.push((arrival.since.as_secs(), lag));
        if let Some(changed_ms) = frame["msg"]["ts_ms"].as_i64() {
            self.change_to_send_ms.push(sent_ms - changed_ms);
        }
    }

    /// Records how long decoding one frame of `kind` took.
    pub fn decode_time(&mut self, conn: Conn, kind: &str, elapsed: Duration) {
        if let Some(stats) = self.by_type.get_mut(&(conn, kind.to_owned())) {
            stats.decode_nanos.push(elapsed.as_nanos() as u64);
        }
    }

    fn track_seq(&mut self, conn: Conn, frame: &Value) {
        let (Some(sid), Some(seq)) = (frame["sid"].as_u64(), frame["seq"].as_u64()) else {
            return;
        };
        let track = self.seq.entry((conn, sid)).or_insert(SeqTrack {
            first: seq,
            last: seq.wrapping_sub(1),
            messages: 0,
            gaps: 0,
        });
        if seq != track.last.wrapping_add(1) {
            track.gaps += 1;
            let kind = frame["type"].as_str().unwrap_or("");
            let example = format!("{conn:?} sid {sid}: {} then {seq} ({kind})", track.last);
            self.gap_examples.push(example);
        }
        track.last = seq;
        track.messages += 1;
    }

    fn check_fields(&mut self, kind: &str, frame: &Value) {
        if !KNOWN_TYPES.contains(&kind) {
            self.unknown_types.insert(kind.to_owned());
            return;
        }
        let top = frame.as_object().into_iter().flat_map(|map| map.keys());
        for key in top.filter(|key| !ENVELOPE_FIELDS.contains(&key.as_str())) {
            self.unknown_fields.insert(format!("{kind}.{key}"));
        }
        let Some(known) = known_msg_fields(kind) else {
            return;
        };
        let inner = frame["msg"]
            .as_object()
            .into_iter()
            .flat_map(|map| map.keys());
        for key in inner.filter(|key| !known.contains(&key.as_str())) {
            self.unknown_fields.insert(format!("{kind}.msg.{key}"));
        }
    }

    fn count_second(&mut self, kind: &str, bytes: usize, since: Duration) {
        let second = since.as_secs() as usize;
        if self.per_second.len() <= second {
            self.per_second.resize(second + 1, SecondTotals::default());
        }
        let totals = &mut self.per_second[second];
        totals.frames += 1;
        totals.bytes += bytes as u64;
        totals.deltas += u64::from(kind == "orderbook_delta");
    }
}

/// The documented `msg` fields of the types the codec decodes.
fn known_msg_fields(kind: &str) -> Option<&'static [&'static str]> {
    Some(match kind {
        "orderbook_snapshot" => &[
            "market_ticker",
            "market_id",
            "clock",
            "yes_dollars_fp",
            "no_dollars_fp",
        ],
        "orderbook_delta" => &[
            "market_ticker",
            "market_id",
            "clock",
            "price_dollars",
            "delta_fp",
            "side",
            "ts",
            "ts_ms",
            "client_order_id",
            "subaccount",
        ],
        "subscribed" => &["channel", "sid"],
        "ok" => &["market_tickers", "market_ids"],
        "error" => &["code", "msg"],
        "market_lifecycle_v2" => LIFECYCLE_FIELDS,
        _ => return None,
    })
}

/// The first 400 characters of `text`, for the report.
pub fn preview(text: &str) -> String {
    const LIMIT: usize = 400;
    match text.char_indices().nth(LIMIT) {
        Some((end, _)) => format!("{}... ({} bytes)", &text[..end], text.len()),
        None => text.to_owned(),
    }
}
