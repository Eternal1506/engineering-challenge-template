//! [`KalshiCodec`] decodes Kalshi websocket frames and encodes commands.
//!
//! Every frame is one JSON message: an envelope (`type`, `id`, `sid`, `seq`,
//! `sending_ts_ms`) around a type-specific `msg`. The envelope is parsed
//! first with `msg` left raw, then `msg` is parsed for the types the core
//! uses. Strings are borrowed from the frame where possible.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};

use super::lifecycle::LifecycleMsg;
use super::prices::{parse_kalshi_price, parse_no_bid_as_ask_price};
use crate::Side;
use crate::core::events::{
    Command, Decoded, FeedBody, FeedErrorKind, FeedEvent, Inbound, LevelChange, QuantityChange,
    StreamPosition, SubId,
};
use crate::core::fixed::parse_fixed;
use crate::exchange::FeedCodec;

const OVERFLOW_CODE: i64 = 25;
const UNKNOWN_SUBSCRIPTION_CODE: i64 = 7;

/// The Kalshi websocket codec.
///
/// - A YES level at p becomes a bid at p; a NO level at q becomes an ask at
///   1 − q.
/// - `seq_hint` is the market's `clock`. The live feed does not send `clock`
///   yet (see `NOTES.md`), so `seq_hint` is `None` until it does.
/// - The stream position is the envelope's `sid` and `seq`. Kalshi numbers
///   `ok` replies, `get_snapshot` replies and lifecycle messages in the same
///   `seq` sequence as book data, so every frame type carries its position.
/// - `reply_to` is the envelope's `id`, which Kalshi sets on replies to
///   commands, including `get_snapshot` snapshots.
/// - A delta's exchange time is `ts_ms`, falling back to `sending_ts_ms`; a
///   snapshot's is `sending_ts_ms`.
/// - Order book subscriptions ask for `use_yes_price: false`, so NO prices
///   arrive as NO bids.
#[derive(Debug, Default, Clone, Copy)]
pub struct KalshiCodec;

impl FeedCodec for KalshiCodec {
    fn encode(&self, command: &Command) -> String {
        command_json(command).to_string()
    }

    fn decode(
        &self,
        text: &str,
        received_at: DateTime<Utc>,
        out: &mut Vec<Decoded>,
    ) -> anyhow::Result<()> {
        let envelope: Envelope = serde_json::from_str(text).context("invalid Kalshi frame")?;
        out.push(Decoded {
            position: envelope.position(),
            reply_to: envelope.id,
            message: envelope.message(received_at)?,
        });
        Ok(())
    }
}

/// The fields every Kalshi message may carry, with `msg` left unparsed.
#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct SnapshotMsg<'a> {
    market_ticker: &'a str,
    clock: Option<u64>,
    #[serde(borrow, default)]
    yes_dollars_fp: Vec<(&'a str, &'a str)>,
    #[serde(borrow, default)]
    no_dollars_fp: Vec<(&'a str, &'a str)>,
}

#[derive(Deserialize)]
struct DeltaMsg<'a> {
    market_ticker: &'a str,
    clock: Option<u64>,
    price_dollars: &'a str,
    delta_fp: &'a str,
    side: &'a str,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct SubscribedMsg {
    sid: u64,
}

#[derive(Deserialize)]
struct OkMsg<'a> {
    #[serde(borrow)]
    market_tickers: Option<Vec<&'a str>>,
}

#[derive(Deserialize)]
struct ErrorMsg<'a> {
    code: i64,
    #[serde(borrow)]
    msg: Cow<'a, str>,
}

impl<'a> Envelope<'a> {
    fn position(&self) -> Option<StreamPosition> {
        let (sid, seq) = self.sid.zip(self.seq)?;
        Some(StreamPosition {
            sub: SubId(sid),
            seq,
        })
    }

    fn message(&self, received_at: DateTime<Utc>) -> anyhow::Result<Inbound> {
        match self.kind {
            "orderbook_snapshot" => self.snapshot(received_at).map(Inbound::Feed),
            "orderbook_delta" => self.delta(received_at).map(Inbound::Feed),
            "market_lifecycle_v2" => self.lifecycle(),
            "subscribed" => self.subscribed(),
            "ok" => self.ack(),
            "unsubscribed" => self.unsubscribed(),
            "error" => self.error(),
            _ => Ok(Inbound::Ignored),
        }
    }

    fn snapshot(&self, received_at: DateTime<Utc>) -> anyhow::Result<FeedEvent> {
        self.require_position()?;
        let msg: SnapshotMsg = self.msg()?;
        let ticker = msg.market_ticker;
        let bids = levels(&msg.yes_dollars_fp, parse_kalshi_price)
            .with_context(|| self.invalid(ticker, "yes_dollars_fp"))?;
        let asks = levels(&msg.no_dollars_fp, parse_no_bid_as_ask_price)
            .with_context(|| self.invalid(ticker, "no_dollars_fp"))?;
        Ok(FeedEvent {
            symbol: ticker.into(),
            received_at,
            exchange_ts: self.sending_ts_ms.and_then(DateTime::from_timestamp_millis),
            seq_hint: msg.clock,
            body: FeedBody::Snapshot { bids, asks },
        })
    }

    fn delta(&self, received_at: DateTime<Utc>) -> anyhow::Result<FeedEvent> {
        self.require_position()?;
        let msg: DeltaMsg = self.msg()?;
        let changed_ms = msg.ts_ms.or(self.sending_ts_ms);
        Ok(FeedEvent {
            symbol: msg.market_ticker.into(),
            received_at,
            exchange_ts: changed_ms.and_then(DateTime::from_timestamp_millis),
            seq_hint: msg.clock,
            body: FeedBody::Level(self.level_change(&msg)?),
        })
    }

    fn level_change(&self, msg: &DeltaMsg) -> anyhow::Result<LevelChange> {
        let ticker = msg.market_ticker;
        let (side, price) = match msg.side {
            "yes" => (Side::Buy, parse_kalshi_price(msg.price_dollars)),
            "no" => (Side::Sell, parse_no_bid_as_ask_price(msg.price_dollars)),
            other => anyhow::bail!("{}: unknown side {other:?}", self.invalid(ticker, "side")),
        };
        let price = price.with_context(|| self.invalid(ticker, "price_dollars"))?;
        let delta = parse_fixed(msg.delta_fp).with_context(|| self.invalid(ticker, "delta_fp"))?;
        Ok(LevelChange {
            side,
            price,
            change: QuantityChange::Delta(delta),
        })
    }

    fn lifecycle(&self) -> anyhow::Result<Inbound> {
        let msg: LifecycleMsg = self.msg()?;
        let event = msg.into_event()?;
        Ok(event.map_or(Inbound::Ignored, Inbound::Lifecycle))
    }

    fn subscribed(&self) -> anyhow::Result<Inbound> {
        self.require_id()?;
        let msg: SubscribedMsg = self.msg()?;
        Ok(Inbound::Subscribed {
            sub: SubId(msg.sid),
        })
    }

    fn ack(&self) -> anyhow::Result<Inbound> {
        self.require_id()?;
        let symbols = match self.msg {
            Some(_) => self.msg::<OkMsg>()?.market_tickers,
            None => None,
        };
        Ok(Inbound::Ack {
            sub: self.sub()?,
            symbols: symbols.map(|tickers| tickers.into_iter().map(Arc::from).collect()),
        })
    }

    fn unsubscribed(&self) -> anyhow::Result<Inbound> {
        self.require_id()?;
        Ok(Inbound::Unsubscribed { sub: self.sub()? })
    }

    fn error(&self) -> anyhow::Result<Inbound> {
        let msg: ErrorMsg = self.msg()?;
        let kind = match msg.code {
            OVERFLOW_CODE => FeedErrorKind::Overflow,
            UNKNOWN_SUBSCRIPTION_CODE => FeedErrorKind::UnknownSubscription,
            code => FeedErrorKind::Other {
                code,
                message: msg.msg.into_owned(),
            },
        };
        Ok(Inbound::Error {
            sub: self.sid.map(SubId),
            kind,
        })
    }

    fn msg<T: Deserialize<'a>>(&self) -> anyhow::Result<T> {
        let raw = self
            .msg
            .ok_or_else(|| anyhow!("{} message has no msg", self.kind))?;
        serde_json::from_str(raw.get()).with_context(|| format!("invalid {} msg", self.kind))
    }

    /// Replies are matched to commands by id, so one without an id is
    /// unusable.
    fn require_id(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.id.is_some(), "{} message has no id", self.kind);
        Ok(())
    }

    /// Book data must be numbered so that lost messages can be detected.
    fn require_position(&self) -> anyhow::Result<()> {
        self.sub()?;
        anyhow::ensure!(self.seq.is_some(), "{} message has no seq", self.kind);
        Ok(())
    }

    fn sub(&self) -> anyhow::Result<SubId> {
        let sid = self
            .sid
            .ok_or_else(|| anyhow!("{} message has no sid", self.kind))?;
        Ok(SubId(sid))
    }

    /// The context for a malformed `field` in this message for `ticker`.
    fn invalid(&self, ticker: &str, field: &str) -> String {
        format!("{} for {ticker} has an invalid {field}", self.kind)
    }
}

/// Parses `[price, quantity]` pairs, mapping each price with `parse_price`.
fn levels(
    raw: &[(&str, &str)],
    parse_price: fn(&str) -> anyhow::Result<i64>,
) -> anyhow::Result<Vec<(i64, i64)>> {
    raw.iter()
        .map(|&(price, quantity)| Ok((parse_price(price)?, parse_fixed(quantity)?)))
        .collect()
}

fn command_json(command: &Command) -> Value {
    match command {
        Command::Subscribe { id, symbols } => subscribe(
            *id,
            json!({
                "channels": ["orderbook_delta"],
                "market_tickers": tickers(symbols),
                "use_yes_price": false,
            }),
        ),
        Command::SubscribeLifecycle { id } => {
            subscribe(*id, json!({ "channels": ["market_lifecycle_v2"] }))
        }
        Command::AddSymbols { id, sub, symbols } => update(*id, *sub, symbols, "add_markets"),
        Command::RemoveSymbols { id, sub, symbols } => update(*id, *sub, symbols, "delete_markets"),
        Command::RequestSnapshot { id, sub, symbols } => update(*id, *sub, symbols, "get_snapshot"),
        Command::Unsubscribe { id, sub } => {
            json!({ "id": id, "cmd": "unsubscribe", "params": { "sids": [sub.0] } })
        }
    }
}

fn subscribe(id: u64, params: Value) -> Value {
    json!({ "id": id, "cmd": "subscribe", "params": params })
}

fn update(id: u64, sub: SubId, symbols: &[Arc<str>], action: &str) -> Value {
    let params = json!({ "sids": [sub.0], "market_tickers": tickers(symbols), "action": action });
    json!({ "id": id, "cmd": "update_subscription", "params": params })
}

fn tickers(symbols: &[Arc<str>]) -> Vec<&str> {
    symbols.iter().map(|symbol| &**symbol).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    /// A frame captured from the live feed by `probe --fixtures-dir`.
    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!("../../../tests/fixtures/kalshi/", $name, ".json"))
        };
    }

    fn received_at() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_800_000_000_000).unwrap()
    }

    fn millis(ms: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(ms)
    }

    fn position(sid: u64, seq: u64) -> Option<StreamPosition> {
        Some(StreamPosition {
            sub: SubId(sid),
            seq,
        })
    }

    fn decode(text: &str) -> anyhow::Result<Decoded> {
        let mut out = Vec::new();
        KalshiCodec.decode(text, received_at(), &mut out)?;
        assert_eq!(out.len(), 1, "one Kalshi frame is one message");
        Ok(out.remove(0))
    }

    fn decode_value(frame: Value) -> anyhow::Result<Decoded> {
        decode(&frame.to_string())
    }

    fn feed(decoded: Decoded) -> FeedEvent {
        match decoded.message {
            Inbound::Feed(event) => event,
            other => panic!("expected a feed event, got {other:?}"),
        }
    }

    fn decode_error(frame: Value) -> String {
        format!("{:#}", decode_value(frame).unwrap_err())
    }

    fn delta_frame(side: &str, price: &str, delta: &str) -> Value {
        json!({
            "type": "orderbook_delta", "sid": 4, "seq": 9, "sending_ts_ms": 1_800_000_000_100_i64,
            "msg": {
                "market_ticker": "KXTEST-1", "market_id": "m", "clock": 77,
                "price_dollars": price, "delta_fp": delta, "side": side,
                "ts_ms": 1_800_000_000_050_i64
            }
        })
    }

    fn without(mut frame: Value, path: &[&str]) -> Value {
        let (last, parents) = path.split_last().unwrap();
        let object = parents
            .iter()
            .fold(&mut frame, |value, key| &mut value[*key]);
        object.as_object_mut().unwrap().remove(*last);
        frame
    }

    fn symbols(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|&name| Arc::from(name)).collect()
    }

    fn encoded(command: Command) -> Value {
        serde_json::from_str(&KalshiCodec.encode(&command)).unwrap()
    }

    #[test]
    fn captured_snapshot_maps_yes_to_bids_and_no_to_complementary_asks() {
        let expected = Decoded {
            position: position(1, 373),
            reply_to: None,
            message: Inbound::Feed(FeedEvent {
                symbol: "KXGBPUSDAD-26OCT26-1.3310".into(),
                received_at: received_at(),
                exchange_ts: millis(1_793_028_702_776),
                seq_hint: None,
                body: FeedBody::Snapshot {
                    bids: vec![(870_000, 5_000_000)],
                    asks: vec![(990_000, 5_000_000)],
                },
            }),
        };
        assert_eq!(decode(fixture!("snapshot_both_sides")).unwrap(), expected);
    }

    #[test]
    fn captured_snapshot_with_an_absent_side_has_an_empty_side() {
        let body = feed(decode(fixture!("snapshot_one_side_absent")).unwrap()).body;
        let expected = FeedBody::Snapshot {
            bids: vec![],
            asks: vec![(960_000, 15_000_000)],
        };
        assert_eq!(body, expected);
    }

    #[test]
    fn captured_snapshot_with_both_sides_absent_is_empty() {
        let body = feed(decode(fixture!("snapshot_empty")).unwrap()).body;
        let expected = FeedBody::Snapshot {
            bids: vec![],
            asks: vec![],
        };
        assert_eq!(body, expected);
    }

    #[test]
    fn captured_get_snapshot_reply_decodes_every_level_and_its_command_id() {
        let decoded = decode(fixture!("snapshot_reply_to_get_snapshot")).unwrap();
        let reply_to = decoded.reply_to;
        let FeedBody::Snapshot { bids, asks } = feed(decoded).body else {
            panic!("expected a snapshot");
        };
        let best_bid = bids.iter().map(|&(price, _)| price).max();
        let best_ask = asks.iter().map(|&(price, _)| price).min();
        assert_eq!(
            (reply_to, bids.len(), asks.len(), best_bid, best_ask),
            (Some(5), 6, 13, Some(60_000), Some(710_000))
        );
    }

    #[test]
    fn captured_yes_delta_is_a_bid_change_stamped_with_ts_ms() {
        let decoded = decode(fixture!("delta_yes")).unwrap();
        let at = decoded.position;
        let event = feed(decoded);
        let change = LevelChange {
            side: Side::Buy,
            price: 30_000,
            change: QuantityChange::Delta(14_000_000),
        };
        assert_eq!(
            (event.body, event.exchange_ts, event.seq_hint, at),
            (
                FeedBody::Level(change),
                millis(1_793_028_862_640),
                None,
                position(1, 122_240)
            )
        );
    }

    #[test]
    fn captured_no_delta_is_an_ask_change_at_the_complementary_price() {
        let event = feed(decode(fixture!("delta_no")).unwrap());
        let change = LevelChange {
            side: Side::Sell,
            price: 830_000,
            change: QuantityChange::Delta(-3_000_000),
        };
        assert_eq!(
            (&*event.symbol, event.body, event.exchange_ts),
            (
                "KXUNSEC-27JAN-VORB",
                FeedBody::Level(change),
                millis(1_793_028_705_693)
            )
        );
    }

    #[test]
    fn captured_subscribed_reply_carries_the_new_sid_and_command_id() {
        let expected = Decoded {
            position: None,
            reply_to: Some(1),
            message: Inbound::Subscribed { sub: SubId(1) },
        };
        assert_eq!(decode(fixture!("subscribed")).unwrap(), expected);
    }

    #[test]
    fn captured_ok_reply_lists_the_whole_subscription_at_its_position() {
        let decoded = decode(fixture!("ok")).unwrap();
        let Inbound::Ack { sub, symbols } = decoded.message else {
            panic!("expected an ack");
        };
        let symbols = symbols.unwrap();
        assert_eq!(
            (decoded.reply_to, decoded.position, sub, symbols.len()),
            (Some(3), position(1, 20_678), SubId(1), 2001)
        );
        assert_eq!(&*symbols[0], "KX10YRRATE15M-26OCT261145-45");
    }

    #[test]
    fn captured_unknown_sid_error_is_unknown_subscription() {
        let expected = Decoded {
            position: None,
            reply_to: Some(4),
            message: Inbound::Error {
                sub: None,
                kind: FeedErrorKind::UnknownSubscription,
            },
        };
        assert_eq!(decode(fixture!("error_code_7")).unwrap(), expected);
    }

    #[test]
    fn captured_event_lifecycle_is_ignored_but_keeps_its_position() {
        let expected = Decoded {
            position: position(1, 90),
            reply_to: None,
            message: Inbound::Ignored,
        };
        assert_eq!(decode(fixture!("event_lifecycle")).unwrap(), expected);
    }

    #[test]
    fn no_delta_at_030_becomes_an_ask_at_700000() {
        let event = feed(decode_value(delta_frame("no", "0.3000", "-12.50")).unwrap());
        let expected = LevelChange {
            side: Side::Sell,
            price: 700_000,
            change: QuantityChange::Delta(-12_500_000),
        };
        assert_eq!(event.body, FeedBody::Level(expected));
    }

    #[test]
    fn delta_carries_ts_ms_clock_and_position() {
        let decoded = decode_value(delta_frame("yes", "0.4100", "3.00")).unwrap();
        let at = decoded.position;
        let event = feed(decoded);
        assert_eq!(
            (event.exchange_ts, event.seq_hint, at),
            (millis(1_800_000_000_050), Some(77), position(4, 9))
        );
        assert_eq!(
            (&*event.symbol, event.received_at),
            ("KXTEST-1", received_at())
        );
    }

    #[test]
    fn delta_without_ts_ms_falls_back_to_sending_ts_ms() {
        let frame = without(delta_frame("yes", "0.4100", "3.00"), &["msg", "ts_ms"]);
        let event = feed(decode_value(frame).unwrap());
        assert_eq!(event.exchange_ts, millis(1_800_000_000_100));
    }

    #[test]
    fn snapshot_without_sending_ts_ms_has_no_exchange_time() {
        let frame = json!({
            "type": "orderbook_snapshot", "sid": 1, "seq": 1,
            "msg": { "market_ticker": "KXTEST-1", "no_dollars_fp": [["0.3000", "5.00"]] }
        });
        let event = feed(decode_value(frame).unwrap());
        let expected = FeedBody::Snapshot {
            bids: vec![],
            asks: vec![(700_000, 5_000_000)],
        };
        assert_eq!((event.exchange_ts, event.body), (None, expected));
    }

    #[test]
    fn malformed_price_names_the_ticker_and_the_field() {
        let error = decode_error(delta_frame("yes", "0.4x", "1.00"));
        assert!(
            error.contains("KXTEST-1") && error.contains("price_dollars"),
            "{error}"
        );
    }

    #[test]
    fn out_of_range_no_price_names_the_ticker_and_the_field() {
        let error = decode_error(delta_frame("no", "1.0100", "1.00"));
        assert!(
            error.contains("KXTEST-1") && error.contains("price_dollars"),
            "{error}"
        );
    }

    #[test]
    fn malformed_quantity_names_the_ticker_and_the_field() {
        let error = decode_error(delta_frame("yes", "0.4100", "lots"));
        assert!(
            error.contains("KXTEST-1") && error.contains("delta_fp"),
            "{error}"
        );
    }

    #[test]
    fn malformed_snapshot_level_names_the_ticker_and_the_side() {
        let frame = json!({
            "type": "orderbook_snapshot", "sid": 1, "seq": 1,
            "msg": { "market_ticker": "KXTEST-1", "yes_dollars_fp": [["0.30", "-"]] }
        });
        let error = decode_error(frame);
        assert!(
            error.contains("KXTEST-1") && error.contains("yes_dollars_fp"),
            "{error}"
        );
    }

    #[test]
    fn unknown_side_is_an_error() {
        let error = decode_error(delta_frame("maybe", "0.4100", "1.00"));
        assert!(error.contains("\"maybe\""), "{error}");
    }

    #[test]
    fn delta_without_sid_is_an_error() {
        let frame = without(delta_frame("yes", "0.4100", "1.00"), &["sid"]);
        assert!(decode_error(frame).contains("no sid"));
    }

    #[test]
    fn delta_without_seq_is_an_error() {
        let frame = without(delta_frame("yes", "0.4100", "1.00"), &["seq"]);
        assert!(decode_error(frame).contains("no seq"));
    }

    #[test]
    fn invalid_json_appends_nothing() {
        let mut out = Vec::new();
        let result = KalshiCodec.decode("{not json", received_at(), &mut out);
        assert!(result.is_err() && out.is_empty());
    }

    #[test]
    fn unknown_type_is_ignored_with_its_position() {
        let frame =
            json!({ "type": "ticker", "sid": 3, "seq": 8, "msg": { "market_ticker": "T" } });
        let decoded = decode_value(frame).unwrap();
        assert_eq!(
            (decoded.message, decoded.position),
            (Inbound::Ignored, position(3, 8))
        );
    }

    #[test]
    fn error_25_is_overflow_and_keeps_its_sid() {
        let frame = json!({
            "type": "error", "sid": 1,
            "msg": { "code": 25, "msg": "Subscription buffer overflow" }
        });
        let expected = Inbound::Error {
            sub: Some(SubId(1)),
            kind: FeedErrorKind::Overflow,
        };
        assert_eq!(decode_value(frame).unwrap().message, expected);
    }

    #[test]
    fn other_error_codes_keep_code_and_message() {
        let frame = json!({ "id": 8, "type": "error", "msg": { "code": 8, "msg": "Unknown channel name" } });
        let kind = FeedErrorKind::Other {
            code: 8,
            message: "Unknown channel name".to_owned(),
        };
        let decoded = decode_value(frame).unwrap();
        assert_eq!(
            (decoded.reply_to, decoded.message),
            (Some(8), Inbound::Error { sub: None, kind })
        );
    }

    #[test]
    fn ok_without_msg_has_no_symbols() {
        let frame = json!({ "id": 5, "sid": 2, "seq": 3, "type": "ok" });
        let expected = Decoded {
            position: position(2, 3),
            reply_to: Some(5),
            message: Inbound::Ack {
                sub: SubId(2),
                symbols: None,
            },
        };
        assert_eq!(decode_value(frame).unwrap(), expected);
    }

    #[test]
    fn ok_without_sid_is_an_error() {
        let frame = json!({ "id": 5, "seq": 3, "type": "ok" });
        assert!(decode_error(frame).contains("no sid"));
    }

    #[test]
    fn reply_without_id_is_an_error() {
        let frame =
            json!({ "type": "subscribed", "msg": { "channel": "orderbook_delta", "sid": 1 } });
        assert!(decode_error(frame).contains("no id"));
    }

    #[test]
    fn unsubscribed_reply_carries_sid_command_id_and_position() {
        let frame = json!({ "id": 102, "sid": 2, "seq": 7, "type": "unsubscribed" });
        let expected = Decoded {
            position: position(2, 7),
            reply_to: Some(102),
            message: Inbound::Unsubscribed { sub: SubId(2) },
        };
        assert_eq!(decode_value(frame).unwrap(), expected);
    }

    #[test]
    fn encodes_subscribe_with_no_leg_prices() {
        let command = Command::Subscribe {
            id: 1,
            symbols: symbols(&["A", "B"]),
        };
        let expected = json!({
            "id": 1, "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"], "market_tickers": ["A", "B"],
                "use_yes_price": false
            }
        });
        assert_eq!(encoded(command), expected);
    }

    #[test]
    fn encodes_lifecycle_subscribe_without_tickers() {
        let expected = json!({
            "id": 2, "cmd": "subscribe", "params": { "channels": ["market_lifecycle_v2"] }
        });
        assert_eq!(encoded(Command::SubscribeLifecycle { id: 2 }), expected);
    }

    #[test]
    fn encodes_each_update_subscription_action() {
        let sub = SubId(7);
        let commands = [
            Command::AddSymbols {
                id: 3,
                sub,
                symbols: symbols(&["A"]),
            },
            Command::RemoveSymbols {
                id: 3,
                sub,
                symbols: symbols(&["A"]),
            },
            Command::RequestSnapshot {
                id: 3,
                sub,
                symbols: symbols(&["A"]),
            },
        ];
        let expected = ["add_markets", "delete_markets", "get_snapshot"].map(|action| {
            json!({
                "id": 3, "cmd": "update_subscription",
                "params": { "sids": [7], "market_tickers": ["A"], "action": action }
            })
        });
        assert_eq!(commands.map(encoded), expected);
    }

    #[test]
    fn encodes_unsubscribe() {
        let expected = json!({ "id": 4, "cmd": "unsubscribe", "params": { "sids": [7] } });
        let command = Command::Unsubscribe {
            id: 4,
            sub: SubId(7),
        };
        assert_eq!(encoded(command), expected);
    }
}
