//! Helpers shared by the writer and verify integration tests. Each test crate
//! uses a different subset of them.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::core::events::{FeedBody, FeedEvent, LevelChange, QuantityChange};
use engineering_challenge::core::writer::{
    CHUNK_SIZE, WriterConfig, WriterMessage, WriterStats, spawn_writer,
};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};
use tokio::sync::mpsc;

/// A `(price, quantity)` level.
pub type Level = (i64, i64);

/// `(price, quantity)` levels.
pub type Levels = Vec<Level>;

/// One message read back from a recording.
#[derive(Debug, Clone, PartialEq)]
pub enum Recorded {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

/// A fixed instant plus `millis` milliseconds.
pub fn at(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(1_700_000_000_000 + millis).unwrap()
}

/// A feed snapshot of `symbol` received at `at(0)` with no exchange time.
pub fn feed_snapshot(symbol: &str, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> FeedEvent {
    feed_event(
        symbol,
        FeedBody::Snapshot {
            bids: bids.to_vec(),
            asks: asks.to_vec(),
        },
    )
}

/// A feed level change of `symbol` received at `at(0)` with no exchange
/// time.
pub fn feed_level(symbol: &str, change: LevelChange) -> FeedEvent {
    feed_event(symbol, FeedBody::Level(change))
}

/// A change to the `side` level at `price`.
pub fn level(side: Side, price: i64, change: QuantityChange) -> LevelChange {
    LevelChange {
        side,
        price,
        change,
    }
}

/// A change to the bid level at `price`.
pub fn bid(price: i64, change: QuantityChange) -> LevelChange {
    level(Side::Buy, price, change)
}

/// A change to the ask level at `price`.
pub fn ask(price: i64, change: QuantityChange) -> LevelChange {
    level(Side::Sell, price, change)
}

fn feed_event(symbol: &str, body: FeedBody) -> FeedEvent {
    FeedEvent {
        symbol: Arc::from(symbol),
        received_at: at(0),
        exchange_ts: None,
        seq_hint: None,
        body,
    }
}

/// `event` received at `at(received)`, with exchange time `at(exchange)`
/// if given.
pub fn stamped(event: FeedEvent, received: i64, exchange: Option<i64>) -> WriterMessage {
    WriterMessage::Event(FeedEvent {
        received_at: at(received),
        exchange_ts: exchange.map(at),
        ..event
    })
}

/// A `Snapshot` message with the given sides, best first.
pub fn snapshot_message(symbol: &str, times: (i64, i64), sides: (&[Level], &[Level])) -> Recorded {
    Recorded::Snapshot(SnapshotDomain {
        received_at: at(times.0),
        exchange_timestamp: at(times.1),
        bids: sides
            .0
            .iter()
            .map(|&(price_level, quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: sides
            .1
            .iter()
            .map(|&(price_level, quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: symbol.to_owned(),
    })
}

/// A `PriceLevelUpdate` message: `level` is `(side, price, quantity)`.
pub fn update_message(symbol: &str, times: (i64, i64), level: (Side, i64, i64)) -> Recorded {
    let (side, price_level, quantity) = level;
    Recorded::Update(PriceLevelUpdateDomain {
        backtest_timestamp: at(times.0),
        exchange_timestamp: at(times.1),
        price_level,
        quantity,
        side,
        symbol: symbol.to_owned(),
    })
}

/// Reads every message of the recording at `path`.
pub fn read_recording(path: &Path) -> anyhow::Result<Vec<Recorded>> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut messages = Vec::new();
    while let Some(message) = reader.read_msg()? {
        messages.push(match message {
            AnyMessage::Snapshot(decoder) => {
                Recorded::Snapshot(SnapshotDomain::try_from_decoder(decoder)?)
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                Recorded::Update(PriceLevelUpdateDomain::try_from_decoder(decoder)?)
            }
            _ => anyhow::bail!("unknown SBE message"),
        });
    }
    Ok(messages)
}

/// Writes `messages` to `path` with the template writer, as a recorder run
/// would.
pub fn write_recording(path: &Path, messages: &[Recorded]) -> anyhow::Result<()> {
    let mut writer = AlignedSbeWriter::create(path, CHUNK_SIZE)?;
    for message in messages {
        match message {
            Recorded::Snapshot(snapshot) => writer.write_msg(snapshot)?,
            Recorded::Update(update) => writer.write_msg(update)?,
        }
    }
    writer.flush()
}

/// What a writer run produced.
pub struct WriterRun {
    pub stats: WriterStats,
    pub messages: Vec<Recorded>,
    pub resnapshot_requests: Vec<Arc<str>>,
}

/// Runs the writer on `messages` with output to `output`, shuts it down, and
/// returns its stats and the resnapshot requests it sent. The snapshot tick
/// is an hour, so no periodic snapshot is written unless `configure` changes
/// it.
pub fn write_all(
    output: &Path,
    messages: Vec<WriterMessage>,
    configure: impl FnOnce(&mut WriterConfig),
) -> anyhow::Result<(WriterStats, Vec<Arc<str>>)> {
    let (requests, mut request_receiver) = mpsc::channel(16);
    let mut config = WriterConfig::new(output, requests);
    config.snapshot_tick = std::time::Duration::from_secs(3600);
    configure(&mut config);
    let (sender, handle) = spawn_writer(config)?;
    for message in messages {
        sender.blocking_send(message)?;
    }
    drop(sender);
    let stats = handle.join()?;
    let requests = std::iter::from_fn(|| request_receiver.try_recv().ok()).collect();
    Ok((stats, requests))
}

/// [`write_all`], then reads the output back.
pub fn run_writer(
    output: &Path,
    messages: Vec<WriterMessage>,
    configure: impl FnOnce(&mut WriterConfig),
) -> anyhow::Result<WriterRun> {
    let (stats, resnapshot_requests) = write_all(output, messages, configure)?;
    Ok(WriterRun {
        stats,
        messages: read_recording(output)?,
        resnapshot_requests,
    })
}

/// A snapshot's sides as `(price, quantity)` levels, best first.
pub fn snapshot_sides(snapshot: &SnapshotDomain) -> (Levels, Levels) {
    let bids = snapshot.bids.iter().map(|e| (e.price_level, e.quantity));
    let asks = snapshot.asks.iter().map(|e| (e.price_level, e.quantity));
    (bids.collect(), asks.collect())
}
