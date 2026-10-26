//! Types that cross the boundary between exchange integrations and the core.
//!
//! An exchange codec turns each websocket frame into [`Decoded`] messages and
//! each [`Command`] into a frame. Prices and quantities are fixed point,
//! scaled by [`FIXED_POINT_SCALE`](crate::core::fixed::FIXED_POINT_SCALE).

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::Side;

/// How a feed reports the new size of one price level, in fixed point.
///
/// Some exchanges send the absolute size of a level after the change; others,
/// such as Kalshi, send the signed change to add to the current size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityChange {
    /// The level's total quantity after the change.
    Absolute(i64),
    /// The signed amount to add to the level's current quantity.
    Delta(i64),
}

/// A change to one price level of one book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelChange {
    /// Which side of the book the level is on.
    pub side: Side,
    /// The level's price, in fixed point.
    pub price: i64,
    /// How the level's quantity changes.
    pub change: QuantityChange,
}

/// An exchange's identifier for one subscription on one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubId(pub u64);

/// Where a message sits in its subscription's numbered stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamPosition {
    /// The subscription.
    pub sub: SubId,
    /// The message's number in that subscription's stream.
    pub seq: u64,
}

/// One decoded message together with the envelope fields any message type
/// may carry.
///
/// Contract: on one connection, in arrival order, the messages that have a
/// `position` are contiguous per subscription, whatever their type. A jump
/// means the connection lost messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    /// The message's place in its subscription's stream, if numbered.
    pub position: Option<StreamPosition>,
    /// The id of the command this message answers, if it answers one.
    pub reply_to: Option<u64>,
    /// The message itself.
    pub message: Inbound,
}

/// One market-data message for one symbol.
///
/// The ordering contract every codec must meet:
///
/// - `seq_hint` never decreases for a symbol, on any connection, and is the
///   same on every connection for the same change. Several events may share a
///   value.
/// - A [`FeedBody::Snapshot`] covers every event for its symbol whose
///   `seq_hint` is less than or equal to its own, and none above it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedEvent {
    /// The exchange's market identifier.
    pub symbol: Arc<str>,
    /// When the frame arrived, stamped before decoding.
    pub received_at: DateTime<Utc>,
    /// When the exchange made the change or sent the snapshot, if it says.
    pub exchange_ts: Option<DateTime<Utc>>,
    /// The exchange's per-symbol ordering value, if it sends one.
    pub seq_hint: Option<u64>,
    /// The book data.
    pub body: FeedBody,
}

/// The book data a [`FeedEvent`] carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedBody {
    /// The whole book, as `(price, quantity)` levels in no particular order.
    Snapshot {
        /// Bid levels.
        bids: Vec<(i64, i64)>,
        /// Ask levels.
        asks: Vec<(i64, i64)>,
    },
    /// A change to one level.
    Level(LevelChange),
}

/// Why the exchange rejected a command or dropped a subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedErrorKind {
    /// The subscription fell too far behind and the exchange dropped it.
    Overflow,
    /// A command named a subscription the exchange does not know.
    UnknownSubscription,
    /// Any other error, with the exchange's code and text.
    Other {
        /// The exchange's error code.
        code: i64,
        /// The exchange's description.
        message: String,
    },
}

/// A market lifecycle change, in exchange-neutral terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    /// A new market was listed.
    Created {
        /// The new market.
        symbol: Arc<str>,
        /// When trading opens, if the exchange says.
        open_ts: Option<DateTime<Utc>>,
        /// When trading closes, if the exchange says.
        close_ts: Option<DateTime<Utc>>,
    },
    /// Trading opened or resumed.
    Activated {
        /// The market.
        symbol: Arc<str>,
    },
    /// Trading paused.
    Deactivated {
        /// The market.
        symbol: Arc<str>,
    },
    /// The scheduled close time changed.
    CloseChanged {
        /// The market.
        symbol: Arc<str>,
        /// The new close time.
        close_ts: DateTime<Utc>,
    },
    /// The outcome is known; trading has ended.
    Determined {
        /// The market.
        symbol: Arc<str>,
    },
    /// The market paid out.
    Settled {
        /// The market.
        symbol: Arc<str>,
    },
}

/// One decoded message from an exchange connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inbound {
    /// Book data.
    Feed(FeedEvent),
    /// The command in [`Decoded::reply_to`], always set for this message,
    /// created subscription `sub`.
    Subscribed {
        /// The new subscription.
        sub: SubId,
    },
    /// The command in [`Decoded::reply_to`], always set for this message,
    /// changed subscription `sub`.
    Ack {
        /// The subscription the command changed.
        sub: SubId,
        /// The subscription's full symbol list afterwards, if the exchange
        /// sends it.
        symbols: Option<Vec<Arc<str>>>,
    },
    /// The command in [`Decoded::reply_to`], always set for this message,
    /// ended subscription `sub`.
    Unsubscribed {
        /// The ended subscription.
        sub: SubId,
    },
    /// The exchange reported an error, in reply to the command in
    /// [`Decoded::reply_to`] if set.
    Error {
        /// The subscription the error concerns, if it names one.
        sub: Option<SubId>,
        /// What went wrong.
        kind: FeedErrorKind,
    },
    /// A market lifecycle change.
    Lifecycle(LifecycleEvent),
    /// A message the core does not use.
    Ignored,
}

/// A request to send to an exchange connection. Every command carries an id
/// that the exchange echoes in its reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Subscribe to the order books of `symbols`, as one new subscription.
    Subscribe {
        /// The command's id.
        id: u64,
        /// The markets to subscribe.
        symbols: Vec<Arc<str>>,
    },
    /// Subscribe to every market's lifecycle changes.
    SubscribeLifecycle {
        /// The command's id.
        id: u64,
    },
    /// Add `symbols` to subscription `sub`.
    AddSymbols {
        /// The command's id.
        id: u64,
        /// The subscription to change.
        sub: SubId,
        /// The markets to add.
        symbols: Vec<Arc<str>>,
    },
    /// Remove `symbols` from subscription `sub`.
    RemoveSymbols {
        /// The command's id.
        id: u64,
        /// The subscription to change.
        sub: SubId,
        /// The markets to remove.
        symbols: Vec<Arc<str>>,
    },
    /// Ask for fresh snapshots of `symbols` on subscription `sub`, without
    /// changing it.
    RequestSnapshot {
        /// The command's id.
        id: u64,
        /// The subscription the snapshots arrive on.
        sub: SubId,
        /// The markets to snapshot.
        symbols: Vec<Arc<str>>,
    },
    /// End subscription `sub`.
    Unsubscribe {
        /// The command's id.
        id: u64,
        /// The subscription to end.
        sub: SubId,
    },
}
