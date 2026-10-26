//! Exchange-specific integrations, and the exchange-agnostic types and traits
//! the core uses to talk to them.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::core::events::{Command, Decoded};

pub mod kalshi;

/// The trading state of a market, in exchange-neutral terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketStatus {
    /// Listed but not yet open for trading.
    Unopened,
    /// Open for trading.
    Open,
    /// Temporarily halted; expected to reopen.
    Paused,
    /// No longer trading, outcome not yet settled.
    Closed,
    /// Outcome determined or settled.
    Settled,
}

/// One market as listed by an exchange's catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketInfo {
    /// The exchange's identifier for the market, written as the SBE symbol.
    pub symbol: Arc<str>,
    /// The market's trading state when it was listed.
    pub status: MarketStatus,
    /// When trading opens, if the exchange says.
    pub open_ts: Option<DateTime<Utc>>,
    /// When trading closes, if the exchange says.
    pub close_ts: Option<DateTime<Utc>>,
}

/// An exchange's list of markets.
pub trait MarketCatalog {
    /// Lists every market with `status`, sending each page to `pages` as it
    /// arrives so the caller can act before the listing finishes.
    ///
    /// Returns once the last page has been sent. Fails if the exchange cannot
    /// be reached after retries, a market cannot be parsed, or `pages` is
    /// closed before the listing finishes.
    fn list_markets(
        &self,
        status: MarketStatus,
        pages: mpsc::Sender<Vec<MarketInfo>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// Where an exchange's market-data websocket lives and how to authenticate
/// to it.
pub trait FeedEndpoint {
    /// The websocket URL.
    fn url(&self) -> &str;

    /// The extra HTTP headers for a handshake made at `now`, such as a fresh
    /// signature. Called once per connection attempt.
    fn handshake_headers(&self, now: DateTime<Utc>) -> Vec<(&'static str, String)>;
}

/// Translates between an exchange's websocket messages and the core's
/// [`Command`] and [`Decoded`] types.
pub trait FeedCodec {
    /// The text frame that sends `command`.
    fn encode(&self, command: &Command) -> String;

    /// Decodes one text frame received at `received_at`, appending every
    /// message it carries to `out`. Message types the core does not use
    /// decode to [`Inbound::Ignored`](crate::core::events::Inbound::Ignored),
    /// still with their stream position, so sequence checks see them.
    ///
    /// Fails, appending nothing, if the frame is not valid JSON or a field
    /// the core uses is missing or malformed; the error names the field and,
    /// where known, the symbol. A frame that fails still used its slot in its
    /// subscription's stream, so the next message on that subscription shows
    /// a gap.
    fn decode(
        &self,
        text: &str,
        received_at: DateTime<Utc>,
        out: &mut Vec<Decoded>,
    ) -> anyhow::Result<()>;
}
