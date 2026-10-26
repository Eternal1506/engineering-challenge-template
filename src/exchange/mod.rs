//! Exchange-specific integrations, and the exchange-agnostic types and traits
//! the core uses to talk to them.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

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
