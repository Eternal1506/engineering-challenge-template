//! `probe`: a live check of the Kalshi websocket feed.
//!
//! Subscribes the first N open markets on one connection and every market's
//! lifecycle on another, decodes every frame with [`KalshiCodec`], keeps a
//! local book per market, and reports what it saw on stderr. Partway through
//! it asks for fresh snapshots, subscribes an unopened market, and sends a
//! command for an unknown subscription, to record the replies.
//!
//! [`KalshiCodec`]: engineering_challenge::exchange::kalshi::KalshiCodec

mod books;
mod findings;
mod fixtures;
mod markets;
mod report;
mod session;
mod socket;

use engineering_challenge::cli::ProbeArgs;
use engineering_challenge::exchange::kalshi::{KalshiCredentials, KalshiEndpoint, RequestSigner};

use session::Session;

/// Runs the probe described by `args`, authenticating with `credentials`.
pub async fn run(args: &ProbeArgs, credentials: &KalshiCredentials) -> anyhow::Result<()> {
    let markets = markets::fetch(args.tickers.get()).await?;
    tracing::info!(
        open = markets.open.len(),
        unopened = ?markets.unopened,
        "picked markets; watching the live feed for {:?}",
        args.duration
    );
    let endpoint = KalshiEndpoint::production(RequestSigner::new(credentials)?);
    let mut session = Session::new(markets);
    session.run(&endpoint, args.duration).await?;
    let seen = session.observations();
    report::print(seen, args.duration);
    if let Some(dir) = &args.fixtures_dir {
        seen.fixtures.write(dir)?;
        tracing::info!(?dir, "saved fixtures");
    }
    Ok(())
}
