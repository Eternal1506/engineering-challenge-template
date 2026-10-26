//! Picks the markets the probe subscribes, from the REST catalog.

use std::sync::Arc;

use engineering_challenge::exchange::kalshi::KalshiCatalog;
use engineering_challenge::exchange::{MarketCatalog, MarketInfo, MarketStatus};
use tokio::sync::mpsc;

/// The markets one probe run subscribes.
pub struct ProbeMarkets {
    /// Open markets, in catalog order.
    pub open: Vec<Arc<str>>,
    /// One market that has not opened yet, if the catalog lists any.
    pub unopened: Option<Arc<str>>,
}

/// Lists the first `limit` open markets and one unopened market.
pub async fn fetch(limit: usize) -> anyhow::Result<ProbeMarkets> {
    let catalog = KalshiCatalog::production()?;
    let open = first_symbols(&catalog, MarketStatus::Open, limit).await?;
    let unopened = first_symbols(&catalog, MarketStatus::Unopened, 1).await?;
    Ok(ProbeMarkets {
        open,
        unopened: unopened.into_iter().next(),
    })
}

/// The first `limit` symbols with `status`. Stops the listing early once it
/// has enough, so the listing's "receiver closed" error is expected then.
async fn first_symbols(
    catalog: &KalshiCatalog,
    status: MarketStatus,
    limit: usize,
) -> anyhow::Result<Vec<Arc<str>>> {
    let (sender, receiver) = mpsc::channel(2);
    let (listing, symbols) = tokio::join!(
        catalog.list_markets(status, sender),
        take_symbols(receiver, limit)
    );
    if symbols.len() < limit {
        listing?;
    }
    Ok(symbols)
}

async fn take_symbols(mut pages: mpsc::Receiver<Vec<MarketInfo>>, limit: usize) -> Vec<Arc<str>> {
    let mut symbols = Vec::with_capacity(limit);
    while symbols.len() < limit {
        let Some(page) = pages.recv().await else {
            break;
        };
        let wanted = limit - symbols.len();
        symbols.extend(page.into_iter().take(wanted).map(|market| market.symbol));
    }
    symbols
}
