//! [`MockCatalog`]: an in-memory market catalog whose pages the test hands
//! over one at a time.

use std::sync::Arc;

use engineering_challenge::exchange::{ListingReport, MarketCatalog, MarketInfo, MarketStatus};
use tokio::sync::{Mutex, mpsc};

/// A catalog that serves whatever pages the test sends through its
/// [`CatalogPages`], in order, and finishes a listing once the test drops
/// them. It ignores the status filter, so a test can list markets in any
/// state.
pub struct MockCatalog {
    pages: Mutex<mpsc::UnboundedReceiver<Vec<MarketInfo>>>,
}

/// The test's end of a [`MockCatalog`]: each page sent is served next.
pub struct CatalogPages(mpsc::UnboundedSender<Vec<MarketInfo>>);

impl MockCatalog {
    /// A catalog and the sender of its pages.
    pub fn new() -> (Self, CatalogPages) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let catalog = Self {
            pages: Mutex::new(receiver),
        };
        (catalog, CatalogPages(sender))
    }
}

impl CatalogPages {
    /// Serves a page of `markets`, given as `(symbol, status)`.
    pub fn send(&self, markets: &[(&str, MarketStatus)]) {
        let page = markets
            .iter()
            .map(|&(symbol, status)| market(symbol, status))
            .collect();
        self.0.send(page).expect("the catalog is gone");
    }

    /// Serves a page of open markets.
    pub fn send_open(&self, symbols: &[&str]) {
        let markets: Vec<_> = symbols
            .iter()
            .map(|&symbol| (symbol, MarketStatus::Open))
            .collect();
        self.send(&markets);
    }
}

/// A market listed with `status` and no open or close time.
pub fn market(symbol: &str, status: MarketStatus) -> MarketInfo {
    MarketInfo {
        symbol: Arc::from(symbol),
        status,
        open_ts: None,
        close_ts: None,
    }
}

impl MarketCatalog for MockCatalog {
    async fn list_markets(
        &self,
        _status: MarketStatus,
        pages: mpsc::Sender<Vec<MarketInfo>>,
    ) -> anyhow::Result<ListingReport> {
        let mut source = self.pages.lock().await;
        while let Some(page) = source.recv().await {
            pages.send(page).await?;
        }
        Ok(ListingReport::default())
    }
}
