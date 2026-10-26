//! [`MockCatalog`]: an in-memory market catalog whose pages the test hands
//! over one at a time.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use anyhow::Context;
use engineering_challenge::exchange::{ListingReport, MarketCatalog, MarketInfo, MarketStatus};
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

/// How long a test waits for the catalog to be asked for a listing.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How many listings a startup walk makes: open markets, then paused ones.
const STARTUP_LISTINGS: usize = 2;

/// A catalog for a startup walk of two listings, then scripted ones.
///
/// The two startup listings, open markets then paused ones, serve whatever
/// pages the test sends through its [`CatalogPages`], in order, and finish
/// once the test drops them. Every later listing, such as a reconcile's
/// walks, waits for the next one the test scripts through
/// [`MockCatalog::walks`]. It ignores the status filter, so a test can list
/// markets in any state.
pub struct MockCatalog {
    startup: [Mutex<mpsc::UnboundedReceiver<Vec<MarketInfo>>>; STARTUP_LISTINGS],
    listings: AtomicUsize,
    later: Mutex<mpsc::UnboundedReceiver<Listing>>,
    later_sender: mpsc::UnboundedSender<Listing>,
    requests: mpsc::UnboundedSender<MarketStatus>,
    request_receiver: std::sync::Mutex<Option<mpsc::UnboundedReceiver<MarketStatus>>>,
}

/// The test's end of a [`MockCatalog`]'s startup walk: each page sent is
/// served next in its listing.
pub struct CatalogPages {
    open: mpsc::UnboundedSender<Vec<MarketInfo>>,
    paused: mpsc::UnboundedSender<Vec<MarketInfo>>,
}

/// The test's end of a [`MockCatalog`]'s listings after the startup walk.
pub struct CatalogWalks {
    listings: mpsc::UnboundedSender<Listing>,
    requests: mpsc::UnboundedReceiver<MarketStatus>,
}

/// One scripted listing after the startup walk.
struct Listing {
    markets: Vec<MarketInfo>,
    skipped: Vec<Arc<str>>,
}

impl MockCatalog {
    /// A catalog and the sender of its startup listings' pages.
    pub fn new() -> (Self, CatalogPages) {
        let (open, open_pages) = mpsc::unbounded_channel();
        let (paused, paused_pages) = mpsc::unbounded_channel();
        let (later_sender, later) = mpsc::unbounded_channel();
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let catalog = Self {
            startup: [Mutex::new(open_pages), Mutex::new(paused_pages)],
            listings: AtomicUsize::new(0),
            later: Mutex::new(later),
            later_sender,
            requests,
            request_receiver: std::sync::Mutex::new(Some(request_receiver)),
        };
        (catalog, CatalogPages { open, paused })
    }

    /// The script for the listings after the startup walk. Call it once.
    pub fn walks(&self) -> CatalogWalks {
        let mut receiver = self
            .request_receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        CatalogWalks {
            listings: self.later_sender.clone(),
            requests: receiver.take().expect("the walks were taken already"),
        }
    }

    /// Serves startup listing `index` until the test drops its pages.
    async fn serve_startup(
        &self,
        index: usize,
        pages: mpsc::Sender<Vec<MarketInfo>>,
    ) -> anyhow::Result<()> {
        let mut source = self.startup[index].lock().await;
        while let Some(page) = source.recv().await {
            pages.send(page).await?;
        }
        Ok(())
    }

    async fn serve_later(
        &self,
        status: MarketStatus,
        pages: mpsc::Sender<Vec<MarketInfo>>,
    ) -> anyhow::Result<ListingReport> {
        let _ = self.requests.send(status);
        let listing = self.later.lock().await.recv().await;
        let listing = listing.context("no listing scripted")?;
        if !listing.markets.is_empty() {
            pages.send(listing.markets).await?;
        }
        Ok(ListingReport {
            skipped: listing.skipped,
        })
    }
}

impl CatalogPages {
    /// Serves a page of `markets`, given as `(symbol, status)`.
    pub fn send(&self, markets: &[(&str, MarketStatus)]) {
        let page = markets
            .iter()
            .map(|&(symbol, status)| market(symbol, status))
            .collect();
        self.send_markets(page);
    }

    /// Serves a page of open markets.
    pub fn send_open(&self, symbols: &[&str]) {
        let markets: Vec<_> = symbols
            .iter()
            .map(|&symbol| (symbol, MarketStatus::Open))
            .collect();
        self.send(&markets);
    }

    /// Serves a page of `markets` as given.
    pub fn send_markets(&self, markets: Vec<MarketInfo>) {
        self.open.send(markets).expect("the catalog is gone");
    }

    /// Serves a page of `markets`, as given, in the startup's paused
    /// listing.
    pub fn send_paused_markets(&self, markets: Vec<MarketInfo>) {
        self.paused.send(markets).expect("the catalog is gone");
    }
}

impl CatalogWalks {
    /// Scripts the next listing: `markets`, given as `(symbol, status)`, in
    /// one page, with `skipped` named as markets it could not parse.
    pub fn serve(&self, markets: &[(&str, MarketStatus)], skipped: &[&str]) {
        let listing = Listing {
            markets: markets
                .iter()
                .map(|&(symbol, status)| market(symbol, status))
                .collect(),
            skipped: skipped.iter().map(|&symbol| Arc::from(symbol)).collect(),
        };
        let sent = self.listings.send(listing);
        sent.unwrap_or_else(|_| panic!("the catalog is gone"));
    }

    /// The status filter of the next listing asked for after the startup
    /// walk.
    pub async fn next_request(&mut self) -> MarketStatus {
        let request = timeout(REQUEST_TIMEOUT, self.requests.recv()).await;
        let request = request.expect("no listing asked for in time");
        request.expect("the catalog is gone")
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
        status: MarketStatus,
        pages: mpsc::Sender<Vec<MarketInfo>>,
    ) -> anyhow::Result<ListingReport> {
        let index = self.listings.fetch_add(1, Ordering::SeqCst);
        if index >= STARTUP_LISTINGS {
            return self.serve_later(status, pages).await;
        }
        self.serve_startup(index, pages).await?;
        Ok(ListingReport::default())
    }
}
