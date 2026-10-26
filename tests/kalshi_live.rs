//! Live checks against the production Kalshi API. Run them with
//! `cargo test --test kalshi_live -- --ignored --nocapture`.
//!
//! `live_authenticated_request`, `live_connection_smoke` and
//! `live_bogus_ticker_probe` read credentials from the directory in
//! `KALSHI_SECRETS_DIR`, and never print them.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use engineering_challenge::core::events::{FeedBody, FeedEvent, Inbound};
use engineering_challenge::core::ws::{Connection, ConnectionEvent, Request, Subscription};
use engineering_challenge::exchange::kalshi::{
    KalshiCatalog, KalshiCodec, KalshiCredentials, KalshiEndpoint, REST_BASE_URL, RequestSigner,
};
use engineering_challenge::exchange::{MarketCatalog, MarketInfo, MarketStatus};
use reqwest::{StatusCode, Url};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Lists every market with `status`, printing the page count and timing, and
/// checks that no market was skipped.
async fn list_all(status: MarketStatus) -> Vec<MarketInfo> {
    let catalog = KalshiCatalog::production().unwrap();
    let (sender, mut receiver) = mpsc::channel(4);
    let start = Instant::now();
    let collect = async {
        let mut pages = Vec::new();
        while let Some(page) = receiver.recv().await {
            pages.push(page);
        }
        pages
    };
    let (listing, pages) = tokio::join!(catalog.list_markets(status, sender), collect);
    let report = listing.unwrap();
    println!(
        "{status:?}: {} pages in {:.1} s, {} skipped",
        pages.len(),
        start.elapsed().as_secs_f64(),
        report.skipped.len()
    );
    assert_eq!(report.skipped, Vec::<std::sync::Arc<str>>::new());
    pages.into_iter().flatten().collect()
}

fn status_counts(markets: &[MarketInfo]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for market in markets {
        *counts.entry(format!("{:?}", market.status)).or_insert(0) += 1;
    }
    counts
}

#[tokio::test]
#[ignore = "calls the live Kalshi API"]
async fn live_open_markets_walk() {
    let markets = list_all(MarketStatus::Open).await;
    let count = |keep: fn(&MarketInfo) -> bool| markets.iter().filter(|m| keep(m)).count();
    let multivariate = count(|market| market.symbol.starts_with("KXMVE"));
    let without_close = count(|market| market.close_ts.is_none());
    let distinct: BTreeSet<&str> = markets.iter().map(|market| &*market.symbol).collect();
    println!(
        "open markets: {} ({} distinct)",
        markets.len(),
        distinct.len()
    );
    println!("statuses: {:?}", status_counts(&markets));
    println!("KXMVE tickers: {multivariate}, missing close_ts: {without_close}");
    assert!(!markets.is_empty());
    assert_eq!((multivariate, without_close), (0, 0));
}

#[tokio::test]
#[ignore = "calls the live Kalshi API"]
async fn live_paused_status_accepted() {
    let markets = list_all(MarketStatus::Paused).await;
    println!("paused markets: {}", markets.len());
    println!("statuses: {:?}", status_counts(&markets));
}

#[tokio::test]
#[ignore = "calls the live Kalshi API with the account's credentials"]
async fn live_authenticated_request() {
    let signer = signer_from_env();
    let url = Url::parse(&format!("{REST_BASE_URL}/portfolio/balance")).unwrap();

    let mut request = reqwest::Client::new().get(url.clone());
    for (name, value) in signer.headers("GET", url.path(), Utc::now()) {
        request = request.header(name, value);
    }
    let status = request.send().await.unwrap().status();

    println!("GET {}: HTTP {status}", url.path());
    assert_eq!(status, StatusCode::OK);
}

/// A signer for the credentials in `KALSHI_SECRETS_DIR`.
fn signer_from_env() -> RequestSigner {
    let secrets_dir = std::env::var("KALSHI_SECRETS_DIR").expect("set KALSHI_SECRETS_DIR");
    let credentials = KalshiCredentials::load(&PathBuf::from(secrets_dir)).unwrap();
    RequestSigner::new(&credentials).unwrap()
}

/// The first `limit` open markets, stopping the listing once it has them.
async fn first_open_symbols(limit: usize) -> Vec<Arc<str>> {
    let catalog = KalshiCatalog::production().unwrap();
    let (sender, mut pages) = mpsc::channel::<Vec<MarketInfo>>(2);
    let take = async move {
        let mut symbols = Vec::new();
        while symbols.len() < limit {
            let Some(page) = pages.recv().await else {
                break;
            };
            symbols.extend(page.into_iter().map(|market| market.symbol));
        }
        symbols.truncate(limit);
        symbols
    };
    let (_, symbols) = tokio::join!(catalog.list_markets(MarketStatus::Open, sender), take);
    symbols
}

/// What one connection reported.
#[derive(Debug, Default)]
struct Tally {
    connected: usize,
    disconnected: usize,
    gaps: usize,
    rejected: usize,
    errors: usize,
    deltas: usize,
    untagged_snapshots: usize,
    tagged_snapshots: usize,
    /// Every event other than book data, described in arrival order.
    notes: Vec<String>,
    /// The ticker list of the last `ok` reply.
    last_ack: Option<Vec<Arc<str>>>,
    snapshot_symbols: BTreeSet<Arc<str>>,
}

impl Tally {
    fn print(&self) {
        println!(
            "Connected {}, Disconnected {}, Gap {}, Rejected {}, error frames {}",
            self.connected, self.disconnected, self.gaps, self.rejected, self.errors
        );
        println!(
            "snapshots: {} untagged, {} tagged; deltas: {}; distinct snapshot symbols: {}",
            self.untagged_snapshots,
            self.tagged_snapshots,
            self.deltas,
            self.snapshot_symbols.len()
        );
        self.notes.iter().for_each(|note| println!("  {note}"));
    }

    fn count(&mut self, event: ConnectionEvent) {
        match &event {
            ConnectionEvent::Connected => self.connected += 1,
            ConnectionEvent::Disconnected { .. } => self.disconnected += 1,
            ConnectionEvent::Gap { .. } => self.gaps += 1,
            ConnectionEvent::Rejected { .. } => self.rejected += 1,
            ConnectionEvent::Frame { decoded, tag } => {
                return self.count_message(&decoded.message, decoded.reply_to, *tag);
            }
        }
        self.notes.push(format!("{event:?}"));
    }

    fn count_message(&mut self, message: &Inbound, reply_to: Option<u64>, tag: Option<u64>) {
        match message {
            Inbound::Feed(event) => self.count_feed(event, tag),
            Inbound::Ack { sub, symbols } => {
                let count = symbols.as_ref().map(Vec::len);
                let note = format!("Ack {{ sub: {}, tickers: {count:?} }}", sub.0);
                self.notes.push(format!("{note} reply_to {reply_to:?}"));
                self.last_ack.clone_from(symbols);
            }
            other => {
                self.errors += usize::from(matches!(other, Inbound::Error { .. }));
                self.notes.push(format!("{other:?} reply_to {reply_to:?}"));
            }
        }
    }

    fn count_feed(&mut self, event: &FeedEvent, tag: Option<u64>) {
        match (&event.body, tag) {
            (FeedBody::Level(_), _) => self.deltas += 1,
            (FeedBody::Snapshot { .. }, None) => self.untagged_snapshots += 1,
            (FeedBody::Snapshot { .. }, Some(_)) => self.tagged_snapshots += 1,
        }
        if matches!(event.body, FeedBody::Snapshot { .. }) {
            self.snapshot_symbols.insert(event.symbol.clone());
        }
    }
}

/// A live connection on its own task.
struct LiveRun {
    requests: mpsc::UnboundedSender<Request>,
    events: mpsc::Receiver<ConnectionEvent>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl LiveRun {
    fn start(symbols: &[Arc<str>]) -> Self {
        let subscription = Subscription::Books(symbols.iter().cloned().collect());
        let endpoint = KalshiEndpoint::production(signer_from_env());
        let connection = Connection::new(endpoint, KalshiCodec, subscription);
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let (event_sender, events) = mpsc::channel(65_536);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(connection.run(request_receiver, event_sender, cancel.clone()));
        Self {
            requests,
            events,
            cancel,
            task,
        }
    }

    /// Sends each request in `plan` at its offset in seconds from now.
    fn schedule(&self, plan: Vec<(u64, Request)>) {
        let requests = self.requests.clone();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now();
            for (at, request) in plan {
                tokio::time::sleep_until(start + Duration::from_secs(at)).await;
                requests.send(request).unwrap();
            }
        });
    }

    /// Tallies events for `duration`, or until the connection ends.
    async fn watch(&mut self, duration: Duration) -> Tally {
        let mut tally = Tally::default();
        let deadline = tokio::time::sleep(duration);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                () = &mut deadline => return tally,
                event = self.events.recv() => match event {
                    Some(event) => tally.count(event),
                    None => return tally,
                },
            }
        }
    }

    /// Cancels the connection and returns how long it took to shut down.
    async fn stop(self) -> Duration {
        let stopping = Instant::now();
        self.cancel.cancel();
        self.task.await.unwrap();
        stopping.elapsed()
    }
}

const SNAPSHOT_TAG: u64 = 7;

/// 1,200 tickers (three subscribe batches) for 30 s, with a tagged
/// `get_snapshot` of 5 tickers at 10 s and a remove then re-add of one
/// ticker at 15 s and 18 s.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "connects to the live Kalshi websocket with the account's credentials"]
async fn live_connection_smoke() {
    let symbols = first_open_symbols(1200).await;
    let mut run = LiveRun::start(&symbols);
    let toggled = vec![symbols[10].clone()];
    run.schedule(vec![
        (
            10,
            Request::RequestSnapshot {
                tag: SNAPSHOT_TAG,
                symbols: symbols[..5].to_vec(),
            },
        ),
        (15, Request::Remove(toggled.clone())),
        (18, Request::Add(toggled)),
    ]);
    let tally = run.watch(Duration::from_secs(30)).await;
    let shutdown = run.stop().await;

    println!("subscribed {} tickers for 30 s", symbols.len());
    tally.print();
    println!("shut down in {shutdown:?}");
    assert_eq!((tally.connected, tally.disconnected, tally.gaps), (1, 0, 0));
    assert_eq!((tally.errors, tally.rejected), (0, 0));
    assert!(tally.untagged_snapshots >= symbols.len(), "one per ticker");
    assert_eq!(tally.tagged_snapshots, 5);
    assert!(shutdown < Duration::from_secs(2));
}

/// Records how Kalshi answers a subscribe that names one ticker that does
/// not exist among 10 that do, and an add of a ticker that does not exist.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "connects to the live Kalshi websocket with the account's credentials"]
async fn live_bogus_ticker_probe() {
    let mut symbols = first_open_symbols(10).await;
    symbols.push(Arc::from("KXBOGUS-NOPE"));
    let mut run = LiveRun::start(&symbols);
    run.schedule(vec![(5, Request::Add(vec![Arc::from("KXBOGUS-NOPE2")]))]);
    let tally = run.watch(Duration::from_secs(10)).await;
    run.stop().await;

    println!("subscribed 10 real tickers and KXBOGUS-NOPE, added KXBOGUS-NOPE2 at 5 s");
    tally.print();
    for bogus in ["KXBOGUS-NOPE", "KXBOGUS-NOPE2"] {
        let snapshot = tally.snapshot_symbols.contains(bogus);
        let listed = tally
            .last_ack
            .as_ref()
            .map(|tickers| tickers.iter().any(|ticker| &**ticker == bogus));
        println!("{bogus}: snapshot {snapshot}, in the last ok's ticker list {listed:?}");
    }
}
