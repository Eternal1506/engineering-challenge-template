//! Live checks against the production Kalshi API. Run them with
//! `cargo test --test kalshi_live -- --ignored --nocapture`.
//!
//! `live_authenticated_request` reads credentials from the directory in
//! `KALSHI_SECRETS_DIR`, and prints only the HTTP status.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use chrono::Utc;
use engineering_challenge::exchange::kalshi::{
    KalshiCatalog, KalshiCredentials, REST_BASE_URL, RequestSigner,
};
use engineering_challenge::exchange::{MarketCatalog, MarketInfo, MarketStatus};
use reqwest::{StatusCode, Url};
use tokio::sync::mpsc;

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
    let secrets_dir = std::env::var("KALSHI_SECRETS_DIR").expect("set KALSHI_SECRETS_DIR");
    let credentials = KalshiCredentials::load(&PathBuf::from(secrets_dir)).unwrap();
    let signer = RequestSigner::new(&credentials).unwrap();
    let url = Url::parse(&format!("{REST_BASE_URL}/portfolio/balance")).unwrap();

    let mut request = reqwest::Client::new().get(url.clone());
    for (name, value) in signer.headers("GET", url.path(), Utc::now()) {
        request = request.header(name, value);
    }
    let status = request.send().await.unwrap().status();

    println!("GET {}: HTTP {status}", url.path());
    assert_eq!(status, StatusCode::OK);
}
