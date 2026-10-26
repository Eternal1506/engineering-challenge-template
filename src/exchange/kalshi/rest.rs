//! [`KalshiCatalog`] lists Kalshi markets through the public REST API.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use tokio::sync::mpsc;

use super::throttle::{Backoff, TokenBucket};
use crate::exchange::{ListingReport, MarketCatalog, MarketInfo, MarketStatus};

/// The production REST API, including its `/trade-api/v2` prefix.
pub const REST_BASE_URL: &str = "https://external-api.kalshi.com/trade-api/v2";

const PAGE_LIMIT: &str = "1000";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const ERROR_BODY_PREVIEW_CHARS: usize = 200;

/// The steady request rate. Live walks at 15 per second with bursts of 5
/// drew a 429 about every 2 s.
const REQUESTS_PER_SECOND: u32 = 10;

/// The most requests sent back to back. One keeps the pacing even.
const REQUEST_BURST: u32 = 1;

const PRODUCTION_LIMITS: CatalogLimits = CatalogLimits {
    requests_per_second: REQUESTS_PER_SECOND,
    burst: REQUEST_BURST,
    backoff: Backoff {
        first_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        max_attempts: 8,
    },
    max_pages: 10_000,
};

/// Lists regular (non-multivariate) Kalshi markets from `GET /markets`.
///
/// Requests are paced at 10 per second, one at a time, and a 429, a
/// 5xx or a transport error is retried with exponential backoff (1 s doubling
/// to 30 s, 8 attempts). A listing stops with an error after 10,000 pages.
///
/// The request budget belongs to the catalog, so a process should build one
/// catalog and share it between every listing it runs.
pub struct KalshiCatalog {
    http: reqwest::Client,
    markets_url: Url,
    bucket: TokenBucket,
    backoff: Backoff,
    max_pages: usize,
}

/// How hard a catalog may push the API, and how long a listing may run.
#[derive(Debug, Clone, Copy)]
struct CatalogLimits {
    requests_per_second: u32,
    burst: u32,
    backoff: Backoff,
    max_pages: usize,
}

/// Why one attempt at fetching a page failed.
enum AttemptError {
    /// Worth retrying after a backoff; the text says what went wrong.
    Retryable(String),
    /// Retrying would fail the same way.
    Fatal(anyhow::Error),
}

/// One page of `GET /markets`.
#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<RawMarket>,
    cursor: Option<String>,
}

/// The fields of a Kalshi market the recorder uses.
#[derive(Deserialize)]
struct RawMarket {
    ticker: String,
    status: String,
    open_time: Option<String>,
    close_time: Option<String>,
}

impl KalshiCatalog {
    /// A catalog for the production API at [`REST_BASE_URL`].
    pub fn production() -> anyhow::Result<Self> {
        Self::new(REST_BASE_URL)
    }

    /// A catalog for the API at `base_url`, which includes the
    /// `/trade-api/v2` prefix.
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        Self::with_limits(base_url, PRODUCTION_LIMITS)
    }

    fn with_limits(base_url: &str, limits: CatalogLimits) -> anyhow::Result<Self> {
        let markets_url = format!("{}/markets", base_url.trim_end_matches('/'));
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build the HTTP client")?;
        Ok(Self {
            http,
            markets_url: Url::parse(&markets_url)
                .with_context(|| format!("invalid Kalshi REST base URL {base_url:?}"))?,
            bucket: TokenBucket::new(limits.requests_per_second, limits.burst),
            backoff: limits.backoff,
            max_pages: limits.max_pages,
        })
    }

    /// Fetches one page, retrying with backoff until it succeeds, fails for
    /// good, or runs out of attempts.
    async fn fetch_page(
        &self,
        status: MarketStatus,
        cursor: Option<&str>,
    ) -> anyhow::Result<MarketsPage> {
        let query = page_query(status, cursor);
        let mut attempt = 0;
        loop {
            attempt += 1;
            self.bucket.acquire().await;
            let reason = match self.try_fetch_page(&query).await {
                Ok(page) => return Ok(page),
                Err(AttemptError::Fatal(error)) => return Err(error),
                Err(AttemptError::Retryable(reason)) => reason,
            };
            let Some(delay) = self.backoff.delay_after(attempt) else {
                anyhow::bail!(
                    "GET {} failed after {attempt} attempts: {reason}",
                    self.markets_url
                );
            };
            tracing::warn!(attempt, ?delay, %reason, "Kalshi markets request failed, retrying");
            tokio::time::sleep(delay).await;
        }
    }

    async fn try_fetch_page(&self, query: &[(&str, &str)]) -> Result<MarketsPage, AttemptError> {
        let request = self.http.get(self.markets_url.clone()).query(query);
        let response = request.send().await.map_err(retryable)?;
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return Err(AttemptError::Retryable(format!("HTTP {status}")));
        }
        let body = response.bytes().await.map_err(retryable)?;
        if !status.is_success() {
            let preview = String::from_utf8_lossy(&body);
            let preview: String = preview.chars().take(ERROR_BODY_PREVIEW_CHARS).collect();
            let error = anyhow!("GET {} returned HTTP {status}: {preview}", self.markets_url);
            return Err(AttemptError::Fatal(error));
        }
        serde_json::from_slice(&body)
            .context("failed to parse the Kalshi markets page")
            .map_err(AttemptError::Fatal)
    }
}

impl MarketCatalog for KalshiCatalog {
    async fn list_markets(
        &self,
        status: MarketStatus,
        pages: mpsc::Sender<Vec<MarketInfo>>,
    ) -> anyhow::Result<ListingReport> {
        let mut report = ListingReport::default();
        let mut cursor: Option<String> = None;
        for _ in 0..self.max_pages {
            let page = self.fetch_page(status, cursor.as_deref()).await?;
            let (markets, skipped) = parse_markets(page.markets);
            report.skipped.extend(skipped);
            send_page(&pages, markets).await?;
            let Some(next) = page.cursor.filter(|next| !next.is_empty()) else {
                return Ok(report);
            };
            anyhow::ensure!(
                cursor.as_deref() != Some(next.as_str()),
                "Kalshi returned cursor {next:?} twice in a row"
            );
            cursor = Some(next);
        }
        anyhow::bail!(
            "Kalshi {status:?} listing ran past {} pages",
            self.max_pages
        )
    }
}

impl RawMarket {
    fn to_market_info(&self) -> anyhow::Result<MarketInfo> {
        let ticker = &self.ticker;
        Ok(MarketInfo {
            symbol: ticker.as_str().into(),
            status: parse_status(&self.status, ticker)?,
            open_ts: parse_time(self.open_time.as_deref(), "open_time", ticker)?,
            close_ts: parse_time(self.close_time.as_deref(), "close_time", ticker)?,
        })
    }
}

/// Converts a page of raw markets. Markets that cannot be parsed are logged
/// and returned separately as tickers.
fn parse_markets(raw: Vec<RawMarket>) -> (Vec<MarketInfo>, Vec<Arc<str>>) {
    let mut markets = Vec::with_capacity(raw.len());
    let mut skipped = Vec::new();
    for market in raw {
        match market.to_market_info() {
            Ok(info) => markets.push(info),
            Err(error) => {
                tracing::warn!("skipping a Kalshi market: {error:#}");
                skipped.push(market.ticker.into());
            }
        }
    }
    (markets, skipped)
}

/// Sends a page downstream, unless it is empty.
async fn send_page(
    pages: &mpsc::Sender<Vec<MarketInfo>>,
    markets: Vec<MarketInfo>,
) -> anyhow::Result<()> {
    if markets.is_empty() {
        return Ok(());
    }
    tracing::debug!(count = markets.len(), "fetched a page of Kalshi markets");
    pages
        .send(markets)
        .await
        .map_err(|_| anyhow!("market page receiver closed before the listing finished"))
}

fn retryable(error: reqwest::Error) -> AttemptError {
    AttemptError::Retryable(error.to_string())
}

fn page_query(status: MarketStatus, cursor: Option<&str>) -> Vec<(&'static str, &str)> {
    let mut query = vec![
        ("status", query_status(status)),
        ("mve_filter", "exclude"),
        ("limit", PAGE_LIMIT),
    ];
    query.extend(cursor.map(|cursor| ("cursor", cursor)));
    query
}

/// The `status` filter value Kalshi accepts on `GET /markets`.
fn query_status(status: MarketStatus) -> &'static str {
    match status {
        MarketStatus::Unopened => "unopened",
        MarketStatus::Open => "open",
        MarketStatus::Paused => "paused",
        MarketStatus::Closed => "closed",
        MarketStatus::Settled => "settled",
    }
}

/// Maps the `status` Kalshi reports on a market. Every state after the
/// outcome is known (`determined` through `finalized`) counts as settled.
fn parse_status(value: &str, ticker: &str) -> anyhow::Result<MarketStatus> {
    Ok(match value {
        "initialized" => MarketStatus::Unopened,
        "active" => MarketStatus::Open,
        "inactive" => MarketStatus::Paused,
        "closed" => MarketStatus::Closed,
        "determined" | "disputed" | "amended" | "finalized" => MarketStatus::Settled,
        unknown => anyhow::bail!("market {ticker} has unknown status {unknown:?}"),
    })
}

/// Parses an RFC 3339 time field, treating a missing or empty value as absent.
fn parse_time(
    value: Option<&str>,
    field: &str,
    ticker: &str,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let time = DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("market {ticker} has an invalid {field} {value:?}"))?;
    Ok(Some(time.with_timezone(&Utc)))
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use serde_json::{Value, json};
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockBuilder, MockServer, ResponseTemplate};

    use super::*;

    const TEST_BACKOFF: Backoff = Backoff {
        first_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(100),
        max_attempts: 3,
    };
    const TEST_LIMITS: CatalogLimits = CatalogLimits {
        requests_per_second: 1000,
        burst: 1000,
        backoff: TEST_BACKOFF,
        max_pages: 3,
    };

    type Listing = (Vec<Vec<MarketInfo>>, ListingReport);

    fn market(ticker: &str, status: &str) -> Value {
        json!({
            "ticker": ticker,
            "status": status,
            "open_time": "2026-10-01T12:00:00Z",
            "close_time": "2026-10-31T23:59:59Z",
            "yes_bid_dollars": "0.4200",
        })
    }

    fn page_response(tickers: &[&str], cursor: Option<&str>) -> ResponseTemplate {
        let markets = tickers
            .iter()
            .map(|ticker| market(ticker, "active"))
            .collect();
        markets_response(markets, cursor)
    }

    fn markets_response(markets: Vec<Value>, cursor: Option<&str>) -> ResponseTemplate {
        let body = match cursor {
            Some(cursor) => json!({ "markets": markets, "cursor": cursor }),
            None => json!({ "markets": markets }),
        };
        ResponseTemplate::new(200).set_body_json(body)
    }

    fn open_markets_request() -> MockBuilder {
        Mock::given(method("GET"))
            .and(path("/trade-api/v2/markets"))
            .and(query_param("status", "open"))
            .and(query_param("mve_filter", "exclude"))
            .and(query_param("limit", "1000"))
    }

    fn page_chain(pages: &[(Option<&str>, &[&str], &str)]) -> Vec<Mock> {
        let page_mock = |&(cursor, tickers, next): &(Option<&str>, &[&str], &str)| {
            let request = match cursor {
                None => open_markets_request().and(query_param_is_missing("cursor")),
                Some(cursor) => open_markets_request().and(query_param("cursor", cursor)),
            };
            request.respond_with(page_response(tickers, Some(next)))
        };
        pages.iter().map(page_mock).collect()
    }

    async fn mount_all(server: &MockServer, mocks: Vec<Mock>) {
        for mock in mocks {
            mock.mount(server).await;
        }
    }

    async fn list_at(base_url: &str) -> anyhow::Result<Listing> {
        let catalog = KalshiCatalog::with_limits(base_url, TEST_LIMITS)?;
        let (sender, mut receiver) = mpsc::channel(16);
        let report = catalog.list_markets(MarketStatus::Open, sender).await?;
        let mut pages = Vec::new();
        while let Some(page) = receiver.recv().await {
            pages.push(page);
        }
        Ok((pages, report))
    }

    async fn list_open(server: &MockServer) -> anyhow::Result<Listing> {
        list_at(&format!("{}/trade-api/v2", server.uri())).await
    }

    async fn open_pages(server: &MockServer) -> Vec<Vec<MarketInfo>> {
        list_open(server).await.unwrap().0
    }

    async fn listing_error(server: &MockServer) -> String {
        format!("{:#}", list_open(server).await.unwrap_err())
    }

    fn symbols(pages: &[Vec<MarketInfo>]) -> Vec<Vec<&str>> {
        pages
            .iter()
            .map(|page| page.iter().map(|market| &*market.symbol).collect())
            .collect()
    }

    async fn request_count(server: &MockServer) -> usize {
        server.received_requests().await.unwrap().len()
    }

    #[tokio::test]
    async fn follows_the_cursor_until_it_is_empty() {
        let server = MockServer::start().await;
        let chain = page_chain(&[
            (None, &["A", "B"][..], "c2"),
            (Some("c2"), &["C"][..], "c3"),
            (Some("c3"), &["D"][..], ""),
        ]);
        let chain = chain.into_iter().map(|mock| mock.expect(1)).collect();
        mount_all(&server, chain).await;

        let pages = open_pages(&server).await;

        assert_eq!(symbols(&pages), [vec!["A", "B"], vec!["C"], vec!["D"]]);
    }

    #[tokio::test]
    async fn a_missing_cursor_ends_the_listing() {
        let server = MockServer::start().await;
        open_markets_request()
            .respond_with(page_response(&["A"], None))
            .expect(1)
            .mount(&server)
            .await;

        assert_eq!(symbols(&open_pages(&server).await), [vec!["A"]]);
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_the_listing() {
        let server = MockServer::start().await;
        let chain = page_chain(&[(None, &["A"][..], "c1"), (Some("c1"), &["B"][..], "c1")]);
        mount_all(&server, chain).await;

        let error = listing_error(&server).await;

        assert_eq!(error, r#"Kalshi returned cursor "c1" twice in a row"#);
    }

    #[tokio::test]
    async fn a_listing_longer_than_the_page_cap_fails() {
        let server = MockServer::start().await;
        let chain = page_chain(&[
            (None, &["A"][..], "c1"),
            (Some("c1"), &["B"][..], "c2"),
            (Some("c2"), &["C"][..], "c3"),
            (Some("c3"), &["D"][..], ""),
        ]);
        mount_all(&server, chain).await;

        let error = listing_error(&server).await;

        assert_eq!(error, "Kalshi Open listing ran past 3 pages");
        assert_eq!(request_count(&server).await, 3);
    }

    #[tokio::test]
    async fn empty_pages_are_not_sent() {
        let server = MockServer::start().await;
        let chain = page_chain(&[(None, &[][..], "c1"), (Some("c1"), &["A"][..], "")]);
        mount_all(&server, chain).await;

        assert_eq!(symbols(&open_pages(&server).await), [vec!["A"]]);
    }

    #[tokio::test]
    async fn a_market_with_an_unknown_status_is_skipped_and_the_listing_continues() {
        let server = MockServer::start().await;
        let first_page = vec![market("KXTEST-1", "frozen"), market("B", "active")];
        open_markets_request()
            .and(query_param_is_missing("cursor"))
            .respond_with(markets_response(first_page, Some("c1")))
            .mount(&server)
            .await;
        open_markets_request()
            .and(query_param("cursor", "c1"))
            .respond_with(page_response(&["C"], None))
            .mount(&server)
            .await;

        let (pages, report) = list_open(&server).await.unwrap();

        assert_eq!(
            (symbols(&pages), report.skipped),
            (vec![vec!["B"], vec!["C"]], vec![Arc::from("KXTEST-1")])
        );
    }

    #[tokio::test]
    async fn a_market_with_an_invalid_time_is_skipped() {
        let server = MockServer::start().await;
        let mut invalid = market("KXTEST-2", "active");
        invalid["close_time"] = json!("tomorrow");
        open_markets_request()
            .respond_with(markets_response(vec![invalid, market("B", "active")], None))
            .mount(&server)
            .await;

        let (pages, report) = list_open(&server).await.unwrap();

        assert_eq!(
            (symbols(&pages), report.skipped),
            (vec![vec!["B"]], vec![Arc::from("KXTEST-2")])
        );
    }

    #[tokio::test]
    async fn markets_carry_status_and_times() {
        let server = MockServer::start().await;
        open_markets_request()
            .respond_with(page_response(&["A"], None))
            .mount(&server)
            .await;

        let pages = open_pages(&server).await;

        let expected = MarketInfo {
            symbol: "A".into(),
            status: MarketStatus::Open,
            open_ts: Some("2026-10-01T12:00:00Z".parse().unwrap()),
            close_ts: Some("2026-10-31T23:59:59Z".parse().unwrap()),
        };
        assert_eq!(pages, [vec![expected]]);
    }

    #[tokio::test]
    async fn rate_limited_request_backs_off_then_succeeds() {
        let server = MockServer::start().await;
        open_markets_request()
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        open_markets_request()
            .respond_with(page_response(&["A"], None))
            .mount(&server)
            .await;
        let start = Instant::now();

        let pages = open_pages(&server).await;

        assert!(start.elapsed() >= TEST_BACKOFF.first_delay);
        assert_eq!(
            (symbols(&pages), request_count(&server).await),
            (vec![vec!["A"]], 2)
        );
    }

    #[tokio::test]
    async fn server_errors_give_up_after_the_last_attempt() {
        let server = MockServer::start().await;
        open_markets_request()
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let error = listing_error(&server).await;

        assert!(
            error.contains("after 3 attempts") && error.contains("503"),
            "{error}"
        );
        assert_eq!(request_count(&server).await, 3);
    }

    #[tokio::test]
    async fn transport_errors_are_retried_until_the_last_attempt() {
        let closed_port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .unwrap()
            .port();

        let listing = list_at(&format!("http://127.0.0.1:{closed_port}/trade-api/v2")).await;

        let error = format!("{:#}", listing.unwrap_err());
        assert!(error.contains("after 3 attempts"), "{error}");
    }

    #[tokio::test]
    async fn client_errors_fail_without_retrying() {
        let server = MockServer::start().await;
        open_markets_request()
            .respond_with(ResponseTemplate::new(400).set_body_string("bad status filter"))
            .mount(&server)
            .await;

        let error = listing_error(&server).await;

        assert!(
            error.contains("400") && error.contains("bad status filter"),
            "{error}"
        );
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn a_malformed_body_fails_without_retrying() {
        let server = MockServer::start().await;
        open_markets_request()
            .respond_with(ResponseTemplate::new(200).set_body_string("{"))
            .mount(&server)
            .await;

        let error = listing_error(&server).await;

        assert!(error.contains("failed to parse"), "{error}");
        assert_eq!(request_count(&server).await, 1);
    }

    #[test]
    fn every_known_kalshi_status_maps_to_a_market_status() {
        let mapped: Vec<_> = [
            "initialized",
            "active",
            "inactive",
            "closed",
            "determined",
            "disputed",
            "amended",
            "finalized",
        ]
        .iter()
        .map(|value| parse_status(value, "T").unwrap())
        .collect();
        use MarketStatus::*;
        let expected = [
            Unopened, Open, Paused, Closed, Settled, Settled, Settled, Settled,
        ];
        assert_eq!(mapped, expected);
    }

    #[test]
    fn an_unknown_status_names_the_value_and_the_ticker() {
        let error = parse_status("open", "KXTEST-1").unwrap_err().to_string();
        assert_eq!(error, r#"market KXTEST-1 has unknown status "open""#);
    }

    #[test]
    fn every_market_status_has_a_query_value() {
        use MarketStatus::*;
        let values = [Unopened, Open, Paused, Closed, Settled].map(query_status);
        assert_eq!(values, ["unopened", "open", "paused", "closed", "settled"]);
    }

    #[test]
    fn an_invalid_time_names_the_field_and_the_ticker() {
        let error = parse_time(Some("tomorrow"), "close_time", "KXTEST-1").unwrap_err();
        assert_eq!(
            error.to_string(),
            r#"market KXTEST-1 has an invalid close_time "tomorrow""#
        );
    }

    #[test]
    fn an_empty_time_is_absent() {
        assert_eq!(parse_time(Some(""), "open_time", "T").unwrap(), None);
    }
}
