//! Following market lifecycles: lifecycle events, the sweep of open and
//! close times, reconciles with the catalog, and batched subscription
//! changes.

use chrono::{TimeDelta, Utc};
use engineering_challenge::core::stats::CounterValues;
use engineering_challenge::exchange::{MarketInfo, MarketStatus};
use serde_json::Value;

use super::mock_exchange::market;
use super::*;

/// The lifecycle subscription the mock exchange names.
const LIFECYCLE_SUB: u64 = 90;

/// The exchange's side of the lifecycle connection.
struct LifecycleFeed {
    client: MockClient,
    seq: u64,
}

impl LifecycleFeed {
    /// Accepts the lifecycle connection and names its subscription.
    async fn accept(server: &MockServer) -> Self {
        let mut client = server.accept().await;
        let command = client.next_command().await;
        assert_eq!(command.cmd, "subscribe_lifecycle");
        client
            .reply(&command, frames::subscribed(LIFECYCLE_SUB))
            .await;
        Self { client, seq: 0 }
    }

    /// Sends `event` for `symbol`.
    async fn send(&mut self, event: &str, symbol: &str) {
        self.send_with(event, symbol, |_| {}).await;
    }

    /// Sends `event` for `symbol`, after `extend` has added to the frame.
    async fn send_with(&mut self, event: &str, symbol: &str, extend: impl FnOnce(&mut Value)) {
        self.seq += 1;
        let mut frame = frames::lifecycle(LIFECYCLE_SUB, self.seq, event, symbol);
        extend(&mut frame);
        self.client.send(frame).await;
    }

    /// Answers the client's close frame in the background.
    fn answer_close(mut self) -> JoinHandle<()> {
        tokio::spawn(async move { self.client.expect_close().await })
    }
}

/// Unix milliseconds `offset_ms` from now.
fn from_now(offset_ms: i64) -> i64 {
    (Utc::now() + TimeDelta::milliseconds(offset_ms)).timestamp_millis()
}

/// An open market closing `offset_ms` from now.
fn closing_in(symbol: &str, offset_ms: i64) -> MarketInfo {
    MarketInfo {
        close_ts: chrono::DateTime::from_timestamp_millis(from_now(offset_ms)),
        ..market(symbol, MarketStatus::Open)
    }
}

/// A recorder following lifecycle changes over one shard.
struct Following {
    recorder: Recorder,
    walks: CatalogWalks,
    lifecycle: LifecycleFeed,
    shard: Shard,
}

impl Following {
    /// Starts recording `markets`, listed in one page, with `config`, and
    /// waits for their books.
    async fn start(config: CoordinatorConfig, markets: Vec<MarketInfo>) -> Self {
        let (server, lifecycle_server) = (MockServer::start().await, MockServer::start().await);
        let (mut recorder, walks) = Recorder::following(&server, &lifecycle_server, config);
        let lifecycle = LifecycleFeed::accept(&lifecycle_server).await;
        let count = markets.len() as u64;
        recorder.pages().send_markets(markets);
        recorder.end_listing();
        let mut shard = Shard::accept(&server, 1).await;
        shard.send_books().await;
        recorder.output.wait_for(count, 0).await;
        Self {
            recorder,
            walks,
            lifecycle,
            shard,
        }
    }

    /// [`Following::start`] over open markets named `names`, with shards
    /// of ten.
    async fn open(names: &[&str]) -> Self {
        Self::start(config(10), open(names)).await
    }

    /// The next command the shard gets, as `(cmd, symbols)`.
    async fn next_command(&mut self) -> (String, Vec<String>) {
        let command = self.shard.client.next_command().await;
        (command.cmd, command.symbols)
    }

    /// Waits until `reached` holds for the counters.
    async fn wait_for_counts(&self, reached: impl Fn(&CounterValues) -> bool) {
        let waiting = async {
            while !reached(&self.recorder.counters.read()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        timeout(STEP_TIMEOUT, waiting)
            .await
            .expect("the counters did not get there in time");
    }

    /// Waits until `count` reconciles have finished.
    async fn wait_for_reconciles(&self, count: u64) {
        let finished = |counts: &CounterValues| counts.reconciles >= count;
        self.wait_for_counts(finished).await;
    }

    /// Stops the recorder and returns the recording, which must verify
    /// clean.
    async fn stop(self) -> Finished {
        let closes = vec![self.shard.answer_close(), self.lifecycle.answer_close()];
        let (_, finished) = self.recorder.stop().await;
        closed(closes).await;
        finished
    }
}

fn open(names: &[&str]) -> Vec<MarketInfo> {
    let open = |&name: &&str| market(name, MarketStatus::Open);
    names.iter().map(open).collect()
}

fn command(cmd: &str, symbols: &[&str]) -> (String, Vec<String>) {
    let symbols = symbols.iter().map(|&symbol| symbol.to_owned()).collect();
    (cmd.to_owned(), symbols)
}

#[tokio::test]
async fn a_market_created_already_open_is_subscribed() {
    let mut following = Following::open(&["A"]).await;
    let opened = from_now(-1000);
    let open_ts = |frame: &mut Value| frame["open_ts"] = opened.into();
    following.lifecycle.send_with("created", "B", open_ts).await;
    let added = following.next_command().await;
    let counts = following.recorder.counters.read();
    following.stop().await;
    assert_eq!(
        (added, counts.lifecycle_events, counts.added.lifecycle),
        (command("add", &["B"]), 1, 1)
    );
}

#[tokio::test]
async fn a_market_created_to_open_later_is_subscribed_once_its_open_time_passes() {
    let mut following = Following::open(&["A"]).await;
    let opens = from_now(700);
    let open_ts = |frame: &mut Value| frame["open_ts"] = opens.into();
    following.lifecycle.send_with("created", "B", open_ts).await;
    let shard = &mut following.shard.client;
    shard.assert_no_command(Duration::from_millis(400)).await;
    let added = following.next_command().await;
    let swept = following.recorder.counters.read().added.sweep;
    following.stop().await;
    assert_eq!((added, swept), (command("add", &["B"]), 1));
}

/// A is determined and B settled: both are removed in one command, get a
/// final snapshot, and nothing the exchange sends for them afterwards is
/// recorded.
#[tokio::test]
async fn determined_and_settled_markets_get_a_final_snapshot_and_nothing_after() {
    let mut following = Following::open(&["A", "B", "C"]).await;
    following.lifecycle.send("determined", "A").await;
    following.lifecycle.send("settled", "B").await;
    let removed = following.next_command().await;
    let shard = &mut following.shard;
    for symbol in ["A", "B", "C"] {
        shard.send_level(symbol, ("buy", 400_000, 10)).await;
    }
    following.recorder.output.wait_for(5, 1).await;
    let finished = following.stop().await;
    let recorded = ["A", "B", "C"].map(|symbol| finished.of(symbol));
    let ended = vec![sent_book(), sent_book()];
    let changed = vec![sent_book(), Shape::Update(Side::Buy, 400_000, 110)];
    let expected = [ended.clone(), ended, changed];
    assert_eq!(
        (removed, recorded),
        (command("remove", &["A", "B"]), expected)
    );
}

#[tokio::test]
async fn a_market_whose_close_time_passes_is_removed_by_the_sweep() {
    let markets = vec![closing_in("A", 1500), market("B", MarketStatus::Open)];
    let mut following = Following::start(config(10), markets).await;
    let removed = following.next_command().await;
    let swept = following.recorder.counters.read().removed.sweep;
    let finished = following.stop().await;
    assert_eq!(
        (removed, swept, finished.of("A")),
        (command("remove", &["A"]), 1, vec![sent_book(), sent_book()])
    );
}

/// Starts recording A, closing in 1.3 s, and B, waits for the sweep to
/// remove A, then moves A's close time an hour later and sends `after`
/// lifecycle events for A. Returns the shard's commands from the removal
/// on.
async fn reopen_a(after: &[&str]) -> [(String, Vec<String>); 3] {
    let markets = vec![closing_in("A", 1300), market("B", MarketStatus::Open)];
    let mut following = Following::start(config(10), markets).await;
    let removed = following.next_command().await;
    let closes = from_now(3_600_000);
    let close_ts = |frame: &mut Value| frame["close_ts"] = closes.into();
    let lifecycle = &mut following.lifecycle;
    lifecycle.send_with("close_changed", "A", close_ts).await;
    for &event in after {
        lifecycle.send(event, "A").await;
    }
    let added = following.next_command().await;
    let snapshot = following.next_command().await;
    following.stop().await;
    [removed, added, snapshot]
}

fn removed_then_added_again() -> [(String, Vec<String>); 3] {
    [
        command("remove", &["A"]),
        command("add", &["A"]),
        command("snapshot", &["A"]),
    ]
}

/// A reopen can arrive as a later close time followed by an activation.
#[tokio::test]
async fn a_market_reopened_after_its_close_is_subscribed_again() {
    assert_eq!(reopen_a(&["activated"]).await, removed_then_added_again());
}

/// Kalshi has not been seen to send `activated` live, so a later close
/// time alone records the market again.
#[tokio::test]
async fn a_market_whose_close_moves_later_is_subscribed_again_without_an_activation() {
    assert_eq!(reopen_a(&[]).await, removed_then_added_again());
}

/// B is determined before the startup listing names it as open, which is
/// staler news.
#[tokio::test]
async fn the_startup_listing_does_not_record_a_market_determined_before_it_arrives() {
    let (server, lifecycle_server) = (MockServer::start().await, MockServer::start().await);
    let (mut recorder, _walks) = Recorder::following(&server, &lifecycle_server, config(10));
    let mut lifecycle = LifecycleFeed::accept(&lifecycle_server).await;
    lifecycle.send("determined", "B").await;
    wait_for_lifecycle_events(&recorder.counters, 1).await;
    recorder.list_open(&["A", "B"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(1, 0).await;
    let subscribed = shard.symbols.clone();
    let closes = vec![shard.answer_close(), lifecycle.answer_close()];
    recorder.stop().await;
    closed(closes).await;
    assert_eq!(subscribed, vec!["A".to_owned()]);
}

/// A, closing in 2 s, is listed open at startup. Its close then moves an
/// hour later, after which the startup's paused listing gives the old
/// close again. That listing is staler than the lifecycle event, so A must
/// not be swept at the old close.
#[tokio::test]
async fn the_startup_listing_keeps_a_close_time_moved_since_startup() {
    let (server, lifecycle_server) = (MockServer::start().await, MockServer::start().await);
    let (mut recorder, _walks) = Recorder::following(&server, &lifecycle_server, config(10));
    let mut lifecycle = LifecycleFeed::accept(&lifecycle_server).await;
    let old_close = closing_in("A", 2000);
    recorder.pages().send_markets(vec![old_close.clone()]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(1, 0).await;
    let closes = from_now(3_600_000);
    let close_ts = |frame: &mut Value| frame["close_ts"] = closes.into();
    lifecycle.send_with("close_changed", "A", close_ts).await;
    wait_for_lifecycle_events(&recorder.counters, 1).await;
    recorder.pages().send_paused_markets(vec![old_close]);
    recorder.end_listing();
    shard.client.assert_no_command(Duration::from_secs(3)).await;
    let closes = vec![shard.answer_close(), lifecycle.answer_close()];
    recorder.stop().await;
    closed(closes).await;
}

/// Waits until `counters` show `count` lifecycle events.
async fn wait_for_lifecycle_events(counters: &RecorderCounters, count: u64) {
    let waiting = async {
        while counters.read().lifecycle_events < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    timeout(STEP_TIMEOUT, waiting)
        .await
        .expect("the lifecycle events did not arrive in time");
}

/// Shards of ten, with a reconcile due every 200 ms; each starts once the
/// startup listing and the walk before it are done.
fn reconciling() -> CoordinatorConfig {
    let mut config = config(10);
    config.lifecycle.reconcile_interval = Duration::from_millis(200);
    config
}

/// Starts recording open markets `names` with reconciles, and serves the
/// first reconcile's open and paused walks.
async fn reconciled(
    names: &[&str],
    open_walk: &[(&str, MarketStatus)],
    paused_walk: &[(&str, MarketStatus)],
    skipped: &[&str],
) -> Following {
    let following = Following::start(reconciling(), open(names)).await;
    following.walks.serve(open_walk, skipped);
    following.walks.serve(paused_walk, &[]);
    following.wait_for_reconciles(1).await;
    following
}

const OPEN: MarketStatus = MarketStatus::Open;

#[tokio::test]
async fn a_reconcile_adds_a_missing_open_market() {
    let walk = [("A", OPEN), ("B", OPEN)];
    let mut following = reconciled(&["A"], &walk, &[], &[]).await;
    let added = following.next_command().await;
    let counts = following.recorder.counters.read();
    following.stop().await;
    assert_eq!((added, counts.added.reconcile), (command("add", &["B"]), 1));
}

/// B, open and with no close time, is missing from the first reconcile's
/// open and paused listings, which is not yet enough, and from the second
/// reconcile's too.
#[tokio::test]
async fn a_reconcile_removes_a_market_two_walks_in_a_row_missed() {
    let mut following = reconciled(&["A", "B"], &[("A", OPEN)], &[], &[]).await;
    let shard = &mut following.shard.client;
    shard.assert_no_command(Duration::from_millis(300)).await;
    following.walks.serve(&[("A", OPEN)], &[]);
    following.walks.serve(&[], &[]);
    following.wait_for_reconciles(2).await;
    let removed = following.next_command().await;
    let counts = following.recorder.counters.read();
    following.stop().await;
    assert_eq!(
        (removed, counts.removed.reconcile),
        (command("remove", &["B"]), 1)
    );
}

#[tokio::test]
async fn a_reconcile_keeps_a_market_listed_again_after_one_walk_missed_it() {
    let mut following = reconciled(&["A", "B"], &[("A", OPEN)], &[], &[]).await;
    following.walks.serve(&[("A", OPEN), ("B", OPEN)], &[]);
    following.walks.serve(&[], &[]);
    following.walks.serve(&[("A", OPEN)], &[]);
    following.walks.serve(&[], &[]);
    following.wait_for_reconciles(3).await;
    let shard = &mut following.shard.client;
    shard.assert_no_command(Duration::from_millis(300)).await;
    following.stop().await;
}

#[tokio::test]
async fn a_reconcile_removes_a_market_listed_as_closed() {
    let paused_walk = [("B", MarketStatus::Closed)];
    let mut following = reconciled(&["A", "B"], &[("A", OPEN)], &paused_walk, &[]).await;
    let removed = following.next_command().await;
    following.stop().await;
    assert_eq!(removed, command("remove", &["B"]));
}

/// The first walk misses B; the second skips it, which is no evidence.
#[tokio::test]
async fn a_reconcile_keeps_markets_the_walk_skipped() {
    let mut following = reconciled(&["A", "B"], &[("A", OPEN)], &[], &[]).await;
    following.walks.serve(&[("A", OPEN)], &["B"]);
    following.walks.serve(&[], &[]);
    following.wait_for_reconciles(2).await;
    let shard = &mut following.shard.client;
    shard.assert_no_command(Duration::from_millis(300)).await;
    following.stop().await;
}

/// Both walks miss B, but B is paused after the second walk started, which
/// is fresher news than the walk.
#[tokio::test]
async fn a_reconcile_keeps_a_market_with_a_lifecycle_event_after_the_walk_started() {
    let mut following = reconciled(&["A", "B"], &[("A", OPEN)], &[], &[]).await;
    for expected in [MarketStatus::Open, MarketStatus::Paused, MarketStatus::Open] {
        assert_eq!(following.walks.next_request().await, expected);
    }
    following.lifecycle.send("deactivated", "B").await;
    following
        .wait_for_counts(|counts| counts.lifecycle_events >= 1)
        .await;
    following.walks.serve(&[("A", OPEN)], &[]);
    following.walks.serve(&[], &[]);
    following.wait_for_reconciles(2).await;
    let shard = &mut following.shard.client;
    shard.assert_no_command(Duration::from_millis(300)).await;
    following.stop().await;
}

#[tokio::test]
async fn a_lifecycle_gap_starts_a_reconcile_at_once() {
    let mut following = Following::open(&["A"]).await;
    following.lifecycle.send("activated", "A").await;
    following.lifecycle.seq += 2;
    following.lifecycle.send("activated", "A").await;
    let requested = following.walks.next_request().await;
    following.stop().await;
    assert_eq!(requested, MarketStatus::Open);
}

/// Fifty markets created within the batch window reach the shard as one
/// command.
#[tokio::test]
async fn fifty_lifecycle_adds_within_the_batch_window_are_one_add_per_shard() {
    let mut config = config(100);
    config.lifecycle.batch_window = Duration::from_millis(500);
    let mut following = Following::start(config, open(&["A"])).await;
    let names: Vec<String> = (0..50).map(|index| format!("N{index:02}")).collect();
    for name in &names {
        following.lifecycle.send("created", name).await;
    }
    let added = following.next_command().await;
    let shard = &mut following.shard.client;
    shard.assert_no_command(Duration::from_millis(300)).await;
    following.stop().await;
    assert_eq!(added, ("add".to_owned(), names));
}
