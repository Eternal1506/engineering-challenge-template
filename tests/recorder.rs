//! The whole recording pipeline, coordinator, shards, connections and writer,
//! against the mock exchange and an in-memory catalog, over real localhost
//! sockets and a real output file.

mod common;
#[path = "mock_exchange/mod.rs"]
mod mock_exchange;

#[path = "recorder/lifecycle.rs"]
mod lifecycle;
#[path = "recorder/recovery.rs"]
mod recovery;
#[path = "recorder/sharding.rs"]
mod sharding;
#[path = "recorder/shutdown.rs"]
mod shutdown;
#[path = "recorder/stalls.rs"]
mod stalls;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::{Levels, Recorded, read_recording, snapshot_sides, verify_clean};
use engineering_challenge::Side;
use engineering_challenge::core::coordinator::{
    Coordinator, CoordinatorConfig, Feed, LifecycleTimings, WriterLink,
};
use engineering_challenge::core::stats::RecorderCounters;
use engineering_challenge::core::writer::{
    WriterConfig, WriterHandle, WriterProgress, WriterStats, spawn_writer,
};
use engineering_challenge::core::ws::ConnectionConfig;
use mock_exchange::{
    CatalogPages, CatalogWalks, MockCatalog, MockClient, MockCodec, MockEndpoint, MockServer,
    frames,
};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// Every book the mock exchange sends: one bid and one ask level.
const BIDS: &[(i64, i64)] = &[(400_000, 100)];
const ASKS: &[(i64, i64)] = &[(600_000, 50)];

/// A coordinator with shards of `shard_size`, quick reconnects, no stats
/// line during a test, subscription changes batched for 100 ms, a sweep
/// every 100 ms and no reconcile within a test.
fn config(shard_size: usize) -> CoordinatorConfig {
    CoordinatorConfig {
        shard_size,
        connection: ConnectionConfig {
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
            ..ConnectionConfig::default()
        },
        shard_event_capacity: 1024,
        stats_interval: Duration::from_secs(3600),
        stall_timeout: Duration::from_secs(2),
        lifecycle: LifecycleTimings {
            batch_window: Duration::from_millis(100),
            sweep_interval: Duration::from_millis(100),
            reconcile_interval: Duration::from_secs(3600),
        },
    }
}

/// The mock exchange's feed at `server`, without lifecycle changes.
fn mock_feed(server: &MockServer) -> Feed<MockEndpoint, MockCodec> {
    Feed {
        endpoint: server.endpoint(),
        lifecycle_endpoint: None,
        codec: MockCodec::default(),
    }
}

/// The mock exchange's feed at `server`, with lifecycle changes from
/// `lifecycle`.
fn following_feed(server: &MockServer, lifecycle: &MockServer) -> Feed<MockEndpoint, MockCodec> {
    Feed {
        lifecycle_endpoint: Some(lifecycle.endpoint()),
        ..mock_feed(server)
    }
}

/// Starts a writer to `path` whose periodic snapshots are an hour apart,
/// so a test's output holds only what the exchange sent, after `tune` has
/// adjusted its config.
fn start_writer(path: &Path, tune: impl FnOnce(&mut WriterConfig)) -> (WriterLink, WriterHandle) {
    let (requests, resnapshot_requests) = mpsc::channel(64);
    let mut writer_config = WriterConfig {
        snapshot_tick: Duration::from_secs(3600),
        ..WriterConfig::new(path, requests)
    };
    tune(&mut writer_config);
    let (messages, writer) = spawn_writer(writer_config).unwrap();
    let link = WriterLink {
        messages,
        resnapshot_requests,
        progress: writer.progress(),
    };
    (link, writer)
}

/// A coordinator for `server`, not yet running, and the writer it records
/// through, writing to a temporary file.
struct Pipeline {
    coordinator: Coordinator<MockEndpoint, MockCodec>,
    output: Output,
}

/// The writer thread and the file it writes.
struct Output {
    writer: WriterHandle,
    progress: Arc<WriterProgress>,
    path: PathBuf,
    _dir: TempDir,
}

impl Pipeline {
    fn new(server: &MockServer, config: CoordinatorConfig) -> Self {
        Self::with_feed(mock_feed(server), config)
    }

    fn with_feed(feed: Feed<MockEndpoint, MockCodec>, config: CoordinatorConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recording.sbe");
        let (link, writer) = start_writer(&path, |_| {});
        let output = Output {
            progress: link.progress.clone(),
            writer,
            path,
            _dir: dir,
        };
        let coordinator = Coordinator::new(feed, link, config);
        Self {
            coordinator,
            output,
        }
    }
}

impl Output {
    /// Waits until the writer has written at least `snapshots` snapshots and
    /// `updates` updates.
    async fn wait_for(&self, snapshots: u64, updates: u64) {
        let written = || {
            self.progress.snapshots_written() >= snapshots
                && self.progress.updates_written() >= updates
        };
        let waiting = async {
            while !written() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        timeout(STEP_TIMEOUT, waiting)
            .await
            .expect("the writer did not write in time");
    }

    /// Waits for the writer to finish, once every sender is gone, and reads
    /// the recording back, failing unless it verifies clean.
    async fn finish(self) -> Finished {
        let stats = tokio::task::spawn_blocking(move || self.writer.join())
            .await
            .unwrap()
            .unwrap();
        verify_clean(&self.path).unwrap();
        Finished {
            stats,
            messages: read_recording(&self.path).unwrap(),
        }
    }
}

/// What a finished recording holds.
struct Finished {
    stats: WriterStats,
    messages: Vec<Recorded>,
}

impl Finished {
    /// The messages for `symbol`, in order.
    fn of(&self, symbol: &str) -> Vec<Shape> {
        let mine = |message: &&Recorded| symbol_of(message) == symbol;
        self.messages.iter().filter(mine).map(shape).collect()
    }
}

/// A recorded message without its times.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Shape {
    Snapshot(Levels, Levels),
    Update(Side, i64, i64),
}

fn shape(message: &Recorded) -> Shape {
    match message {
        Recorded::Snapshot(snapshot) => {
            let (bids, asks) = snapshot_sides(snapshot);
            Shape::Snapshot(bids, asks)
        }
        Recorded::Update(update) => Shape::Update(update.side, update.price_level, update.quantity),
    }
}

fn symbol_of(message: &Recorded) -> &str {
    match message {
        Recorded::Snapshot(snapshot) => &snapshot.symbol,
        Recorded::Update(update) => &update.symbol,
    }
}

/// The snapshot of a book the mock exchange sent.
fn sent_book() -> Shape {
    Shape::Snapshot(BIDS.to_vec(), ASKS.to_vec())
}

/// A pipeline whose coordinator is running against a [`MockCatalog`].
struct Recorder {
    pages: Option<CatalogPages>,
    cancel: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
    counters: Arc<RecorderCounters>,
    output: Output,
}

impl Recorder {
    /// Starts recording from `server` with shards of `shard_size`. The
    /// catalog serves the pages the test sends until it ends the listing.
    fn start(server: &MockServer, shard_size: usize) -> Self {
        Self::start_with(server, config(shard_size))
    }

    /// [`Recorder::start`] with `config`.
    fn start_with(server: &MockServer, config: CoordinatorConfig) -> Self {
        let (catalog, pages) = MockCatalog::new();
        Self::run(Pipeline::new(server, config), catalog, pages)
    }

    /// Starts recording from `server` with `config`, following lifecycle
    /// changes from `lifecycle`. Returns the recorder and the script for
    /// the catalog's listings after the startup walk.
    fn following(
        server: &MockServer,
        lifecycle: &MockServer,
        config: CoordinatorConfig,
    ) -> (Self, CatalogWalks) {
        let (catalog, pages) = MockCatalog::new();
        let walks = catalog.walks();
        let pipeline = Pipeline::with_feed(following_feed(server, lifecycle), config);
        (Self::run(pipeline, catalog, pages), walks)
    }

    fn run(pipeline: Pipeline, catalog: MockCatalog, pages: CatalogPages) -> Self {
        let Pipeline {
            coordinator,
            output,
        } = pipeline;
        let counters = coordinator.counters();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(coordinator.run(catalog, cancel.clone()));
        Self {
            pages: Some(pages),
            cancel,
            task,
            counters,
            output,
        }
    }

    /// The catalog's pages, while the listing lasts.
    fn pages(&self) -> &CatalogPages {
        self.pages.as_ref().expect("the listing has ended")
    }

    /// Lists one page of open markets and ends the listing.
    fn list_open(&mut self, symbols: &[&str]) {
        self.pages().send_open(symbols);
        self.end_listing();
    }

    fn end_listing(&mut self) {
        self.pages = None;
    }

    /// Cancels the recorder, waits for the coordinator and then the writer
    /// to finish, and returns how long the coordinator took to stop along
    /// with the recording, which must verify clean.
    async fn stop(self) -> (Duration, Finished) {
        let started = Instant::now();
        self.cancel.cancel();
        let ran = timeout(STEP_TIMEOUT, self.task).await;
        ran.expect("the coordinator did not stop in time")
            .unwrap()
            .unwrap();
        let took = started.elapsed();
        (took, self.output.finish().await)
    }
}

/// The exchange's side of one shard's connection.
struct Shard {
    client: MockClient,
    sub: u64,
    symbols: Vec<String>,
    seq: u64,
}

impl Shard {
    /// Accepts the next connection and names its subscription `sub`.
    async fn accept(server: &MockServer, sub: u64) -> Self {
        let mut client = server.accept().await;
        let command = client.next_command().await;
        assert_eq!(command.cmd, "subscribe");
        client.reply(&command, frames::subscribed(sub)).await;
        Self {
            client,
            sub,
            symbols: command.symbols,
            seq: 0,
        }
    }

    /// Accepts the next `count` connections, naming their subscriptions 1,
    /// 2 and so on.
    async fn accept_all(server: &MockServer, count: u64) -> Vec<Self> {
        let mut shards = Vec::new();
        for sub in 1..=count {
            shards.push(Self::accept(server, sub).await);
        }
        shards
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Sends the standard book for every subscribed symbol.
    async fn send_books(&mut self) {
        for symbol in self.symbols.clone() {
            let seq = self.next_seq();
            let frame = frames::book(self.sub, seq, &symbol, (BIDS, ASKS));
            self.client.send(frame).await;
        }
    }

    /// Sends a change of `qty` to the `side` level of `symbol` at `price`.
    async fn send_level(&mut self, symbol: &str, change: (&str, i64, i64)) {
        let seq = self.next_seq();
        let frame = frames::level(self.sub, seq, symbol, change);
        self.client.send(frame).await;
    }

    fn owns(&self, symbol: &str) -> bool {
        self.symbols.iter().any(|owned| owned == symbol)
    }

    /// Answers the client's close frame in the background.
    fn answer_close(mut self) -> JoinHandle<()> {
        tokio::spawn(async move { self.client.expect_close().await })
    }
}

/// The shard of `shards` that subscribed `symbol`.
fn owner_of<'s>(shards: &'s mut [Shard], symbol: &str) -> &'s mut Shard {
    shards
        .iter_mut()
        .find(|shard| shard.owns(symbol))
        .expect("no shard subscribed the symbol")
}

/// Answers every shard's close frame in the background.
fn answer_closes(shards: Vec<Shard>) -> Vec<JoinHandle<()>> {
    shards.into_iter().map(Shard::answer_close).collect()
}

/// Waits for every background close answer, failing if a client went away
/// without a close frame.
async fn closed(answers: Vec<JoinHandle<()>>) {
    for answer in answers {
        answer.await.unwrap();
    }
}
