//! The [`Coordinator`]: it lists the exchange's markets, spreads them over
//! shards, starts and closes the shards, routes the writer's resnapshot
//! requests to them, and logs the stats line.

mod registry;

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::FutureExt;
use futures_util::future::{FusedFuture, join_all};
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::core::shard::{self, ShardCommand, ShardContext, ShardHandle, ShardId};
use crate::core::stats::{self, RecorderCounters, StatsReporter};
use crate::core::writer::{WriterMessage, WriterProgress};
use crate::core::ws::ConnectionConfig;
use crate::exchange::{
    FeedCodec, FeedEndpoint, ListingReport, MarketCatalog, MarketInfo, MarketStatus,
};
use registry::{Placement, Registry, Removal};

/// The default most markets on one shard.
pub const DEFAULT_SHARD_SIZE: usize = 2_000;

/// The default longest time shutdown waits for the shards while the writer
/// shows no sign of moving.
pub const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// How many listed pages may wait for the coordinator before the listing
/// pauses.
const PAGE_BUFFER: usize = 4;

/// How often the coordinator samples the writer channel and checks whether
/// the stats line is due.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// How a [`Coordinator`] shards and monitors.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// The most markets on one shard.
    pub shard_size: usize,
    /// Timings and limits for each shard's connection.
    pub connection: ConnectionConfig,
    /// The capacity of the channel from each connection to its shard.
    pub shard_event_capacity: usize,
    /// The time between two stats lines.
    pub stats_interval: Duration,
    /// How long shutdown waits for the shards to hand their last events to
    /// the writer while the writer shows no sign of moving, before
    /// abandoning the rest. A writer that keeps writing, even into a slow
    /// reader, is not abandoned, except in the cases
    /// [`WriterProgress::stalled_for`] lists.
    pub stall_timeout: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            shard_size: DEFAULT_SHARD_SIZE,
            connection: ConnectionConfig::default(),
            shard_event_capacity: shard::DEFAULT_EVENT_CAPACITY,
            stats_interval: stats::DEFAULT_STATS_INTERVAL,
            stall_timeout: DEFAULT_STALL_TIMEOUT,
        }
    }
}

/// An exchange's market-data feed: where it is and how it speaks.
#[derive(Debug)]
pub struct Feed<E, C> {
    /// The websocket endpoint.
    pub endpoint: E,
    /// The message codec.
    pub codec: C,
}

/// The coordinator's channels to and from the writer thread.
#[derive(Debug)]
pub struct WriterLink {
    /// The writer's input channel.
    pub messages: mpsc::Sender<WriterMessage>,
    /// The symbols the writer wants fresh snapshots of.
    pub resnapshot_requests: mpsc::Receiver<Arc<str>>,
    /// The writer's running counts, for the stats line.
    pub progress: Arc<WriterProgress>,
}

/// Owns the market registry and the shards.
///
/// [`Coordinator::run`] lists the open markets and records them until
/// cancelled. The coordinator holds the writer's input channel, as does each
/// shard, so the writer finishes once the coordinator and its shards have
/// shut down.
pub struct Coordinator<E, C> {
    context: ShardContext<E, C>,
    registry: Registry,
    shards: HashMap<ShardId, ShardHandle>,
    /// Shards closing because their last market was removed.
    closing: Vec<JoinHandle<()>>,
    /// Markets removed whose books the writer was told to forget.
    forgotten: HashSet<Arc<str>>,
    resnapshot_requests: mpsc::Receiver<Arc<str>>,
    stats: StatsReporter,
    startup: Startup,
    progress: Arc<WriterProgress>,
    stall_timeout: Duration,
}

/// Startup milestones, each logged once.
struct Startup {
    began: Instant,
    listed: bool,
    all_connected: bool,
}

impl<E, C> Coordinator<E, C>
where
    E: FeedEndpoint + Send + Sync + 'static,
    C: FeedCodec + Send + Sync + 'static,
{
    /// A coordinator with no markets yet, recording `feed` through `writer`.
    pub fn new(feed: Feed<E, C>, writer: WriterLink, config: CoordinatorConfig) -> Self {
        let context = shard_context(feed, writer.messages, &config);
        let counters = context.counters.clone();
        Self {
            context,
            registry: Registry::new(config.shard_size),
            shards: HashMap::new(),
            closing: Vec::new(),
            forgotten: HashSet::new(),
            resnapshot_requests: writer.resnapshot_requests,
            stats: StatsReporter::new(counters, writer.progress.clone(), config.stats_interval),
            startup: Startup::new(),
            progress: writer.progress,
            stall_timeout: config.stall_timeout,
        }
    }

    /// Lists `catalog`'s open markets, recording each page's markets as it
    /// arrives, then keeps recording until `cancel` fires, and shuts down.
    ///
    /// Fails, after shutting down, if the listing fails.
    pub async fn run<K: MarketCatalog>(
        mut self,
        catalog: K,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let served = self.serve(&catalog, &cancel).await;
        self.shutdown().await;
        served
    }

    /// Records the markets of `markets` that are open or paused and not yet
    /// recorded, starting shards as needed. A market recorded before gets a
    /// fresh snapshot requested, since its old subscription may still be
    /// winding down.
    pub async fn add_markets(&mut self, markets: Vec<MarketInfo>) {
        let recordable = markets.into_iter().filter(is_recordable);
        let placements = self.registry.place(recordable.map(|market| market.symbol));
        for placement in placements {
            let shard = placement.shard;
            let tracked = self.track_again(&placement.symbols).await;
            self.apply(placement);
            for symbol in tracked {
                self.send_to(shard, ShardCommand::Resnapshot(symbol));
            }
        }
    }

    /// Stops recording `symbol`: unsubscribes it, or closes its shard if it
    /// was the shard's last market, and has the writer write a final
    /// snapshot and drop the book.
    pub async fn remove_market(&mut self, symbol: Arc<str>) {
        match self.registry.remove(&symbol) {
            Some(Removal::Unsubscribe(shard)) => {
                self.send_to(shard, ShardCommand::Remove(vec![symbol.clone()]));
            }
            Some(Removal::Close(shard)) => self.close_shard(shard),
            None => {
                tracing::debug!(%symbol, "asked to remove a market not recorded");
                return;
            }
        }
        self.forgotten.insert(symbol.clone());
        self.send_to_writer(WriterMessage::Forget(symbol)).await;
    }

    /// How many shards are open.
    pub fn shard_count(&self) -> usize {
        self.registry.shard_count()
    }

    /// The counters the shards keep.
    pub fn counters(&self) -> Arc<RecorderCounters> {
        self.context.counters.clone()
    }

    /// Closes every shard and waits for each to hand its last events to the
    /// writer. If the writer shows no sign of moving for the stall timeout
    /// meanwhile, abandons the events it has not taken. Then logs a last
    /// stats line and drops the writer's channel.
    pub async fn shutdown(mut self) {
        self.context.cancel.cancel();
        let open = self.shards.drain().map(|(_, shard)| shard.close());
        let tasks: Vec<_> = open.chain(self.closing.drain(..)).collect();
        let mut joined = pin!(join_all(tasks));
        let results = tokio::select! {
            biased;
            results = &mut joined => results,
            () = self.progress.stalled_for(self.stall_timeout) => {
                tracing::warn!(timeout = ?self.stall_timeout, "the writer stalled while draining");
                self.context.abandon.cancel();
                joined.await
            }
        };
        results.into_iter().for_each(log_shard_end);
        let shards = self.registry.shard_count();
        self.stats.log_now(&self.context.writer, shards);
    }

    async fn serve<K: MarketCatalog>(
        &mut self,
        catalog: &K,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let (page_sender, mut pages) = mpsc::channel(PAGE_BUFFER);
        let listing = catalog.list_markets(MarketStatus::Open, page_sender);
        let mut listing = pin!(listing.fuse());
        let mut samples = tokio::time::interval(SAMPLE_INTERVAL);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(page) = pages.recv() => self.add_markets(page).await,
                listed = &mut listing, if !listing.is_terminated() => {
                    self.add_buffered_pages(&mut pages).await;
                    self.finish_listing(listed)?;
                }
                Some(symbol) = self.resnapshot_requests.recv() => self.route_resnapshot(symbol),
                _ = samples.tick() => self.on_sample(),
            }
        }
    }

    /// Records the pages a finished listing left waiting in `pages`.
    async fn add_buffered_pages(&mut self, pages: &mut mpsc::Receiver<Vec<MarketInfo>>) {
        while let Ok(page) = pages.try_recv() {
            self.add_markets(page).await;
        }
    }

    fn finish_listing(&mut self, listed: anyhow::Result<ListingReport>) -> anyhow::Result<()> {
        let report = listed.context("listing the open markets")?;
        self.startup.listed = true;
        tracing::info!(
            markets = self.registry.market_count(),
            shards = self.registry.shard_count(),
            skipped = report.skipped.len(),
            elapsed_ms = self.startup.began.elapsed().as_millis(),
            "listed the open markets"
        );
        Ok(())
    }

    fn on_sample(&mut self) {
        let shards = self.registry.shard_count();
        self.stats.sample(&self.context.writer, shards);
        let up = self.context.counters.read().connections_up;
        if self.startup.reached_all_connected(up, shards) {
            let elapsed_ms = self.startup.began.elapsed().as_millis();
            tracing::info!(shards, elapsed_ms, "every shard is connected");
        }
    }

    fn apply(&mut self, placement: Placement) {
        let Placement {
            shard,
            opens,
            symbols,
        } = placement;
        tracing::debug!(shard = shard.0, ?symbols, "placed markets");
        if opens {
            tracing::info!(shard = shard.0, markets = symbols.len(), "opening a shard");
            let handle = ShardHandle::spawn(shard, symbols.into_iter().collect(), &self.context);
            self.shards.insert(shard, handle);
        } else {
            self.send_to(shard, ShardCommand::Add(symbols));
        }
    }

    /// Tells the writer to accept events again for any of `symbols` it was
    /// told to forget, before they are subscribed again, and returns those.
    async fn track_again(&mut self, symbols: &[Arc<str>]) -> Vec<Arc<str>> {
        let tracked: Vec<_> = symbols
            .iter()
            .filter(|symbol| self.forgotten.remove(*symbol))
            .cloned()
            .collect();
        for symbol in &tracked {
            self.send_to_writer(WriterMessage::Track(symbol.clone()))
                .await;
        }
        tracked
    }

    fn route_resnapshot(&mut self, symbol: Arc<str>) {
        let Some(shard) = self.registry.owner(&symbol) else {
            tracing::debug!(%symbol, "dropped a resnapshot request for a market not recorded");
            return;
        };
        self.context.counters.note_resnapshot_request();
        tracing::info!(%symbol, shard = shard.0, "requesting a fresh snapshot");
        self.send_to(shard, ShardCommand::Resnapshot(symbol));
    }

    fn close_shard(&mut self, shard: ShardId) {
        let Some(handle) = self.shards.remove(&shard) else {
            return;
        };
        tracing::info!(shard = shard.0, "closing a shard with no markets left");
        self.reap_closed();
        self.closing.push(handle.close());
    }

    /// Drops the closing shards that have finished, logging any that
    /// panicked.
    fn reap_closed(&mut self) {
        let (finished, running) = self
            .closing
            .drain(..)
            .partition(|task: &JoinHandle<()>| task.is_finished());
        self.closing = running;
        let ends = finished.into_iter().filter_map(FutureExt::now_or_never);
        ends.for_each(log_shard_end);
    }

    fn send_to(&self, shard: ShardId, command: ShardCommand) {
        match self.shards.get(&shard) {
            Some(handle) => handle.send(command),
            None => tracing::warn!(shard = shard.0, ?command, "no such shard"),
        }
    }

    async fn send_to_writer(&self, message: WriterMessage) {
        if self.context.writer.send(message).await.is_err() {
            tracing::error!("the writer has stopped");
        }
    }
}

/// The context every shard of a new coordinator shares, with fresh counters
/// and tokens.
fn shard_context<E, C>(
    feed: Feed<E, C>,
    writer: mpsc::Sender<WriterMessage>,
    config: &CoordinatorConfig,
) -> ShardContext<E, C> {
    ShardContext {
        endpoint: Arc::new(feed.endpoint),
        codec: Arc::new(feed.codec),
        connection: config.connection.clone(),
        event_capacity: config.shard_event_capacity,
        writer,
        counters: Arc::new(RecorderCounters::default()),
        cancel: CancellationToken::new(),
        abandon: CancellationToken::new(),
    }
}

/// Logs a shard task that panicked.
fn log_shard_end(ended: Result<(), JoinError>) {
    if let Err(error) = ended {
        tracing::error!(%error, "a shard task failed");
    }
}

impl Startup {
    fn new() -> Self {
        Self {
            began: Instant::now(),
            listed: false,
            all_connected: false,
        }
    }

    /// True the first time every one of `shards` is up after the listing.
    fn reached_all_connected(&mut self, up: usize, shards: usize) -> bool {
        let reached = self.listed && !self.all_connected && up >= shards;
        self.all_connected |= reached;
        reached
    }
}

/// Whether a market is worth subscribing: open, or paused and expected to
/// reopen.
fn is_recordable(market: &MarketInfo) -> bool {
    matches!(market.status, MarketStatus::Open | MarketStatus::Paused)
}
