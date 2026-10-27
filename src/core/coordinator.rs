//! The [`Coordinator`]: it lists the exchange's markets, spreads them over
//! shards, follows each market's lifecycle, starts and closes the shards,
//! routes the writer's resnapshot requests to them, and logs the stats line.
//!
//! Markets come and go in four ways:
//!
//! - The startup listing of open and paused markets records each page as
//!   it arrives.
//! - A lifecycle connection reports markets created, opened, paused, given
//!   a new close time, determined and settled.
//! - A sweep records markets whose open time has passed and stops recording
//!   those whose close time has, since neither emits a lifecycle event.
//! - A periodic reconcile walks the catalog's open and paused markets, and
//!   also runs at once when the lifecycle connection may have lost events.
//!   It removes a market that should be trading only once two walks in a
//!   row have missed it.
//!
//! Changes from the last three collect for a short window and then go to
//! each shard as one command, so a burst of lifecycle events does not
//! become a command per market.

mod budget;
mod lifecycle;
mod markets;
mod reconcile;
mod registry;
mod timers;
mod upkeep;
mod walks;

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use futures_util::FutureExt;
use futures_util::future::{FusedFuture, join_all};
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::core::shard::{
    self, FailoverTimings, ResyncTimings, ShardCommand, ShardContext, ShardHandle, ShardId,
    ShardPlan,
};
use crate::core::stats::{self, Census, ChangeCause, RecorderCounters, StatsReporter};
use crate::core::writer::{WriterMessage, WriterProgress};
use crate::core::ws::{ConnectionConfig, ConnectionEvent};
use crate::exchange::{FeedCodec, FeedEndpoint, MarketCatalog, MarketInfo};
use budget::ConnectionBudget;
use lifecycle::LifecycleLink;
use markets::{MarketStates, Verdict};
use registry::{Changes, Registry, Update};
use timers::{Tick, Timers};
use upkeep::Upkeep;
use walks::{WalkResult, Walks, walk_catalog};

/// The default most markets on one shard.
pub const DEFAULT_SHARD_SIZE: usize = 2_000;

/// The default connections per shard: a primary and a standby.
pub const DEFAULT_REPLICAS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// The default most connections the recorder holds at once, below the
/// exchange's limit of about 200 to leave room for old sockets still
/// closing on the exchange's side.
pub const DEFAULT_MAX_CONNECTIONS: usize = 190;

/// The default longest time shutdown waits for the shards while the writer
/// shows no sign of moving.
pub const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// How many listed pages may wait for the coordinator before a listing
/// pauses.
const PAGE_BUFFER: usize = 4;

/// How a [`Coordinator`] shards and monitors.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// The most markets on one shard.
    pub shard_size: usize,
    /// The connections per shard: one primary, the rest standbys.
    pub replicas: NonZeroUsize,
    /// The most connections the recorder holds at once, the lifecycle
    /// connection included. A shard whose replicas would go beyond it runs
    /// on one connection.
    pub max_connections: usize,
    /// When each shard's standbys start and may take over.
    pub failover: FailoverTimings,
    /// Timings and limits for each shard's connection.
    pub connection: ConnectionConfig,
    /// The capacity of the channel from each connection to its shard.
    pub shard_event_capacity: usize,
    /// How each shard waits for the snapshots a resync asks for.
    pub resync: ResyncTimings,
    /// The time between two stats lines.
    pub stats_interval: Duration,
    /// How long shutdown waits for the shards to hand their last events to
    /// the writer while the writer shows no sign of moving, before
    /// abandoning the rest. A writer that keeps writing, even into a slow
    /// reader, is not abandoned, except in the cases
    /// [`WriterProgress::stalled_for`] lists.
    pub stall_timeout: Duration,
    /// How often market lifecycle work happens.
    pub lifecycle: LifecycleTimings,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            shard_size: DEFAULT_SHARD_SIZE,
            replicas: DEFAULT_REPLICAS,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            failover: FailoverTimings::default(),
            connection: ConnectionConfig::default(),
            shard_event_capacity: shard::DEFAULT_EVENT_CAPACITY,
            resync: ResyncTimings::default(),
            stats_interval: stats::DEFAULT_STATS_INTERVAL,
            stall_timeout: DEFAULT_STALL_TIMEOUT,
            lifecycle: LifecycleTimings::default(),
        }
    }
}

/// How often a [`Coordinator`] applies and checks market lifecycle
/// changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleTimings {
    /// How long subscription changes collect, from the first, before they
    /// go to the shards together.
    pub batch_window: Duration,
    /// How often to record the markets whose open time has passed and stop
    /// recording those whose close time has.
    pub sweep_interval: Duration,
    /// How often to reconcile the markets with the catalog.
    pub reconcile_interval: Duration,
}

impl Default for LifecycleTimings {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_secs(1),
            sweep_interval: Duration::from_secs(10),
            reconcile_interval: Duration::from_secs(600),
        }
    }
}

/// An exchange's market-data feed: where it is and how it speaks.
#[derive(Debug)]
pub struct Feed<E, C> {
    /// The websocket endpoint for order books.
    pub endpoint: E,
    /// The websocket endpoint for market lifecycle changes, often the same
    /// place as `endpoint`. Without one, the coordinator learns of new and
    /// ended markets only from the sweep and the reconcile.
    pub lifecycle_endpoint: Option<E>,
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
/// [`Coordinator::run`] lists the open and paused markets and records them,
/// following their lifecycle, until cancelled. The coordinator holds the
/// writer's input channel, as does each shard, so the writer finishes once
/// the coordinator and its shards have shut down.
pub struct Coordinator<E, C> {
    context: ShardContext<E, C>,
    registry: Registry,
    markets: MarketStates,
    shards: HashMap<ShardId, ShardHandle>,
    /// Shards closing because their last market was removed.
    closing: Vec<JoinHandle<()>>,
    upkeep: Upkeep<E>,
    resnapshot_requests: mpsc::Receiver<Arc<str>>,
    stats: StatsReporter,
    startup: Startup,
    progress: Arc<WriterProgress>,
    stall_timeout: Duration,
    budget: ConnectionBudget,
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
    pub fn new(mut feed: Feed<E, C>, writer: WriterLink, config: CoordinatorConfig) -> Self {
        let upkeep = Upkeep::new(feed.lifecycle_endpoint.take(), config.lifecycle.clone());
        let context = shard_context(feed, writer.messages, &config);
        let counters = context.counters.clone();
        Self {
            context,
            registry: Registry::new(config.shard_size),
            markets: MarketStates::default(),
            shards: HashMap::new(),
            closing: Vec::new(),
            upkeep,
            resnapshot_requests: writer.resnapshot_requests,
            stats: StatsReporter::new(counters, writer.progress.clone(), config.stats_interval),
            startup: Startup::new(),
            progress: writer.progress,
            stall_timeout: config.stall_timeout,
            budget: ConnectionBudget::new(config.replicas, config.max_connections),
        }
    }

    /// Starts listening for lifecycle changes, lists `catalog`'s open and
    /// paused markets, recording each page's markets as it arrives, then
    /// keeps recording and following the markets' lifecycle until `cancel`
    /// fires, and shuts down. A market a lifecycle event or the sweep
    /// changed after the listing started keeps that change.
    ///
    /// Fails, after shutting down, if the startup listing fails.
    pub async fn run<K: MarketCatalog>(
        mut self,
        catalog: K,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let served = self.serve(&catalog, &cancel).await;
        self.shutdown().await;
        served
    }

    /// Records, at once, the markets of `markets` that are open or paused,
    /// not past their close time and not yet recorded, starting shards as
    /// needed, and stops recording known markets listed as closed or
    /// settled. A market recorded before goes back to its old shard if it
    /// has room, and gets a fresh snapshot requested, since its old
    /// subscription may still be winding down.
    pub async fn add_markets(&mut self, markets: Vec<MarketInfo>) {
        let now = Utc::now();
        for market in &markets {
            let verdict = self.markets.on_listed(market, now);
            self.follow(&market.symbol, verdict);
        }
        self.flush().await;
    }

    /// Stops recording `symbol` at once and forgets it: unsubscribes it, or
    /// closes its shard if it was the shard's last market, and has the
    /// writer write a final snapshot and drop the book.
    pub async fn remove_market(&mut self, symbol: Arc<str>) {
        self.markets.remove(&symbol);
        if !self.registry.remove(&symbol) {
            tracing::debug!(%symbol, "asked to remove a market not recorded");
            return;
        }
        self.flush().await;
    }

    /// How many shards are open.
    pub fn shard_count(&self) -> usize {
        self.registry.shard_count()
    }

    /// The counters the shards and the coordinator keep.
    pub fn counters(&self) -> Arc<RecorderCounters> {
        self.context.counters.clone()
    }

    /// Closes every shard and the lifecycle connection, and waits for each
    /// shard to hand its last events to the writer. If the writer shows no
    /// sign of moving for the stall timeout meanwhile, abandons the events
    /// it has not taken. Then logs a last stats line and drops the writer's
    /// channel. Subscription changes not yet sent are dropped.
    pub async fn shutdown(mut self) {
        self.context.cancel.cancel();
        let lifecycle = self.upkeep.link.take().map(LifecycleLink::into_task);
        let open = self.shards.drain().map(|(_, shard)| shard.close());
        let closing = self.closing.drain(..).chain(lifecycle);
        let tasks: Vec<_> = open.chain(closing).collect();
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
        results.into_iter().for_each(log_task_end);
        self.stats.log_now(&self.context.writer, self.census());
    }

    async fn serve<K: MarketCatalog>(
        &mut self,
        catalog: &K,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let (page_sender, mut pages) = mpsc::channel(PAGE_BUFFER);
        let mut listing = pin!(walk_catalog(catalog, page_sender).fuse());
        let mut walks = Walks::new(|pages| walk_catalog(catalog, pages));
        let mut timers = Timers::new(&self.upkeep.timings);
        self.start_lifecycle();
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(page) = pages.recv() => self.add_listed(page).await,
                listed = &mut listing, if !listing.is_terminated() => {
                    self.add_buffered_pages(&mut pages).await;
                    self.finish_listing(listed)?;
                }
                Some(event) = next_lifecycle_event(&mut self.upkeep.link) => {
                    self.on_lifecycle(event);
                }
                walked = walks.next() => self.on_walk(walked),
                Some(symbol) = self.resnapshot_requests.recv() => self.route_resnapshot(symbol),
                () = sleep_until(self.upkeep.flush_at) => self.flush().await,
                tick = timers.next() => self.on_tick(tick),
            }
            self.start_reconcile_if_wanted(&mut walks);
        }
    }

    /// Records a page of the startup listing. A market a lifecycle event or
    /// the sweep changed since the coordinator started keeps that fresher
    /// state; the listing only fills in open and close times it lacks.
    async fn add_listed(&mut self, page: Vec<MarketInfo>) {
        let started = self.startup.began;
        let (unchanged, changed): (Vec<_>, Vec<_>) = page.into_iter().partition(|market| {
            let known = self.markets.get(&market.symbol);
            known.is_none_or(|state| state.quiet_since(started))
        });
        for market in &changed {
            self.markets.fill_times(market);
        }
        self.add_markets(unchanged).await;
    }

    /// Records the pages a finished listing left waiting in `pages`.
    async fn add_buffered_pages(&mut self, pages: &mut mpsc::Receiver<Vec<MarketInfo>>) {
        while let Ok(page) = pages.try_recv() {
            self.add_listed(page).await;
        }
    }

    fn finish_listing(&mut self, listed: WalkResult) -> anyhow::Result<()> {
        let skipped = listed.context("listing the markets at startup")?;
        self.startup.listed = true;
        tracing::info!(
            markets = self.registry.market_count(),
            shards = self.registry.shard_count(),
            skipped = skipped.len(),
            elapsed_ms = self.startup.began.elapsed().as_millis(),
            "listed the open and paused markets"
        );
        Ok(())
    }

    fn on_tick(&mut self, tick: Tick) {
        match tick {
            Tick::Sample => self.on_sample(),
            Tick::Sweep => self.sweep(),
            Tick::Reconcile => self.upkeep.reconcile_wanted = true,
        }
    }

    fn on_sample(&mut self) {
        let census = self.census();
        self.stats.sample(&self.context.writer, census);
        let up = self.context.counters.read().primaries_up;
        let shards = census.shards;
        if self.startup.reached_all_connected(up, shards) {
            let elapsed_ms = self.startup.began.elapsed().as_millis();
            tracing::info!(shards, elapsed_ms, "every shard is connected");
        }
    }

    fn census(&self) -> Census {
        Census {
            shards: self.registry.shard_count(),
            markets: self.registry.market_count(),
            known: self.markets.len(),
        }
    }

    /// Applies `verdict` to the registry, counting a change against
    /// `cause`; true if the registry changed.
    fn follow_for(&mut self, cause: ChangeCause, symbol: &Arc<str>, verdict: Verdict) -> bool {
        let changed = self.follow(symbol, verdict);
        let counters = &self.context.counters;
        match verdict {
            Verdict::Subscribe if changed => counters.note_added(cause),
            Verdict::Unsubscribe if changed => counters.note_removed(cause),
            _ => {}
        }
        changed
    }

    /// Applies `verdict` to the registry, and schedules a flush if it
    /// changed; true if it did.
    fn follow(&mut self, symbol: &Arc<str>, verdict: Verdict) -> bool {
        let changed = match verdict {
            Verdict::Subscribe => self.registry.place(symbol),
            Verdict::Unsubscribe => self.registry.remove(symbol),
            Verdict::Keep => false,
        };
        let upkeep = &mut self.upkeep;
        if changed && upkeep.flush_at.is_none() {
            upkeep.flush_at = Some(Instant::now() + upkeep.timings.batch_window);
        }
        changed
    }

    /// Sends the registry changes collected so far: tells the writer which
    /// markets to forget and which to track again, closes, starts and
    /// updates shards, and asks for fresh snapshots of returning markets.
    /// Closing first leaves the closed shards' connections out of the
    /// budget for the new ones.
    async fn flush(&mut self) {
        self.upkeep.flush_at = None;
        let changes = self.registry.take_changes(Instant::now());
        self.tell_writer(&changes).await;
        for shard in changes.closed {
            self.close_shard(shard);
        }
        for (shard, symbols) in changes.opened {
            self.open_shard(shard, symbols);
        }
        for (shard, update) in changes.updated {
            self.update_shard(shard, update);
        }
        for symbol in changes.returned {
            if let Some(shard) = self.registry.owner(&symbol) {
                self.send_to(shard, ShardCommand::Resnapshot(symbol));
            }
        }
    }

    /// Starts `shard` on `symbols` with as many replicas as the connection
    /// budget leaves room for.
    fn open_shard(&mut self, shard: ShardId, symbols: BTreeSet<Arc<str>>) {
        let replicas = self.replicas_for_new_shard();
        let markets = symbols.len();
        tracing::info!(shard = shard.0, markets, replicas, "opening a shard");
        let plan = ShardPlan { symbols, replicas };
        let handle = ShardHandle::spawn(shard, plan, &self.context);
        self.shards.insert(shard, handle);
    }

    /// The replicas a new shard gets: the configured count, or one if the
    /// open shards leave no room for more, which is counted and logged.
    fn replicas_for_new_shard(&self) -> NonZeroUsize {
        let open = self.shards.values().map(ShardHandle::replicas).sum();
        let replicas = self.budget.replicas_for(open);
        if self.budget.is_short(replicas) {
            self.context.counters.note_unreplicated_shard();
            let max_connections = self.budget.max_connections();
            tracing::warn!(
                open,
                max_connections,
                "no room for standbys, opening a lone shard"
            );
        }
        replicas
    }

    /// Has the writer forget departed markets, writing their final
    /// snapshots, and accept events again for returning ones before they
    /// are subscribed.
    async fn tell_writer(&self, changes: &Changes) {
        for symbol in &changes.departed {
            self.send_to_writer(WriterMessage::Forget(symbol.clone()))
                .await;
        }
        for symbol in &changes.returned {
            self.send_to_writer(WriterMessage::Track(symbol.clone()))
                .await;
        }
    }

    fn update_shard(&self, shard: ShardId, update: Update) {
        tracing::debug!(shard = shard.0, ?update, "changing a shard's markets");
        if !update.removed.is_empty() {
            self.send_to(shard, ShardCommand::Remove(update.removed));
        }
        if !update.added.is_empty() {
            self.send_to(shard, ShardCommand::Add(update.added));
        }
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
        ends.for_each(log_task_end);
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

/// The context every shard of a new coordinator shares, with the book
/// endpoint and codec of `feed`, and fresh counters and tokens.
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
        resync: config.resync.clone(),
        failover: config.failover.clone(),
        writer,
        counters: Arc::new(RecorderCounters::default()),
        cancel: CancellationToken::new(),
        abandon: CancellationToken::new(),
    }
}

/// The next event of the lifecycle connection, if there is one; never
/// resolves without it.
async fn next_lifecycle_event(link: &mut Option<LifecycleLink>) -> Option<ConnectionEvent> {
    match link {
        Some(link) => link.next().await,
        None => std::future::pending().await,
    }
}

/// Sleeps until `deadline`; never resolves without one.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Logs a shard or lifecycle connection task that panicked.
fn log_task_end(ended: Result<(), JoinError>) {
    if let Err(error) = ended {
        tracing::error!(%error, "a connection task failed");
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
