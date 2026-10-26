//! Keeping the recorded markets in line with the exchange: lifecycle
//! events, the sweep, and reconciles with the catalog.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::lifecycle::{LifecycleLink, LifecycleNews};
use super::markets::{Moment, Verdict};
use super::reconcile::{Leftovers, Reconcile};
use super::walks::{WalkEvent, WalkResult, Walks};
use super::{ChangeCause, Coordinator, LifecycleTimings};
use crate::core::events::LifecycleEvent;
use crate::core::ws::ConnectionEvent;
use crate::exchange::{FeedCodec, FeedEndpoint, MarketInfo};

/// The coordinator's lifecycle work: the lifecycle connection, batched
/// subscription changes and reconciles.
pub(super) struct Upkeep<E> {
    /// The lifecycle endpoint, until the connection starts.
    pub(super) endpoint: Option<E>,
    pub(super) link: Option<LifecycleLink>,
    pub(super) timings: LifecycleTimings,
    /// When the subscription changes collected so far go to the shards.
    pub(super) flush_at: Option<Instant>,
    pub(super) reconciling: Option<Reconcile>,
    /// Whether a reconcile should start as soon as none is running.
    pub(super) reconcile_wanted: bool,
    /// Markets that should be trading but that the last finished walk
    /// missed.
    pub(super) absent: HashSet<Arc<str>>,
}

impl<E> Upkeep<E> {
    /// No lifecycle work yet, with the lifecycle connection to start at
    /// `endpoint`, if any.
    pub(super) fn new(endpoint: Option<E>, timings: LifecycleTimings) -> Self {
        Self {
            endpoint,
            link: None,
            timings,
            flush_at: None,
            reconciling: None,
            reconcile_wanted: false,
            absent: HashSet::new(),
        }
    }
}

impl<E, C> Coordinator<E, C>
where
    E: FeedEndpoint + Send + Sync + 'static,
    C: FeedCodec + Send + Sync + 'static,
{
    /// Starts the lifecycle connection, if the feed has an endpoint for it.
    pub(super) fn start_lifecycle(&mut self) {
        let endpoint = self.upkeep.endpoint.take();
        let link = endpoint.map(|endpoint| LifecycleLink::spawn(endpoint, &self.context));
        self.upkeep.link = link;
    }

    /// Acts on an event of the lifecycle connection: applies a lifecycle
    /// change, or asks for a reconcile if changes may have been lost.
    pub(super) fn on_lifecycle(&mut self, event: ConnectionEvent) {
        let Some(link) = self.upkeep.link.as_mut() else {
            return;
        };
        match link.interpret(event) {
            LifecycleNews::Changed(event) => self.apply_lifecycle(&event),
            LifecycleNews::Missed => {
                tracing::info!("lifecycle events may have been lost, reconciling");
                self.upkeep.reconcile_wanted = true;
            }
            LifecycleNews::Nothing => {}
        }
    }

    /// Records the markets whose open time has passed, stops recording
    /// those past their close time, and forgets long-departed markets.
    pub(super) fn sweep(&mut self) {
        self.registry.expire_departed(Instant::now());
        let swept = self.markets.sweep(Moment::now());
        let opened = self.sweep_out(&swept.opened, Verdict::Subscribe);
        let closed = self.sweep_out(&swept.closed, Verdict::Unsubscribe);
        if opened + closed > 0 {
            tracing::info!(opened, closed, "swept markets past their open or close");
        }
    }

    /// Acts on what the reconcile walk hands over.
    pub(super) fn on_walk(&mut self, event: WalkEvent) {
        match event {
            WalkEvent::Page(page) => self.reconcile_page(&page),
            WalkEvent::Finished(walked) => self.finish_reconcile(walked),
        }
    }

    /// Starts a reconcile if one is wanted, the startup listing is done and
    /// no walk is running.
    pub(super) fn start_reconcile_if_wanted<S, F>(&mut self, walks: &mut Walks<S, F>)
    where
        S: FnMut(mpsc::Sender<Vec<MarketInfo>>) -> F,
        F: Future<Output = WalkResult>,
    {
        let upkeep = &mut self.upkeep;
        if !(upkeep.reconcile_wanted && self.startup.listed && walks.is_idle()) {
            return;
        }
        upkeep.reconcile_wanted = false;
        upkeep.reconciling = Some(Reconcile::start(upkeep.absent.clone()));
        walks.start();
        tracing::debug!("reconciling the markets with the catalog");
    }

    fn apply_lifecycle(&mut self, event: &LifecycleEvent) {
        self.context.counters.note_lifecycle_event();
        let verdict = self.markets.on_event(event, Moment::now());
        tracing::debug!(?event, ?verdict, "lifecycle event");
        self.follow_for(ChangeCause::Lifecycle, event.symbol(), verdict);
    }

    /// Applies `verdict` to each of `symbols` for the sweep, and returns
    /// how many changed.
    fn sweep_out(&mut self, symbols: &[Arc<str>], verdict: Verdict) -> usize {
        let mut changed = 0;
        for symbol in symbols {
            if self.follow_for(ChangeCause::Sweep, symbol, verdict) {
                tracing::debug!(%symbol, ?verdict, "the open or close time passed");
                changed += 1;
            }
        }
        changed
    }

    /// Applies one page of the walk to every market neither a lifecycle
    /// event nor the sweep changed since the walk started.
    fn reconcile_page(&mut self, page: &[MarketInfo]) {
        let Some(mut reconcile) = self.upkeep.reconciling.take() else {
            return;
        };
        let now = Utc::now();
        for market in page {
            if reconcile.see(&market.symbol, self.markets.get(&market.symbol)) {
                let verdict = self.markets.on_listed(market, now);
                if self.follow_for(ChangeCause::Reconcile, &market.symbol, verdict) {
                    reconcile.tally(verdict);
                }
            }
        }
        self.upkeep.reconciling = Some(reconcile);
    }

    /// Ends the reconcile: once the walk has succeeded, forgets the markets
    /// it shows are gone and notes those it missed for the first time, then
    /// logs the outcome. A failed walk changes nothing more.
    fn finish_reconcile(&mut self, walked: WalkResult) {
        let Some(mut reconcile) = self.upkeep.reconciling.take() else {
            return;
        };
        self.context.counters.note_reconcile();
        match walked {
            Ok(skipped) => {
                let skipped: HashSet<Arc<str>> = skipped.into_iter().collect();
                let leftovers = reconcile.leftovers(&self.markets, &skipped, Utc::now());
                let absent = self.forget_leftovers(&mut reconcile, leftovers);
                reconcile.log_finished(skipped.len(), absent);
            }
            Err(error) => reconcile.log_failed(&error),
        }
    }

    /// Forgets the ended and missing markets, logging each missing one,
    /// and keeps the absent ones for the next walk; returns how many those
    /// are.
    fn forget_leftovers(&mut self, reconcile: &mut Reconcile, leftovers: Leftovers) -> usize {
        for symbol in &leftovers.missing {
            self.log_missing(symbol);
        }
        let gone = leftovers.ended.iter().chain(&leftovers.missing);
        for symbol in gone {
            self.markets.remove(symbol);
            if self.follow_for(ChangeCause::Reconcile, symbol, Verdict::Unsubscribe) {
                reconcile.tally(Verdict::Unsubscribe);
            }
        }
        let absent = leftovers.absent.len();
        self.upkeep.absent = leftovers.absent;
        absent
    }

    fn log_missing(&self, symbol: &Arc<str>) {
        let state = self.markets.get(symbol);
        tracing::info!(
            %symbol,
            status = ?state.map(|state| state.status()),
            close_ts = ?state.and_then(|state| state.close_ts()),
            "two walks in a row missed the market, removing it"
        );
    }
}
