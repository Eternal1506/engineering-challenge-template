//! [`Walks`]: catalog walks run one at a time alongside the coordinator's
//! loop, their pages handed over as they arrive.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::mpsc;

use super::PAGE_BUFFER;
use crate::exchange::{MarketCatalog, MarketInfo, MarketStatus};

/// How a walk ended: the markets it could not parse, or why it failed.
pub(super) type WalkResult = anyhow::Result<Vec<Arc<str>>>;

/// What a running walk hands over next.
#[derive(Debug)]
pub(super) enum WalkEvent {
    /// One page of listed markets.
    Page(Vec<MarketInfo>),
    /// The walk ended, after every page was handed over.
    Finished(WalkResult),
}

/// Runs the walks `start_walk` makes, one at a time. A walk is polled only
/// from [`Walks::next`], so it runs while the coordinator's loop waits for
/// its next input.
pub(super) struct Walks<S, F>
where
    F: Future<Output = WalkResult>,
{
    start_walk: S,
    sender: mpsc::Sender<Vec<MarketInfo>>,
    pages: mpsc::Receiver<Vec<MarketInfo>>,
    running: Option<Pin<Box<F>>>,
    finished: Option<WalkResult>,
}

impl<S, F> Walks<S, F>
where
    S: FnMut(mpsc::Sender<Vec<MarketInfo>>) -> F,
    F: Future<Output = WalkResult>,
{
    /// No walk yet; each one is made by calling `start_walk` with the
    /// sender for its pages.
    pub(super) fn new(start_walk: S) -> Self {
        let (sender, pages) = mpsc::channel(PAGE_BUFFER);
        Self {
            start_walk,
            sender,
            pages,
            running: None,
            finished: None,
        }
    }

    /// Whether no walk is running or waiting to hand over its end.
    pub(super) fn is_idle(&self) -> bool {
        self.running.is_none() && self.finished.is_none()
    }

    /// Starts a walk. Call only when idle.
    pub(super) fn start(&mut self) {
        let walk = (self.start_walk)(self.sender.clone());
        self.running = Some(Box::pin(walk));
    }

    /// The running walk's next page, or its end once every page has been
    /// handed over. Never resolves while idle.
    pub(super) async fn next(&mut self) -> WalkEvent {
        loop {
            if let Ok(page) = self.pages.try_recv() {
                return WalkEvent::Page(page);
            }
            if let Some(result) = self.finished.take() {
                return WalkEvent::Finished(result);
            }
            let Some(running) = self.running.as_mut() else {
                return std::future::pending().await;
            };
            tokio::select! {
                biased;
                Some(page) = self.pages.recv() => return WalkEvent::Page(page),
                result = running => {
                    self.running = None;
                    self.finished = Some(result);
                }
            }
        }
    }
}

/// Lists `catalog`'s open markets, then its paused ones, sending every page
/// to `pages`. Returns the markets either listing skipped.
pub(super) async fn walk_catalog<K: MarketCatalog>(
    catalog: &K,
    pages: mpsc::Sender<Vec<MarketInfo>>,
) -> WalkResult {
    let open = catalog.list_markets(MarketStatus::Open, pages.clone());
    let open = open.await.context("listing the open markets")?;
    let paused = catalog.list_markets(MarketStatus::Paused, pages);
    let paused = paused.await.context("listing the paused markets")?;
    Ok([open.skipped, paused.skipped].concat())
}
