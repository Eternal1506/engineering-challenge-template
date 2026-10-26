//! Local books built from the live feed exactly as the recorder will build
//! them, plus the consistency checks the probe reports.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use engineering_challenge::core::book::Book;
use engineering_challenge::core::events::{FeedBody, FeedEvent, LevelChange};

type Levels = Vec<(i64, i64)>;

/// One market's book and the clocks seen for it.
#[derive(Default)]
struct TrackedBook {
    book: Book,
    snapshot_clock: Option<u64>,
    last_clock: Option<u64>,
    deltas: u64,
}

/// A `get_snapshot` reply compared with the local book just before it.
pub struct Comparison {
    /// The market.
    pub symbol: Arc<str>,
    /// Whether every level matched.
    pub matched: bool,
    /// The newest clock applied to the local book.
    pub local_clock: Option<u64>,
    /// The reply's clock.
    pub snapshot_clock: Option<u64>,
    /// How many levels each held, local then reply.
    pub level_counts: (usize, usize),
    /// The levels that differed, as `side price: local -> reply`.
    pub differences: Vec<String>,
    /// For a mismatch, the deltas that followed the reply for this market.
    pub followups: Vec<String>,
    /// For a mismatch, how many following deltas it took the pre-reply
    /// local book to equal the reply, if it ever did: the reply already
    /// held changes whose deltas were still on their way.
    pub caught_up_after: Option<usize>,
}

/// A mismatched market's pre-reply local book, still fed the deltas that
/// follow, to see whether it catches up with the reply.
struct Shadow {
    book: Book,
    reply: (Levels, Levels),
    comparison: usize,
    deltas_seen: usize,
}

const MAX_FOLLOWUPS: usize = 10;

/// Counters over every snapshot and delta applied.
#[derive(Default)]
pub struct BookStats {
    pub snapshots: u64,
    pub snapshots_with_clock: u64,
    pub snapshots_with_exchange_ts: u64,
    pub crossed_snapshots: u64,
    pub deltas: u64,
    pub deltas_with_clock: u64,
    pub deltas_with_exchange_ts: u64,
    pub deltas_before_snapshot: u64,
    pub negative_results: u64,
    pub deltas_covered_by_snapshot: u64,
    pub clock_regressions: u64,
}

/// Every subscribed market's book.
#[derive(Default)]
pub struct Books {
    tracked: HashMap<Arc<str>, TrackedBook>,
    first_snapshot_after: HashMap<Arc<str>, Duration>,
    /// Markets whose next snapshot answers `get_snapshot` and is compared.
    awaiting_reply: HashSet<Arc<str>>,
    shadows: HashMap<Arc<str>, Shadow>,
    pub stats: BookStats,
    pub comparisons: Vec<Comparison>,
}

impl Books {
    /// Applies a snapshot event that arrived `since` subscribing. Other
    /// events are left alone.
    pub fn on_snapshot(&mut self, event: &FeedEvent, since: Duration) {
        let FeedBody::Snapshot { bids, asks } = &event.body else {
            return;
        };
        self.count_snapshot(event, bids, asks);
        self.first_snapshot_after
            .entry(event.symbol.clone())
            .or_insert(since);
        if self.awaiting_reply.remove(&event.symbol) {
            self.compare(event, bids, asks);
        }
        let tracked = self.tracked.entry(event.symbol.clone()).or_default();
        tracked
            .book
            .replace(bids.clone(), asks.clone(), event.exchange_ts);
        tracked.snapshot_clock = event.seq_hint;
        tracked.last_clock = event.seq_hint;
    }

    /// Applies one delta, checking its clock against the book's.
    pub fn on_delta(&mut self, event: &FeedEvent, change: LevelChange) {
        self.count_delta(event);
        self.follow_mismatch(event, change);
        let Some(tracked) = self.tracked.get_mut(&event.symbol) else {
            self.stats.deltas_before_snapshot += 1;
            return;
        };
        let clock = event.seq_hint;
        let covered = clock
            .zip(tracked.snapshot_clock)
            .is_some_and(|(c, s)| c <= s);
        let regressed = clock
            .zip(tracked.last_clock)
            .is_some_and(|(c, last)| c < last);
        self.stats.deltas_covered_by_snapshot += u64::from(covered);
        self.stats.clock_regressions += u64::from(regressed);
        tracked.last_clock = tracked.last_clock.max(clock);
        tracked.deltas += 1;
        let applied = tracked.book.apply(change, event.exchange_ts);
        self.stats.negative_results += u64::from(!applied.consistent);
    }

    /// The `count` markets with the most deltas so far, which will have
    /// their next snapshot compared with the local book.
    pub fn await_replies_for_most_active(&mut self, count: usize) -> Vec<Arc<str>> {
        let mut active: Vec<_> = self.tracked.iter().collect();
        active.sort_by_key(|(_, tracked)| std::cmp::Reverse(tracked.deltas));
        let chosen: Vec<Arc<str>> = active
            .into_iter()
            .take(count)
            .map(|(symbol, _)| symbol.clone())
            .collect();
        self.awaiting_reply = chosen.iter().cloned().collect();
        chosen
    }

    /// Markets still waiting for their `get_snapshot` reply.
    pub fn replies_outstanding(&self) -> usize {
        self.awaiting_reply.len()
    }

    /// How many of `symbols` got a snapshot, and the latest first-snapshot
    /// time among them.
    pub fn initial_snapshots(&self, symbols: &[Arc<str>]) -> (usize, Option<Duration>) {
        let times: Vec<Duration> = symbols
            .iter()
            .filter_map(|symbol| self.first_snapshot_after.get(symbol).copied())
            .collect();
        (times.len(), times.into_iter().max())
    }

    /// Local books whose best bid is at or above their best ask.
    pub fn crossed_books(&self) -> usize {
        self.tracked
            .values()
            .filter(|tracked| {
                let (bids, asks) = sorted_levels(&tracked.book);
                is_crossed(&bids, &asks)
            })
            .count()
    }

    fn count_snapshot(&mut self, event: &FeedEvent, bids: &Levels, asks: &Levels) {
        self.stats.snapshots += 1;
        self.stats.snapshots_with_clock += u64::from(event.seq_hint.is_some());
        self.stats.snapshots_with_exchange_ts += u64::from(event.exchange_ts.is_some());
        self.stats.crossed_snapshots += u64::from(is_crossed(bids, asks));
    }

    fn count_delta(&mut self, event: &FeedEvent) {
        self.stats.deltas += 1;
        self.stats.deltas_with_clock += u64::from(event.seq_hint.is_some());
        self.stats.deltas_with_exchange_ts += u64::from(event.exchange_ts.is_some());
    }

    fn compare(&mut self, event: &FeedEvent, bids: &Levels, asks: &Levels) {
        let Some(tracked) = self.tracked.get(&event.symbol) else {
            return;
        };
        let local = sorted_levels(&tracked.book);
        let reply = (best_first(bids, true), best_first(asks, false));
        let matched = local == reply;
        let comparison = Comparison {
            symbol: event.symbol.clone(),
            matched,
            local_clock: tracked.last_clock,
            snapshot_clock: event.seq_hint,
            level_counts: (local.0.len() + local.1.len(), reply.0.len() + reply.1.len()),
            differences: differences(&local, &reply),
            followups: Vec::new(),
            caught_up_after: None,
        };
        if !matched {
            self.start_shadow(event.symbol.clone(), local, reply);
        }
        self.comparisons.push(comparison);
    }

    fn start_shadow(&mut self, symbol: Arc<str>, local: (Levels, Levels), reply: (Levels, Levels)) {
        let mut book = Book::default();
        book.replace(local.0, local.1, None);
        let shadow = Shadow {
            book,
            reply,
            comparison: self.comparisons.len(),
            deltas_seen: 0,
        };
        self.shadows.insert(symbol, shadow);
    }

    /// Feeds a delta to the market's shadow book, if it has one, and notes
    /// when the shadow first equals the reply.
    fn follow_mismatch(&mut self, event: &FeedEvent, change: LevelChange) {
        let Some(shadow) = self.shadows.get_mut(&event.symbol) else {
            return;
        };
        let _ = shadow.book.apply(change, None);
        shadow.deltas_seen += 1;
        let comparison = &mut self.comparisons[shadow.comparison];
        comparison.followups.push(format!("{change:?}"));
        if sorted_levels(&shadow.book) == shadow.reply {
            comparison.caught_up_after = Some(shadow.deltas_seen);
        }
        if comparison.caught_up_after.is_some() || shadow.deltas_seen >= MAX_FOLLOWUPS {
            self.shadows.remove(&event.symbol);
        }
    }
}

/// The levels whose quantities differ between `local` and `reply`.
fn differences(local: &(Levels, Levels), reply: &(Levels, Levels)) -> Vec<String> {
    let mut found = side_differences("bid", &local.0, &reply.0);
    found.extend(side_differences("ask", &local.1, &reply.1));
    found
}

fn side_differences(side: &str, local: &Levels, reply: &Levels) -> Vec<String> {
    let local: HashMap<i64, i64> = local.iter().copied().collect();
    let reply: HashMap<i64, i64> = reply.iter().copied().collect();
    let mut prices: Vec<i64> = local.keys().chain(reply.keys()).copied().collect();
    prices.sort_unstable();
    prices.dedup();
    prices
        .into_iter()
        .filter(|price| local.get(price) != reply.get(price))
        .map(|price| {
            let (before, after) = (local.get(&price), reply.get(&price));
            format!("{side} {price}: {before:?} -> {after:?}")
        })
        .collect()
}

/// A book's bids and asks, best price first.
fn sorted_levels(book: &Book) -> (Levels, Levels) {
    let snapshot = book.to_snapshot("", Utc::now());
    let bids = snapshot.bids.iter().map(|e| (e.price_level, e.quantity));
    let asks = snapshot.asks.iter().map(|e| (e.price_level, e.quantity));
    (bids.collect(), asks.collect())
}

/// Positive levels sorted best first: highest first for bids.
fn best_first(levels: &Levels, descending: bool) -> Levels {
    let mut sorted: Levels = levels.iter().copied().filter(|&(_, q)| q > 0).collect();
    sorted.sort_unstable();
    if descending {
        sorted.reverse();
    }
    sorted
}

fn is_crossed(bids: &Levels, asks: &Levels) -> bool {
    let best_bid = bids.iter().map(|&(price, _)| price).max();
    let best_ask = asks.iter().map(|&(price, _)| price).min();
    matches!((best_bid, best_ask), (Some(bid), Some(ask)) if bid >= ask)
}
