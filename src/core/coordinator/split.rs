//! Splitting a shard that keeps overflowing: [`SplitRule`] says how many
//! overflow incidents within how long call for it, and [`OverflowLog`]
//! keeps each shard's recent ones.
//!
//! Only the primary's overflows count, and overflows within
//! [`SplitRule::incident_gap`] of an incident's first are the same
//! incident, so one burst of load that overflows the primary, and the
//! standby that takes over from it, counts once.
//!
//! A split moves every other market of the shard, in ticker order, to a new
//! shard, so markets of one series, whose tickers sort together and which
//! tend to be busy together, end up on both. The order of their events at
//! the writer is kept by opening the new shard only once the old one has
//! released them: the old shard gets a
//! [`Release`](ShardCommand::Release) of the moved markets, and reports
//! [`Released`](ShardReport::Released) once it has stopped forwarding
//! them, when every event of theirs it forwarded is already in the writer's
//! channel. Only then does the new shard start, so its subscribe snapshots,
//! which replace the moved markets' books, come after all of them. Until it
//! starts, changes to the new shard's markets are applied to the markets it
//! will start with, and the old shard's overflows are not counted.
//!
//! Only two things start a waiting shard: the old shard's `Released`, or
//! the coordinator finding, as it reaps the shards it closed, that the old
//! shard's task has ended. A supervisor handles every command queued for it
//! before it stops, so a closed shard reports `Released` first anyway. A
//! supervisor that ends on its own while still open, which today happens
//! only once the writer has stopped or been abandoned on shutdown, never
//! starts its waiting shard.
//!
//! A split that would take the recorder past its connection budget is
//! skipped.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::Coordinator;
use super::registry::Update;
use crate::core::shard::{ShardCommand, ShardId, ShardReport};
use crate::exchange::{FeedCodec, FeedEndpoint};

/// When a shard is split in two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitRule {
    /// How many overflow incidents of a shard's primary split the shard.
    pub overflows: usize,
    /// The span those incidents must fall within.
    pub window: Duration,
    /// How long after an incident's first overflow later ones belong to
    /// the same incident.
    pub incident_gap: Duration,
}

impl Default for SplitRule {
    fn default() -> Self {
        Self {
            overflows: 2,
            window: Duration::from_secs(600),
            incident_gap: Duration::from_secs(30),
        }
    }
}

/// When each shard's overflow incidents within the rule's window began.
#[derive(Debug)]
pub(super) struct OverflowLog {
    rule: SplitRule,
    recent: HashMap<ShardId, VecDeque<Instant>>,
}

impl OverflowLog {
    pub(super) fn new(rule: SplitRule) -> Self {
        Self {
            rule,
            recent: HashMap::new(),
        }
    }

    /// Notes an overflow of `shard` at `now`; true once the shard has had
    /// the rule's count of incidents within its window, which starts its
    /// count over.
    pub(super) fn note(&mut self, shard: ShardId, now: Instant) -> bool {
        let (window, gap) = (self.rule.window, self.rule.incident_gap);
        let within = |span: Duration| move |at: &Instant| now.saturating_duration_since(*at) < span;
        let recent = self.recent.entry(shard).or_default();
        recent.retain(within(window));
        if recent.back().is_some_and(within(gap)) {
            return false;
        }
        recent.push_back(now);
        let due = recent.len() >= self.rule.overflows;
        if due {
            self.recent.remove(&shard);
        }
        due
    }

    /// Forgets `shard`, which has closed.
    pub(super) fn forget(&mut self, shard: ShardId) {
        self.recent.remove(&shard);
    }
}

/// The new shards of splits, each waiting for the shard it was split from
/// to release its markets.
#[derive(Debug, Default)]
pub(super) struct WaitingShards {
    /// By the shard split from: the new shard and the markets it will start
    /// with.
    by_parent: HashMap<ShardId, (ShardId, BTreeSet<Arc<str>>)>,
}

impl WaitingShards {
    /// Has `new` wait, with `symbols`, for `parent` to release them.
    fn wait(&mut self, parent: ShardId, new: ShardId, symbols: BTreeSet<Arc<str>>) {
        self.by_parent.insert(parent, (new, symbols));
    }

    /// Takes the shard waiting for `parent`, with its markets.
    pub(super) fn take(&mut self, parent: ShardId) -> Option<(ShardId, BTreeSet<Arc<str>>)> {
        self.by_parent.remove(&parent)
    }

    /// Whether a split shard waits for `parent`.
    fn has_parent(&self, parent: ShardId) -> bool {
        self.by_parent.contains_key(&parent)
    }

    /// How many shards wait.
    fn len(&self) -> usize {
        self.by_parent.len()
    }

    /// Whether `shard` is waiting to start.
    pub(super) fn holds(&self, shard: ShardId) -> bool {
        self.by_parent.values().any(|(new, _)| *new == shard)
    }

    /// Applies `update` to the markets `shard` will start with; false if it
    /// is not waiting.
    pub(super) fn update(&mut self, shard: ShardId, update: &Update) -> bool {
        let waiting = self.by_parent.values_mut().find(|(new, _)| *new == shard);
        let Some((_, symbols)) = waiting else {
            return false;
        };
        update.removed.iter().for_each(|symbol| {
            symbols.remove(symbol);
        });
        symbols.extend(update.added.iter().cloned());
        true
    }

    /// Stops waiting with `shard`, closed before it started; false if it
    /// was not waiting.
    pub(super) fn cancel(&mut self, shard: ShardId) -> bool {
        let before = self.by_parent.len();
        self.by_parent.retain(|_, (new, _)| *new != shard);
        self.by_parent.len() < before
    }
}

impl<E, C> Coordinator<E, C>
where
    E: FeedEndpoint + Send + Sync + 'static,
    C: FeedCodec + Send + Sync + 'static,
{
    /// Acts on what a shard reports.
    pub(super) async fn on_report(&mut self, report: ShardReport) {
        match report {
            ShardReport::Overflowed(shard) if self.waiting.has_parent(shard) => {
                tracing::debug!(shard = shard.0, "an overflow of a shard already splitting");
            }
            ShardReport::Overflowed(shard) => {
                if self.overflows.note(shard, Instant::now()) {
                    self.split(shard).await;
                }
            }
            ShardReport::Released(parent) => self.start_waiting(parent),
        }
    }

    /// Starts the shard split from `parent`, if one waits for it.
    pub(super) fn start_waiting(&mut self, parent: ShardId) {
        if let Some((new, symbols)) = self.waiting.take(parent) {
            self.open_shard(new, symbols);
        }
    }

    /// Moves half of `shard`'s markets to a new shard, if the shard is
    /// still open, not already splitting, has at least two markets, and the
    /// connection budget has room. The new shard starts once `shard` has
    /// released its markets.
    async fn split(&mut self, shard: ShardId) {
        self.flush().await;
        if !self.shards.contains_key(&shard) || self.waiting.has_parent(shard) {
            return;
        }
        if !self.room_to_split(shard) {
            return;
        }
        let Some((new, moved)) = self.registry.split(shard) else {
            return;
        };
        self.context.counters.note_split();
        let markets = moved.len();
        tracing::warn!(
            shard = shard.0,
            new = new.0,
            markets,
            "splitting a shard that keeps overflowing"
        );
        let release = ShardCommand::Release(moved.iter().cloned().collect());
        self.send_to(shard, release);
        self.waiting.wait(shard, new, moved);
    }

    /// Whether the connection budget has room for another shard beside
    /// the open ones and those waiting to start; logs and counts a split of
    /// `shard` skipped if not.
    fn room_to_split(&self, shard: ShardId) -> bool {
        let open = self.open_connections() + self.waiting.len();
        if self.budget.has_room_for_shard(open) {
            return true;
        }
        self.context.counters.note_split_skipped();
        let max_connections = self.budget.max_connections();
        tracing::warn!(
            shard = shard.0,
            open,
            max_connections,
            "no room to split a shard"
        );
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    fn log() -> OverflowLog {
        OverflowLog::new(SplitRule::default())
    }

    #[test]
    fn a_second_incident_within_the_window_splits() {
        let mut log = log();
        let start = Instant::now();
        let first = log.note(ShardId(0), start);
        let second = log.note(ShardId(0), start + MINUTE * 9);
        assert_eq!((first, second), (false, true));
    }

    #[test]
    fn overflows_within_the_incident_gap_are_one_incident() {
        let mut log = log();
        let start = Instant::now();
        log.note(ShardId(0), start);
        let within = log.note(ShardId(0), start + Duration::from_secs(29));
        let after = log.note(ShardId(0), start + Duration::from_secs(30));
        assert_eq!((within, after), (false, true));
    }

    #[test]
    fn incidents_further_apart_than_the_window_do_not_split() {
        let mut log = log();
        let start = Instant::now();
        log.note(ShardId(0), start);
        assert!(!log.note(ShardId(0), start + MINUTE * 10));
    }

    #[test]
    fn incidents_of_different_shards_count_apart() {
        let mut log = log();
        let start = Instant::now();
        log.note(ShardId(0), start);
        assert!(!log.note(ShardId(1), start + MINUTE));
    }

    #[test]
    fn a_split_starts_the_count_over() {
        let mut log = log();
        let start = Instant::now();
        log.note(ShardId(0), start);
        log.note(ShardId(0), start + MINUTE);
        assert!(!log.note(ShardId(0), start + MINUTE * 2));
    }

    #[test]
    fn a_closed_shard_starts_its_count_over() {
        let mut log = log();
        let start = Instant::now();
        log.note(ShardId(0), start);
        log.forget(ShardId(0));
        assert!(!log.note(ShardId(0), start + MINUTE));
    }

    fn symbols(names: &[&str]) -> BTreeSet<Arc<str>> {
        names.iter().map(|&name| Arc::from(name)).collect()
    }

    #[test]
    fn a_waiting_shard_starts_with_the_changes_made_meanwhile() {
        let mut waiting = WaitingShards::default();
        waiting.wait(ShardId(0), ShardId(1), symbols(&["B", "D"]));
        let update = Update {
            removed: vec![Arc::from("B")],
            added: vec![Arc::from("X")],
        };
        let applied = [ShardId(1), ShardId(0)].map(|shard| waiting.update(shard, &update));
        let started = waiting.take(ShardId(0));
        let expected = Some((ShardId(1), symbols(&["D", "X"])));
        assert_eq!((applied, started), ([true, false], expected));
    }

    #[test]
    fn a_cancelled_shard_no_longer_waits() {
        let mut waiting = WaitingShards::default();
        waiting.wait(ShardId(0), ShardId(1), symbols(&["B"]));
        let cancelled = waiting.cancel(ShardId(1));
        let left = (waiting.holds(ShardId(1)), waiting.take(ShardId(0)));
        assert_eq!((cancelled, left), (true, (false, None)));
    }
}
