//! [`AuditRotation`]: which of a shard's books to audit next, and which
//! snapshot requests were audits.
//!
//! Every [`BookAudit::interval`], the shard asks its primary for fresh
//! snapshots of the next [`BookAudit::markets`] markets in ticker order,
//! wrapping around, so every book is audited in turn. The replies go to the
//! writer to be compared with its books rather than written.

use std::collections::{BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

/// How many recent audit requests' tags are remembered. Rounds are an
/// interval apart and replies come within the connection's reply timeout,
/// so only the latest one or two can still be answered.
const REMEMBERED_TAGS: usize = 4;

/// How often a shard audits its books, and how many at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookAudit {
    /// The time between two audit rounds.
    pub interval: Duration,
    /// How many markets each round audits; none if zero.
    pub markets: usize,
}

impl Default for BookAudit {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            markets: 5,
        }
    }
}

/// One shard's place in its audit rotation.
#[derive(Debug)]
pub(super) struct AuditRotation {
    audit: BookAudit,
    next_round: Instant,
    /// The last market audited, which the next round starts after.
    last: Option<Arc<str>>,
    /// The tags of the latest audit requests, oldest first.
    tags: VecDeque<u64>,
}

impl AuditRotation {
    /// A rotation on `audit` whose first round is an interval from now.
    pub(super) fn new(audit: BookAudit) -> Self {
        Self {
            next_round: Instant::now() + audit.interval,
            audit,
            last: None,
            tags: VecDeque::new(),
        }
    }

    /// When the next round is due; never if rounds audit no market.
    pub(super) fn next_round(&self) -> Option<Instant> {
        (self.audit.markets > 0).then_some(self.next_round)
    }

    /// Skips the round due, at `now`, while the primary cannot be asked.
    pub(super) fn postpone(&mut self, now: Instant) {
        self.next_round = now + self.audit.interval;
    }

    /// Schedules the next round an interval after `now` and returns the
    /// markets of `symbols` this one audits: the next ones after the last
    /// audited, wrapping around, leaving out those `skip` names.
    pub(super) fn round(
        &mut self,
        symbols: &BTreeSet<Arc<str>>,
        now: Instant,
        skip: impl Fn(&str) -> bool,
    ) -> Vec<Arc<str>> {
        self.postpone(now);
        let after = self.last.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
        let rest = symbols.range::<Arc<str>, _>((after, Bound::Unbounded));
        let wrapped = rest.chain(symbols).take(symbols.len());
        let unskipped = wrapped.filter(|symbol| !skip(symbol));
        let chosen: Vec<Arc<str>> = unskipped.take(self.audit.markets).cloned().collect();
        self.last = chosen.last().cloned().or(self.last.take());
        chosen
    }

    /// Remembers `tag` as an audit request's.
    pub(super) fn note_tag(&mut self, tag: u64) {
        if self.tags.len() == REMEMBERED_TAGS {
            self.tags.pop_front();
        }
        self.tags.push_back(tag);
    }

    /// Whether `tag` is a recent audit request's.
    pub(super) fn is_audit(&self, tag: u64) -> bool {
        self.tags.contains(&tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(names: &[&str]) -> BTreeSet<Arc<str>> {
        names.iter().map(|&name| Arc::from(name)).collect()
    }

    fn rotation(markets: usize) -> AuditRotation {
        AuditRotation::new(BookAudit {
            markets,
            ..BookAudit::default()
        })
    }

    fn names(chosen: &[Arc<str>]) -> Vec<&str> {
        chosen.iter().map(|symbol| &**symbol).collect()
    }

    #[test]
    fn rounds_walk_the_markets_in_order_and_wrap_around() {
        let mut rotation = rotation(2);
        let markets = symbols(&["A", "B", "C"]);
        let now = Instant::now();
        let first = rotation.round(&markets, now, |_| false);
        let second = rotation.round(&markets, now, |_| false);
        let rounds = (names(&first), names(&second));
        assert_eq!(rounds, (vec!["A", "B"], vec!["C", "A"]));
    }

    #[test]
    fn a_round_never_audits_a_market_twice() {
        let chosen = rotation(5).round(&symbols(&["A", "B"]), Instant::now(), |_| false);
        assert_eq!(names(&chosen), ["A", "B"]);
    }

    #[test]
    fn skipped_markets_are_left_out() {
        let markets = symbols(&["A", "B", "C"]);
        let chosen = rotation(2).round(&markets, Instant::now(), |symbol| symbol == "A");
        assert_eq!(names(&chosen), ["B", "C"]);
    }

    #[test]
    fn a_round_resumes_after_a_market_since_removed() {
        let mut rotation = rotation(1);
        let now = Instant::now();
        rotation.round(&symbols(&["A", "B", "C"]), now, |_| false);
        let chosen = rotation.round(&symbols(&["B", "C"]), now, |_| false);
        assert_eq!(names(&chosen), ["B"]);
    }

    #[test]
    fn a_round_schedules_the_next_an_interval_later() {
        let mut rotation = rotation(1);
        let now = Instant::now();
        rotation.round(&symbols(&["A"]), now, |_| false);
        assert_eq!(rotation.next_round(), Some(now + Duration::from_secs(60)));
    }

    #[test]
    fn rounds_of_no_markets_are_never_due() {
        assert_eq!(rotation(0).next_round(), None);
    }

    #[test]
    fn only_the_latest_audit_tags_are_remembered() {
        let mut rotation = rotation(1);
        (1..=5).for_each(|tag| rotation.note_tag(tag));
        let known = [1, 2, 5].map(|tag| rotation.is_audit(tag));
        assert_eq!(known, [false, true, true]);
    }
}
