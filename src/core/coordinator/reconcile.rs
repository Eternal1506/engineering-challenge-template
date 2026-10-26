//! [`Reconcile`]: one pass that brings the coordinator's markets in line
//! with the catalog, after lifecycle events may have been missed.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::time::Instant;

use super::markets::{MarketState, MarketStates, Verdict};

/// A reconcile in progress: when its walk of the catalog started, which
/// markets the walk listed, the markets the previous walk missed, and the
/// subscription changes made so far.
///
/// A lifecycle event that arrives after the walk started is fresher than
/// the walk, and so is the sweep opening a market then, so the walk's word
/// on that market is ignored.
#[derive(Debug)]
pub(super) struct Reconcile {
    started: Instant,
    seen: HashSet<Arc<str>>,
    previously_absent: HashSet<Arc<str>>,
    added: usize,
    removed: usize,
}

/// The markets a finished walk shows are gone, or may be.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Leftovers {
    /// Ended markets: closed, settled or past their close time. Forget
    /// them.
    pub(super) ended: Vec<Arc<str>>,
    /// Markets that should be trading but that this walk and the previous
    /// one both missed. Forget them.
    pub(super) missing: Vec<Arc<str>>,
    /// Markets that should be trading but that only this walk missed.
    /// Forget them if the next walk misses them too.
    pub(super) absent: HashSet<Arc<str>>,
}

impl Reconcile {
    /// A reconcile whose walk starts now, after a walk that missed
    /// `previously_absent`.
    pub(super) fn start(previously_absent: HashSet<Arc<str>>) -> Self {
        Self {
            started: Instant::now(),
            seen: HashSet::new(),
            previously_absent,
            added: 0,
            removed: 0,
        }
    }

    /// Notes that the walk listed `symbol`, whose state is `known`. True if
    /// the listing may decide the market's state: neither a lifecycle event
    /// nor the sweep has changed it since the walk started.
    pub(super) fn see(&mut self, symbol: &Arc<str>, known: Option<&MarketState>) -> bool {
        self.seen.insert(symbol.clone());
        known.is_none_or(|state| state.quiet_since(self.started))
    }

    /// Counts a subscription change the reconcile made.
    pub(super) fn tally(&mut self, verdict: Verdict) {
        match verdict {
            Verdict::Subscribe => self.added += 1,
            Verdict::Unsubscribe => self.removed += 1,
            Verdict::Keep => {}
        }
    }

    /// What the finished walk shows of `markets` at `now`, given the
    /// markets it could not parse. Only markets neither a lifecycle event
    /// nor the sweep changed since the walk started, and that the walk did
    /// not skip, are left over. Of those, an ended one has gone, and one
    /// that should be trading but is not listed has gone once two walks in
    /// a row missed it. A market waiting to open is expected to be missing.
    pub(super) fn leftovers(
        &self,
        markets: &MarketStates,
        skipped: &HashSet<Arc<str>>,
        now: DateTime<Utc>,
    ) -> Leftovers {
        let mut leftovers = Leftovers::default();
        let settled = |state: &MarketState| state.quiet_since(self.started);
        let candidates = markets
            .iter()
            .filter(|(symbol, state)| settled(state) && !skipped.contains(*symbol));
        for (symbol, state) in candidates {
            if state.has_ended(now) {
                leftovers.ended.push(symbol.clone());
            } else if !self.seen.contains(symbol) && !state.is_unopened() {
                self.note_missing(&mut leftovers, symbol);
            }
        }
        leftovers.ended.sort_unstable();
        leftovers.missing.sort_unstable();
        leftovers
    }

    /// Logs the reconcile's outcome, with the number of markets the walk
    /// skipped and of those it missed for the first time.
    pub(super) fn log_finished(&self, skipped: usize, absent: usize) {
        tracing::info!(
            added = self.added,
            removed = self.removed,
            skipped,
            absent_once = absent,
            duration_ms = self.started.elapsed().as_millis(),
            "reconciled the markets with the catalog"
        );
    }

    /// Logs a walk that failed, after which nothing was removed.
    pub(super) fn log_failed(&self, error: &anyhow::Error) {
        tracing::warn!(
            added = self.added,
            removed = self.removed,
            duration_ms = self.started.elapsed().as_millis(),
            error = format!("{error:#}"),
            "the reconcile walk failed, so it removed nothing"
        );
    }

    /// Notes a market that should be trading but that the walk missed.
    fn note_missing(&self, leftovers: &mut Leftovers, symbol: &Arc<str>) {
        if self.previously_absent.contains(symbol) {
            leftovers.missing.push(symbol.clone());
        } else {
            leftovers.absent.insert(symbol.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::core::coordinator::markets::Moment;
    use crate::core::events::LifecycleEvent;
    use crate::exchange::{MarketInfo, MarketStatus};

    fn symbol(name: &str) -> Arc<str> {
        name.into()
    }

    fn symbols(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|&name| symbol(name)).collect()
    }

    fn listed(name: &str, status: MarketStatus) -> MarketInfo {
        MarketInfo {
            symbol: symbol(name),
            status,
            open_ts: None,
            close_ts: None,
        }
    }

    /// States with `names` listed open.
    fn open_states(names: &[&str]) -> MarketStates {
        let mut states = MarketStates::default();
        for &name in names {
            states.on_listed(&listed(name, MarketStatus::Open), Utc::now());
        }
        states
    }

    /// A reconcile, after a walk that missed `absent`, that saw `names`
    /// listed in `states`.
    fn walked(states: &MarketStates, names: &[&str], absent: &[&str]) -> Reconcile {
        let previously_absent = symbols(absent).into_iter().collect();
        let mut reconcile = Reconcile::start(previously_absent);
        for &name in names {
            reconcile.see(&symbol(name), states.get(name));
        }
        reconcile
    }

    fn leftovers(reconcile: &Reconcile, states: &MarketStates) -> Leftovers {
        reconcile.leftovers(states, &HashSet::new(), Utc::now())
    }

    #[test]
    fn a_market_one_walk_misses_is_only_noted_absent() {
        let states = open_states(&["A", "B"]);
        let found = leftovers(&walked(&states, &["A"], &[]), &states);
        let expected = Leftovers {
            absent: HashSet::from([symbol("B")]),
            ..Leftovers::default()
        };
        assert_eq!(found, expected);
    }

    #[test]
    fn a_market_two_walks_in_a_row_miss_is_missing() {
        let states = open_states(&["A", "B"]);
        let found = leftovers(&walked(&states, &["A"], &["B"]), &states);
        assert_eq!(found.missing, symbols(&["B"]));
    }

    #[test]
    fn a_market_listed_again_is_no_longer_absent() {
        let states = open_states(&["A", "B"]);
        let found = leftovers(&walked(&states, &["A", "B"], &["B"]), &states);
        assert_eq!(found, Leftovers::default());
    }

    #[test]
    fn a_market_with_a_lifecycle_event_after_the_walk_started_stays() {
        let mut states = open_states(&["A", "B"]);
        let reconcile = walked(&states, &["A"], &["B"]);
        let event = LifecycleEvent::Deactivated {
            symbol: symbol("B"),
        };
        states.on_event(&event, Moment::now());
        assert_eq!(leftovers(&reconcile, &states), Leftovers::default());
    }

    #[test]
    fn a_skipped_market_stays() {
        let states = open_states(&["A", "B"]);
        let reconcile = walked(&states, &["A"], &["B"]);
        let skipped = HashSet::from([symbol("B")]);
        let found = reconcile.leftovers(&states, &skipped, Utc::now());
        assert_eq!(found, Leftovers::default());
    }

    #[test]
    fn a_market_waiting_to_open_stays() {
        let mut states = open_states(&["A"]);
        let at = Moment::now();
        let created = LifecycleEvent::Created {
            symbol: symbol("B"),
            open_ts: Some(at.wall + TimeDelta::hours(1)),
            close_ts: None,
        };
        states.on_event(&created, at);
        let reconcile = walked(&states, &["A"], &["B"]);
        assert_eq!(leftovers(&reconcile, &states), Leftovers::default());
    }

    #[test]
    fn an_ended_market_goes_after_one_walk_even_if_listed() {
        let mut states = open_states(&["A"]);
        let reconcile = walked(&states, &["A"], &[]);
        states.on_listed(&listed("A", MarketStatus::Closed), Utc::now());
        assert_eq!(leftovers(&reconcile, &states).ended, symbols(&["A"]));
    }

    #[test]
    fn a_market_past_its_close_time_goes_after_one_walk() {
        let mut states = MarketStates::default();
        let market = MarketInfo {
            close_ts: Some(Utc::now() - TimeDelta::minutes(1)),
            ..listed("A", MarketStatus::Open)
        };
        states.on_listed(&market, Utc::now());
        let reconcile = walked(&states, &[], &[]);
        assert_eq!(leftovers(&reconcile, &states).ended, symbols(&["A"]));
    }

    /// The walk may have passed the market's place in the listing before
    /// the market opened.
    #[test]
    fn a_market_the_sweep_opened_after_the_walk_started_stays() {
        let mut states = MarketStates::default();
        let at = Moment::now();
        let created = LifecycleEvent::Created {
            symbol: symbol("A"),
            open_ts: Some(at.wall + TimeDelta::minutes(1)),
            close_ts: None,
        };
        states.on_event(&created, at);
        let reconcile = walked(&states, &[], &["A"]);
        let opened = Moment {
            wall: at.wall + TimeDelta::minutes(2),
            instant: Instant::now(),
        };
        let swept = states.sweep(opened);
        let found = leftovers(&reconcile, &states);
        let expected = (symbols(&["A"]), Leftovers::default());
        assert_eq!((swept.opened, found), expected);
    }

    #[test]
    fn a_walk_cannot_decide_a_market_with_a_newer_lifecycle_event() {
        let mut states = MarketStates::default();
        let mut reconcile = Reconcile::start(HashSet::new());
        let event = LifecycleEvent::Settled {
            symbol: symbol("A"),
        };
        states.on_event(&event, Moment::now());
        assert!(!reconcile.see(&symbol("A"), states.get("A")));
    }
}
