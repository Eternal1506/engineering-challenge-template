//! [`MarketStates`]: what the coordinator knows of each market's trading
//! state, and the rules that turn lifecycle events, catalog listings and the
//! passing of time into subscription changes.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::time::Instant;

use crate::core::events::LifecycleEvent;
use crate::exchange::{MarketInfo, MarketStatus};

/// What the coordinator knows of one market. Its status is the exchange's:
/// passing the close time does not change it, but a market past its close
/// time is not recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MarketState {
    status: MarketStatus,
    open_ts: Option<DateTime<Utc>>,
    close_ts: Option<DateTime<Utc>>,
    /// When the market's state last changed other than by a catalog
    /// listing: by a lifecycle event, or by the sweep opening it at its
    /// open time, which the exchange passes without an event.
    last_change: Option<Instant>,
}

/// What a market's new state asks of its subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verdict {
    /// Record the market, if it is not recorded already.
    Subscribe,
    /// Stop recording the market, if it is recorded.
    Unsubscribe,
    /// Leave the subscription as it is.
    Keep,
}

/// When something happened: by the wall clock, which open and close times
/// are measured against, and by the monotonic clock, which orders changes
/// against a listing.
#[derive(Debug, Clone, Copy)]
pub(super) struct Moment {
    /// The wall-clock time.
    pub(super) wall: DateTime<Utc>,
    /// The monotonic time.
    pub(super) instant: Instant,
}

impl Moment {
    /// This moment.
    pub(super) fn now() -> Self {
        Self {
            wall: Utc::now(),
            instant: Instant::now(),
        }
    }
}

/// The markets a sweep found due.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Swept {
    /// Markets whose open time has just passed, to record.
    pub(super) opened: Vec<Arc<str>>,
    /// Open or paused markets past their close time, to stop recording if
    /// they are recorded.
    pub(super) closed: Vec<Arc<str>>,
}

impl MarketState {
    fn new(status: MarketStatus) -> Self {
        Self {
            status,
            open_ts: None,
            close_ts: None,
            last_change: None,
        }
    }

    /// The market's status, as the exchange last gave it.
    pub(super) fn status(&self) -> MarketStatus {
        self.status
    }

    /// When the market closes, if known.
    pub(super) fn close_ts(&self) -> Option<DateTime<Utc>> {
        self.close_ts
    }

    /// Whether the market is worth recording at `now`: open or paused, and
    /// not past its close time.
    fn is_wanted(&self, now: DateTime<Utc>) -> bool {
        self.is_trading() && !self.is_past_close(now)
    }

    fn verdict(&self, now: DateTime<Utc>) -> Verdict {
        if self.is_wanted(now) {
            Verdict::Subscribe
        } else {
            Verdict::Keep
        }
    }

    /// Whether the market has not opened yet.
    pub(super) fn is_unopened(&self) -> bool {
        self.status == MarketStatus::Unopened
    }

    /// Whether the market has stopped trading at `now`: closed, settled, or
    /// past its close time.
    pub(super) fn has_ended(&self, now: DateTime<Utc>) -> bool {
        let ended = matches!(self.status, MarketStatus::Closed | MarketStatus::Settled);
        ended || self.is_past_close(now)
    }

    /// Whether neither a lifecycle event nor the sweep changed the market
    /// at or after `start`.
    pub(super) fn quiet_since(&self, start: Instant) -> bool {
        self.last_change.is_none_or(|last| last < start)
    }

    fn is_trading(&self) -> bool {
        matches!(self.status, MarketStatus::Open | MarketStatus::Paused)
    }

    fn is_past_close(&self, now: DateTime<Utc>) -> bool {
        self.close_ts.is_some_and(|close| close <= now)
    }

    fn opens_by(&self, now: DateTime<Utc>) -> bool {
        self.is_unopened() && self.open_ts.is_some_and(|open| open <= now)
    }
}

/// Every market the coordinator knows of, recorded or not: markets waiting
/// to open, open and paused ones, and ended ones kept until a reconcile
/// shows nothing more is coming for them.
#[derive(Debug, Default)]
pub(super) struct MarketStates {
    states: HashMap<Arc<str>, MarketState>,
}

impl MarketStates {
    /// Applies a lifecycle event that arrived at `at`.
    ///
    /// - `Created` registers the market, open if its open time has passed.
    /// - `Activated` opens the market, registering it if unknown.
    /// - `Deactivated` pauses an open market, which stays recorded.
    /// - `CloseChanged` moves the close time, which records the market
    ///   again if it was past its old one, with or without an `Activated`.
    /// - `Determined` and `Settled` end the market for good.
    ///
    /// `Deactivated` and `CloseChanged` for an unknown market are ignored;
    /// the next reconcile finds it if it trades.
    pub(super) fn on_event(&mut self, event: &LifecycleEvent, at: Moment) -> Verdict {
        match event {
            LifecycleEvent::Created {
                symbol,
                open_ts,
                close_ts,
            } => self.create(symbol, (*open_ts, *close_ts), at),
            LifecycleEvent::Activated { symbol } => self.activate(symbol, at),
            LifecycleEvent::Deactivated { symbol } => self.touch(symbol, at, pause),
            LifecycleEvent::CloseChanged { symbol, close_ts } => {
                self.touch(symbol, at, |state| state.close_ts = Some(*close_ts))
            }
            LifecycleEvent::Determined { symbol } | LifecycleEvent::Settled { symbol } => {
                self.settle(symbol, at)
            }
        }
    }

    /// Applies a catalog listing of `market` at `now`. Its own status
    /// decides: an open or paused market is registered, or refreshed, and
    /// recorded unless past its close time; a known market listed as closed
    /// or settled ends. A market already settled stays settled, and others
    /// are ignored.
    pub(super) fn on_listed(&mut self, market: &MarketInfo, now: DateTime<Utc>) -> Verdict {
        let known = self.states.get(&market.symbol);
        if known.is_some_and(|state| state.status == MarketStatus::Settled) {
            return Verdict::Keep;
        }
        match market.status {
            MarketStatus::Open | MarketStatus::Paused => self.refresh(market, now),
            MarketStatus::Closed | MarketStatus::Settled => {
                let Some(state) = self.states.get_mut(&market.symbol) else {
                    return Verdict::Keep;
                };
                state.status = market.status;
                Verdict::Unsubscribe
            }
            MarketStatus::Unopened => Verdict::Keep,
        }
    }

    /// Opens the markets whose open time has passed by `at`, noting the
    /// change at `at`, and finds the open or paused ones past their close
    /// time.
    pub(super) fn sweep(&mut self, at: Moment) -> Swept {
        let mut swept = Swept::default();
        for (symbol, state) in &mut self.states {
            if state.opens_by(at.wall) {
                state.status = MarketStatus::Open;
                state.last_change = Some(at.instant);
                if state.is_wanted(at.wall) {
                    swept.opened.push(symbol.clone());
                }
            }
            if state.is_trading() && state.is_past_close(at.wall) {
                swept.closed.push(symbol.clone());
            }
        }
        swept.opened.sort_unstable();
        swept.closed.sort_unstable();
        swept
    }

    /// Fills in a known market's open and close times from a listing of
    /// it, where the market has none, as when it was first seen through an
    /// `Activated`.
    pub(super) fn fill_times(&mut self, market: &MarketInfo) {
        if let Some(state) = self.states.get_mut(&market.symbol) {
            state.open_ts = state.open_ts.or(market.open_ts);
            state.close_ts = state.close_ts.or(market.close_ts);
        }
    }

    /// What is known of `symbol`.
    pub(super) fn get(&self, symbol: &str) -> Option<&MarketState> {
        self.states.get(symbol)
    }

    /// Every known market.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &MarketState)> {
        self.states.iter()
    }

    /// Forgets `symbol`.
    pub(super) fn remove(&mut self, symbol: &str) {
        self.states.remove(symbol);
    }

    /// How many markets are known.
    pub(super) fn len(&self) -> usize {
        self.states.len()
    }

    fn entry(&mut self, symbol: &Arc<str>, status: MarketStatus) -> &mut MarketState {
        let entry = self.states.entry(symbol.clone());
        entry.or_insert_with(|| MarketState::new(status))
    }

    /// Registers or refreshes a market listed as open or paused.
    fn refresh(&mut self, market: &MarketInfo, now: DateTime<Utc>) -> Verdict {
        let state = self.entry(&market.symbol, market.status);
        state.status = market.status;
        (state.open_ts, state.close_ts) = (market.open_ts, market.close_ts);
        state.verdict(now)
    }

    fn create(&mut self, symbol: &Arc<str>, times: TimeRange, at: Moment) -> Verdict {
        let (open_ts, close_ts) = times;
        let opened = open_ts.is_none_or(|open| open <= at.wall);
        let status = if opened {
            MarketStatus::Open
        } else {
            MarketStatus::Unopened
        };
        let state = MarketState {
            status,
            open_ts,
            close_ts,
            last_change: Some(at.instant),
        };
        let verdict = state.verdict(at.wall);
        self.states.insert(symbol.clone(), state);
        verdict
    }

    fn activate(&mut self, symbol: &Arc<str>, at: Moment) -> Verdict {
        let state = self.entry(symbol, MarketStatus::Open);
        state.status = MarketStatus::Open;
        state.last_change = Some(at.instant);
        state.verdict(at.wall)
    }

    /// Applies `change` to a known market and returns what its new state
    /// asks for; ignores an unknown one.
    fn touch(
        &mut self,
        symbol: &str,
        at: Moment,
        change: impl FnOnce(&mut MarketState),
    ) -> Verdict {
        let Some(state) = self.states.get_mut(symbol) else {
            return Verdict::Keep;
        };
        change(state);
        state.last_change = Some(at.instant);
        state.verdict(at.wall)
    }

    /// Ends the market for good. An unknown market is still noted, so a
    /// listing that saw it before it ended does not record it.
    fn settle(&mut self, symbol: &Arc<str>, at: Moment) -> Verdict {
        let state = self.entry(symbol, MarketStatus::Settled);
        state.status = MarketStatus::Settled;
        state.last_change = Some(at.instant);
        Verdict::Unsubscribe
    }
}

/// A market's open and close times.
type TimeRange = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

fn pause(state: &mut MarketState) {
    if state.status == MarketStatus::Open {
        state.status = MarketStatus::Paused;
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    fn moment() -> Moment {
        Moment::now()
    }

    /// `count` minutes after `at` by the wall clock, a moment later by the
    /// monotonic clock.
    fn after(at: Moment, count: i64) -> Moment {
        Moment {
            wall: minutes(at.wall, count),
            instant: at.instant + std::time::Duration::from_millis(1),
        }
    }

    fn minutes(from: DateTime<Utc>, count: i64) -> DateTime<Utc> {
        from + TimeDelta::minutes(count)
    }

    fn symbol(name: &str) -> Arc<str> {
        name.into()
    }

    fn created(name: &str, at: Moment, open_in: i64, close_in: i64) -> LifecycleEvent {
        LifecycleEvent::Created {
            symbol: symbol(name),
            open_ts: Some(minutes(at.wall, open_in)),
            close_ts: Some(minutes(at.wall, close_in)),
        }
    }

    fn activated(name: &str) -> LifecycleEvent {
        LifecycleEvent::Activated {
            symbol: symbol(name),
        }
    }

    fn deactivated(name: &str) -> LifecycleEvent {
        LifecycleEvent::Deactivated {
            symbol: symbol(name),
        }
    }

    fn close_changed(name: &str, close_ts: DateTime<Utc>) -> LifecycleEvent {
        LifecycleEvent::CloseChanged {
            symbol: symbol(name),
            close_ts,
        }
    }

    fn listed(name: &str, status: MarketStatus, close_ts: Option<DateTime<Utc>>) -> MarketInfo {
        MarketInfo {
            symbol: symbol(name),
            status,
            open_ts: None,
            close_ts,
        }
    }

    /// States with `name` open, closing in an hour, from a lifecycle event.
    fn with_open(name: &str, at: Moment) -> MarketStates {
        let mut states = MarketStates::default();
        states.on_event(&created(name, at, -1, 60), at);
        states
    }

    fn status(states: &MarketStates, name: &str) -> Option<MarketStatus> {
        states.get(name).map(MarketState::status)
    }

    #[test]
    fn a_created_market_already_open_is_subscribed() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_event(&created("A", at, -1, 60), at);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Subscribe, Some(MarketStatus::Open))
        );
    }

    #[test]
    fn a_created_market_without_an_open_time_is_subscribed() {
        let at = moment();
        let event = LifecycleEvent::Created {
            symbol: symbol("A"),
            open_ts: None,
            close_ts: None,
        };
        let verdict = MarketStates::default().on_event(&event, at);
        assert_eq!(verdict, Verdict::Subscribe);
    }

    #[test]
    fn a_created_market_opening_later_waits() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_event(&created("A", at, 5, 60), at);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Keep, Some(MarketStatus::Unopened))
        );
    }

    #[test]
    fn the_sweep_opens_a_market_once_its_open_time_passes() {
        let at = moment();
        let mut states = MarketStates::default();
        states.on_event(&created("A", at, 5, 60), at);
        let early = states.sweep(after(at, 4));
        let due = states.sweep(after(at, 5));
        assert_eq!(
            (early.opened, due.opened),
            (Vec::<Arc<str>>::new(), vec![symbol("A")])
        );
    }

    #[test]
    fn the_sweep_opens_a_market_only_once() {
        let at = moment();
        let mut states = MarketStates::default();
        states.on_event(&created("A", at, 5, 60), at);
        states.sweep(after(at, 5));
        assert!(states.sweep(after(after(at, 5), 1)).opened.is_empty());
    }

    #[test]
    fn the_sweep_finds_a_market_past_its_close_time_and_keeps_its_status() {
        let at = moment();
        let mut states = with_open("A", at);
        let early = states.sweep(after(at, 59));
        let due = states.sweep(after(at, 60));
        assert_eq!(
            (early.closed.len(), due.closed, status(&states, "A")),
            (0, vec![symbol("A")], Some(MarketStatus::Open))
        );
    }

    #[test]
    fn activated_opens_an_unknown_market() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_event(&activated("A"), at);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Subscribe, Some(MarketStatus::Open))
        );
    }

    #[test]
    fn deactivated_pauses_a_market_and_keeps_it_recorded() {
        let at = moment();
        let mut states = with_open("A", at);
        let verdict = states.on_event(&deactivated("A"), at);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Subscribe, Some(MarketStatus::Paused))
        );
    }

    #[test]
    fn activated_resumes_a_paused_market() {
        let at = moment();
        let mut states = with_open("A", at);
        states.on_event(&deactivated("A"), at);
        let verdict = states.on_event(&activated("A"), at);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Subscribe, Some(MarketStatus::Open))
        );
    }

    #[test]
    fn deactivated_ignores_an_unknown_market() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_event(&deactivated("A"), at);
        assert_eq!((verdict, states.len()), (Verdict::Keep, 0));
    }

    #[test]
    fn close_changed_ignores_an_unknown_market() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_event(&close_changed("A", minutes(at.wall, 60)), at);
        assert_eq!((verdict, states.len()), (Verdict::Keep, 0));
    }

    #[test]
    fn a_later_close_alone_records_a_market_past_its_old_close_again() {
        let at = moment();
        let mut states = with_open("A", at);
        let later = after(at, 61);
        states.sweep(later);
        let moved = close_changed("A", minutes(at.wall, 120));
        assert_eq!(states.on_event(&moved, later), Verdict::Subscribe);
    }

    #[test]
    fn activated_before_a_later_close_records_the_market_again() {
        let at = moment();
        let mut states = with_open("A", at);
        let later = after(at, 61);
        states.sweep(later);
        let after_activated = states.on_event(&activated("A"), later);
        let moved = close_changed("A", minutes(at.wall, 120));
        let after_move = states.on_event(&moved, later);
        assert_eq!(
            (after_activated, after_move),
            (Verdict::Keep, Verdict::Subscribe)
        );
    }

    #[test]
    fn an_earlier_close_is_swept() {
        let at = moment();
        let mut states = with_open("A", at);
        states.on_event(&close_changed("A", minutes(at.wall, 1)), at);
        assert_eq!(states.sweep(after(at, 2)).closed, vec![symbol("A")]);
    }

    #[test]
    fn determined_and_settled_end_a_market() {
        let at = moment();
        let mut states = with_open("A", at);
        states.on_event(&created("B", at, -1, 60), at);
        let determined = LifecycleEvent::Determined {
            symbol: symbol("A"),
        };
        let determined = states.on_event(&determined, at);
        let settled = LifecycleEvent::Settled {
            symbol: symbol("B"),
        };
        let settled = states.on_event(&settled, at);
        assert_eq!(
            (determined, settled, status(&states, "A")),
            (
                Verdict::Unsubscribe,
                Verdict::Unsubscribe,
                Some(MarketStatus::Settled)
            )
        );
    }

    #[test]
    fn a_listing_does_not_revive_a_settled_market() {
        let at = moment();
        let mut states = with_open("A", at);
        let determined = LifecycleEvent::Determined {
            symbol: symbol("A"),
        };
        states.on_event(&determined, at);
        let verdict = states.on_listed(&listed("A", MarketStatus::Open, None), at.wall);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Keep, Some(MarketStatus::Settled))
        );
    }

    #[test]
    fn a_listed_open_market_is_subscribed() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_listed(&listed("A", MarketStatus::Paused, None), at.wall);
        assert_eq!(
            (verdict, status(&states, "A")),
            (Verdict::Subscribe, Some(MarketStatus::Paused))
        );
    }

    #[test]
    fn a_listed_market_past_its_close_time_is_not_subscribed() {
        let at = moment();
        let market = listed("A", MarketStatus::Open, Some(minutes(at.wall, -1)));
        let verdict = MarketStates::default().on_listed(&market, at.wall);
        assert_eq!(verdict, Verdict::Keep);
    }

    #[test]
    fn a_known_market_listed_as_closed_is_unsubscribed() {
        let at = moment();
        let mut states = with_open("A", at);
        let verdict = states.on_listed(&listed("A", MarketStatus::Closed, None), at.wall);
        assert_eq!(verdict, Verdict::Unsubscribe);
    }

    #[test]
    fn an_unknown_market_listed_as_settled_is_ignored() {
        let at = moment();
        let mut states = MarketStates::default();
        let verdict = states.on_listed(&listed("A", MarketStatus::Settled, None), at.wall);
        assert_eq!((verdict, states.len()), (Verdict::Keep, 0));
    }

    #[test]
    fn a_listing_fills_in_times_a_market_lacks_and_keeps_those_it_has() {
        let at = moment();
        let mut states = with_open("A", at);
        states.on_event(&activated("B"), at);
        let listed_close = Some(minutes(at.wall, 5));
        for name in ["A", "B"] {
            states.fill_times(&listed(name, MarketStatus::Open, listed_close));
        }
        let close = |name| states.get(name).and_then(MarketState::close_ts);
        assert_eq!(
            (close("A"), close("B")),
            (Some(minutes(at.wall, 60)), listed_close)
        );
    }

    #[test]
    fn a_market_past_its_close_time_has_ended() {
        let at = moment();
        let states = with_open("A", at);
        let state = states.get("A").unwrap();
        assert_eq!(
            (
                state.has_ended(minutes(at.wall, 59)),
                state.has_ended(minutes(at.wall, 60))
            ),
            (false, true)
        );
    }

    #[test]
    fn a_market_is_quiet_since_a_time_after_its_last_change() {
        let at = moment();
        let states = with_open("A", at);
        let state = states.get("A").unwrap();
        let later = at.instant + std::time::Duration::from_millis(1);
        assert_eq!(
            (state.quiet_since(at.instant), state.quiet_since(later)),
            (false, true)
        );
    }
}
