//! [`ReplicaSet`]: a shard's replicas, which of them is the primary, when
//! the standbys start, which standby may take over and when, the readings
//! the health checks compare, and fair intake of their events.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;

use super::health::{Reading, Readings};
use super::replica::{Replica, ReplicaFactory, ReplicaHealth};
use super::{FailoverTimings, ShardPlan};
use crate::core::stats::ReplicaRole;
use crate::core::ws::{ConnectionEvent, Request};

/// What one of a shard's replicas delivered.
pub(super) enum ReplicaInput {
    /// An event from replica `replica`'s connection.
    Event {
        replica: usize,
        event: ConnectionEvent,
    },
    /// Replica `replica`'s connection closed its event channel: its task
    /// ended.
    Ended {
        replica: usize,
        /// Whether its socket was up until then.
        was_connected: bool,
    },
}

/// A shard's connections. Exactly one, the primary, feeds the writer; the
/// others are standbys, subscribed to the same symbols.
pub(super) struct ReplicaSet {
    replicas: Vec<Replica>,
    primary: usize,
    /// When the primary last changed.
    promoted_at: Instant,
    /// The replica whose events are polled first next time.
    next_poll: usize,
    /// How many replicas the shard runs once its standbys have started.
    wanted: usize,
    timings: FailoverTimings,
    /// When the standbys start, once the primary has first connected.
    standbys_due: Option<Instant>,
    /// While the primary is down and a standby is warming up, when the
    /// first one will be warm enough to take over.
    promotion_due: Option<Instant>,
    factory: ReplicaFactory,
}

impl ReplicaSet {
    /// Starts the primary of `plan` with `factory`; the standbys start
    /// [`FailoverTimings::standby_delay`] after it first connects.
    pub(super) fn start(
        factory: ReplicaFactory,
        plan: &ShardPlan,
        timings: FailoverTimings,
    ) -> Self {
        Self {
            replicas: vec![factory.start(0, plan.symbols.clone())],
            primary: 0,
            promoted_at: Instant::now(),
            next_poll: 0,
            wanted: plan.replicas.get(),
            timings,
            standbys_due: None,
            promotion_due: None,
            factory,
        }
    }

    /// The primary's index.
    pub(super) fn primary(&self) -> usize {
        self.primary
    }

    /// Whether replica `index` is the primary.
    pub(super) fn is_primary(&self, index: usize) -> bool {
        index == self.primary
    }

    /// The part replica `index` plays.
    pub(super) fn role(&self, index: usize) -> ReplicaRole {
        if self.is_primary(index) {
            ReplicaRole::Primary
        } else {
            ReplicaRole::Standby
        }
    }

    /// Whether the primary's socket is up.
    pub(super) fn primary_connected(&self) -> bool {
        self.replicas[self.primary].is_connected()
    }

    /// What the supervisor has seen of replica `index`.
    pub(super) fn health(&self, index: usize) -> &ReplicaHealth {
        self.replicas[index].health()
    }

    /// Passes `request` to the primary.
    pub(super) fn request_primary(&self, request: Request) {
        self.request(self.primary, request);
    }

    /// Passes `request` to replica `index`.
    pub(super) fn request(&self, index: usize, request: Request) {
        self.replicas[index].request(request);
    }

    /// Passes `request` to every replica.
    pub(super) fn request_all(&self, request: &Request) {
        for replica in &self.replicas {
            replica.request(request.clone());
        }
    }

    /// Notes what `event` from replica `index` says about its health.
    pub(super) fn observe(&mut self, index: usize, event: &ConnectionEvent) {
        self.replicas[index].observe(event);
    }

    /// Whether any standby's connection is still running.
    pub(super) fn has_live_standby(&self) -> bool {
        let mut replicas = self.replicas.iter().enumerate();
        replicas.any(|(index, replica)| index != self.primary && !replica.is_ended())
    }

    /// The primary connected: the standbys, if not yet started or due,
    /// become due after the standby delay.
    pub(super) fn schedule_standbys(&mut self) {
        if self.replicas.len() < self.wanted && self.standbys_due.is_none() {
            self.standbys_due = Some(Instant::now() + self.timings.standby_delay);
        }
    }

    /// When the standbys are due to start, if they are.
    pub(super) fn standbys_due(&self) -> Option<Instant> {
        self.standbys_due
    }

    /// Starts the standbys, subscribing `symbols`, unless the shard is
    /// closing. Returns how many started.
    pub(super) fn start_standbys(&mut self, symbols: &BTreeSet<Arc<str>>) -> usize {
        self.standbys_due = None;
        if self.factory.is_cancelled() {
            return 0;
        }
        let first = self.replicas.len();
        for index in first..self.wanted {
            let standby = self.factory.start(index, symbols.clone());
            self.replicas.push(standby);
        }
        self.wanted - first
    }

    /// The standby connected the longest, if it has been connected for at
    /// least the warmup.
    pub(super) fn eligible_standby(&self) -> Option<usize> {
        let warm = self.warm_standbys(Instant::now());
        let (longest, _) = warm.max_by_key(|&(_, uptime)| uptime)?;
        Some(longest)
    }

    /// Each standby connected for at least the warmup at `now`, and not
    /// asked to reconnect, with how long it has been connected.
    fn warm_standbys(&self, now: Instant) -> impl Iterator<Item = (usize, Duration)> + '_ {
        let uptime = move |(index, replica): (usize, &Replica)| Some((index, replica.uptime(now)?));
        let uptimes = self.ready_standbys().filter_map(uptime);
        uptimes.filter(|&(_, uptime)| uptime >= self.timings.warmup)
    }

    /// Each standby whose socket is up and not asked to reconnect.
    fn ready_standbys(&self) -> impl Iterator<Item = (usize, &Replica)> {
        let replicas = self.replicas.iter().enumerate();
        let standbys = replicas.filter(move |&(index, _)| index != self.primary);
        standbys.filter(|(_, replica)| replica.is_ready())
    }

    /// Asks replica `index` to drop its socket and reconnect. It may not
    /// take over until its new socket connects.
    pub(super) fn reconnect(&mut self, index: usize) {
        self.replicas[index].reconnect();
    }

    /// The connected primary's recent traffic at `now`, and each warm
    /// standby's.
    pub(super) fn readings(&self, now: Instant) -> Readings {
        let standbys = self.warm_standbys(now);
        let readings = standbys.filter_map(|(index, _)| self.reading(index, now));
        Readings {
            primary: self.reading(self.primary, now),
            standbys: readings.collect(),
        }
    }

    /// Replica `index`'s recent traffic at `now`, while it is connected.
    fn reading(&self, index: usize, now: Instant) -> Option<Reading> {
        let replica = &self.replicas[index];
        let health = replica.health();
        Some(Reading {
            replica: index,
            uptime: replica.uptime(now)?,
            level_changes: health.recent_level_changes(now),
            median_lag_ms: health.median_lag_ms(now),
        })
    }

    /// Sets the promotion due when the first connected standby not asked to
    /// reconnect finishes its warmup; false if there is no such standby.
    pub(super) fn arm_promotion(&mut self) -> bool {
        let standbys = self.ready_standbys();
        let since = standbys.filter_map(|(_, replica)| replica.connected_since());
        let due = since.min().map(|since| since + self.timings.warmup);
        self.promotion_due = due;
        due.is_some()
    }

    /// Sets the promotion due once the reconnect grace has passed: the
    /// primary is reconnecting as the shard asked, and a standby takes over
    /// only if it is not back by then.
    pub(super) fn arm_reconnect_grace(&mut self) {
        self.promotion_due = Some(Instant::now() + self.timings.reconnect_grace);
    }

    /// No promotion is waited for any more.
    pub(super) fn disarm_promotion(&mut self) {
        self.promotion_due = None;
    }

    /// When a warming standby may take over from a primary that is down.
    pub(super) fn promotion_due(&self) -> Option<Instant> {
        self.promotion_due
    }

    /// Makes replica `index` the primary and the old primary a standby;
    /// returns the old primary's index.
    pub(super) fn promote(&mut self, index: usize) -> usize {
        self.promoted_at = Instant::now();
        self.promotion_due = None;
        std::mem::replace(&mut self.primary, index)
    }

    /// How long the primary's socket has been the primary: since it
    /// connected or since its replica was promoted, whichever is later;
    /// zero while it is down.
    pub(super) fn primary_for(&self) -> Duration {
        let now = Instant::now();
        let uptime = self.replicas[self.primary].uptime(now);
        uptime.map_or(Duration::ZERO, |up| up.min(now - self.promoted_at))
    }

    /// The next event or ended connection of any replica, polling each in
    /// turn from where the last poll left off; `None` once every
    /// connection's end has been reported.
    pub(super) async fn next_input(&mut self) -> Option<ReplicaInput> {
        std::future::poll_fn(|context| self.poll_input(context)).await
    }

    fn poll_input(&mut self, context: &mut Context<'_>) -> Poll<Option<ReplicaInput>> {
        let count = self.replicas.len();
        let mut open = false;
        for offset in 0..count {
            let index = (self.next_poll + offset) % count;
            let input = match self.poll_replica(index, context) {
                Poll::Ready(input) => input,
                Poll::Pending => {
                    open |= !self.replicas[index].is_ended();
                    continue;
                }
            };
            self.next_poll = (index + 1) % count;
            return Poll::Ready(Some(input));
        }
        if open {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }

    /// Replica `index`'s next event, or its end the first time its channel
    /// is found closed; pending for a replica whose end was reported.
    fn poll_replica(&mut self, index: usize, context: &mut Context<'_>) -> Poll<ReplicaInput> {
        let replica = &mut self.replicas[index];
        if replica.is_ended() {
            return Poll::Pending;
        }
        match replica.events().poll_recv(context) {
            Poll::Ready(Some(event)) => Poll::Ready(ReplicaInput::Event {
                replica: index,
                event,
            }),
            Poll::Ready(None) => Poll::Ready(ReplicaInput::Ended {
                replica: index,
                was_connected: replica.end(),
            }),
            Poll::Pending => Poll::Pending,
        }
    }

    /// How many events each replica has queued, by index.
    pub(super) fn queued(&self) -> Vec<usize> {
        let lengths = self.replicas.iter().map(Replica::queued);
        lengths.collect()
    }

    /// Replica `index`'s next queued event, if it has one.
    pub(super) fn try_next(&mut self, index: usize) -> Option<ConnectionEvent> {
        self.replicas[index].events().try_recv().ok()
    }

    /// Shuts every replica down.
    pub(super) fn cancel_all(&self) {
        self.factory.cancel_all();
    }

    /// Whether the shard is shutting down.
    pub(super) fn is_cancelled(&self) -> bool {
        self.factory.is_cancelled()
    }

    /// The role of each replica whose socket is up.
    pub(super) fn connected_roles(&self) -> Vec<ReplicaRole> {
        let replicas = self.replicas.iter().enumerate();
        let connected = replicas.filter(|(_, replica)| replica.is_connected());
        connected.map(|(index, _)| self.role(index)).collect()
    }

    /// How many replicas have started.
    pub(super) fn started_count(&self) -> usize {
        self.replicas.len()
    }

    /// Waits for every connection task to end.
    pub(super) async fn join(self) {
        for replica in self.replicas {
            replica.join().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    /// A pair of detached replicas, both connected, that may take over at
    /// once.
    fn connected_pair() -> ReplicaSet {
        let plan = ShardPlan {
            symbols: BTreeSet::new(),
            replicas: NonZeroUsize::new(2).unwrap(),
        };
        let timings = FailoverTimings {
            warmup: Duration::ZERO,
            ..FailoverTimings::default()
        };
        let mut replicas = ReplicaSet::start(ReplicaFactory::detached(), &plan, timings);
        replicas.start_standbys(&BTreeSet::new());
        replicas.observe(0, &ConnectionEvent::Connected);
        replicas.observe(1, &ConnectionEvent::Connected);
        replicas
    }

    /// Replica 0 hands over to 1 and is asked to reconnect; then 1 is lost
    /// before 0's socket goes. 0 may not take over until it connects again.
    #[tokio::test]
    async fn a_replica_asked_to_reconnect_takes_over_only_once_connected_again() {
        let mut replicas = connected_pair();
        let demoted = replicas.promote(1);
        replicas.reconnect(demoted);
        let lost = ConnectionEvent::Disconnected {
            reason: "lost".to_owned(),
        };
        replicas.observe(1, &lost);
        let before = (replicas.eligible_standby(), replicas.arm_promotion());
        replicas.observe(0, &lost);
        replicas.observe(0, &ConnectionEvent::Connected);
        let after = replicas.eligible_standby();
        assert_eq!((before, after), ((None, false), Some(0)));
    }
}
