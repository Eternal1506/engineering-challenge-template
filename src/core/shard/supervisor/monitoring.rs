//! The supervisor's watch over its markets and books: the checks for
//! markets gone silent on the primary, the book audits, and what it reports
//! to the coordinator. Silent markets and audits wait while the primary is
//! down or reconnecting.

use std::sync::Arc;

use tokio::time::Instant;

use super::Supervisor;
use crate::core::events::{Decoded, FeedBody, FeedErrorKind, Inbound};
use crate::core::shard::ShardReport;
use crate::core::shard::stale::{DeadSocket, Evidence, Finding, Seen};
use crate::core::stats::FailoverCause;
use crate::core::ws::{ConnectionEvent, Request};

impl Supervisor {
    /// When the next silence check is due; never once the shard is closing.
    pub(super) fn stale_check_due(&self) -> Option<Instant> {
        (!self.replicas.is_cancelled()).then(|| self.stale.next_check())
    }

    /// When the next audit round is due; never if rounds audit nothing, or
    /// once the shard is closing.
    pub(super) fn audit_round_due(&self) -> Option<Instant> {
        let due = self.audits.next_round();
        due.filter(|_| !self.replicas.is_cancelled())
    }

    /// Checks the primary's markets for silence and acts on what it finds:
    /// resubscribes the silent ones, or treats the socket as dead.
    pub(super) fn on_stale_check(&mut self) {
        let now = Instant::now();
        if !self.primary_ready() {
            return self.stale.postpone(now);
        }
        let check = self.stale.check(now, self.evidence(now));
        self.activity.show(check.activity);
        match check.finding {
            Finding::Nothing => {}
            Finding::Resubscribe(symbols) => self.resubscribe(symbols),
            Finding::DeadSocket(dead) => self.treat_primary_as_dead(dead),
        }
    }

    /// Notes a level change standby `standby` delivered in `decoded`, which
    /// shows its market is not just quiet.
    pub(super) fn note_standby_frame(&mut self, standby: usize, decoded: &Decoded) {
        let Inbound::Feed(event) = &decoded.message else {
            return;
        };
        if matches!(event.body, FeedBody::Level(_)) {
            let seen = Seen::now(event.exchange_ts);
            self.stale.note_standby_change(&event.symbol, standby, seen);
        }
    }

    /// Asks the primary for snapshots of the next markets in the audit
    /// rotation, leaving out those held back, whose snapshot is on its way.
    pub(super) fn on_audit_round(&mut self) {
        let now = Instant::now();
        if !self.primary_ready() {
            return self.audits.postpone(now);
        }
        let held = |symbol: &str| self.resync.holds_back(symbol);
        let symbols = self.audits.round(&self.symbols, now, held);
        if symbols.is_empty() {
            return;
        }
        let markets = symbols.len();
        let tag = self.request_snapshot(symbols);
        self.audits.note_tag(tag);
        tracing::debug!(tag, markets, "auditing books");
    }

    /// Counts an overflow `event` of replica `replica` shows, and reports
    /// it to the coordinator if the replica is the primary: a standby's
    /// overflow comes from the same load, and does not make the shard
    /// overflow more.
    pub(super) fn note_overflow(&self, replica: usize, event: &ConnectionEvent) {
        if !is_overflow(event) {
            return;
        }
        self.counters.note_overflow();
        if self.replicas.is_primary(replica) {
            self.report(ShardReport::Overflowed(self.id));
        }
    }

    /// Tells the coordinator `report`.
    pub(super) fn report(&self, report: ShardReport) {
        if self.reports.send(report).is_err() {
            tracing::debug!(?report, "dropped a report for a coordinator that has gone");
        }
    }

    /// What it takes for a market to count as silent at `now`: a standby's
    /// sighting of a change the primary missed, while some warm standby
    /// delivered level changes over the health window; silence alone
    /// otherwise, as a standby that delivers nothing can tell nothing.
    fn evidence(&self, now: Instant) -> Evidence {
        let readings = self.replicas.readings(now);
        let mut standbys = readings.standbys.iter();
        if standbys.any(|reading| reading.level_changes > 0) {
            Evidence::Standby
        } else {
            Evidence::Silence
        }
    }

    /// Whether the primary's socket is up and is not being replaced, so its
    /// markets can be judged and asked for.
    fn primary_ready(&self) -> bool {
        let replaced = self.reconnect_requested || self.replicas.is_cancelled();
        self.replicas.primary_connected() && !replaced
    }

    /// Resubscribes `symbols` on the primary, a removal and then an add,
    /// holding them back until the add's snapshot.
    fn resubscribe(&mut self, symbols: Vec<Arc<str>>) {
        self.counters.note_resubscribes(symbols.len());
        self.resync.hold(&symbols);
        let (markets, first) = (symbols.len(), symbols.first().cloned());
        tracing::info!(markets, ?first, "resubscribing markets gone silent");
        let removal = Request::Remove(symbols.clone());
        self.replicas.request_primary(removal);
        self.replicas.request_primary(Request::Add(symbols));
    }

    /// So many busy markets went silent at once that the primary's socket
    /// is taken to be dead: the standby that saw them change takes over if
    /// it may, or else the primary reconnects.
    fn treat_primary_as_dead(&mut self, dead: DeadSocket) {
        self.counters.note_stale_escalation();
        let DeadSocket {
            silent,
            watched,
            witness,
        } = dead;
        tracing::warn!(
            silent,
            watched,
            witness,
            "too many busy markets went silent at once"
        );
        match witness.filter(|&standby| self.may_hand_over_to(standby)) {
            Some(standby) => self.hand_over(standby, FailoverCause::Stale),
            None => {
                tracing::warn!("no standby can take over, reconnecting the primary");
                self.ask_primary_to_reconnect();
            }
        }
    }

    /// Whether `standby` is warm and ready, and the shard is not cooling
    /// down from another takeover.
    fn may_hand_over_to(&self, standby: usize) -> bool {
        let now = Instant::now();
        let readings = self.replicas.readings(now);
        let mut warm = readings.standbys.iter();
        warm.any(|reading| reading.replica == standby) && self.health.may_take_over(now)
    }
}

/// Whether `event` is an overflow error.
fn is_overflow(event: &ConnectionEvent) -> bool {
    let ConnectionEvent::Frame { decoded, .. } = event else {
        return false;
    };
    let overflow = FeedErrorKind::Overflow;
    matches!(&decoded.message, Inbound::Error { kind, .. } if *kind == overflow)
}
