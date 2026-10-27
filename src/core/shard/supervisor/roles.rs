//! The supervisor's side of its replicas' roles: the primary connecting or
//! lost, standbys connecting and taking over, and connection tasks that
//! end.
//!
//! A primary whose task ends outside shutdown is covered by inspection
//! only: the mock exchange has no way to end a connection's task.

use super::Supervisor;
use crate::core::stats::{FailoverCause, ReplicaRole};

impl Supervisor {
    /// A new primary socket's first message for each symbol is its
    /// subscribe snapshot, so every symbol is released. The standbys start
    /// after the primary first connects.
    pub(super) fn on_connected(&mut self) {
        let released = self.resync.clear();
        self.reconnect_requested = false;
        self.replicas.disarm_promotion();
        self.counters.note_connected(ReplicaRole::Primary);
        self.replicas.schedule_standbys();
        let replica = self.replicas.primary();
        tracing::info!(replica, released, "the primary connected");
    }

    /// The primary's socket was lost and the resync deadline no longer
    /// applies. A primary the shard asked to reconnect resubscribes, which
    /// is what it was asked for, and is replaced only if it is not back
    /// within the reconnect grace; any other is replaced if a standby can.
    pub(super) fn on_primary_lost(&mut self, reason: &str) {
        self.counters.note_disconnected(ReplicaRole::Primary);
        self.resync.stop_clock();
        if self.replicas.is_cancelled() {
            return;
        }
        if std::mem::take(&mut self.reconnect_requested) {
            self.replicas.arm_reconnect_grace();
            tracing::info!(reason, "the primary is reconnecting as asked");
        } else if !self.replace_primary(reason) {
            tracing::info!(reason, "disconnected, reconnecting");
        }
    }

    /// The primary is down. A warm standby takes over at once; failing
    /// that, the first connected standby takes over once warm, unless the
    /// primary is back by then. False if neither: no standby is connected.
    fn replace_primary(&mut self, reason: &str) -> bool {
        if let Some(standby) = self.replicas.eligible_standby() {
            self.fail_over(standby, reason);
            return true;
        }
        let armed = self.replicas.arm_promotion();
        if armed {
            tracing::info!(reason, "the primary is down, a standby is warming up");
        }
        armed
    }

    /// A standby has warmed up, or the reconnect grace has run out, while
    /// the primary is down.
    pub(super) fn on_promotion_due(&mut self) {
        self.replicas.disarm_promotion();
        let back = self.replicas.primary_connected();
        if back || self.replicas.is_cancelled() {
            return;
        }
        if !self.replace_primary("the primary stayed down") {
            tracing::info!("the primary is still down, waiting for it or a standby");
        }
    }

    /// Makes replica `replica` the primary and returns the old primary. A
    /// reconnect asked of the old primary no longer concerns the shard.
    fn promote(&mut self, replica: usize) -> usize {
        self.reconnect_requested = false;
        self.replicas.promote(replica)
    }

    /// Makes the connected `standby` the primary and resyncs every symbol
    /// on it. The old primary, already reconnecting, becomes a standby.
    fn fail_over(&mut self, standby: usize, reason: &str) {
        let from = self.promote(standby);
        self.counters.note_promoted(FailoverCause::Disconnect);
        let tag = self.resync_all();
        let health = self.replicas.health(standby);
        let (events, gaps) = (health.feed_events(), health.gaps());
        tracing::warn!(
            reason,
            from,
            to = standby,
            tag,
            events,
            gaps,
            "the primary is down, failing over to a standby"
        );
    }

    /// A standby that connects while the primary is down becomes the
    /// primary, its new socket releasing every symbol.
    pub(super) fn on_standby_connected(&mut self, replica: usize) {
        if self.replicas.primary_connected() {
            self.counters.note_connected(ReplicaRole::Standby);
            tracing::info!(replica, "a standby connected");
            return;
        }
        let from = self.promote(replica);
        self.counters.note_failover(FailoverCause::Disconnect);
        tracing::warn!(from, to = replica, "a standby connected first, promoted");
        self.on_connected();
    }

    pub(super) fn on_standby_lost(&mut self, replica: usize, reason: &str) {
        self.counters.note_disconnected(ReplicaRole::Standby);
        tracing::info!(replica, reason, "a standby disconnected, reconnecting");
    }

    /// Replica `replica`'s connection task ended, its socket up until then
    /// if `was_connected`, which counts it down. That is expected once the
    /// shard is closing. Otherwise a lost primary is replaced, and a shard
    /// with no other connection left stops.
    pub(super) fn on_replica_ended(&mut self, replica: usize, was_connected: bool) {
        let role = self.replicas.role(replica);
        if was_connected {
            self.counters.note_closed(role);
        }
        if self.replicas.is_cancelled() {
            return;
        }
        tracing::error!(replica, ?role, "a connection task ended unexpectedly");
        if role == ReplicaRole::Primary {
            self.replace_ended_primary();
        }
    }

    /// Replaces a primary whose task ended, or stops the shard if no other
    /// connection is left.
    fn replace_ended_primary(&mut self) {
        self.reconnect_requested = false;
        self.resync.stop_clock();
        if !self.replicas.has_live_standby() {
            tracing::error!("no connection is left, stopping the shard");
            return self.replicas.cancel_all();
        }
        if !self.replace_primary("the connection task ended") {
            tracing::warn!("the shard has no live connection until a standby connects");
        }
    }
}
