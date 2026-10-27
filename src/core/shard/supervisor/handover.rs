//! The supervisor's side of a warm standby taking over from a primary that
//! is still connected: on a health check that finds the primary silent or
//! lagging, when a market held past the resync deadline proves live, or
//! when too many busy markets go silent at once.

use std::sync::Arc;

use tokio::time::Instant;

use super::Supervisor;
use crate::core::shard::health::Takeover;
use crate::core::stats::FailoverCause;

impl Supervisor {
    /// When the next health check is due: never while the shard runs only
    /// its primary, or once it is closing.
    pub(super) fn health_check_due(&self) -> Option<Instant> {
        let paired = self.replicas.started_count() > 1;
        let open = !self.replicas.is_cancelled();
        (paired && open).then(|| self.health.next_check())
    }

    /// Compares the replicas' recent traffic, handing over to a standby if
    /// the primary is silent or lagging.
    pub(super) fn on_health_check(&mut self) {
        let now = Instant::now();
        let readings = self.replicas.readings(now);
        if let Some(Takeover { standby, cause }) = self.health.check(&readings, now) {
            self.hand_over(standby, cause);
        }
    }

    /// Held `symbol` changed after the resync deadline, so it is live yet
    /// unanswered. A warm standby that is delivering level changes takes
    /// over, unless the shard is cooling down from another takeover or
    /// closing; failing that, `symbol`'s snapshot is asked for again.
    pub(super) fn on_live_held_market(&mut self, symbol: Arc<str>) {
        let now = Instant::now();
        let readings = self.replicas.readings(now);
        let standby = self.health.delivering_standby(&readings, now);
        match standby.filter(|_| !self.replicas.is_cancelled()) {
            Some(standby) => self.hand_over(standby, FailoverCause::Resync),
            None => self.resnapshot_held(symbol),
        }
    }

    /// Makes the connected `standby` the primary in place of the connected
    /// primary, because of `cause`, and resyncs every symbol on it. The old
    /// primary is asked to reconnect, and does so as a standby.
    pub(super) fn hand_over(&mut self, standby: usize, cause: FailoverCause) {
        let from = self.promote(standby);
        self.health.note_takeover(Instant::now());
        self.counters.note_failover(cause);
        self.replicas.reconnect(from);
        let tag = self.resync_all();
        tracing::warn!(
            ?cause,
            from,
            to = standby,
            tag,
            "the primary is unhealthy, handing over to a standby"
        );
    }
}
