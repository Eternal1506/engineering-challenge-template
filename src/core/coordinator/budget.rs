//! [`ConnectionBudget`]: how many connections a new shard may open.

use std::num::NonZeroUsize;

/// Connections the recorder keeps outside the shards: the lifecycle
/// connection.
const RESERVED_CONNECTIONS: usize = 1;

/// The configured replicas per shard and the most connections the recorder
/// may hold at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConnectionBudget {
    replicas: NonZeroUsize,
    max_connections: usize,
}

impl ConnectionBudget {
    pub(super) fn new(replicas: NonZeroUsize, max_connections: usize) -> Self {
        Self {
            replicas,
            max_connections,
        }
    }

    /// The connections a new shard gets while the open shards hold `open`:
    /// the configured replicas if they fit beside the reserved connections,
    /// else one, since recording every market comes before redundancy.
    pub(super) fn replicas_for(&self, open: usize) -> NonZeroUsize {
        let wanted = open + self.replicas.get() + RESERVED_CONNECTIONS;
        if wanted <= self.max_connections {
            self.replicas
        } else {
            NonZeroUsize::MIN
        }
    }

    /// Whether a new shard of at least one connection fits beside the
    /// reserved connections while the open shards hold `open`.
    pub(super) fn has_room_for_shard(&self, open: usize) -> bool {
        open + 1 + RESERVED_CONNECTIONS <= self.max_connections
    }

    /// Whether a shard of `replicas` runs short of the configured count.
    pub(super) fn is_short(&self, replicas: NonZeroUsize) -> bool {
        replicas < self.replicas
    }

    /// The most connections the recorder may hold at once.
    pub(super) fn max_connections(&self) -> usize {
        self.max_connections
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs_within(max_connections: usize) -> ConnectionBudget {
        ConnectionBudget::new(NonZeroUsize::new(2).unwrap(), max_connections)
    }

    #[test]
    fn a_shard_that_fits_with_the_lifecycle_connection_gets_every_replica() {
        assert_eq!(pairs_within(190).replicas_for(187).get(), 2);
    }

    #[test]
    fn a_shard_that_would_exceed_the_budget_gets_one_connection() {
        assert_eq!(pairs_within(190).replicas_for(188), NonZeroUsize::MIN);
    }

    #[test]
    fn a_seventy_fifth_paired_shard_fits_within_the_default_budget() {
        assert_eq!(pairs_within(190).replicas_for(2 * 74).get(), 2);
    }

    #[test]
    fn a_shard_has_room_while_one_more_connection_fits() {
        let budget = pairs_within(190);
        let room = [188, 189].map(|open| budget.has_room_for_shard(open));
        assert_eq!(room, [true, false]);
    }

    #[test]
    fn only_fewer_replicas_than_configured_are_short() {
        let budget = pairs_within(190);
        let two = NonZeroUsize::new(2).unwrap();
        let short = (budget.is_short(NonZeroUsize::MIN), budget.is_short(two));
        assert_eq!(short, (true, false));
    }
}
