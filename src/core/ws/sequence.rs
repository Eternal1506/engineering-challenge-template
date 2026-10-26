//! [`SequenceCheck`]: contiguity of numbered messages per subscription.

use std::collections::HashMap;

use super::ConnectionEvent;
use crate::core::events::{StreamPosition, SubId};

/// The last sequence number seen on each subscription of one socket.
#[derive(Debug, Default)]
pub(super) struct SequenceCheck {
    last: HashMap<SubId, u64>,
}

impl SequenceCheck {
    /// Records `position` and returns the [`ConnectionEvent::Gap`] it
    /// reveals, if any. The first number seen on a subscription is its
    /// baseline, and after a gap the check continues from the new number.
    pub(super) fn observe(&mut self, position: StreamPosition) -> Option<ConnectionEvent> {
        let previous = self.last.insert(position.sub, position.seq)?;
        let expected = previous.wrapping_add(1);
        (position.seq != expected).then_some(ConnectionEvent::Gap {
            sub: position.sub,
            expected,
            got: position.seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(sub: u64, seq: u64) -> StreamPosition {
        StreamPosition {
            sub: SubId(sub),
            seq,
        }
    }

    fn gaps(check: &mut SequenceCheck, positions: &[(u64, u64)]) -> Vec<ConnectionEvent> {
        positions
            .iter()
            .filter_map(|&(sub, seq)| check.observe(at(sub, seq)))
            .collect()
    }

    #[test]
    fn the_first_number_is_the_baseline() {
        assert_eq!(gaps(&mut SequenceCheck::default(), &[(1, 57), (1, 58)]), []);
    }

    #[test]
    fn a_jump_is_a_gap_and_the_check_continues_from_it() {
        let found = gaps(
            &mut SequenceCheck::default(),
            &[(1, 1), (1, 2), (1, 5), (1, 6)],
        );
        let gap = ConnectionEvent::Gap {
            sub: SubId(1),
            expected: 3,
            got: 5,
        };
        assert_eq!(found, [gap]);
    }

    #[test]
    fn a_repeated_number_is_a_gap() {
        let found = gaps(&mut SequenceCheck::default(), &[(1, 4), (1, 4)]);
        let gap = ConnectionEvent::Gap {
            sub: SubId(1),
            expected: 5,
            got: 4,
        };
        assert_eq!(found, [gap]);
    }

    #[test]
    fn subscriptions_are_checked_separately() {
        let found = gaps(
            &mut SequenceCheck::default(),
            &[(1, 1), (2, 10), (1, 2), (2, 11)],
        );
        assert_eq!(found, []);
    }
}
