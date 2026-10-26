//! [`Desired`]: what a connection should be subscribed to, and what it still
//! has to ask the exchange for on the current socket.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::Subscription;
use crate::core::events::Command;

/// A connection's wanted subscription, tracked against what it has asked for
/// on the current socket.
#[derive(Debug)]
pub(super) enum Desired {
    /// An order book subscription over a set of symbols.
    Books(BookSymbols),
    /// The lifecycle subscription, and whether it still has to be asked for.
    Lifecycle {
        /// True until the subscribe goes out on the current socket.
        unrequested: bool,
    },
}

/// The symbols an order book connection wants, split by what the current
/// socket still has to be told.
#[derive(Debug, Default)]
pub(super) struct BookSymbols {
    wanted: BTreeSet<Arc<str>>,
    /// Wanted but not yet asked for on this socket.
    to_add: BTreeSet<Arc<str>>,
    /// Asked for on this socket but no longer wanted.
    to_remove: BTreeSet<Arc<str>>,
}

/// One `Subscribe` command's worth of the subscription.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SubscribeBatch {
    /// Subscribe these order books.
    Books(Vec<Arc<str>>),
    /// Subscribe to lifecycle changes.
    Lifecycle,
}

impl SubscribeBatch {
    /// The command that sends this batch with id `id`.
    pub(super) fn into_command(self, id: u64) -> Command {
        match self {
            Self::Books(symbols) => Command::Subscribe { id, symbols },
            Self::Lifecycle => Command::SubscribeLifecycle { id },
        }
    }
}

impl Desired {
    /// The state before any socket has asked for anything.
    pub(super) fn new(subscription: Subscription) -> Self {
        let mut desired = match subscription {
            Subscription::Books(wanted) => Self::Books(BookSymbols {
                wanted,
                ..BookSymbols::default()
            }),
            Subscription::Lifecycle => Self::Lifecycle { unrequested: true },
        };
        desired.forget_requests();
        desired
    }

    /// Starts a new socket, which has asked for nothing yet.
    pub(super) fn forget_requests(&mut self) {
        match self {
            Self::Books(books) => {
                books.to_add = books.wanted.clone();
                books.to_remove.clear();
            }
            Self::Lifecycle { unrequested } => *unrequested = true,
        }
    }

    /// Wants `symbols` too.
    pub(super) fn add(&mut self, symbols: Vec<Arc<str>>) {
        if let Some(books) = self.books_mut("add") {
            symbols.into_iter().for_each(|symbol| books.add(symbol));
        }
    }

    /// Stops wanting `symbols`.
    pub(super) fn remove(&mut self, symbols: &[Arc<str>]) {
        if let Some(books) = self.books_mut("remove") {
            symbols.iter().for_each(|symbol| books.remove(symbol));
        }
    }

    /// The symbols of `symbols` that are wanted and already asked for on
    /// this socket.
    pub(super) fn asked_for(&self, symbols: Vec<Arc<str>>) -> Vec<Arc<str>> {
        let Self::Books(books) = self else {
            return Vec::new();
        };
        let asked =
            |symbol: &Arc<str>| books.wanted.contains(symbol) && !books.to_add.contains(symbol);
        symbols.into_iter().filter(asked).collect()
    }

    /// Whether some of the subscription has not been asked for yet.
    pub(super) fn has_unrequested(&self) -> bool {
        match self {
            Self::Books(books) => !books.to_add.is_empty(),
            Self::Lifecycle { unrequested } => *unrequested,
        }
    }

    /// The next subscribe batch, of at most `limit` symbols, marked as asked
    /// for; `None` once everything has been.
    pub(super) fn next_subscribe(&mut self, limit: usize) -> Option<SubscribeBatch> {
        match self {
            Self::Books(books) => books.take_to_add(limit).map(SubscribeBatch::Books),
            Self::Lifecycle { unrequested } => {
                std::mem::take(unrequested).then_some(SubscribeBatch::Lifecycle)
            }
        }
    }

    /// Every symbol still to add, marked as asked for.
    pub(super) fn take_added(&mut self) -> Vec<Arc<str>> {
        match self {
            Self::Books(books) => std::mem::take(&mut books.to_add).into_iter().collect(),
            Self::Lifecycle { .. } => Vec::new(),
        }
    }

    /// Every symbol still to remove, marked as done.
    pub(super) fn take_removed(&mut self) -> Vec<Arc<str>> {
        match self {
            Self::Books(books) => std::mem::take(&mut books.to_remove).into_iter().collect(),
            Self::Lifecycle { .. } => Vec::new(),
        }
    }

    /// The book symbols, or `None` with a warning on a lifecycle
    /// connection, which takes no symbols.
    fn books_mut(&mut self, action: &str) -> Option<&mut BookSymbols> {
        match self {
            Self::Books(books) => Some(books),
            Self::Lifecycle { .. } => {
                tracing::warn!(action, "ignored a symbol request on a lifecycle connection");
                None
            }
        }
    }
}

impl BookSymbols {
    fn add(&mut self, symbol: Arc<str>) {
        if self.wanted.insert(symbol.clone()) && !self.to_remove.remove(&symbol) {
            self.to_add.insert(symbol);
        }
    }

    fn remove(&mut self, symbol: &Arc<str>) {
        if self.wanted.remove(symbol) && !self.to_add.remove(symbol) {
            self.to_remove.insert(symbol.clone());
        }
    }

    fn take_to_add(&mut self, limit: usize) -> Option<Vec<Arc<str>>> {
        let batch: Vec<Arc<str>> = std::iter::from_fn(|| self.to_add.pop_first())
            .take(limit)
            .collect();
        (!batch.is_empty()).then_some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|&name| Arc::from(name)).collect()
    }

    fn books(names: &[&str]) -> Desired {
        Desired::new(Subscription::Books(symbols(names).into_iter().collect()))
    }

    fn batches(desired: &mut Desired, limit: usize) -> Vec<SubscribeBatch> {
        std::iter::from_fn(|| desired.next_subscribe(limit)).collect()
    }

    #[test]
    fn the_subscription_is_split_into_batches_of_the_limit() {
        let mut desired = books(&["A", "B", "C", "D", "E"]);
        let expected = vec![
            SubscribeBatch::Books(symbols(&["A", "B"])),
            SubscribeBatch::Books(symbols(&["C", "D"])),
            SubscribeBatch::Books(symbols(&["E"])),
        ];
        assert_eq!(batches(&mut desired, 2), expected);
    }

    #[test]
    fn an_added_symbol_joins_the_remaining_batches() {
        let mut desired = books(&["A", "B"]);
        desired.next_subscribe(1);
        desired.add(symbols(&["C"]));
        assert_eq!(
            batches(&mut desired, 10),
            vec![SubscribeBatch::Books(symbols(&["B", "C"]))]
        );
    }

    #[test]
    fn removing_a_requested_symbol_needs_a_remove_command() {
        let mut desired = books(&["A", "B"]);
        batches(&mut desired, 10);
        desired.remove(&symbols(&["A"]));
        assert_eq!(desired.take_removed(), symbols(&["A"]));
    }

    #[test]
    fn removing_an_unrequested_symbol_needs_no_command() {
        let mut desired = books(&["A", "B"]);
        desired.remove(&symbols(&["B"]));
        assert_eq!(
            (batches(&mut desired, 10), desired.take_removed()),
            (vec![SubscribeBatch::Books(symbols(&["A"]))], vec![])
        );
    }

    #[test]
    fn re_adding_a_symbol_pending_removal_cancels_the_removal() {
        let mut desired = books(&["A"]);
        batches(&mut desired, 10);
        desired.remove(&symbols(&["A"]));
        desired.add(symbols(&["A"]));
        assert_eq!(
            (desired.take_added(), desired.take_removed()),
            (vec![], vec![])
        );
    }

    #[test]
    fn a_new_socket_asks_for_the_current_set_again() {
        let mut desired = books(&["A", "B"]);
        batches(&mut desired, 10);
        desired.add(symbols(&["C"]));
        desired.remove(&symbols(&["A"]));
        desired.forget_requests();
        assert_eq!(
            (batches(&mut desired, 10), desired.take_removed()),
            (vec![SubscribeBatch::Books(symbols(&["B", "C"]))], vec![])
        );
    }

    #[test]
    fn only_wanted_symbols_already_asked_for_count_as_asked_for() {
        let mut desired = books(&["A", "B", "C"]);
        desired.next_subscribe(2);
        let asked = desired.asked_for(symbols(&["A", "B", "C", "Z"]));
        assert_eq!(asked, symbols(&["A", "B"]));
    }

    #[test]
    fn a_lifecycle_subscription_is_one_batch_per_socket() {
        let mut desired = Desired::new(Subscription::Lifecycle);
        let first = batches(&mut desired, 10);
        desired.forget_requests();
        let second = batches(&mut desired, 10);
        assert_eq!(
            (first, second),
            (
                vec![SubscribeBatch::Lifecycle],
                vec![SubscribeBatch::Lifecycle]
            )
        );
    }

    #[test]
    fn a_lifecycle_subscription_ignores_symbol_requests() {
        let mut desired = Desired::new(Subscription::Lifecycle);
        batches(&mut desired, 10);
        desired.add(symbols(&["A"]));
        assert!(!desired.has_unrequested());
    }
}
