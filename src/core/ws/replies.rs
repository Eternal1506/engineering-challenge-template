//! [`Replies`]: the commands sent on one socket that still await replies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::core::events::{Command, Inbound};

/// Commands awaiting replies, by id.
///
/// A snapshot request is answered by one snapshot per symbol; every other
/// command by one reply. An error reply settles a command at once. Commands
/// still unanswered after the timeout are forgotten, since the exchange
/// silently skips markets it has dropped, and a reply that comes later is
/// treated as a reply to an unknown command. So tags are best effort.
#[derive(Debug)]
pub(super) struct Replies {
    awaiting: HashMap<u64, Awaiting>,
    timeout: Duration,
}

/// What a reply means to the owner.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Answer {
    /// The owner's tag on the request the reply answers, if it had one.
    pub(super) tag: Option<u64>,
    /// When an error rejected a subscribe or an add, the symbols it named.
    pub(super) rejected: Option<Vec<Arc<str>>>,
}

#[derive(Debug)]
struct Awaiting {
    kind: &'static str,
    tag: Option<u64>,
    /// The symbols to report if an error rejects the command; `None` for
    /// commands whose rejection the owner is not told about.
    rejectable: Option<Vec<Arc<str>>>,
    replies_left: usize,
    sent_at: Instant,
}

impl Replies {
    /// No commands yet; unanswered ones are forgotten after `timeout`.
    pub(super) fn new(timeout: Duration) -> Self {
        Self {
            awaiting: HashMap::new(),
            timeout,
        }
    }

    /// Starts waiting for the replies to `command`, which the owner tagged
    /// with `tag`, and forgets commands older than the timeout.
    pub(super) fn expect(&mut self, command: &Command, tag: Option<u64>) {
        self.forget_stale();
        let (id, awaiting) = Awaiting::new(command, tag);
        self.awaiting.insert(id, awaiting);
    }

    /// Counts `message` as a reply to command `id`, logging an error reply
    /// with the kind of command it rejected. A command older than the
    /// timeout is forgotten first.
    pub(super) fn on_reply(&mut self, id: u64, message: &Inbound) -> Answer {
        self.forget_if_stale(id);
        let Some(awaiting) = self.awaiting.get_mut(&id) else {
            tracing::debug!(id, "a reply to a command this socket is not waiting on");
            return Answer::default();
        };
        let answer = awaiting.answer(id, message);
        if awaiting.replies_left == 0 {
            self.awaiting.remove(&id);
        }
        answer
    }

    fn forget_stale(&mut self) {
        let timeout = self.timeout;
        self.awaiting.retain(|&id, awaiting| {
            let stale = awaiting.is_stale(timeout);
            if stale {
                awaiting.log_given_up(id);
            }
            !stale
        });
    }

    fn forget_if_stale(&mut self, id: u64) {
        let timeout = self.timeout;
        let Some(awaiting) = self.awaiting.get(&id) else {
            return;
        };
        if awaiting.is_stale(timeout) {
            awaiting.log_given_up(id);
            self.awaiting.remove(&id);
        }
    }

    #[cfg(test)]
    fn is_awaiting(&self, id: u64) -> bool {
        self.awaiting.contains_key(&id)
    }
}

impl Awaiting {
    fn is_stale(&self, timeout: Duration) -> bool {
        self.sent_at.elapsed() >= timeout
    }

    fn log_given_up(&self, id: u64) {
        let (command, missing) = (self.kind, self.replies_left);
        tracing::debug!(id, command, missing, "gave up on replies");
    }

    fn new(command: &Command, tag: Option<u64>) -> (u64, Self) {
        let (id, kind, replies_left, rejectable) = match command {
            Command::Subscribe { id, symbols } => (*id, "subscribe", 1, Some(symbols.clone())),
            Command::SubscribeLifecycle { id } => (*id, "subscribe lifecycle", 1, Some(vec![])),
            Command::AddSymbols { id, symbols, .. } => (*id, "add", 1, Some(symbols.clone())),
            Command::RemoveSymbols { id, .. } => (*id, "remove", 1, None),
            Command::RequestSnapshot { id, symbols, .. } => (*id, "snapshot", symbols.len(), None),
            Command::Unsubscribe { id, .. } => (*id, "unsubscribe", 1, None),
        };
        let awaiting = Self {
            kind,
            tag,
            rejectable,
            replies_left,
            sent_at: Instant::now(),
        };
        (id, awaiting)
    }

    fn answer(&mut self, id: u64, message: &Inbound) -> Answer {
        let Inbound::Error { sub, kind } = message else {
            self.replies_left = self.replies_left.saturating_sub(1);
            return Answer {
                tag: self.tag,
                rejected: None,
            };
        };
        let command = self.kind;
        tracing::warn!(id, command, ?sub, error = ?kind, "the exchange rejected a command");
        self.replies_left = 0;
        Answer {
            tag: self.tag,
            rejected: self.rejectable.take(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::{FeedErrorKind, SubId};

    const TIMEOUT: Duration = Duration::from_secs(30);

    fn symbols(count: usize) -> Vec<Arc<str>> {
        (0..count).map(|n| Arc::from(n.to_string())).collect()
    }

    fn snapshot_request(id: u64, count: usize) -> Command {
        Command::RequestSnapshot {
            id,
            sub: SubId(1),
            symbols: symbols(count),
        }
    }

    fn error() -> Inbound {
        Inbound::Error {
            sub: None,
            kind: FeedErrorKind::UnknownSubscription,
        }
    }

    #[tokio::test]
    async fn a_snapshot_request_waits_for_one_tagged_reply_per_symbol() {
        let mut replies = Replies::new(TIMEOUT);
        replies.expect(&snapshot_request(3, 2), Some(42));
        let first = replies.on_reply(3, &Inbound::Ignored);
        let after_one = replies.is_awaiting(3);
        replies.on_reply(3, &Inbound::Ignored);
        assert_eq!(first.tag, Some(42));
        assert_eq!((after_one, replies.is_awaiting(3)), (true, false));
    }

    #[tokio::test]
    async fn an_error_settles_a_command() {
        let mut replies = Replies::new(TIMEOUT);
        replies.expect(&snapshot_request(4, 5), Some(1));
        let answer = replies.on_reply(4, &error());
        assert_eq!(
            (answer, replies.is_awaiting(4)),
            (
                Answer {
                    tag: Some(1),
                    rejected: None
                },
                false
            )
        );
    }

    #[tokio::test]
    async fn an_error_reply_to_a_subscribe_names_its_symbols() {
        let mut replies = Replies::new(TIMEOUT);
        let subscribe = Command::Subscribe {
            id: 2,
            symbols: symbols(3),
        };
        replies.expect(&subscribe, None);
        assert_eq!(replies.on_reply(2, &error()).rejected, Some(symbols(3)));
    }

    #[tokio::test]
    async fn a_reply_to_an_unknown_command_means_nothing() {
        let mut replies = Replies::new(TIMEOUT);
        assert_eq!(replies.on_reply(9, &error()), Answer::default());
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_commands_are_forgotten_after_the_timeout() {
        let mut replies = Replies::new(TIMEOUT);
        replies.expect(&snapshot_request(1, 2), None);
        tokio::time::advance(TIMEOUT).await;
        replies.expect(&snapshot_request(2, 2), None);
        assert_eq!(
            (replies.is_awaiting(1), replies.is_awaiting(2)),
            (false, true)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_after_the_timeout_is_untagged_without_any_other_send() {
        let mut replies = Replies::new(TIMEOUT);
        replies.expect(&snapshot_request(1, 2), Some(42));
        tokio::time::advance(TIMEOUT).await;
        let answer = replies.on_reply(1, &Inbound::Ignored);
        assert_eq!((answer, replies.is_awaiting(1)), (Answer::default(), false));
    }
}
